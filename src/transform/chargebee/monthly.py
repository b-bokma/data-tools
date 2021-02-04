import awswrangler as wr
import pandas as pd
import boto3
import calendar
from datetime import timedelta
import os
import json
import requests

from functions import col_to_str, col_to_timestamp

session = boto3.Session()
secret_client = session.client('secretsmanager')

source_bucket_name = os.getenv('SOURCE_BUCKETNAME')
target_bucket_name = os.getenv('TARGET_BUCKETNAME')


def extract_monthly_table(df_full, year, month, raw_bucket=source_bucket_name, manual_exchange_rates=None):
    # define variables
    date_from = pd.to_datetime(f"{year}-{month}-01")
    days_in_month = calendar.monthrange(date_from.year, date_from.month)[1]
    date_to = date_from + timedelta(days=days_in_month)
    filter_data_from = date_from
    filter_data_to = date_to

    print("Date From: ", filter_data_from)
    print("Date To: ", filter_data_to)
    # extract data from df_full based on month.
    # Add days time to timedelta if you want to make sure everything is included

    df_full['updated_at'] = pd.to_datetime(df_full['updated_at'])
    chargebee_intermediate_data = df_full[df_full['updated_at'] < filter_data_to].reset_index().drop('index', axis=1)

    # merge customers
    df_customers_raw = wr.s3.read_parquet(path=f"s3://{raw_bucket}/chargebee/customers/")
    df_customers = df_customers_raw.rename(
        columns={'id': 'customer_id', 'deleted': 'customer_deleted', 'name': 'company'})
    df_customers = df_customers[df_customers.updated_at < filter_data_to].reset_index(
    ).drop('index', axis=1)
    df_customers = df_customers.sort_values(['customer_id', 'updated_at'], ascending=[True, False])
    df_customers['rn'] = df_customers.groupby(['customer_id']).cumcount() + 1
    df_customers = df_customers[df_customers.rn == 1]
    df_customers = df_customers.drop_duplicates()

    chargebee_intermediate_data = chargebee_intermediate_data.merge(
        df_customers[['customer_id', 'company', 'customer_deleted']], how='left', on='customer_id')
    chargebee_intermediate_data = chargebee_intermediate_data.sort_values(['company', 'updated_at'],
                                                                          ascending=[True, False])

    if manual_exchange_rates is None:

        # get exchange rate for date to value
        currency_codes = chargebee_intermediate_data['currency_code'].unique()

        output = list()
        date_to_str = date_to.strftime("%Y-%m-%d")

        url = f'https://api.exchangerate.host/{date_to_str}'
        response = requests.get(url)
        data = response.json()

        df = pd.DataFrame(data).reset_index()
        df = df[~df.rates.isna()]
        df = df[['index', 'date', 'base', 'rates']]
        df = df.rename(columns={'index': 'currency_code', 'rates': 'rate'})
        used_exchange_rates = df.drop(['base', 'date'], axis=1)
        used_exchange_rates['rate'] = 1 / used_exchange_rates['rate']
        used_exchange_rates = used_exchange_rates.append(pd.DataFrame({"currency_code": ["EUR"], "rate": [1.0]}))

    chargebee_intermediate_data = chargebee_intermediate_data.merge(used_exchange_rates, on='currency_code', how='left')
    chargebee_intermediate_data = chargebee_intermediate_data.sort_values(['id', 'updated_at'], ascending=[True, False])
    chargebee_intermediate_data['rn'] = chargebee_intermediate_data.groupby(['id']).cumcount() + 1
    chargebee_intermediate_data = chargebee_intermediate_data[chargebee_intermediate_data['rn'] == 1]
    chargebee_intermediate_data = chargebee_intermediate_data.drop_duplicates()

    ## create EUR columns
    # total plan value == value of subscription without discount
    chargebee_intermediate_data.loc[:, 'total_plan_value_EUR'] = chargebee_intermediate_data[
                                                                     'total_plan_value'] * \
                                                                 chargebee_intermediate_data['rate']

    # Total subscription value == value of subscription including discount
    chargebee_intermediate_data.loc[:, 'total_subscription_value_EUR'] = chargebee_intermediate_data[
                                                                             'total_subscription_value'] * \
                                                                         chargebee_intermediate_data['rate']

    # total plan value calculated towards month
    chargebee_intermediate_data.loc[:, 'MRR_EUR'] = chargebee_intermediate_data['total_mrr'] * \
                                                    chargebee_intermediate_data[
                                                        'rate']

    # total subscription value calculated towards month
    chargebee_intermediate_data.loc[:, 'MRR_discount_EUR'] = chargebee_intermediate_data['total_mrr_discount'] * \
                                                             chargebee_intermediate_data[
                                                                 'rate']

    # total arr excl discount
    chargebee_intermediate_data.loc[:, 'total_arr'] = chargebee_intermediate_data['total_mrr'] * 12
    chargebee_intermediate_data.loc[:, 'ARR_EUR'] = chargebee_intermediate_data['total_arr'] * \
                                                    chargebee_intermediate_data[
                                                        'rate']

    # total arr incl discount
    chargebee_intermediate_data.loc[:, 'total_arr_discount'] = chargebee_intermediate_data['total_mrr_discount'] * 12
    chargebee_intermediate_data.loc[:, 'ARR_discount_EUR'] = chargebee_intermediate_data['total_arr_discount'] * \
                                                             chargebee_intermediate_data[
                                                                 'rate']

    # sort based on updated_at add rank per subscription. This so we can filter on last known status later on
    chargebee_intermediate_data = chargebee_intermediate_data.sort_values(by=['id', 'updated_at'],
                                                                          ascending=[False, False])
    chargebee_intermediate_data.loc[:, 'rn'] = chargebee_intermediate_data.groupby(['id']).cumcount() + 1

    # filter on latest known status
    chargebee_intermediate_data_filtered = chargebee_intermediate_data[
        chargebee_intermediate_data['rn'] == chargebee_intermediate_data.groupby('id')[
            'rn'].transform('min')]

    # Create Analysis Table
    # filter data based on contract dates
    chargebee_intermediate_data_filtered = chargebee_intermediate_data_filtered[
        (
                (chargebee_intermediate_data_filtered.current_term_start <= filter_data_to) &
                (chargebee_intermediate_data_filtered.current_term_end >= filter_data_from)
        ) |
        (
                chargebee_intermediate_data_filtered.status == 'future')
        ]

    # in monthly table, set customer_churn to False as default
    chargebee_intermediate_data_filtered.loc[:, 'customer_churn'] = False

    # get list of all customer_id's with non_renewing subscriptions
    non_renewing = chargebee_intermediate_data_filtered[
        chargebee_intermediate_data_filtered.status.isin(['non_renewing', 'cancelled'])][
        'customer_id']

    # remove customers with other active subscriptions from list
    other_active = chargebee_intermediate_data_filtered[
        (chargebee_intermediate_data_filtered.customer_id.isin(non_renewing)) & (
            chargebee_intermediate_data_filtered.status.isin(['active', 'future']))]['customer_id']
    churning_customers = non_renewing[~non_renewing.isin(other_active)]

    # Set customer_churn == True for customers that appear in list
    chargebee_intermediate_data_filtered.loc[
        (chargebee_intermediate_data_filtered.customer_id.isin(churning_customers)), 'customer_churn'] = True

    # in monthly table, set subscription_new and customer_new to False as default
    chargebee_intermediate_data_filtered.loc[:, 'subscription_new'] = False
    chargebee_intermediate_data_filtered.loc[:, 'customer_new'] = False

    chargebee_intermediate_data_filtered.loc[
        chargebee_intermediate_data_filtered.started_at >= filter_data_from, 'subscription_new'] = True

    # Get minimum product start date for customers that have subscription_new == True
    new_business_starting_date = \
        chargebee_intermediate_data_filtered[chargebee_intermediate_data_filtered.customer_id.isin(
            chargebee_intermediate_data_filtered[chargebee_intermediate_data_filtered.subscription_new == True][
                'customer_id'])][
            ['customer_id', 'started_at']].groupby(['customer_id']).min().reset_index()

    # if min starting date is in current month, set column customer_new to True
    # filter data to is not used here, but strictly the last day of the month, so new business on the 1st moves is always in the right month
    chargebee_intermediate_data_filtered.loc[chargebee_intermediate_data_filtered.customer_id.isin(
        new_business_starting_date[(new_business_starting_date.started_at >= filter_data_from) & (
                new_business_starting_date.started_at < filter_data_to)][
            'customer_id']), 'customer_new'] = True

    if 'deleted' in chargebee_intermediate_data_filtered.columns:
        return_df = chargebee_intermediate_data_filtered[chargebee_intermediate_data_filtered['deleted'] == False]
    else:
        return_df = chargebee_intermediate_data_filtered
    return return_df


def create_full_data(raw_bucket):
    df_subscriptions = wr.s3.read_parquet(path=f"s3://{raw_bucket}/chargebee/subscriptions/")
    df_coupons = wr.s3.read_parquet(path=f"s3://{raw_bucket}/chargebee/coupons/")
    df_addons = wr.s3.read_parquet(path=f"s3://{raw_bucket}/chargebee/addons/")
    df_subscriptions = df_subscriptions.drop_duplicates()

    # merge data sets
    # addons
    if 'addons_quantity' and 'addons_amount' in df_subscriptions.columns:
        df_subscriptions = df_subscriptions.drop(['addons_quantity', 'addons_amount'], axis=1)

    df_addons = df_addons.drop_duplicates()
    df_addons = df_addons.rename(columns={'id': 'addon_id', 'amount': 'addons_amount'})
    df_full = df_subscriptions.merge(
        df_addons[['subscription_id', 'updated_at', 'addons_amount']].groupby(
            ['subscription_id', 'updated_at']).sum().reset_index(),
        how='left',
        left_on=['id', 'updated_at'],
        right_on=['subscription_id', 'updated_at']
    )
    df_full = df_full.drop('subscription_id', axis=1)
    df_full = df_full.rename(columns={'addons_amount': 'total_addon_value'})

    # Adding coupon amount
    df_coupons = df_coupons.drop_duplicates()
    df_coupons = df_coupons.rename(columns={'id': 'coupon_id'})
    df_coupons = df_coupons[df_coupons.status == 'active']
    df_coupons = df_coupons.drop(['status', 'coupon_created_at', 'resource_version', 'object', 'currency_code'], axis=1)
    df_full = df_full.merge(df_coupons, how='left', left_on=['id', 'updated_at'],
                            right_on=['subscription_id', 'updated_at'])
    df_full = df_full.rename(columns={'apply_on': 'discount_apply_on'})

    # add calculated columns

    df_full['billing_period'] = df_full['billing_period'].astype(int)

    # total addon value is already created, but NaN needs to be 0
    df_full['total_addon_value'] = df_full.loc[:, 'total_addon_value'].fillna(0)
    # total plan value
    df_full.loc[:, 'total_plan_value'] = df_full['plan_amount'] + df_full['total_addon_value']
    # total discount value
    df_full.loc[(df_full['discount_type'] == 'percentage') & (
            df_full['discount_apply_on'] == 'invoice_amount'), 'total_discount_value'] = df_full.loc[(df_full[
                                                                                                          'discount_type'] == 'percentage') & (
                                                                                                             df_full[
                                                                                                                 'discount_apply_on'] == 'invoice_amount'), 'total_plan_value'] * (
                                                                                                 df_full.loc[(df_full[
                                                                                                                  'discount_type'] == 'percentage') & (
                                                                                                                     df_full[
                                                                                                                         'discount_apply_on'] == 'invoice_amount'), 'discount_percentage'] / 100)
    df_full.loc[(df_full['discount_type'] == 'percentage') & (
            df_full['discount_apply_on'] == 'each_specified_item'), 'total_discount_value'] = df_full.loc[(df_full[
                                                                                                               'discount_type'] == 'percentage') & (
                                                                                                                  df_full[
                                                                                                                      'discount_apply_on'] == 'each_specified_item'), 'plan_amount'] * (
                                                                                                      df_full.loc[(
                                                                                                                          df_full[
                                                                                                                              'discount_type'] == 'percentage') & (
                                                                                                                          df_full[
                                                                                                                              'discount_apply_on'] == 'each_specified_item'), 'discount_percentage'] / 100)
    df_full.loc[df_full['discount_type'] == 'fixed_amount', 'total_discount_value'] = df_full.loc[
        df_full['discount_type'] == 'fixed_amount', 'discount_amount']
    df_full['total_discount_value'] = df_full.loc[:, 'total_discount_value'].fillna(0)

    df_full.loc[:, 'total_plan_value'] = df_full['total_plan_value'].fillna(0)

    # total subscription value incl discount
    df_full.loc[:, 'total_subscription_value'] = (df_full['total_plan_value'] - df_full['total_discount_value'])
    df_full.loc[:, 'total_subscription_value'] = df_full['total_subscription_value'].fillna(0)

    # total MRR
    df_full.loc[:, 'total_mrr'] = df_full['total_plan_value'] / (df_full['billing_period'] * 12)
    df_full.loc[df_full['billing_period_unit'] == 'month', 'total_mrr'] = \
        df_full.loc[df_full['billing_period_unit'] == 'month']['total_plan_value'] / \
        df_full.loc[df_full['billing_period_unit'] == 'month']['billing_period']
    df_full['total_mrr'] = df_full.loc[:, 'total_mrr'].fillna(0)

    # total MRR incl discount
    df_full.loc[:, 'total_mrr_discount'] = df_full['total_subscription_value'] / (df_full['billing_period'] * 12)
    df_full.loc[df_full['billing_period_unit'] == 'month', 'total_mrr_discount'] = \
        df_full.loc[df_full['billing_period_unit'] == 'month']['total_subscription_value'] / \
        df_full.loc[df_full['billing_period_unit'] == 'month']['billing_period']
    df_full['total_mrr_discount'] = df_full.loc[:, 'total_mrr_discount'].fillna(0)

    df_full = df_full.sort_values('id')
    df_full = df_full.drop_duplicates()

    return (df_full)


df_full = create_full_data(raw_bucket=source_bucket_name)


def lambda_handler(Event, Context):
    manual_exchange_rates = None

    if 'year' in Event.keys():
        year = Event['year']
    if 'month' in Event.keys():
        month = Event['month']
    if 'manual_exchange_rates' in Event.keys():
        manual_exchange_rates = Event['manual_exchange_rates']

    df = extract_monthly_table(
        df_full=df_full,
        year=year,
        month=month,
        manual_exchange_rates=manual_exchange_rates
    )
    print(df)

    arr_excl_discount = df[(df.status.isin(['active', 'non_renewing']))]['ARR_EUR'].sum()
    arr_incl_discount = df[(df.status.isin(['active', 'non_renewing']))]['ARR_discount_EUR'].sum()
    mrr_excl_discount = df[df.status.isin(['active', 'non_renewing'])]['MRR_EUR'].sum()
    mrr_incl_discount = df[df.status.isin(['active', 'non_renewing'])]['MRR_discount_EUR'].sum()
    churned = df[(df.customer_churn == True) & (df.status == 'cancelled')]['ARR_EUR'].sum()
    new_clients = df[df.customer_new == True][['company', 'plan_id', 'started_at', 'ARR_EUR']]['ARR_EUR'].sum()
    cross_sell = \
        df[(df.subscription_new == True) & (df.customer_new == False)][['company', 'plan_id', 'started_at', 'ARR_EUR']][
            'ARR_EUR'].sum()

    print(f"Arr excluding discount for {year}-{month} is {arr_excl_discount:.2f}")
    print(f"Arr including discount for {year}-{month} is {arr_incl_discount:.2f}")
    print(f"Mrr excluding discount for {year}-{month} is {mrr_excl_discount:.2f}")
    print(f"Mrr including discount for {year}-{month} is {mrr_incl_discount:.2f}")
    print(f"Total value churned clients for {year}-{month} is {churned:.2f}")
    print(f"Total value new clients for {year}-{month} is {new_clients}")
    print(f"Total value in cross sell for {year}-{month} is {cross_sell}")

    wr.s3.to_parquet(
        df=df,
        path=f"s3://{target_bucket_name}/monthly/chargebee/month={year}-{month}/",
        dataset=True,
        mode="overwrite",
        compression='snappy'
    )

    return ({"statusCode": 200, "Body": {"year": year, "month": month}})

