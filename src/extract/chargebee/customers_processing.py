import boto3
import os
import json
from datetime import datetime
import pandas as pd
import awswrangler as wr
from functions import col_to_str, col_to_timestamp


session = boto3.Session()
client = session.client('secretsmanager')
source_bucket_name = os.getenv('SOURCE_BUCKETNAME')
target_bucket_name = os.getenv('TARGET_BUCKETNAME')


def lambda_handler(Event, Context):

    # set variables
    if 'Instance' in Event.keys():
        instance = Event['Instance']
        secret_string = client.get_secret_value(
            SecretId=f'chargebee_{instance}'
        )['SecretString']
        config = json.loads(secret_string)
        url = config['url']
        token = config['token']
        timezone = config['timezone']

    else:
        return "Invalid Request. Add instance to Event payload"

    print(Event.keys())

    # set last updated key to update config file with
    last_updated_key = 'chargebee_customers_last_updated'
    update_timestamp = int(datetime.utcnow().timestamp())

    df = wr.s3.read_json(
        path=Event['Body']['path'],
        lines=True,
        convert_dates=False,  # dont convert columns to dates
        convert_axes=False
    )

    #####
    # DATA PROCESSING
    # steps taken:
    # setting column types
    # create new data tables from nested lists in subscriptions table
    # Addons, Contract Term, Event Based Addons (One off fees),
    # Remove unwanted nested lists
    ######

    # fix column types for df
    date_columns = ['created_at', 'updated_at', 'vat_number_validated_time']
    for column in date_columns:
        print("Changing ", column, " to datetime")
        col_to_timestamp(df, column, timezone)

    str_columns = ['consolidated_invoicing', 'relationship', 'use_default_hierarchy_settings', 'parent_account_access',
                   'child_account_access', 'cf_holding_id']
    for column in str_columns:
        print("Changing ", column, " to string")
        col_to_str(df, column)

    int_columns = ['promotional_credits', 'refundable_credits', 'excess_payments', 'unbilled_charges']
    for column in int_columns:
        print("Changing ", column, " to int")
        df[column] = df[column].astype('int')

    bool_columns = ['business_customer_without_vat_number']

    for column in bool_columns:
        print("Changing ", column, " to bool")

        if column in df.columns:

            if sum(df[column].notnull()) > 0:
                df.loc[df[column].astype(str) == '0.0', column] = False
                df.loc[df[column].astype(str) == 'False', column] = False
                df.loc[df[column].astype(str) == '1.0', column] = True
                df.loc[df[column].astype(str) == 'True', column] = True

                df[column] = df[column].astype('boolean')

    objects_columns = ['billing_address', 'contacts', 'balances']

    ## Create table from billing address and remove from df
    colname = 'billing_address'

    if colname in df.columns:

        df_temp = df[~pd.isna(df[colname])][['id', 'updated_at', colname]]
        df_new = pd.json_normalize(df_temp[colname])
        df_new['customer_id'] = df_temp['id'].values
        df_new['updated_at'] = df_temp['updated_at'].values

        str_columns = ['city', 'company', 'country', 'email', 'first_name', 'last_name',
                       'line1', 'line2', 'line3', 'object', 'phone', 'state', 'state_code',
                       'validation_status', 'zip']

        for column in str_columns:
            col_to_str(df_new, column)

        df = df.drop(colname, axis=1)

        # save data to s3 bucket
        wr.s3.to_parquet(
            df=df_new,
            path=f"s3://{target_bucket_name}/chargebee/{colname}/entity={instance}/",
            dataset=True,
            mode="append",
            compression='snappy'
        )

    colname = 'contacts'

    if colname in df.columns:

        df_temp = df[~pd.isna(df[colname])][['id', 'updated_at', colname]]
        df_temp = df_temp.explode(colname)
        df_new = pd.json_normalize(df_temp[colname])
        df_new['customer_id'] = df_temp['id'].values
        df_new['updated_at'] = df_temp['updated_at'].values

        str_columns = ['email', 'first_name', 'id', 'last_name', 'object', 'phone',
                       'customer_id']
        date_columns = ['updated_at']

        for column in str_columns:
            col_to_str(df_new, column)

        for column in date_columns:
            col_to_timestamp(df_new, column, timezone)

        df = df.drop(colname, axis=1)

        # save data to s3 bucket
        wr.s3.to_parquet(
            df=df_new,
            path=f"s3://{target_bucket_name}/chargebee/{colname}/entity={instance}/",
            dataset=True,
            mode="append",
            compression='snappy'
        )

    colname = 'balances'

    if colname in df.columns:

        df_temp = df[~pd.isna(df[colname])][['id', 'updated_at', colname]]
        df_temp = df_temp.explode(colname)
        df_new = pd.json_normalize(df_temp[colname])
        df_new['customer_id'] = df_temp['id'].values
        df_new['updated_at'] = df_temp['updated_at'].values

        str_columns = ['balance_currency_code', 'currency_code', 'object', 'customer_id']
        date_column = ['updated_at']

        for column in str_columns:
            col_to_str(df_new, column)

        for column in date_columns:
            col_to_timestamp(df_new, column, timezone)

        df = df.drop(colname, axis=1)

        # save data to s3 bucket
        wr.s3.to_parquet(
            df=df_new,
            path=f"s3://{target_bucket_name}/chargebee/{colname}/entity={instance}/",
            dataset=True,
            mode="append",
            compression='snappy'
        )

    # write df to s3
    if len(df) > 0:
        folder_name = "customers"
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{target_bucket_name}/chargebee/{folder_name}/entity={instance}/",
            dataset=True,
            mode="append",
            compression='snappy'
        )

        # update secrets
    secretId = f'chargebee_{instance}'
    # get original secrets
    original_secret = client.get_secret_value(SecretId=secretId)
    config = json.loads(original_secret['SecretString'])
    # update secrets
    config[last_updated_key] = update_timestamp
    client.update_secret(SecretId=secretId, SecretString=json.dumps(config))