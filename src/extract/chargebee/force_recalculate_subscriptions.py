"""
Run this file locally. Add your own home folder with path.append to import functions correctly.
"""

import boto3
import os
import json
from datetime import datetime
import pandas as pd
import awswrangler as wr
from copy import deepcopy
import requests
from time import sleep

from sys import path
path.append("/Users/bertbokma/Development/Quicksight/Quicksight/layer")

from functions import col_to_str, col_to_timestamp

session = boto3.Session()
client = session.client('secretsmanager')

source_bucket_name = "pp-quicksight-landing-hed2iehhry25"
target_bucket_name = "pp-quicksight-raw-pnj2hfj34mfr"

# Subscriptions

df_download = wr.s3.read_json(
        path=f"s3://{source_bucket_name}/chargebee/subscriptions/",
        lines=True,
        convert_dates=False,  # dont convert columns to dates
        convert_axes=False
    )

# set last updated key to update config file with
last_updated_key = 'chargebee_subscriptions_last_updated'
update_timestamp = int(datetime.utcnow().timestamp())

instances = df_download.entity.unique()

for instance in instances:
    secret_string = client.get_secret_value(
            SecretId=f'chargebee_{instance}'
    )['SecretString']
    config = json.loads(secret_string)
    url = config['url']
    token = config['token']
    timezone = config['timezone']

    df_subscriptions = df_download[df_download.entity == instance]
    print(f"Recalculating subscriptions {instance}")

    #####
    # DATA PROCESSING
    # steps taken:
    # setting column types
    # create new data tables from nested lists in subscriptions table
    # Addons, Contract Term, Event Based Addons (One off fees),
    # Remove unwanted nested lists
    ######

    # set str columns to string, since NaN is seen as int most string columns are loaded as object
    str_columns = ['id', 'plan_id', 'billing_period', 'billing_period_unit', 'customer_id', 'status',
                   'created_from_ip',
                   'object', 'currency_code', 'base_currency_code', 'cf_account_id', 'cf_account_name', 'po_number',
                   'invoice_notes', 'coupon', 'auto_collection']

    for column in str_columns:
        col_to_str(df_subscriptions, column)

    # define datetime columns
    date_columns = ['current_term_start', 'current_term_end', 'next_billing_at', 'created_at', 'started_at',
                    'activated_at', 'updated_at', 'cancelled_at', 'due_since', 'start_date', 'pause_date',
                    'resume_date']

    for column in date_columns:
        col_to_timestamp(df_subscriptions, column, timezone)

    # ADDONS
    if 'addons' in df_subscriptions.columns:
        df_addons = pd.DataFrame(df_subscriptions['addons'])
        df_addons['subscription_id'] = df_subscriptions['id']
        df_addons['updated_at'] = df_subscriptions['updated_at']
        df_addons = df_addons[~df_addons['addons'].isna()]
        df_addons = df_addons.explode('addons')
        ids = deepcopy(df_addons['subscription_id'])
        updated_at = deepcopy(df_addons['updated_at'])
        df_addons = pd.json_normalize(df_addons['addons'])
        df_addons['subscription_id'] = ids.values
        df_addons['updated_at'] = updated_at.values
        df_addons = df_addons.drop_duplicates()

        # drop addons from df_subscriptions
        df_subscriptions = df_subscriptions.drop('addons', axis=1)

        # summarize addons in new columns and re-merge with df_subscriptions (for total addon value)

        addons_merge = df_addons[['subscription_id', 'updated_at', 'quantity', 'amount']].groupby(
            ['subscription_id', 'updated_at']).sum().reset_index()
        addons_merge.columns = ['id', 'updated_at', 'addons_quantity', 'addons_amount']
        df_subscriptions = df_subscriptions.merge(addons_merge, how='left', left_on=['id', 'updated_at'],
                                                  right_on=['id', 'updated_at'])

        str_columns = ['id', 'object']
        for column in str_columns:
            col_to_str(df_addons, column)

        # save addon data to s3 bucket
        folder_name = "addons"

        wr.s3.to_parquet(
            df=df_addons,
            path=f"s3://{target_bucket_name}/chargebee/{folder_name}/entity={instance}/",
            dataset=True,
            mode="append",
            compression='snappy'
        )

    # SHIPPING ADDRESS - we're dropping shipping address. Address will be part of Customer data object
    if 'shipping_address' in df_subscriptions.columns:
        df_shipping_address = df_subscriptions['shipping_address']
        df_shipping_address = pd.json_normalize(df_shipping_address[~df_shipping_address.isna()])
        df_shipping_address['subscription_id'] = df_subscriptions['id'].reset_index().drop('index', axis=1)
        df_shipping_address['updated_at'] = df_subscriptions['updated_at'].reset_index().drop('index', axis=1)
        df_subscriptions = df_subscriptions.drop('shipping_address', axis=1)

    if 'override_relationship' in df_subscriptions:
        df_subscriptions = df_subscriptions.drop('override_relationship', axis=1)

    if 'total_dues' in df_subscriptions.columns:
        df_total_dues = pd.DataFrame(df_subscriptions['total_dues'])
        df_total_dues['subscription_id'] = df_subscriptions['id']
        df_total_dues['updated_at'] = df_subscriptions['updated_at']
        df_total_dues = df_total_dues[~df_total_dues.isna()]
        df_subscriptions = df_subscriptions.drop('total_dues', axis=1)

    # CONTRACT TERM # NOTE: not yet clear how this will be used. It is extract and stored so we can use it if necessary

    if 'contract_term' in df_subscriptions.columns:
        df_contract_term = df_subscriptions['contract_term']
        df_contract_term = df_contract_term[~df_contract_term.isna()]
        df_contract_term = pd.json_normalize(df_contract_term)
        df_contract_term['subscription_id'] = df_subscriptions[~df_subscriptions.contract_term.isna()][
            'id']
        df_contract_term['updated_at'] = df_subscriptions[~df_subscriptions.contract_term.isna()][
            'updated_at']
        df_subscriptions = df_subscriptions.drop('contract_term', axis=1)

        str_columns = ['id', 'status', 'billing_cycle', 'action_at_term_end', 'object', 'subscription_id']
        date_columns = ['contract_start', 'contract_end', 'created_at', 'updated_at']

        for column in str_columns:
            col_to_str(df_contract_term, column)

        for column in date_columns:
            col_to_timestamp(df_contract_term, column, timezone)

        # save contract term data to s3 bucket
        if len(df_contract_term) > 0:
            folder_name = "contract_term"

            wr.s3.to_parquet(
                df=df_contract_term,
                path=f"s3://{target_bucket_name}/chargebee/{folder_name}/entity={instance}/",
                dataset=True,
                mode="append",
                compression='snappy'
            )

        print("contract term updated")
    # Event Based Addons (One off fees)
    if 'event_based_addons' in df_subscriptions.columns:
        df_event_based_addons = pd.DataFrame(df_subscriptions['event_based_addons'])
        df_event_based_addons['subscription_id'] = df_subscriptions['id']
        df_event_based_addons['updated_at'] = df_subscriptions['updated_at']
        df_event_based_addons = df_event_based_addons[~df_event_based_addons['event_based_addons'].isna()]
        df_event_based_addons = df_event_based_addons.explode('event_based_addons')
        df_event_based_addons = df_event_based_addons[~df_event_based_addons.isna()]
        ids = df_event_based_addons['subscription_id'].reset_index().drop('index', axis=1)
        updated_at = df_event_based_addons['updated_at'].reset_index().drop('index', axis=1)
        df_event_based_addons = pd.json_normalize(df_event_based_addons['event_based_addons'])
        df_event_based_addons['subscription_id'] = ids
        df_event_based_addons['updated_at'] = updated_at
        df_subscriptions = df_subscriptions.drop('event_based_addons', axis=1)

        # Fix columns
        str_columns = ['id', 'subscription_id', 'on_event']
        for column in str_columns:
            col_to_str(df_event_based_addons, column)

        # save one off fee data to s3 bucket
        if len(df_event_based_addons) > 0:
            folder_name = "one_off_fees"

            wr.s3.to_parquet(
                df=df_event_based_addons,
                path=f"s3://{target_bucket_name}/chargebee/{folder_name}/entity={instance}/",
                dataset=True,
                mode="append",
                compression='snappy'
            )
            print("One off fee updated")

    if 'charged_event_based_addons' in df_subscriptions.columns:
        df_charged_event_based_addons = pd.DataFrame(df_subscriptions['charged_event_based_addons'])
        df_charged_event_based_addons['subscription_id'] = df_subscriptions['id']
        df_charged_event_based_addons = df_charged_event_based_addons[
            ~df_charged_event_based_addons['charged_event_based_addons'].isna()]
        df_charged_event_based_addons = df_charged_event_based_addons.explode('charged_event_based_addons')
        ids = df_charged_event_based_addons['subscription_id'].reset_index().drop('index', axis=1)
        df_charged_event_based_addons = pd.json_normalize(
            df_charged_event_based_addons['charged_event_based_addons'])
        df_charged_event_based_addons['subscription_id'] = ids
        df_charged_event_based_addons['updated_at'] = updated_at
        df_subscriptions = df_subscriptions.drop('charged_event_based_addons', axis=1)

        if len(df_charged_event_based_addons) > 0:
            folder_name = "one_off_fees"

            wr.s3.to_parquet(
                df=df_charged_event_based_addons,
                path=f"s3://{target_bucket_name}/chargebee/{folder_name}/entity={instance}/",
                dataset=True,
                mode="append",
                compression='snappy'
            )
            print("One off fee updated")

    # COUPONS
    if 'coupons' in df_subscriptions.columns:
        df_coupons = pd.DataFrame(df_subscriptions['coupons'])
        df_coupons['subscription_id'] = df_subscriptions['id']
        df_coupons['updated_at'] = df_subscriptions['updated_at']
        df_coupons = df_coupons[~df_coupons['coupons'].isna()]
        df_coupons = df_coupons.explode('coupons')
        ids = deepcopy(df_coupons['subscription_id'])
        updated_at = deepcopy(df_coupons['updated_at'])
        df_coupons = pd.json_normalize(df_coupons['coupons'])
        df_coupons['subscription_id'] = ids.values
        df_coupons['updated_at'] = updated_at.values
        df_subscriptions = df_subscriptions.drop('coupons', axis=1)

        # extract coupon information from chargebee to map on
        endpoint = 'coupons'
        offset = ''
        output = []

        while offset is not None:

            params = {
                "include_deleted": "true",
                "limit": 100,
                "offset": offset
            }

            result = requests.get(
                url=f"https://{url}.chargebee.com/api/v2/{endpoint}",
                auth=(f"{token}", ""),
                params=params
            )
            r = result.json()

            for row in r['list']:
                output.append(row)

            if 'next_offset' in r.keys():
                offset = r['next_offset']
            else:
                offset = None
                continue
            sleep(5)

        coupons_lookup = pd.DataFrame([x['coupon'] for x in output])
        coupons_lookup = coupons_lookup.rename(
            columns={'id': 'coupon_id', 'name': 'coupon_name', 'created_at': 'coupon_created_at',
                     'updated_at': 'coupon_updated_at'})
        coupons_lookup = coupons_lookup.drop(['object'], axis=1)
        df_coupons.rename(columns={'id': 'coupon_id'})

        if len(df_coupons) > 0:
            df_coupons = df_coupons.merge(coupons_lookup, how='left', on='coupon_id')

            # fix column types
            str_columns = ['coupon_id', 'object', 'subscription_id', 'coupon_name', 'invoice_name', 'discount_type',
                           'duration_type', 'status', 'apply_discount_on', 'apply_on', 'plan_constraint',
                           'addon_constraint',
                           'resource_version', 'currency_code', 'plan_ids', 'addon_ids']

            date_columns = ['apply_till', 'updated_at', 'coupon_created_at', 'coupon_updated_at', 'valid_till',
                            'archived_at']

            for column in str_columns:
                col_to_str(df_coupons, column)

            for column in date_columns:
                col_to_timestamp(df_coupons, column, timezone)

            # save coupon data to s3 bucket
            folder_name = "coupons"
            df_coupons = df_coupons.rename(columns={'coupon_id': 'id'})
            wr.s3.to_parquet(
                df=df_coupons,
                path=f"s3://{target_bucket_name}/chargebee/{folder_name}/entity={instance}/",
                dataset=True,
                mode="append",
                compression='snappy'
            )

            print("coupons updated")

    #### LAST RESULTS TO S3

    # if table is created save data to s3 bucket
    if len(df_subscriptions) > 0:
        folder_name = "subscriptions"
        wr.s3.to_parquet(
            df=df_subscriptions,
            path=f"s3://{target_bucket_name}/chargebee/{folder_name}/entity={instance}/",
            dataset=True,
            mode="append",
            compression='snappy'
        )
        print("Subscriptions updated")

    # update secrets
    secretId = f'chargebee_{instance}'
    # get original secrets
    original_secret = client.get_secret_value(SecretId=secretId)
    config = json.loads(original_secret['SecretString'])
    # update secrets
    config[last_updated_key] = update_timestamp
    client.update_secret(SecretId=secretId, SecretString=json.dumps(config))