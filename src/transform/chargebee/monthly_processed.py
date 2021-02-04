'''
Adding an additional monthly script. This combines data from current month + previous month, so made sense
to split the scripts.
'''

import awswrangler as wr
import pandas as pd
import boto3
import os

session = boto3.Session()
secret_client = session.client('secretsmanager')

source_bucket_name = os.getenv('SOURCE_BUCKETNAME')
target_bucket_name = os.getenv('TARGET_BUCKETNAME')


def lambda_handler(Event, Context):
    if 'Event' in locals():
        if 'Body' in Event.keys():
            month = Event['Body']['month']
            year = Event['Body']['year']

    if month == 1:
        prev_year = year - 1
        prev_month = 12
    else:
        prev_year = year
        prev_month = month - 1

    # Load data frame of selected month
    df = wr.s3.read_parquet(f"s3://{source_bucket_name}/monthly/chargebee/month={year}-{month}")

    # Load data frame of previous month
    try:
        df_prev = wr.s3.read_parquet(f"s3://{source_bucket_name}/monthly/chargebee/month={prev_year}-{prev_month}")
    except:
        return "Folder does not exist"

    # Calculate RX results
    # This is done by taking the ARR of last month / rate of that month * rate of this month.
    # In other words; what would the ARR be based on the subscriptions of last month, with the RX rate of this month
    df_merge = df_prev[df_prev.status.isin(['active', 'non_renewing'])][['id', 'currency_code', 'rate', 'total_arr']]
    df_merge.columns = ['id', 'currency_code_prev_month', 'rate_prev_month', 'total_arr_prev_month']

    df = df.merge(df_merge, on='id', how='left')
    df['ARR_EUR_COMPARE'] = df['rate'] * df['total_arr_prev_month']
    df['ARR_EUR_PREV'] = df['rate_prev_month'] * df['total_arr_prev_month']
    df['RX_result'] = df['ARR_EUR_COMPARE'] - df['ARR_EUR_PREV']

    # Calculate Up or Downsell results
    df['arr_diff'] = df['total_arr'] - df['total_arr_prev_month']
    df['ARR_DIFF_EUR'] = df['arr_diff'] * df['rate']

    int_columns_100 = ['plan_unit_price', 'plan_amount', 'total_addon_value',
                       'total_plan_value', 'mrr', 'setup_fee', 'discount_amount', 'total_discount_value',
                       'total_subscription_value', 'total_mrr', 'total_mrr_discount', 'total_plan_value_EUR',
                       'total_subscription_value_EUR', 'MRR_EUR', 'MRR_discount_EUR',
                       'total_arr', 'ARR_EUR', 'total_arr_discount', 'ARR_discount_EUR',
                       'total_arr_prev_month', 'ARR_EUR_COMPARE', 'ARR_EUR_PREV',
                       'RX_result', 'arr_diff', 'ARR_DIFF_EUR']

    for column in int_columns_100:
        if column in df.columns:
            df[column] = df[column] / 100

    wr.s3.to_parquet(
        df=df,
        path=f"s3://{target_bucket_name}/monthly/processed/month={year}-{month}/",
        dataset=True,
        mode='overwrite'
    )

    return {"statusCode": 200, "Body": "Monthly data created"}

