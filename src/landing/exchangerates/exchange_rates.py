import awswrangler as wr
import pandas as pd
import json
from datetime import datetime
from uuid import uuid4
import boto3
import requests
import os


target_bucket_name = os.getenv('TARGET_BUCKETNAME')

session = boto3.Session()
client = session.client('secretsmanager')


def lambda_handler(Event, Context):

    url = 'https://api.exchangerate.host/latest'
    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame(data)
    df = df.reset_index()
    df = df[~df['rates'].isna()]

    df = df[['index', 'date', 'base', 'rates']]
    df = df.rename(columns={'index': 'currency_code', 'rates': 'rate'})

    # store file
    today = datetime.today()
    filename = f"{today.strftime('%Y-%m-%d')}-{str(uuid4())[:8]}"
    path = f's3://{target_bucket_name}/exchange_rates/{filename}.json'

    wr.s3.to_json(
        df=df,
        path=path,
        lines=True,
        orient='records'
    )

    return_object = {'statusCode': 200, 'Body': {'path': path}}

    return (return_object)
