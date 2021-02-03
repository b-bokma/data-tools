import awswrangler as wr
import pandas as pd
import json
from datetime import datetime
from uuid import uuid4
import boto3
import requests
import os


def get_usd_value(url, access_key, currency):
    '''
    gets USD value of a currency using currencylayer
    '''
    currency = currency.upper()
    params = {
        'access_key': access_key,
        'currencies': f'{currency}',
        'source': 'USD'}
    r = requests.get(url, params)
    value = r.json()['quotes'][f'USD{currency}']
    return value


target_bucket_name = os.getenv('TARGET_BUCKETNAME')
db_name = os.getenv('CHARGEBEE_DB_NAME')

session = boto3.Session()
secret_client = session.client('secretsmanager')


def lambda_handler(Event, Context):
    if 'currency_codes' in Event.keys():
        x = eval(Event['currency_codes'])
        df_currency_codes = pd.DataFrame(x)
        df_currency_codes.columns = ['currency_code']
    # get all currencies used in chargebee data
    else:
        df_currency_codes = pd.DataFrame(data={'currency_code': ['EUR']})

    # retrieve token from secretsmanager
    secret_string = json.loads(secret_client.get_secret_value(SecretId='General')['SecretString'])

    # get USD rate from currency code
    df_currency_codes['USD'] = df_currency_codes.apply(
        lambda x: get_usd_value(url='http://api.currencylayer.com/live', access_key=secret_string['CURRENCYLAYER_KEY'],
                                currency=x['currency_code']), axis=1)

    # get EUR rate from currency code
    EUR_VALUE = df_currency_codes.loc[df_currency_codes.currency_code == 'EUR', 'USD'].values[0]

    df_currency_codes['EUR'] = df_currency_codes['USD'] / EUR_VALUE

    # add timestamp to currency codes
    df_currency_codes['timestamp'] = datetime.now()

    # store file
    today = datetime.today()
    filename = f"{today.strftime('%Y-%m-%d')}-{str(uuid4())[:8]}"
    path = f's3://{target_bucket_name}/exchange_rates/{filename}.json'

    wr.s3.to_json(
        df=df_currency_codes,
        path=path,
        lines=True,
        orient='records'
    )

    return_object = {'statusCode': 200, 'Body': {'path': path}}

    return (return_object)
