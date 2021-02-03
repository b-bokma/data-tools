import boto3
import json
from datetime import datetime, timedelta
import awswrangler as wr
import pandas as pd
from uuid import uuid4
import os

from functions import list_data

session = boto3.Session()
client = session.client('secretsmanager')
target_bucket_name = os.getenv('BUCKETNAME')

def lambda_handler(Event, Context):
    
    if 'instance' in Event.keys():
        instance = Event['instance']
        secret_string = client.get_secret_value(
                SecretId=f'chargebee_{instance}'
                )['SecretString']
        config = json.loads(secret_string)
        url = config['url']
        token = config['token']

    else:
        return "Invalid Request. Add instance to Event payload"

    # retrieve last updated from config
    last_updated_key = 'chargebee_customers_last_downloaded'
    last_updated = None

    if last_updated_key not in config.keys():
        config[last_updated_key] = last_updated

    if 'last_updated' in Event.keys():
        last_updated = Event['last_updated']
        if last_updated == "":
            last_updated = config[last_updated_key]

    customers = [x['customer'] for x in list_data(endpoint='customers', url=url, token=token, last_updated=last_updated)]

    # write last updated to secrets file
    update_timestamp = int(datetime.now().timestamp())

    if len(customers) > 0:

        today = datetime.today()
        filename = f"{today.strftime('%Y-%m-%d')}-{str(uuid4())[:8]}"
        path = f's3://{target_bucket_name}/chargebee/customers/entity={instance}/{filename}.json'
        print(path)

        f = wr.s3.to_json(
            df=pd.DataFrame(customers),
            path=path,
            lines=True,
            orient='records'
        )

        return_object = {'statusCode': 200, 'Body': {'path': path}, 'UpdateTimestamp': update_timestamp, 'Instance': instance}
    else:
        return_object = {'statusCode': 404, 'Body': 'No data found'}

    # update secrets
    secretId = f'chargebee_{instance}'

    # get original secrets
    original_secret = client.get_secret_value(SecretId=secretId)
    config = json.loads(original_secret['SecretString'])

    # update secrets
    config[last_updated_key] = update_timestamp
    client.update_secret(SecretId=secretId, SecretString=json.dumps(config))

    return (return_object)

