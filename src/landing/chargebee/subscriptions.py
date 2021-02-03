import boto3
import os
import json
import pandas as pd
import awswrangler as wr
from uuid import uuid4
from datetime import datetime

from functions import list_data

session = boto3.Session()
client = session.client('secretsmanager')
target_bucket_name = os.getenv('BUCKETNAME')


def lambda_handler(Event, Context):
    # set variables
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
    last_updated_key = 'chargebee_subscriptions_last_downloaded'
    print(last_updated_key)

    last_updated = None

    if last_updated_key not in config.keys():
        config[last_updated_key] = last_updated

    if 'last_updated' in Event.keys():
        last_updated = Event['last_updated']
        if last_updated == "":
            last_updated = config[last_updated_key]

    print(last_updated)
    # Extract data from API endpoint using list_data function
    subscriptions = [x['subscription'] for x in list_data(
        endpoint='subscriptions', url=url, token=token, last_updated=last_updated)]

    # storing timestamp of update to send to SecretsManager when script is finised
    update_timestamp = int(datetime.now().timestamp())

    # write loaded data to S3 to store all, unprocessed data
    if len(subscriptions) > 0:

        today = datetime.today()
        filename = f"{today.strftime('%Y-%m-%d')}-{str(uuid4())[:8]}"
        path = f's3://{target_bucket_name}/chargebee/subscriptions/entity={instance}/{filename}.json'
        print(path)

        f = wr.s3.to_json(
            df=pd.DataFrame(subscriptions),
            path=path,
            lines=True,
            orient='records'
        )
        return_object = {'statusCode': 200, 'Body': {'path': path}, 'UpdateTimestamp': update_timestamp,
                         'Instance': instance}
    else:
        return_object = {'statusCode': 404, 'Body': 'No data found'}

    # reload the config file, and immediately write to config file

    # update secrets
    secretId = f'chargebee_{instance}'
    # get original secrets
    original_secret = client.get_secret_value(SecretId=secretId)
    config = json.loads(original_secret['SecretString'])
    # update secrets
    config[last_updated_key] = update_timestamp
    client.update_secret(SecretId=secretId, SecretString=json.dumps(config))

    return (return_object)
