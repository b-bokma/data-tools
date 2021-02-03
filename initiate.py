""""
Small script that creates the first variables necessary for the application to work
"""

import dotenv
from os import getenv
import boto3

#TODO finish script

# get original secrets
# original_secret = client.get_secret_value(SecretId=secretId)
# config = json.loads(original_secret['SecretString'])

# update secrets
#config[last_updated_key] = update_timestamp
#client.update_secret(SecretId=secretId, SecretString=json.dumps(config))