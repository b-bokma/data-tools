import boto3
import os
import json

session = boto3.Session()
step_function_client = session.client('stepfunctions')
secrets_manager_client = session.client('secretsmanager')

def lambda_handler(Event, Context):

    input = json.dumps({"last_updated":""})

    step_function_arn = os.getenv("STEPFUNCTIONARN")
    r = step_function_client.start_execution(
                stateMachineArn=step_function_arn,
                input=input)
