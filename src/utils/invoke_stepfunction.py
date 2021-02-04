import boto3
import os
import json

session = boto3.Session()
step_function_client = session.client('stepfunctions')
secrets_manager_client = session.client('secretsmanager')

def lambda_handler(Event, Context):

    """
    Either this script gets called from one step function to start the other
    Or it starts with ARNs passed along in the env, scheduled on cron.
    """
    if 'StateMachineArn' in Event.keys():
        step_function_arn = Event['StateMachineArn']
        r = step_function_client.start_execution(
            stateMachineArn=step_function_arn,
            input=json.dumps({"last_updated": ""}))

    else:
        stepfunctions = [os.getenv("CHARGEBEEDOWNLOADARN"), os.getenv("EXCHANGERATESDOWNLOADARN")]

        for stepfunction in stepfunctions:
            step_function_arn = stepfunction
            r = step_function_client.start_execution(
                stateMachineArn=step_function_arn,
                input=json.dumps({"last_updated": ""}))


