"""Script to execute the Glue crawlers, which will make sure the new DB partitions are injected"""
import boto3
import json
from time import sleep

session = boto3.Session(region_name='eu-west-1')
client = session.client('glue')

def lambda_handler(event,context):

    crawlername = event.get('CrawlerName')
    state=client.get_crawler(Name=crawlername)
    status=state["Crawler"]
    return_object = {"State":json.dumps(status['State'])}
    return return_object

