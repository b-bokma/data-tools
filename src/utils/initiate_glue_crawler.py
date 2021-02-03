"""Script to execute the Glue crawlers, which will make sure the new DB partitions are injected"""
import boto3

session = boto3.Session(region_name='eu-west-1')
client = session.client('glue')

def lambda_handler(event,context):
    # Invoke Glue Client

    crawlername = event.get('CrawlerName')
    response = client.start_crawler(Name=crawlername)

    return response
