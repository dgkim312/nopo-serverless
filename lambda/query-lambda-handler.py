import os
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')

TABLE_NAME = os.environ['TABLE_NAME']
GSI1 = os.environ["GSI1"]
GSI1PK = os.environ["GSI1PK"]
GSI1SK = os.environ["GSI1SK"]
GSI2 = os.environ["GSI2"]
GSI2PK = os.environ["GSI2PK"]
GSI2SK = os.environ["GSI2SK"]


def handler(event, context):
    print(event)

    table = dynamodb.Table(TABLE_NAME)

    response = table.query(
        IndexName=GSI1,
        KeyConditionExpression=Key(GSI1PK).eq("1") & Key(GSI1SK).begins_with("1")
    )

    return {
        'statusCode': 200,
        'body': response
    }
