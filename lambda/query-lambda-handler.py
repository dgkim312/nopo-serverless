import os
import boto3
import json
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
        KeyConditionExpression=Key(GSI1PK).eq("1")
    )

    response_attrs = ["pk", "LOCATION_ADDRESS", "RATING_AVG", "RATING_COUNT"]

    items = []

    for item in response['Items']:

        item_dict = {}

        for attr in response_attrs:
            item_dict[attr] = str(item.get(attr))

        items.append(item_dict)

    return {
        'statusCode': 200,
        'body': json.dumps(items)
    }
