import os
import boto3

dynamodb = boto3.resource('dynamodb')

TABLE_NAME = os.environ['TABLE_NAME']

def handler(event, context):
    response = event
    print(response)

    table = dynamodb.Table(TABLE_NAME)

    for record in event['Records']:
        response = table.put_item(
            Item={
                'pk': str(record['dynamodb']['Keys']['pk']['S'])
            }
        )

    return {
        'statusCode': 200,
        'body': response
    }

