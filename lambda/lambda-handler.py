import os
import boto3

dynamodb = boto3.resource('dynamodb')

TABLE_NAME = os.environ['TABLE_NAME']


def handler(event, context):
    response = event
    print(response)

    table = dynamodb.Table(TABLE_NAME)

    for record in event['Records']:
        new_image = record['dynamodb']['NewImage']

        item = {'pk': str(record['dynamodb']['Keys']['pk']['S']),
                'RATING_AVG': str(round(int(new_image['RATING_SUM']['N'])
                                        / int(new_image['RATING_COUNT']['N']), 2))
                }
        for key, value in new_image.items():
            item[key] = [int(v) if k == 'N' else v for k, v in value.items()][0]
        response = table.put_item(
            Item=item
        )

    return {
        'statusCode': 200,
        'body': response
    }
