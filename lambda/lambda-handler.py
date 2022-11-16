import os
import boto3

dynamodb = boto3.resource('dynamodb')

TABLE_NAME = os.environ['TABLE_NAME']
GSI1PK = os.environ["GSI1PK"]
GSI1SK = os.environ["GSI1SK"]
GSI2PK = os.environ["GSI2PK"]
GSI2SK = os.environ["GSI2SK"]


def handler(event, context):
    print(event)

    table = dynamodb.Table(TABLE_NAME)

    for record in event['Records']:

        new_image = record['dynamodb']['NewImage'] | record['dynamodb']['Keys']

        rating_avg = str(
            round(int(new_image['RATING_SUM']['N'])
                  / int(new_image['RATING_COUNT']['N']), 2))

        item = {'RATING_AVG': rating_avg}

        for key, value in new_image.items():
            item[key] = [int(v) if k == 'N' else v for k, v in value.items()][0]

        reversed_issue_date = str(99999999 - int(item["LICENSE_ISSUE_DATE"]))
        location = item["LOCATION_ADDRESS"].split()[0]
        item[GSI1PK] = item["BUSINESS_STATUS_CODE"]
        item[GSI1SK] = "#" + rating_avg + "#" + reversed_issue_date
        item[GSI2PK] = item["BUSINESS_STATUS_CODE"]
        item[GSI2SK] = "#" + location + "#" + rating_avg + "#" + reversed_issue_date

        response = table.put_item(Item=item)

    return {
        'statusCode': 200,
        'body': response
    }
