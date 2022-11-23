import os
from decimal import Decimal
import json
import logging
import boto3
from botocore.exceptions import ClientError
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret(secret_name):
    region_name = "ap-northeast-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    # Your code goes here.

    return secret


db_secret = json.loads(get_secret(os.environ['DB_SECRET']))

try:
    conn = pymysql.connect(
        host=os.environ["RDS_PROXY"],
        user=db_secret["username"],
        passwd=db_secret["password"],
        db="nopo", connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")


dynamodb = boto3.resource('dynamodb')

TABLE_NAME = os.environ['TABLE_NAME']

insert_sql = 'INSERT INTO star_rating (USER_ID, RESTAURANT_ID, STAR_RATING, CREATED_DATE) VALUES (%(userId)s, %(storeId)s, %(starred)s, now())'


def insert_rds(user_id, store_id, starred):
    with conn.cursor() as cur:
        cur.execute(insert_sql, args={"userId": user_id, "storeId": store_id, "starred": starred})
        print("Insert into RDS.")
    conn.commit()


def handler(event, context):
    print(event)
    response = []
    for record in event["Records"]:
        print(record["body"])

        payload = json.loads(record["body"])["payload"]
        pk = payload["storeId"]
        starred = payload["starred"]

        insert_rds(user_id=payload["userId"], store_id=pk, starred=starred)

        table = dynamodb.Table(TABLE_NAME)

        try:
            res = table.update_item(
                Key={'pk': pk},
                UpdateExpression="set RATING_COUNT= RATING_COUNT + :c , RATING_SUM=RATING_SUM + :s",
                ExpressionAttributeValues={':c': 1, ':s': Decimal(starred)},
                ConditionExpression="attribute_exists(pk)",
                ReturnValues="UPDATED_NEW"
            )
            response.append(res)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                print(e.response['Error'])
            elif e.response['Error']['Code'] == "ValidationException":
                res = table.update_item(
                    Key={'pk': pk},
                    UpdateExpression="set RATING_COUNT=:c , RATING_SUM=:s",
                    ExpressionAttributeValues={':c': 1, ':s': Decimal(starred)},
                    ConditionExpression="attribute_exists(pk)",
                    ReturnValues="UPDATED_NEW"
                )
            else:
                print(e.response['Error'])

    return {
        'statusCode': 200,
        'body': response
    }