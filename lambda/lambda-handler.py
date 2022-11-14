def handler(event, context):
    response = event
    print(response)
    return {
        'statusCode': 200,
        'body': response
    }