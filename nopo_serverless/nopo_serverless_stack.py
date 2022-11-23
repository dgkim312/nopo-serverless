from aws_cdk import (
    Stack,
    aws_dynamodb,
    aws_lambda,
    aws_lambda_event_sources,
    aws_apigateway,
    aws_sqs
)
from constructs import Construct


class NopoServerlessStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        raw_table = aws_dynamodb.Table(
            self, "nopo_star_raw_table", partition_key=aws_dynamodb.Attribute(
                name="pk",
                type=aws_dynamodb.AttributeType.STRING
            ),
            stream=aws_dynamodb.StreamViewType.NEW_IMAGE
        )

        function = aws_lambda.Function(
            self, "starRatingCalculation",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            code=aws_lambda.Code.from_asset('lambda'),
            handler='lambda-handler.handler'
        )

        function.add_event_source(
            aws_lambda_event_sources.DynamoEventSource(raw_table,
                                                       starting_position=aws_lambda.StartingPosition.LATEST))

        aggregates_table = aws_dynamodb.Table(
            self, "nopo_star_avg_table", partition_key=aws_dynamodb.Attribute(
                name="pk",
                type=aws_dynamodb.AttributeType.STRING
            )
        )

        gsi1 = 'gsi1'
        gsi1pk = 'gsi1pk'
        gsi1sk = 'gsi1sk'

        aggregates_table.add_global_secondary_index(index_name=gsi1,
                                                    partition_key=aws_dynamodb.Attribute(
                                                        name=gsi1pk,
                                                        type=aws_dynamodb.AttributeType.STRING
                                                    ),
                                                    sort_key=aws_dynamodb.Attribute(
                                                        name=gsi1sk,
                                                        type=aws_dynamodb.AttributeType.STRING
                                                    ),
                                                    projection_type=aws_dynamodb.ProjectionType.ALL
                                                    )

        gsi2 = 'gsi2'
        gsi2pk = 'gsi2pk'
        gsi2sk = 'gsi2sk'

        aggregates_table.add_global_secondary_index(index_name=gsi2,
                                                    partition_key=aws_dynamodb.Attribute(
                                                        name=gsi2pk,
                                                        type=aws_dynamodb.AttributeType.STRING
                                                    ),
                                                    sort_key=aws_dynamodb.Attribute(
                                                        name=gsi2sk,
                                                        type=aws_dynamodb.AttributeType.STRING
                                                    ),
                                                    projection_type=aws_dynamodb.ProjectionType.ALL
                                                    )

        function.add_environment("TABLE_NAME", aggregates_table.table_name)
        function.add_environment("GSI1PK", gsi1pk)
        function.add_environment("GSI1SK", gsi1sk)
        function.add_environment("GSI2PK", gsi2pk)
        function.add_environment("GSI2SK", gsi2sk)

        aggregates_table.grant_write_data(function)

        query_function = aws_lambda.Function(
            self, "starRatingQuery",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            code=aws_lambda.Code.from_asset('lambda'),
            handler='query-lambda-handler.handler'
        )

        aggregates_table.grant_read_data(query_function)

        query_function.add_environment("TABLE_NAME", aggregates_table.table_name)
        query_function.add_environment("GSI1", gsi1)
        query_function.add_environment("GSI1PK", gsi1pk)
        query_function.add_environment("GSI1SK", gsi1sk)
        query_function.add_environment("GSI2", gsi2)
        query_function.add_environment("GSI2PK", gsi2pk)
        query_function.add_environment("GSI2SK", gsi2sk)

        aws_apigateway.LambdaRestApi(self, "TopRatingQueryEndpoint", handler=query_function)

        queue = aws_sqs.Queue(self, "StarQueue")

        star_event_handler = aws_lambda.Function(
            self, "starEventHandler",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            code=aws_lambda.Code.from_asset('lambda'),
            handler='star-event-handler.handler'
        )

        star_event_handler.add_event_source(aws_lambda_event_sources.SqsEventSource(queue))

        raw_table.grant_write_data(star_event_handler)

        star_event_handler.add_environment("TABLE_NAME", raw_table.table_name)
