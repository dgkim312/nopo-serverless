from aws_cdk import (
    Stack,
    aws_dynamodb,
    aws_lambda,
    aws_lambda_event_sources
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

        function.add_environment("TABLE_NAME", aggregates_table.table_name)

        aggregates_table.grant_write_data(function)
