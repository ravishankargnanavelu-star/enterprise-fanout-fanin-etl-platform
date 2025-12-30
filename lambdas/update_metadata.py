import boto3
import time

ddb = boto3.resource("dynamodb")
table = ddb.Table("pipeline_metadata")

def lambda_handler(event, context):
    execution_id = event["execution_id"]

    table.update_item(
        Key={"execution_id": execution_id},
        UpdateExpression="SET #s = :s, completed_ts = :t",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":s": "COMPLETED",
            ":t": int(time.time())
        }
    )
    return event
