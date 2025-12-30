import boto3
import time

ddb = boto3.resource("dynamodb")
table = ddb.Table("pipeline_metadata")

def lambda_handler(event, context):
    execution_id = event["execution_id"]

    response = table.get_item(Key={"execution_id": execution_id})
    if "Item" in response:
        raise Exception("Duplicate execution")

    table.put_item(
        Item={
            "execution_id": execution_id,
            "status": "IN_PROGRESS",
            "created_ts": int(time.time())
        }
    )
    return event
