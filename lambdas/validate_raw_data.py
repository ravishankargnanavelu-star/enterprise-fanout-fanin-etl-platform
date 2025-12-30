import boto3

s3 = boto3.client("s3")

def lambda_handler(event, context):
    bucket = event["bucket"]
    key = event["key"]

    try:
        s3.head_object(Bucket=bucket, Key=key)
        return event
    except Exception:
        raise Exception("Raw data validation failed")
