def lambda_handler(event, context):
    print("Triggering downstream analytics for:", event["domain"])
    return {"status": "DOWNSTREAM_TRIGGERED"}
