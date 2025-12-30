def lambda_handler(event, context):
    print("ETL failed:", event)
    return {"status": "FAILED"}
