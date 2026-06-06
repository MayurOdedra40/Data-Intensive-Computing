"""L5 report  (STAGE-1 STUB, invoked on-demand -- NOT triggered by S3).

Real version (Stage 5) scans the Reviews table for source="devset" and computes the three
required result numbers (positive/neutral/negative counts, # failing profanity, banned
users). For now it just proves it can read DynamoDB and write to the export bucket.

Invoke directly, e.g.:  aws --endpoint-url=http://localhost:4566 lambda invoke \
                          --function-name report /tmp/out.json
"""
import json
import os

import boto3

import config

_ENDPOINT = "http://localhost:4566" if os.getenv("STAGE") == "local" else None
_ddb = boto3.client("dynamodb", endpoint_url=_ENDPOINT)
_s3 = boto3.client("s3", endpoint_url=_ENDPOINT)


def handler(event, context):
    reviews_table = config.get("/dic-a3/tables/reviews")
    export_bucket = config.get("/dic-a3/buckets/export")

    # Stage 5 will do a full scan + source="devset" filter + sentiment/profanity/ban counts.
    # Stage-1 stub just counts the items, to prove the read+write path works.
    count = _ddb.scan(TableName=reviews_table, Select="COUNT")["Count"]
    body = json.dumps({"reviewsItemCount": count})
    _s3.put_object(
        Bucket=export_bucket,
        Key="report.json",
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    return {"statusCode": 200, "reviewsItemCount": count}
