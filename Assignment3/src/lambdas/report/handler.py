"""L5 report  (real implementation, invoked on-demand -- NOT triggered by S3).

Scans the Reviews table (source="devset" only) and the Customers table to produce
the three required result numbers, then writes report.json to the export bucket.

Invoke:
    aws --endpoint-url=http://localhost:4566 lambda invoke \
        --function-name report /tmp/out.json && cat /tmp/out.json
"""
import json
import os

import boto3

import config

_ENDPOINT = "http://localhost:4566" if os.getenv("STAGE") == "local" else None
_ddb = boto3.client("dynamodb", endpoint_url=_ENDPOINT)
_s3  = boto3.client("s3",       endpoint_url=_ENDPOINT)

# Names read once at cold start (reused across warm invocations). report is on-demand, so this is
# mostly for consistency with the chain handlers (which read SSM at module scope, not per call).
REVIEWS_TABLE   = config.get("/dic-a3/tables/reviews")
CUSTOMERS_TABLE = config.get("/dic-a3/tables/customers")
EXPORT_BUCKET   = config.get("/dic-a3/buckets/export")


def _scan_all(table, **kwargs):
    """Paginate through an entire DynamoDB table scan and yield every item."""
    resp = _ddb.scan(TableName=table, **kwargs)
    yield from resp.get("Items", [])
    while "LastEvaluatedKey" in resp:
        resp = _ddb.scan(TableName=table, ExclusiveStartKey=resp["LastEvaluatedKey"], **kwargs)
        yield from resp.get("Items", [])


def handler(event, context):
    # ── Reviews table: only count devset reviews, not cornercase test data ──────────────
    sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
    profanity_failed = 0

    for item in _scan_all(
        REVIEWS_TABLE,
        FilterExpression="#src = :devset",
        ExpressionAttributeNames={"#src": "source"},        # "source" is a reserved word
        ExpressionAttributeValues={":devset": {"S": "devset"}},
    ):
        sentiment = item.get("sentiment", {}).get("S", "neutral")
        sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1

        if item.get("isProfane", {}).get("BOOL", False):
            profanity_failed += 1

    # ── Customers table: collect all banned reviewerIDs ──────────────────────────────────
    banned_users = []
    for item in _scan_all(
        CUSTOMERS_TABLE,
        FilterExpression="banned = :t",
        ExpressionAttributeValues={":t": {"BOOL": True}},
    ):
        banned_users.append(item["reviewerID"]["S"])

    result = {
        "sentiment": sentiment_counts,
        "profanityFailed": profanity_failed,
        "bannedUsers": sorted(banned_users),
    }

    _s3.put_object(
        Bucket=EXPORT_BUCKET,
        Key="report.json",
        Body=json.dumps(result, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    return {"statusCode": 200, **result}
