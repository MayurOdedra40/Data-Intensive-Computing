"""L4 aggregate-and-ban  (REAL logic -- the heart of Stage 1).

Triggered by a new object in the `scored` bucket. It:
  1. Writes the review into the `Reviews` table, but ONLY if it's new (the idempotency gate).
  2. If the review is new AND profane, atomically bumps the customer's impolite counter.
  3. Bans the customer when that counter goes above the threshold (strictly > 3 => the 4th).

We use the LOW-LEVEL dynamodb client (boto3.client, not boto3.resource) so the exact
attribute types ({"S": ...}, {"N": ...}, {"BOOL": ...}) and the ConditionExpression are
explicit. See CONTRACT.md section 5 for the full reasoning.
"""
import os

import boto3
from botocore.exceptions import ClientError

import config
import s3_events

_ENDPOINT = "http://localhost:4566" if os.getenv("STAGE") == "local" else None
_ddb = boto3.client("dynamodb", endpoint_url=_ENDPOINT)

# Read config once at import (warm reuse). Names/threshold come from SSM, never hardcoded.
REVIEWS_TABLE = config.get("/dic-a3/tables/reviews")
CUSTOMERS_TABLE = config.get("/dic-a3/tables/customers")
BAN_THRESHOLD = config.get_int("/dic-a3/config/ban-threshold")


def _put_review_once(envelope) -> bool:
    """Insert into Reviews only if this reviewId is new.

    Returns True if THIS call created the item, False if it already existed (a re-delivery).
    The ConditionExpression is what makes the whole pipeline safe to run twice on one review.
    """
    item = {
        "reviewId":   {"S": envelope["reviewId"]},
        "reviewerID": {"S": envelope["reviewerID"]},
        "asin":       {"S": str(envelope.get("asin", ""))},
        "source":     {"S": envelope.get("source", "devset")},
        "isProfane":  {"BOOL": bool(envelope.get("isProfane", False))},
        "sentiment":  {"S": envelope.get("sentiment", "neutral")},
    }
    try:
        _ddb.put_item(
            TableName=REVIEWS_TABLE,
            Item=item,
            ConditionExpression="attribute_not_exists(reviewId)",
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False  # already processed this exact reviewId -> do NOT count it again
        raise


def _increment_and_maybe_ban(reviewer_id):
    """Atomically +1 the customer's impolite counter, then ban if it exceeds the threshold."""
    resp = _ddb.update_item(
        TableName=CUSTOMERS_TABLE,
        Key={"reviewerID": {"S": reviewer_id}},
        UpdateExpression="ADD impoliteCount :one",
        ExpressionAttributeValues={":one": {"N": "1"}},
        ReturnValues="UPDATED_NEW",  # return the count AFTER the add, in the same call
    )
    new_count = int(resp["Attributes"]["impoliteCount"]["N"])
    if new_count > BAN_THRESHOLD:  # strictly greater -> the 4th impolite review bans
        _ddb.update_item(
            TableName=CUSTOMERS_TABLE,
            Key={"reviewerID": {"S": reviewer_id}},
            UpdateExpression="SET banned = :t",
            ExpressionAttributeValues={":t": {"BOOL": True}},
        )
    return new_count


def handler(event, context):
    # parse_records yields nothing for non-S3 events (e.g. a DynamoDB stream event), so the
    # optional stream trigger can never double-count: only S3 deliveries drive writes here.
    for bucket, key in s3_events.parse_records(event):
        envelope = s3_events.read_envelope(bucket, key)
        is_new = _put_review_once(envelope)
        if is_new and envelope.get("isProfane"):
            _increment_and_maybe_ban(envelope["reviewerID"])
    return {"statusCode": 200}
