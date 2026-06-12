"""Integration tests for the Stage-1 pipeline.

These are NOT unit tests. They require MiniStack to be running with `bash src/run.sh` already
applied (so all 5 Lambdas, the buckets, and the tables exist). They drop real objects into S3
and assert on what ends up in DynamoDB after the chain runs.

Run from the repo root:
    pytest src/tests/test_integration.py -v
"""
import hashlib
import json
import os
import time

import boto3
import pytest

# MiniStack creds/region must be set BEFORE the boto3 clients are built.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

ENDPOINT = "http://localhost:4566"
s3 = boto3.client("s3", endpoint_url=ENDPOINT)
ssm = boto3.client("ssm", endpoint_url=ENDPOINT)
ddb = boto3.client("dynamodb", endpoint_url=ENDPOINT)
lam = boto3.client("lambda", endpoint_url=ENDPOINT)

LAMBDAS = ("preprocess", "profanity", "sentiment", "aggregate", "report")


@pytest.fixture(autouse=True)
def _wait_for_lambdas():
    """Make sure all 5 Lambdas are deployed and active before each test runs."""
    for fn in LAMBDAS:
        lam.get_waiter("function_active").wait(FunctionName=fn)


def param(name):
    """Read an SSM parameter (e.g. a bucket or table name)."""
    return ssm.get_parameter(Name=name)["Parameter"]["Value"]


def make_review_id(reviewer, asin, t, text, summary):
    """Same id formula as CONTRACT.md / loader.py."""
    digest = hashlib.sha1(((text or "") + (summary or "")).encode("utf-8")).hexdigest()[:8]
    return f"{reviewer}_{asin}_{t}_{digest}"


def drop_review(envelope):
    """Upload one envelope into the ingest bucket -> this STARTS the chain."""
    bucket = param("/dic-a3/buckets/ingest")
    key = f"cornercase/{envelope['reviewId']}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(envelope).encode("utf-8"))


def poll_item(table, key, *, want_attr=None, want_value=None, timeout=40, interval=0.5):
    """Poll DynamoDB GetItem until the item exists (and optionally an attribute reaches a value).

    boto3 has a waiter for "S3 object exists" but NOT for "DynamoDB attribute == value", so we
    poll by hand. The chain is asynchronous (several Lambda hops), so results appear
    *eventually* -- never assert immediately after dropping a review.
    """
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = ddb.get_item(TableName=table, Key=key).get("Item")
        if last is not None:
            if want_attr is None:
                return last
            got = last.get(want_attr)
            # got looks like {"N": "3"} or {"BOOL": True}; compare the inner scalar as a string.
            if got is not None and str(next(iter(got.values()))) == str(want_value):
                return last
        time.sleep(interval)
    raise AssertionError(f"timeout waiting on {table} {key}; last seen: {last}")


# ----------------------------------------------------------------------------------------
# Test 1 -- SMOKE: one review travels the whole chain and lands in the Reviews table.
# Proves: S3 trigger -> L1 -> L2 -> L3 -> L4 -> DynamoDB write all work end to end.
# ----------------------------------------------------------------------------------------
def test_smoke_one_review_reaches_reviews_table():
    reviews_table = param("/dic-a3/tables/reviews")
    rid = make_review_id("SMOKE", "ASIN1", 1, "great product", "love")
    drop_review({
        "reviewId": rid, "reviewerID": "SMOKE", "asin": "ASIN1",
        "summary": "love", "reviewText": "great product",
        "overall": 5.0, "unixReviewTime": 1, "source": "cornercase",
    })

    item = poll_item(reviews_table, {"reviewId": {"S": rid}})
    assert item["reviewId"]["S"] == rid
    assert item["reviewerID"]["S"] == "SMOKE"


# ----------------------------------------------------------------------------------------
# Test 2 -- BAN BOUNDARY: 3 impolite => not banned; 4th => banned; re-deliver => count stable.
# Proves: counting impolite reviews, the strict ">3" ban rule, and idempotency on re-delivery.
# ----------------------------------------------------------------------------------------
def test_ban_after_more_than_three_impolite_reviews():
    customers_table = param("/dic-a3/tables/customers")
    # Unique reviewer per run so repeated test runs never collide with each other.
    reviewer = f"BANTEST_{int(time.time())}"

    def impolite(i):
        rid = make_review_id(reviewer, f"A{i}", i, f"bad text {i}", f"s{i}")
        drop_review({
            "reviewId": rid, "reviewerID": reviewer, "asin": f"A{i}",
            "summary": f"s{i}", "reviewText": f"bad text {i}",
            "overall": 1.0, "unixReviewTime": i,
            "source": "cornercase", "forceProfane": True,
        })
        return rid

    # (1) three impolite reviews -> count reaches 3, must NOT be banned yet
    impolite(1)
    impolite(2)
    impolite(3)
    poll_item(customers_table, {"reviewerID": {"S": reviewer}},
              want_attr="impoliteCount", want_value=3)
    item = ddb.get_item(TableName=customers_table,
                        Key={"reviewerID": {"S": reviewer}})["Item"]
    assert item.get("banned", {"BOOL": False})["BOOL"] is False

    # (2) the fourth impolite review -> banned flips true ("more than 3")
    rid4 = impolite(4)
    poll_item(customers_table, {"reviewerID": {"S": reviewer}},
              want_attr="banned", want_value=True)

    # (3) re-deliver the SAME 4th review -> count must STAY 4 (idempotency gate works)
    drop_review({
        "reviewId": rid4, "reviewerID": reviewer, "asin": "A4",
        "summary": "s4", "reviewText": "bad text 4",
        "overall": 1.0, "unixReviewTime": 4,
        "source": "cornercase", "forceProfane": True,
    })
    time.sleep(3)  # give a (buggy) double-count a chance to happen, so we'd catch it
    item = ddb.get_item(TableName=customers_table,
                        Key={"reviewerID": {"S": reviewer}})["Item"]
    assert int(item["impoliteCount"]["N"]) == 4


# ----------------------------------------------------------------------------------------
# Test 3 -- SENTIMENT: a clearly positive review lands in Reviews with sentiment='positive';
# a clearly negative one lands with sentiment='negative'.
# Proves: VADER scoring, overall blending, and threshold mapping all work end to end.
# ----------------------------------------------------------------------------------------
def test_sentiment_classification():
    reviews_table = param("/dic-a3/tables/reviews")

    pos_rid = make_review_id("SENT", "APOS", 200, "excellent wonderful love perfect", "outstanding")
    drop_review({
        "reviewId": pos_rid, "reviewerID": "SENT", "asin": "APOS",
        "summary": "outstanding", "reviewText": "excellent wonderful love perfect",
        "overall": 5.0, "unixReviewTime": 200, "source": "cornercase",
    })

    neg_rid = make_review_id("SENT", "ANEG", 201, "terrible awful waste horrible", "worst ever")
    drop_review({
        "reviewId": neg_rid, "reviewerID": "SENT", "asin": "ANEG",
        "summary": "worst ever", "reviewText": "terrible awful waste horrible",
        "overall": 1.0, "unixReviewTime": 201, "source": "cornercase",
    })

    pos_item = poll_item(reviews_table, {"reviewId": {"S": pos_rid}},
                         want_attr="sentiment", want_value="positive")
    assert pos_item["sentiment"]["S"] == "positive"

    neg_item = poll_item(reviews_table, {"reviewId": {"S": neg_rid}},
                         want_attr="sentiment", want_value="negative")
    assert neg_item["sentiment"]["S"] == "negative"
