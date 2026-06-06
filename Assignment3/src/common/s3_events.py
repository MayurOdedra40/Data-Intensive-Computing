"""Helpers for reading S3 'ObjectCreated' events and moving the envelope between buckets.

Shared by L1/L2/L3 (and L4). Lifted from the tutorial's resize handler so we reuse a
pattern already proven to work on MiniStack.
"""
import json
import os
from urllib.parse import unquote_plus

import boto3

_ENDPOINT = "http://localhost:4566" if os.getenv("STAGE") == "local" else None
_s3 = boto3.client("s3", endpoint_url=_ENDPOINT)


def iter_s3_records(event):
    """Yield the raw records from a Lambda event, tolerating MiniStack's quirks.

    Copied from assignment_3_tutorial/lambdas/resize/handler.py. The important bit is the
    's3:TestEvent' check: the FIRST thing S3 sends a freshly-wired Lambda is a handshake
    event that has NO 'Records'. If we don't skip it, the handler crashes on a missing key.
    """
    if isinstance(event, dict):
        if event.get("Event") == "s3:TestEvent":
            return []
        if isinstance(event.get("Records"), list):
            return event["Records"]
        if "s3" in event:
            return [event]
    if isinstance(event, list):
        return event
    raise ValueError(f"unsupported S3 event payload: {event!r}")


def parse_records(event):
    """Yield (bucket, key) for each real S3 ObjectCreated record.

    Skips the s3:TestEvent handshake AND any record that is not an S3 record (for example a
    DynamoDB *stream* record, which has no "s3" key). That second guard is what makes L4 safe
    if the optional DynamoDB-stream trigger is enabled: stream events simply produce nothing
    here, so they can never cause a double count.
    """
    for record in iter_s3_records(event):
        if "s3" not in record:
            continue
        bucket = record["s3"]["bucket"]["name"]
        # S3 url-encodes keys in events ('my%20file' -> 'my file'); undo that.
        key = unquote_plus(record["s3"]["object"]["key"])
        yield bucket, key


def read_envelope(bucket, key):
    """Download one JSON object from S3 and parse it into a Python dict."""
    obj = _s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())


def write_envelope(bucket, key, envelope):
    """Upload a Python dict to S3 as a JSON object. This is what triggers the NEXT Lambda."""
    _s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(envelope).encode("utf-8"),
        ContentType="application/json",
    )
