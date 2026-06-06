"""loader.py -- push reviews into the pipeline (STAGE-1 stub).

This is how the chain gets STARTED: it uploads review JSON objects into the `reviews-ingest`
bucket, which fires the s3:ObjectCreated notification that triggers L1, and so on.

The loader is the SINGLE owner of `reviewId` and `source` (see CONTRACT.md). Every downstream
Lambda just reads those fields.

Stage 1 only needs to push one (or a few) reviews so you can watch the chain work. The full
resumable loader that streams all 78,829 reviews is Stage 5.

Usage (from the repo root, with MiniStack running and run.sh applied):
    python src/loader.py data/reviews_devset.json 1      # load the first 1 review
    python src/loader.py data/reviews_devset.json 50     # load the first 50 reviews
"""
import hashlib
import json
import os
import sys

import boto3

# Dummy credentials + region so boto3 is happy talking to MiniStack. (Same values run.sh uses.)
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
s3 = boto3.client("s3", endpoint_url=ENDPOINT)
ssm = boto3.client("ssm", endpoint_url=ENDPOINT)


def make_review_id(rec):
    """Build the unique review id (must match CONTRACT.md exactly)."""
    text = (rec.get("reviewText") or "") + (rec.get("summary") or "")
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]
    return f'{rec["reviewerID"]}_{rec["asin"]}_{rec["unixReviewTime"]}_{digest}'


def to_envelope(rec, source="devset"):
    """Turn a raw dataset record into the envelope we drop into the ingest bucket."""
    review_id = make_review_id(rec)
    envelope = {
        "reviewId": review_id,
        "reviewerID": rec["reviewerID"],
        "asin": rec["asin"],
        "summary": rec.get("summary", ""),
        "reviewText": rec.get("reviewText", ""),
        "overall": rec.get("overall"),
        "unixReviewTime": rec.get("unixReviewTime"),
        "source": source,
    }
    return review_id, envelope


def main(path, limit):
    bucket = ssm.get_parameter(Name="/dic-a3/buckets/ingest")["Parameter"]["Value"]
    loaded = 0
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if loaded >= limit:
                break
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            review_id, envelope = to_envelope(rec)
            # Key prefix "reviews/" marks devset rows (human-readable only; `source` is what counts).
            s3.put_object(
                Bucket=bucket,
                Key=f"reviews/{review_id}.json",
                Body=json.dumps(envelope).encode("utf-8"),
                ContentType="application/json",
            )
            loaded += 1
            print(f"loaded {loaded}: {review_id}")
    print(f"done -- {loaded} review(s) dropped into '{bucket}'")


if __name__ == "__main__":
    dataset = sys.argv[1] if len(sys.argv) > 1 else "data/reviews_devset.json"
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    main(dataset, count)
