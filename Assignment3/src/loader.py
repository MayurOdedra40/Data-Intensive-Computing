"""loader.py -- push reviews into the pipeline, with optional batching.

Uploads review envelopes to the reviews-ingest S3 bucket one at a time (memory-safe),
but in configurable batch sizes with a pause between batches so MiniStack and the
Lambda chain have time to drain before the next wave arrives.

This is the SINGLE owner of `reviewId` and `source` (see CONTRACT.md).

Usage:
    python src/loader.py data/reviews_devset.json            # load all, no batching
    python src/loader.py data/reviews_devset.json 1000       # load first 1000
    python src/loader.py data/reviews_devset.json --batch-size 200 --batch-delay 30
    python src/loader.py data/reviews_devset.json 1000 --batch-size 100 --batch-delay 20
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time

import boto3

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
s3  = boto3.client("s3",  endpoint_url=ENDPOINT)
ssm = boto3.client("ssm", endpoint_url=ENDPOINT)


def make_review_id(rec: dict) -> str:
    text = (rec.get("reviewText") or "") + (rec.get("summary") or "")
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]
    return f'{rec["reviewerID"]}_{rec["asin"]}_{rec["unixReviewTime"]}_{digest}'


def to_envelope(rec: dict, source: str = "devset") -> tuple[str, dict]:
    review_id = make_review_id(rec)
    envelope = {
        "reviewId":       review_id,
        "reviewerID":     rec["reviewerID"],
        "asin":           rec["asin"],
        "summary":        rec.get("summary", ""),
        "reviewText":     rec.get("reviewText", ""),
        "overall":        rec.get("overall"),
        "unixReviewTime": rec.get("unixReviewTime"),
        "source":         source,
    }
    return review_id, envelope


def _upload(bucket: str, review_id: str, envelope: dict) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=f"reviews/{review_id}.json",
        Body=json.dumps(envelope).encode("utf-8"),
        ContentType="application/json",
    )


def load(path: str, limit: int, batch_size: int, batch_delay: float) -> None:
    bucket = ssm.get_parameter(Name="/dic-a3/buckets/ingest")["Parameter"]["Value"]

    total_loaded = 0
    batch_num    = 0
    batch_buf: list[tuple[str, dict]] = []

    def _flush_batch() -> None:
        nonlocal total_loaded, batch_num
        if not batch_buf:
            return
        batch_num += 1
        print(f"\n  -- Batch {batch_num}: uploading {len(batch_buf)} reviews "
              f"(total so far: {total_loaded + len(batch_buf)}) --", flush=True)
        for rid, env in batch_buf:
            _upload(bucket, rid, env)
            total_loaded += 1
        batch_buf.clear()

        if batch_delay > 0 and total_loaded < limit:
            print(f"  -- Pausing {batch_delay}s for the pipeline to drain ...", flush=True)
            time.sleep(batch_delay)

    with open(path, encoding="utf-8") as fh:
        for raw_line in fh:
            if total_loaded + len(batch_buf) >= limit:
                break
            line = raw_line.strip()
            if not line:
                continue
            rec = json.loads(line)
            review_id, envelope = to_envelope(rec)
            batch_buf.append((review_id, envelope))

            if len(batch_buf) >= batch_size:
                _flush_batch()

    _flush_batch()   # upload the last (possibly partial) batch
    print(f"\nDone — {total_loaded} review(s) uploaded to '{bucket}'.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Load reviews into the pipeline.")
    parser.add_argument("dataset",    help="Path to reviews JSON-lines file")
    parser.add_argument("limit",      nargs="?", type=int, default=78829,
                        help="Max reviews to load (default: 78829 = full devset)")
    parser.add_argument("--batch-size",  type=int,   default=200,
                        help="Upload this many reviews, then pause (default: 200). "
                             "Set to 0 to disable batching.")
    parser.add_argument("--batch-delay", type=float, default=30.0,
                        help="Seconds to sleep between batches (default: 30)")
    args = parser.parse_args()

    batch_size = args.batch_size if args.batch_size > 0 else args.limit
    load(args.dataset, args.limit, batch_size, args.batch_delay)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
