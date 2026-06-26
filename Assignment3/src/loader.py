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
    """Stable, collision-free id derived from the ENTIRE review object (every field).

    The old id was built from only reviewerID + asin + unixReviewTime + sha1(reviewText+summary),
    so two devset rows that differ ONLY in another field (e.g. `category`) produced the SAME id and
    collided in the Reviews table. Hashing the whole record fixes that.

    - `json.dumps(..., sort_keys=True)` is a canonical serialization: same fields -> same string
      regardless of key order, so the id is deterministic across runs.
    - The full SHA-256 hex digest (64 chars) makes accidental collisions impossible in practice.
    - Two rows now share an id ONLY if they are byte-for-byte identical (a genuine duplicate row),
      which is exactly when the idempotency gate SHOULD treat them as one review.
    """
    canonical = json.dumps(rec, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def to_envelope(rec: dict, source: str = "devset", review_id: str | None = None) -> tuple[str, dict]:
    review_id = review_id or make_review_id(rec)
    envelope = {
        "reviewId":       review_id,
        "reviewerID":     rec["reviewerID"],
        "asin":           rec["asin"],
        "summary":        rec.get("summary", ""),
        "reviewText":     rec.get("reviewText", ""),
        # Default a missing/null star rating to neutral (3.0). A raw null here would otherwise
        # travel down the chain and crash L3 sentiment on float(None). The real devset always
        # has `overall`, so this only guards malformed input.
        "overall":        rec["overall"] if rec.get("overall") is not None else 3.0,
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


def load(path: str, limit: int, batch_size: int, batch_delay: float,
         skip: int = 0, skip_ids: set | None = None) -> None:
    bucket = ssm.get_parameter(Name="/dic-a3/buckets/ingest")["Parameter"]["Value"]
    skip_ids = skip_ids or set()

    total_loaded = 0
    batch_num    = 0
    batch_buf: list[tuple[str, dict]] = []
    skipped = 0
    skipped_by_id = 0

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
            if skipped < skip:
                skipped += 1
                continue
            rec = json.loads(line)
            review_id = make_review_id(rec)
            # Skip reviews already processed by an earlier run (segment). This is what keeps runs
            # disjoint so their segment files can be added together (see checkpoints.py).
            if review_id in skip_ids:
                skipped_by_id += 1
                continue
            _, envelope = to_envelope(rec, review_id=review_id)
            batch_buf.append((review_id, envelope))

            if len(batch_buf) >= batch_size:
                _flush_batch()

    _flush_batch()   # upload the last (possibly partial) batch
    if skipped_by_id:
        print(f"  (skipped {skipped_by_id} review(s) already processed by earlier runs)")
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
    parser.add_argument("--skip", type=int, default=0,
                        help="Skip the first N reviews by position (default: 0)")
    parser.add_argument("--skip-ids-file", default=None,
                        help="Path to a newline-delimited list of reviewIds to skip "
                             "(reviews already processed by earlier runs). Used for resume.")
    args = parser.parse_args()

    skip_ids: set = set()
    if args.skip_ids_file and os.path.exists(args.skip_ids_file):
        with open(args.skip_ids_file, encoding="utf-8") as fh:
            skip_ids = {ln.strip() for ln in fh if ln.strip()}
        print(f"Loaded {len(skip_ids)} already-processed reviewId(s) to skip.")

    batch_size = args.batch_size if args.batch_size > 0 else args.limit
    load(args.dataset, args.limit, batch_size, args.batch_delay, args.skip, skip_ids)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
