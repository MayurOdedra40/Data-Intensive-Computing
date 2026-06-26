"""generate_results.py -- run after the full devset has been processed.

Queries DynamoDB directly (no Lambda needed) and writes results.json next to
this script. Also uploads it to the results-export S3 bucket.

Usage:
    python src/generate_results.py
    python src/generate_results.py --endpoint http://localhost:4566   # explicit endpoint
    python src/generate_results.py --no-upload                        # skip S3 upload
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import boto3

# ── AWS / MiniStack connection ────────────────────────────────────────────────────────────

def _make_clients(endpoint: str):
    kwargs = dict(
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    return boto3.client("dynamodb", **kwargs), boto3.client("ssm", **kwargs), boto3.client("s3", **kwargs)


# ── DynamoDB / SSM helpers ──────────────────────────────────────────────────────────────────

def _param(ssm, name: str) -> str:
    """Read one SSM String parameter value."""
    return ssm.get_parameter(Name=name)["Parameter"]["Value"]


def _scan_all(ddb, table: str, **kwargs):
    """Paginate through all items in a DynamoDB table scan."""
    resp = ddb.scan(TableName=table, **kwargs)
    items = list(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = ddb.scan(
            TableName=table,
            ExclusiveStartKey=resp["LastEvaluatedKey"],
            **kwargs,
        )
        items.extend(resp.get("Items", []))
    return items


# ── Core logic ────────────────────────────────────────────────────────────────────────────

def compute_results(ddb, ssm) -> dict:
    """Read DynamoDB and return the three required result numbers."""

    def param(name):
        return ssm.get_parameter(Name=name)["Parameter"]["Value"]

    reviews_table   = param("/dic-a3/tables/reviews")
    customers_table = param("/dic-a3/tables/customers")

    # ── Sentiment counts + profanity failures (devset only) ──────────────────────────────
    print(f"Scanning '{reviews_table}' table (devset reviews only) ...", flush=True)

    sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
    profanity_failed = 0

    devset_items = _scan_all(
        ddb,
        reviews_table,
        FilterExpression="#src = :devset",
        ExpressionAttributeNames={"#src": "source"},   # "source" is a DynamoDB reserved word
        ExpressionAttributeValues={":devset": {"S": "devset"}},
    )

    for item in devset_items:
        sentiment = item.get("sentiment", {}).get("S", "neutral")
        sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
        if item.get("isProfane", {}).get("BOOL", False):
            profanity_failed += 1

    total_reviews = sum(sentiment_counts.values())

    # ── Banned users ─────────────────────────────────────────────────────────────────────
    print(f"Scanning '{customers_table}' table for banned users ...", flush=True)

    banned_items = _scan_all(
        ddb,
        customers_table,
        FilterExpression="banned = :t",
        ExpressionAttributeValues={":t": {"BOOL": True}},
    )
    banned_users = sorted(item["reviewerID"]["S"] for item in banned_items)

    return {
        "totalDevsetReviews": total_reviews,
        "sentiment": sentiment_counts,
        "profanityFailed": profanity_failed,
        "bannedUsers": banned_users,
    }


def compute_state(ddb, ssm) -> dict:
    """Read DynamoDB and return the FULL resumable state (superset of compute_results).

    This is the single source the checkpointer snapshots to disk and finalize_results.py reads
    back. Unlike compute_results it ALSO returns the per-customer impolite counts (so we can "see
    when it reaches 3") and the configured ban threshold, so the final results can be recomputed
    purely from the saved file with no DynamoDB access. It is intentionally quiet -- the checkpointer
    calls it every ~30s and does its own concise logging.

    Shape (also the on-disk intermediate_results.json schema, minus `updatedAt`):
        {
          "processedCount": int,                      # devset rows in the Reviews table
          "sentiment": {"positive","neutral","negative": int},
          "profanityFailed": int,
          "banThreshold": int,
          "userProfaneCounts": {reviewerID: int},     # Customers.impoliteCount per user
          "bannedUsers": [reviewerID, ...],           # users with count > banThreshold
        }
    """
    reviews_table   = _param(ssm, "/dic-a3/tables/reviews")
    customers_table = _param(ssm, "/dic-a3/tables/customers")
    ban_threshold   = int(_param(ssm, "/dic-a3/config/ban-threshold"))

    # ── Reviews table: sentiment counts + profanity failures + the reviewId set (devset only) ──
    # processed_ids is what makes runs disjoint: the next run skips these reviewIds, so segments
    # never overlap and can simply be added together (see checkpoints.py).
    sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
    profanity_failed = 0
    processed_ids = []
    for item in _scan_all(
        ddb,
        reviews_table,
        FilterExpression="#src = :devset",
        ExpressionAttributeNames={"#src": "source"},   # "source" is a DynamoDB reserved word
        ExpressionAttributeValues={":devset": {"S": "devset"}},
    ):
        sentiment = item.get("sentiment", {}).get("S", "neutral")
        sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
        if item.get("isProfane", {}).get("BOOL", False):
            profanity_failed += 1
        rid = item.get("reviewId", {}).get("S")
        if rid:
            processed_ids.append(rid)

    # ── Customers table: per-user impolite counts (the ban ledger) ────────────────────────────
    # L4 only writes a Customers row when a review is profane, so every row has impoliteCount >= 1.
    user_profane_counts = {}
    for item in _scan_all(ddb, customers_table):
        reviewer_id = item["reviewerID"]["S"]
        count = int(item.get("impoliteCount", {}).get("N", "0"))
        user_profane_counts[reviewer_id] = count

    banned_users = sorted(
        uid for uid, count in user_profane_counts.items() if count > ban_threshold
    )

    return {
        "processedCount": sum(sentiment_counts.values()),
        "sentiment": sentiment_counts,
        "profanityFailed": profanity_failed,
        "banThreshold": ban_threshold,
        "userProfaneCounts": user_profane_counts,
        "bannedUsers": banned_users,
        "processedReviewIds": processed_ids,
    }


def print_summary(results: dict) -> None:
    s = results["sentiment"]
    total = results["totalDevsetReviews"]
    print()
    print("=" * 50)
    print("  RESULTS  (devset reviews only)")
    print("=" * 50)
    print(f"  Total reviews processed : {total}")
    print(f"  Positive                : {s['positive']}")
    print(f"  Neutral                 : {s['neutral']}")
    print(f"  Negative                : {s['negative']}")
    print(f"  Failed profanity check  : {results['profanityFailed']}")
    banned = results["bannedUsers"]
    if banned:
        print(f"  Banned users ({len(banned)})       :")
        for uid in banned:
            print(f"    - {uid}")
    else:
        print("  Banned users            : none")
    print("=" * 50)
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Assignment-3 results from DynamoDB.")
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        help="MiniStack / LocalStack endpoint URL (default: http://localhost:4566)",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip uploading results.json to the results-export S3 bucket",
    )
    parser.add_argument(
        "--out",
        default=str(Path(__file__).parent.parent / "results.json"),
        help="Local path to write the results JSON file",
    )
    args = parser.parse_args()

    ddb, ssm, s3 = _make_clients(args.endpoint)

    try:
        results = compute_results(ddb, ssm)
    except Exception as exc:
        print(f"ERROR: could not read from MiniStack — is it running? ({exc})", file=sys.stderr)
        return 1

    # ── Write local file ──────────────────────────────────────────────────────────────────
    out_path = Path(args.out)
    out_path.write_text(json.dumps(results, indent=2))
    print(f"Results written to: {out_path}")

    print_summary(results)

    # ── Upload to S3 (optional) ───────────────────────────────────────────────────────────
    if not args.no_upload:
        try:
            export_bucket = ssm.get_parameter(Name="/dic-a3/buckets/export")["Parameter"]["Value"]
            s3.put_object(
                Bucket=export_bucket,
                Key="report.json",
                Body=out_path.read_bytes(),
                ContentType="application/json",
            )
            print(f"Also uploaded to s3://{export_bucket}/report.json")
        except Exception as exc:
            print(f"Warning: S3 upload failed (results.json is still saved locally): {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
