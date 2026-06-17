"""Full-devset STRESS test for the Stage-1 pipeline.

This is the heavy one: it pushes the ENTIRE reviews_devset.json (78,829 rows) through the
live pipeline (4 Lambda hops each => ~315k Lambda invocations) and asserts the system stays
correct under load -- no data loss, no duplicates, every review classified, and the report
numbers reconcile with a direct table scan.

Because it can run for many minutes and mutates the real `devset` data in DynamoDB, it is
OPT-IN: it is skipped unless you set RUN_STRESS=1.

    # full devset (default), from src/:
    RUN_STRESS=1 pytest tests/test_stress.py -v -s

    # quick mechanism check on a slice:
    RUN_STRESS=1 STRESS_COUNT=300 STRESS_BATCH_DELAY=3 pytest tests/test_stress.py -v -s

Requirements: MiniStack running with `bash src/run.sh` already applied, and the dataset on
disk (data/reviews_devset.json). Tune with the env vars documented below.

Env knobs:
    RUN_STRESS=1            enable the test (otherwise skipped)
    STRESS_COUNT=78829     how many reviews to push (default = full devset)
    STRESS_BATCH_SIZE=200  upload this many, then pause (mirrors loader / run_cluster.sh)
    STRESS_BATCH_DELAY=15  seconds between batches so the chain can drain (avoids OOM)
    STRESS_STALL=240       fail if DynamoDB count makes NO progress for this many seconds
    STRESS_TIMEOUT=...     absolute ceiling in seconds (default scales with STRESS_COUNT)
    STRESS_DATA=<path>     dataset path (default: ../data/reviews_devset.json)

Locally the chain drains at only a few reviews/sec, so the full devset takes a few HOURS --
that is expected. The real guard is STRESS_STALL (a crashed review stops progress and fails
fast); the absolute ceiling is just a backstop and defaults large.
"""
import json
import os
import sys
import time
from pathlib import Path

import boto3
import pytest

# MiniStack creds/region must be set BEFORE the boto3 clients are built.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

# Make the project modules (loader.py, generate_results.py) importable regardless of cwd.
_SRC = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_SRC))

import loader              # noqa: E402  (single owner of reviewId/source)
import generate_results   # noqa: E402

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
ssm = boto3.client("ssm", endpoint_url=ENDPOINT)
ddb = boto3.client("dynamodb", endpoint_url=ENDPOINT)
lam = boto3.client("lambda", endpoint_url=ENDPOINT)

# ── knobs ─────────────────────────────────────────────────────────────────────────────────
RUN_STRESS  = os.environ.get("RUN_STRESS") == "1"
STRESS_COUNT = int(os.environ.get("STRESS_COUNT", "78829"))
BATCH_SIZE   = int(os.environ.get("STRESS_BATCH_SIZE", "200"))
BATCH_DELAY  = float(os.environ.get("STRESS_BATCH_DELAY", "15"))
STALL        = float(os.environ.get("STRESS_STALL", "240"))
# Absolute backstop. Local drain is only a few reviews/sec, so scale generously with the
# review count; the real guard against a stuck pipeline is STALL, not this ceiling.
TIMEOUT      = float(os.environ.get("STRESS_TIMEOUT", str(max(1800, STRESS_COUNT * 2))))
DATA = os.environ.get("STRESS_DATA", str(_SRC.parent / "data" / "reviews_devset.json"))

pytestmark = pytest.mark.skipif(
    not RUN_STRESS, reason="opt-in: set RUN_STRESS=1 to run the full-devset stress test")

LABELS = {"positive", "neutral", "negative"}


def param(name):
    return ssm.get_parameter(Name=name)["Parameter"]["Value"]


def expected_review_ids(path, limit):
    """Compute the set of unique reviewIds the loader WILL produce for the first `limit` rows.

    Mirrors loader.load's line handling (skip blanks, stop at `limit`). Exact-duplicate rows
    collapse to the same reviewId (the sha1 content suffix), so the unique-id count can be
    slightly below the row count -- that collapsed set is the real no-data-loss target.
    """
    ids, seen = set(), 0
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            if seen >= limit:
                break
            line = raw.strip()
            if not line:
                continue
            ids.add(loader.make_review_id(json.loads(line)))
            seen += 1
    return ids, seen


def devset_count(table):
    """COUNT of source=='devset' rows currently in the Reviews table."""
    total = 0
    kwargs = dict(
        TableName=table, Select="COUNT",
        FilterExpression="#src = :d",
        ExpressionAttributeNames={"#src": "source"},
        ExpressionAttributeValues={":d": {"S": "devset"}},
    )
    resp = ddb.scan(**kwargs)
    total += resp["Count"]
    while "LastEvaluatedKey" in resp:
        resp = ddb.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **kwargs)
        total += resp["Count"]
    return total


def scan_devset_rows(table):
    """Scan every devset review and return (reviewIds set, sentiment Counter, profane count)."""
    ids = set()
    sentiments = {"positive": 0, "neutral": 0, "negative": 0}
    bad_labels = []
    profane = 0
    kwargs = dict(
        TableName=table,
        ProjectionExpression="reviewId, sentiment, isProfane",
        FilterExpression="#src = :d",
        ExpressionAttributeNames={"#src": "source"},
        ExpressionAttributeValues={":d": {"S": "devset"}},
    )
    resp = ddb.scan(**kwargs)
    pages = [resp]
    while "LastEvaluatedKey" in resp:
        resp = ddb.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **kwargs)
        pages.append(resp)
    for page in pages:
        for it in page.get("Items", []):
            ids.add(it["reviewId"]["S"])
            label = it.get("sentiment", {}).get("S", "neutral")
            if label in sentiments:
                sentiments[label] += 1
            else:
                bad_labels.append(label)
            if it.get("isProfane", {}).get("BOOL", False):
                profane += 1
    return ids, sentiments, profane, bad_labels


def test_full_devset_stress():
    assert os.path.exists(DATA), f"dataset not found: {DATA} (set STRESS_DATA)"

    reviews_table = param("/dic-a3/tables/reviews")

    # 1) Work out exactly which unique reviewIds should exist when we're done.
    expected_ids, rows_read = expected_review_ids(DATA, STRESS_COUNT)
    target = len(expected_ids)
    print(f"\n[stress] rows read={rows_read}  unique reviewIds={target}  "
          f"(dup rows collapsed={rows_read - target})", flush=True)
    assert target > 0, "no reviews to load"

    baseline = devset_count(reviews_table)
    print(f"[stress] devset rows already in table before load: {baseline}", flush=True)

    # 2) Push the whole devset through the pipeline (batched, like run_cluster.sh).
    t0 = time.time()
    loader.load(DATA, STRESS_COUNT, BATCH_SIZE, BATCH_DELAY)
    print(f"[stress] upload finished in {time.time() - t0:.0f}s; waiting for drain ...",
          flush=True)

    # 3) Poll until every expected review has landed in DynamoDB. Fail FAST if progress
    #    stalls (a review that crashes a stage never arrives, so `done` stops climbing);
    #    the absolute deadline is only a backstop.
    deadline = time.time() + TIMEOUT
    interval = 15 if target > 2000 else 3
    done = devset_count(reviews_table)
    last_done = done
    last_progress = time.time()
    while done < target and time.time() < deadline:
        time.sleep(interval)
        done = devset_count(reviews_table)
        now = time.time()
        if done > last_done:
            last_done, last_progress = done, now
        elif now - last_progress > STALL:
            pytest.fail(
                f"pipeline STALLED at {done}/{target} for >{STALL:.0f}s -- a review likely "
                f"crashed a stage (check Lambda logs). {target - done} never arrived.")
        print(f"[stress] in DynamoDB={done}/{target}  ({now - t0:.0f}s elapsed)", flush=True)

    assert done >= target, (
        f"hit the absolute ceiling: only {done}/{target} devset reviews reached DynamoDB "
        f"within {TIMEOUT:.0f}s. Raise STRESS_TIMEOUT if the pipeline was still progressing.")

    # 4) Correctness under load: scan everything and check the invariants.
    actual_ids, sentiments, profane, bad_labels = scan_devset_rows(reviews_table)

    missing = expected_ids - actual_ids
    assert not missing, f"{len(missing)} expected reviewIds never arrived, e.g. {list(missing)[:5]}"

    # No duplicates: distinct reviewIds == COUNT (idempotency gate held under load).
    assert len(actual_ids) == done, \
        f"duplicate rows: COUNT={done} but only {len(actual_ids)} distinct reviewIds"

    # Every review classified into exactly the three allowed labels.
    assert not bad_labels, f"unexpected sentiment labels: {set(bad_labels)}"
    total_classified = sum(sentiments.values())
    assert total_classified == len(actual_ids), \
        f"sentiment counts sum {total_classified} != row count {len(actual_ids)}"

    assert 0 <= profane <= len(actual_ids), f"impossible profanity count {profane}"

    # 5) The report path must reconcile with the direct scan (single source of truth).
    _ddb, _ssm, _ = generate_results._make_clients(ENDPOINT)
    report = generate_results.compute_results(_ddb, _ssm)
    assert report["totalDevsetReviews"] == total_classified, \
        f"report total {report['totalDevsetReviews']} != scan total {total_classified}"
    assert report["sentiment"] == sentiments, \
        f"report sentiment {report['sentiment']} != scan {sentiments}"
    assert report["profanityFailed"] == profane, \
        f"report profanityFailed {report['profanityFailed']} != scan {profane}"

    print(
        f"\n[stress] OK  total={total_classified}  "
        f"pos={sentiments['positive']} neu={sentiments['neutral']} neg={sentiments['negative']}  "
        f"profane={profane}  banned={len(report['bannedUsers'])}  "
        f"in {time.time() - t0:.0f}s", flush=True)
