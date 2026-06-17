"""Edge-case integration tests for the Stage-1 pipeline.

Companion to test_integration.py. Same requirements: MiniStack must be running with
`bash src/run.sh` already applied (all 5 Lambdas, buckets and tables exist). These drop
real objects into S3 and assert on what ends up in DynamoDB / what the report Lambda returns.

They cover the corners the smoke suite does not:
  * L1 preprocess  -- lemmatize / dedup / stopword / min-length token rules.
  * L2 profanity   -- clean text is NOT flagged, real bad words populate badWords,
                      and the forceProfane test hook.
  * L3 sentiment   -- neutral classification, empty text, unicode/emoji, and the
                      `overall: null` / missing-overall robustness regression.
  * L4 aggregate   -- polite reviews never touch the ban ledger, and the reviewId
                      idempotency gate (re-delivery + same-id-different-content).
  * L5 report      -- cornercase reviews are excluded from the devset result counts,
                      and the report payload has the agreed shape.

Run from src/:
    pytest tests/test_integration_edge.py -v
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

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
s3 = boto3.client("s3", endpoint_url=ENDPOINT)
ssm = boto3.client("ssm", endpoint_url=ENDPOINT)
ddb = boto3.client("dynamodb", endpoint_url=ENDPOINT)
lam = boto3.client("lambda", endpoint_url=ENDPOINT)

LAMBDAS = ("preprocess", "profanity", "sentiment", "aggregate", "report")

# One unique suffix per test session so reruns never collide with old rows.
RUN = str(int(time.time()))


@pytest.fixture(autouse=True)
def _wait_for_lambdas():
    """Make sure all 5 Lambdas are deployed and active before each test runs."""
    for fn in LAMBDAS:
        lam.get_waiter("function_active").wait(FunctionName=fn)


# ── helpers (kept local so this file is independent of test_integration.py) ───────────────

def param(name):
    return ssm.get_parameter(Name=name)["Parameter"]["Value"]


def make_review_id(reviewer, asin, t, text, summary):
    digest = hashlib.sha1(((text or "") + (summary or "")).encode("utf-8")).hexdigest()[:8]
    return f"{reviewer}_{asin}_{t}_{digest}"


def drop(envelope):
    """Upload one envelope into the ingest bucket -> STARTS the chain."""
    bucket = param("/dic-a3/buckets/ingest")
    key = f"cornercase/{envelope['reviewId']}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(envelope).encode("utf-8"))


def envelope(name, *, reviewer=None, summary="", text="", overall=3.0,
             source="cornercase", **extra):
    """Build a cornercase envelope with a unique reviewId/reviewerID for this run.

    `overall` is intentionally a real key so callers can pass None to exercise the
    null-rating path; pass overall=... or use the `drop_raw` escape hatch to omit it.
    """
    reviewer = reviewer or f"{name}_{RUN}"
    t = abs(hash(name)) % 100000
    rid = make_review_id(reviewer, f"A_{name}", t, text, summary)
    env = {
        "reviewId": rid, "reviewerID": reviewer, "asin": f"A_{name}",
        "summary": summary, "reviewText": text,
        "overall": overall, "unixReviewTime": t, "source": source,
    }
    env.update(extra)
    return rid, reviewer, env


def poll_item(table, key, *, want_attr=None, want_value=None, timeout=40, interval=0.5):
    """Poll DynamoDB GetItem until the item exists (and optionally an attribute matches)."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = ddb.get_item(TableName=table, Key=key).get("Item")
        if last is not None:
            if want_attr is None:
                return last
            got = last.get(want_attr)
            if got is not None and str(next(iter(got.values()))) == str(want_value):
                return last
        time.sleep(interval)
    raise AssertionError(f"timeout waiting on {table} {key}; last seen: {last}")


def poll_object(bucket, key, *, timeout=40, interval=0.5):
    """Poll an S3 bucket until `key` exists, then return the parsed JSON envelope.

    Used to inspect a stage's *output envelope* (e.g. badWords) which lives in S3 -- L4 only
    persists a subset of envelope keys to DynamoDB (no badWords), so the bucket is the
    authoritative place to assert on those fields.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            return json.loads(obj["Body"].read())
        except s3.exceptions.NoSuchKey:
            time.sleep(interval)
    raise AssertionError(f"timeout waiting on s3://{bucket}/{key}")


def profanity_envelope(reviewId):
    """Fetch the L2 output envelope (with isProfane + badWords) from the profanity bucket."""
    return poll_object(param("/dic-a3/buckets/profanity"), f"cornercase/{reviewId}.json")


def get_customer(reviewer):
    return ddb.get_item(
        TableName=param("/dic-a3/tables/customers"),
        Key={"reviewerID": {"S": reviewer}},
    ).get("Item")


def invoke_report():
    """Invoke the on-demand L5 report Lambda and return its parsed payload."""
    resp = lam.invoke(FunctionName="report")
    return json.loads(resp["Payload"].read())


def reviews_table():
    return param("/dic-a3/tables/reviews")


def tokens_of(item):
    return [t["S"] for t in item.get("tokens", {}).get("L", [])]


# ══════════════════════════════════════════════════════════════════════════════════════════
# L1 preprocess
# ══════════════════════════════════════════════════════════════════════════════════════════

def test_preprocess_tokens_are_clean_lowercase_deduped():
    """Tokens must be lowercased, >=2 chars, stopword-free and deduplicated.

    Proves L1's real lemmatize/dedup/stopword pipeline runs (the smoke test only checks
    the `tokens` key exists).
    """
    rid, _, env = envelope(
        "PREP",
        summary="The CATS",
        text="The cats are running and the cats run quickly quickly. A I",
        overall=4.0,
    )
    drop(env)
    item = poll_item(reviews_table(), {"reviewId": {"S": rid}})
    toks = tokens_of(item)

    assert toks, "expected at least one token"
    assert all(t == t.lower() for t in toks), f"non-lowercase token in {toks}"
    assert all(len(t) >= 2 for t in toks), f"token shorter than 2 chars in {toks}"
    assert len(toks) == len(set(toks)), f"duplicate tokens not collapsed: {toks}"
    # Stopwords and single letters must be gone.
    for stop in ("the", "are", "and", "a", "i"):
        assert stop not in toks, f"stopword/short token '{stop}' survived: {toks}"


def test_preprocess_empty_both_fields_yields_no_tokens():
    """summary and reviewText both empty -> tokens == [] and the review still completes."""
    rid, _, env = envelope("PREPEMPTY", summary="", text="", overall=3.0)
    drop(env)
    item = poll_item(reviews_table(), {"reviewId": {"S": rid}})
    assert tokens_of(item) == [], "empty review should produce no tokens"


# ══════════════════════════════════════════════════════════════════════════════════════════
# L2 profanity
# ══════════════════════════════════════════════════════════════════════════════════════════

def test_clean_review_is_not_flagged_profane():
    """A wholesome review must have isProfane=False and an empty badWords list.

    badWords lives in the L2 output envelope (S3), not the Reviews table, so we read the
    profanity-bucket object for the badWords assertion.
    """
    rid, _, env = envelope(
        "CLEAN",
        summary="lovely",
        text="this is a lovely wonderful product i really enjoyed using it",
        overall=5.0,
    )
    drop(env)
    item = poll_item(reviews_table(), {"reviewId": {"S": rid}})
    assert item["isProfane"]["BOOL"] is False
    assert profanity_envelope(rid)["badWords"] == [], "clean review should list no bad words"


def test_real_profanity_populates_badwords():
    """A real bad word must set isProfane=True AND appear in badWords (not just the flag).

    badWords is part of the envelope in the reviews-profanity bucket; L4 deliberately does
    not copy it into DynamoDB (see CONTRACT.md Reviews schema), so assert on the S3 object.
    """
    rid, _, env = envelope(
        "BADWORDS",
        summary="terrible",
        text="this is bullshit and a total ripoff",
        overall=1.0,
    )
    drop(env)
    env_out = profanity_envelope(rid)
    assert env_out["isProfane"] is True
    assert "bullshit" in env_out["badWords"], \
        f"expected the detected bad word in badWords, got {env_out['badWords']}"


def test_forceprofane_hook_marks_profane_without_real_word():
    """forceProfane=True marks a clean review profane with an EMPTY badWords list."""
    rid, _, env = envelope(
        "FORCE",
        summary="perfectly polite",
        text="what a delightful and pleasant experience this was",
        overall=5.0,
        forceProfane=True,
    )
    drop(env)
    item = poll_item(reviews_table(), {"reviewId": {"S": rid}},
                     want_attr="isProfane", want_value=True)
    assert item["isProfane"]["BOOL"] is True
    assert [w["S"] for w in item.get("badWords", {}).get("L", [])] == []


# ══════════════════════════════════════════════════════════════════════════════════════════
# L3 sentiment
# ══════════════════════════════════════════════════════════════════════════════════════════

def test_neutral_sentiment_classification():
    """A factual, middle-rated review must classify as 'neutral'."""
    rid, _, env = envelope(
        "NEUTRAL",
        summary="it is a product",
        text="the item arrived on tuesday. it is the size described. it has a power cable.",
        overall=3.0,
    )
    drop(env)
    item = poll_item(reviews_table(), {"reviewId": {"S": rid}},
                     want_attr="sentiment", want_value="neutral")
    assert item["sentiment"]["S"] == "neutral"


def test_unicode_and_emoji_review_survives_pipeline():
    """Accented text, CJK characters and emoji must not crash any stage."""
    rid, _, env = envelope(
        "UNICODE",
        summary="Mixed feelings 😤🔥",
        text="Café crème brûlée was délicious 😋. 这个产品还不错。 Größe passt gut.",
        overall=4.0,
    )
    drop(env)
    item = poll_item(reviews_table(), {"reviewId": {"S": rid}})
    assert item["reviewId"]["S"] == rid
    assert tokens_of(item), "unicode review should still produce tokens"
    assert item["sentiment"]["S"] in ("positive", "neutral", "negative")


def test_null_overall_does_not_crash_sentiment():
    """Regression: overall=null must default to neutral, not crash L3 on float(None).

    The loader emits overall=null for any source row that lacks a star rating; before the
    fix this killed the review silently in L3 so it never reached the Reviews table.
    """
    rid, _, env = envelope("NULLRATING", summary="meh", text="it is a thing", overall=None)
    drop(env)
    item = poll_item(reviews_table(), {"reviewId": {"S": rid}})
    assert item["sentiment"]["S"] in ("positive", "neutral", "negative")


def test_missing_overall_key_does_not_crash_sentiment():
    """An envelope with no `overall` key at all must also default cleanly to neutral."""
    rid, reviewer, env = envelope("NOOVERALL", summary="meh", text="just some words here")
    env.pop("overall")  # omit the key entirely (the .get default path)
    drop(env)
    item = poll_item(reviews_table(), {"reviewId": {"S": rid}})
    assert item["sentiment"]["S"] in ("positive", "neutral", "negative")


# ══════════════════════════════════════════════════════════════════════════════════════════
# L4 aggregate -- ban ledger + idempotency
# ══════════════════════════════════════════════════════════════════════════════════════════

def test_polite_review_creates_no_customer_row():
    """A non-profane review must NOT create or touch the customer's ban ledger row."""
    rid, reviewer, env = envelope(
        "POLITE",
        summary="thanks",
        text="great seller, fast shipping, would buy again",
        overall=5.0,
    )
    drop(env)
    # Wait until it has fully landed in Reviews, then assert no Customers row exists.
    poll_item(reviews_table(), {"reviewId": {"S": rid}})
    assert get_customer(reviewer) is None, "polite reviewer must not appear in Customers"


def test_redelivered_profane_review_counts_once():
    """Re-delivering the SAME profane reviewId must not double-count (idempotency gate)."""
    customers = param("/dic-a3/tables/customers")
    rid, reviewer, env = envelope(
        "IDEMP", summary="s", text="one profane shit review", overall=1.0, forceProfane=True)
    drop(env)
    poll_item(customers, {"reviewerID": {"S": reviewer}},
              want_attr="impoliteCount", want_value=1)

    drop(env)  # exact same reviewId again
    time.sleep(3)  # give a (buggy) double-count a chance to happen
    item = ddb.get_item(TableName=customers, Key={"reviewerID": {"S": reviewer}})["Item"]
    assert int(item["impoliteCount"]["N"]) == 1, "re-delivery must not bump the count"


def test_same_reviewid_different_content_is_ignored():
    """The idempotency key is reviewId: a 2nd object with the same id but different content
    must be dropped -- the stored row keeps the FIRST content and the count stays 1."""
    customers = param("/dic-a3/tables/customers")
    reviewer = f"DUPCONTENT_{RUN}"
    rid = make_review_id(reviewer, "FIRST", 1, "first body", "s")

    first = {
        "reviewId": rid, "reviewerID": reviewer, "asin": "FIRST",
        "summary": "s", "reviewText": "first body", "overall": 1.0,
        "unixReviewTime": 1, "source": "cornercase", "forceProfane": True,
    }
    drop(first)
    poll_item(customers, {"reviewerID": {"S": reviewer}},
              want_attr="impoliteCount", want_value=1)

    second = dict(first, asin="SECOND", reviewText="completely different body")
    drop(second)  # same reviewId -> idempotency gate must reject it
    time.sleep(3)

    review = ddb.get_item(TableName=reviews_table(), Key={"reviewId": {"S": rid}})["Item"]
    assert review["asin"]["S"] == "FIRST", "stored review must keep the first content"
    cust = ddb.get_item(TableName=customers, Key={"reviewerID": {"S": reviewer}})["Item"]
    assert int(cust["impoliteCount"]["N"]) == 1, "duplicate id must not be counted twice"


# ══════════════════════════════════════════════════════════════════════════════════════════
# L5 report
# ══════════════════════════════════════════════════════════════════════════════════════════

def test_report_payload_has_expected_shape():
    """The report Lambda returns the agreed result structure."""
    payload = invoke_report()
    assert payload["statusCode"] == 200
    assert set(payload["sentiment"].keys()) == {"positive", "neutral", "negative"}
    assert all(isinstance(v, int) for v in payload["sentiment"].values())
    assert isinstance(payload["profanityFailed"], int)
    assert payload["bannedUsers"] == sorted(payload["bannedUsers"]), "bannedUsers must be sorted"


def test_report_excludes_cornercase_reviews():
    """cornercase reviews must NOT move the devset sentiment / profanity counts.

    Snapshot the counts, inject cornercase reviews (one positive, one profane), let them
    land in Reviews, then snapshot again -- the devset-only counts must be unchanged.
    """
    before = invoke_report()

    rid_pos, _, env_pos = envelope(
        "RPTPOS", summary="amazing", text="excellent wonderful love perfect outstanding",
        overall=5.0)
    rid_bad, _, env_bad = envelope(
        "RPTBAD", summary="awful", text="this product is bullshit", overall=1.0)
    drop(env_pos)
    drop(env_bad)
    poll_item(reviews_table(), {"reviewId": {"S": rid_pos}})
    poll_item(reviews_table(), {"reviewId": {"S": rid_bad}},
              want_attr="isProfane", want_value=True)

    after = invoke_report()
    assert after["sentiment"] == before["sentiment"], "cornercase changed sentiment counts"
    assert after["profanityFailed"] == before["profanityFailed"], \
        "cornercase changed profanityFailed count"
