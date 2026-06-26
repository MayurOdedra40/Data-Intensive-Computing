# CONTRACT.md — the frozen interface for Assignment 3

This file is the **single source of truth** that every Lambda and every test agrees on.
Once it is frozen (end of Stage 1), Stages 2–5 program against *this document*, never
against each other's code. If you need to change something here, tell the whole team —
it is a breaking change.

There are three interface "seams" that let people work independently:

1. The **SSM parameters** (the names of buckets/tables/config values).
2. The **envelope** (the JSON object that travels through the pipeline).
3. The **bucket layout** (which bucket feeds which Lambda).

---

## 1. SSM parameters (namespace `/dic-a3/...`)

Nothing is hardcoded in a handler. Every bucket name, table name, and tunable number is
read from SSM Parameter Store at runtime via `common/config.py`.

| SSM name | Value | Meaning |
|---|---|---|
| `/dic-a3/buckets/ingest` | `reviews-ingest` | drop-zone bucket; a new object here STARTS the chain |
| `/dic-a3/buckets/preprocessed` | `reviews-preprocessed` | output of L1 preprocess |
| `/dic-a3/buckets/profanity` | `reviews-profanity` | output of L2 profanity-check |
| `/dic-a3/buckets/scored` | `reviews-scored` | output of L3 sentiment |
| `/dic-a3/buckets/export` | `results-export` | output of L5 report |
| `/dic-a3/tables/reviews` | `Reviews` | one item per processed review |
| `/dic-a3/tables/customers` | `Customers` | one item per reviewer (ban ledger) |
| `/dic-a3/config/ban-threshold` | `3` | ban when impolite count is **strictly greater** than this (so the 4th impolite review bans) |
| `/dic-a3/config/sentiment-pos` | `0.05` | VADER compound at/above this = positive (used in Stage 4) |
| `/dic-a3/config/sentiment-neg` | `-0.05` | VADER compound at/below this = negative (used in Stage 4) |
| `/dic-a3/config/overall-weight` | `0.3` | how much the star rating blends into sentiment, 0..1 (used in Stage 4) |

> **Gotcha:** SSM always returns **strings**. `ban-threshold` comes back as `"3"`, not `3`.
> Always cast: `int(config.get(...))`. `config.py` provides `get_int` / `get_float` for this.

---

## 2. The envelope (the JSON object that flows through the pipeline)

One review = one JSON object = one S3 object. As it moves down the chain, each Lambda
**ADDs its own keys and never deletes or overwrites keys it does not own.** This "additive
envelope" is why stages can be built independently: L3 doesn't care *how* L1 made `tokens`,
only that the key exists.

| Key | Set by | Type | Notes |
|---|---|---|---|
| `reviewId` | **loader** | str | unique id, see §3. The idempotency key. |
| `reviewerID` | loader (raw) | str | the customer; ban counting is per `reviewerID` |
| `asin` | loader (raw) | str | product id |
| `summary` | loader (raw) | str | short review title — used by profanity + sentiment |
| `reviewText` | loader (raw) | str | full review body — used by profanity + sentiment |
| `overall` | loader (raw) | number | star rating 1.0–5.0 — used by sentiment |
| `unixReviewTime` | loader (raw) | int | timestamp; part of `reviewId` |
| `source` | **loader** | str | `"devset"` (counts toward results) or `"cornercase"` (excluded). See §4. |
| `tokens` | L1 preprocess | list[str] | cleaned + lemmatized words. Stage-1 stub writes `[]`. |
| `isProfane` | L2 profanity | bool | true if a bad word was found |
| `badWords` | L2 profanity | list[str] | the bad words found (may be empty) |
| `sentiment` | L3 sentiment | str | `"positive"` / `"neutral"` / `"negative"`. Stub writes `"neutral"`. |
| `sentimentScore` | L3 sentiment | number | the numeric score behind the label |

### Test-only key
| Key | Set by | Type | Notes |
|---|---|---|---|
| `forceProfane` | test/cornercase files | bool | **Test hook.** If `true`, the L2 *stub* marks the review profane without real detection. This lets the ban logic be tested in Stage 1 before real profanity detection exists (Stage 3). Real L2 may ignore it. Never set on real devset reviews. |

### Status rule
Each Lambda only ADDs keys. A review is "done" once it reaches the `Reviews` table (written
by L4). There is no mutable `status` field in Stage 1 — the bucket an object currently lives
in tells you how far it got.

---

## 3. How `reviewId` is built (ONE owner: the loader)

The **loader** computes `reviewId` and puts it in the envelope. Every downstream Lambda just
reads it. This matters: `reviewId` is the idempotency key (see §5), so it must be computed in
exactly one place or duplicates could drift.

```python
import hashlib, json
def make_review_id(rec):
    # Hash the ENTIRE record (every field) with a canonical, key-sorted serialization.
    canonical = json.dumps(rec, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
```

Why hash the whole object? The earlier id used only
`reviewerID_asin_unixReviewTime_sha1(reviewText+summary)`, so devset rows that differ ONLY in
another field (notably `category`) produced the SAME id and collided in the `Reviews` table. Hashing
every field makes the id unique for any row that differs at all. The full SHA-256 digest avoids
truncation collisions. Two rows now share an id ONLY if they are byte-for-byte identical (a genuine
duplicate row) — exactly when the idempotency gate should treat them as one.

---

## 4. Bucket layout & object keys

```
reviews-ingest        --(s3:ObjectCreated)-->  L1 preprocess  --writes-->  reviews-preprocessed
reviews-preprocessed  --(s3:ObjectCreated)-->  L2 profanity   --writes-->  reviews-profanity
reviews-profanity     --(s3:ObjectCreated)-->  L3 sentiment   --writes-->  reviews-scored
reviews-scored        --(s3:ObjectCreated)-->  L4 aggregate   --writes-->  DynamoDB (Reviews, Customers)
                          L5 report (on-demand) --reads DynamoDB--> results-export
```

- **Object key** travels unchanged down the chain: `reviews/<reviewId>.json` for devset rows,
  `cornercase/<name>.json` for engineered test reviews.
- The key prefix (`reviews/` vs `cornercase/`) is just human-readable bookkeeping. The field
  that actually decides whether a review counts toward the official results is **`source`**.
  L5 filters on `source == "devset"`. This is the single source of truth — do not also filter
  on the key prefix, or the two could disagree.

---

## 5. Idempotency & the ban rule (implemented in L4)

S3→Lambda delivery is **at-least-once**: the same event can be delivered more than once. So L4
must be safe to run twice on the same review.

- **Idempotency gate:** L4 inserts into `Reviews` with
  `ConditionExpression="attribute_not_exists(reviewId)"`. The first insert succeeds; any repeat
  throws `ConditionalCheckFailedException`. L4 only continues to counting when the insert
  *actually created* the row. → each unique `reviewId` is counted **at most once**.
- **Counting:** when a new review is profane, L4 does an atomic
  `UpdateExpression="ADD impoliteCount :one"` on the customer (`ReturnValues="UPDATED_NEW"`
  returns the new count in the same call). `ADD` treats a missing number as 0, so the first
  impolite review yields 1.
- **Ban:** when the returned count is **strictly greater than** `ban-threshold` (3), set
  `banned = true`. So: 3 impolite reviews → not banned; the 4th → banned. ("more than 3").

### DynamoDB tables
| Table | Partition key | Stream | Other attributes (schemaless, created on write) |
|---|---|---|---|
| `Reviews` | `reviewId` (S) | `NEW_IMAGE` enabled | `reviewerID`, `asin`, `source`, `isProfane`, `sentiment` |
| `Customers` | `reviewerID` (S) | none | `impoliteCount` (N), `banned` (BOOL) |

> A brand-new customer has **no** `banned` attribute (not `banned=false`). Treat "absent" as
> "not banned" in tests and code.
