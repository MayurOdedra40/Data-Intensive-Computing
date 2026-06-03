# Assignment 3 — Division of Work

Event-driven serverless review pipeline on **MiniStack** (`http://localhost:4566`, ephemeral — recreate everything via `run.sh` after each restart). ≥3 Lambdas, triggered by S3/DynamoDB events, all names from SSM.

## Architecture (sequential S3-relay chain)

```
ingest(S3) → L1 preprocess → preprocessed(S3) → L2 profanity → profanity(S3)
→ L3 sentiment → scored(S3) → L4 aggregate+ban → DynamoDB (Reviews, Customers)
L5 report (on-demand) → reads DynamoDB → results-export(S3)
```

Each hop = an `s3:ObjectCreated:*` notification (the only proven trigger). A growing JSON **envelope** is relayed bucket→bucket; DynamoDB is written only at L4. Optional 2nd trigger: a `NEW_IMAGE` stream on `Reviews → L4`.

**Buckets:** `reviews-ingest`, `reviews-preprocessed`, `reviews-profanity`, `reviews-scored`, `results-export`
**Tables:** `Reviews` (PK `reviewId`), `Customers` (PK `reviewerID`)
**SSM:** `/dic-a3/buckets/*`, `/dic-a3/tables/*`, `/dic-a3/config/ban-threshold=3`, `.../sentiment-pos=0.05`, `.../sentiment-neg=-0.05`, `.../overall-weight=0.3`

**Envelope** (additive — each Lambda only ADDs its own keys):
```json
{"reviewId","reviewerID","asin","source","summary","reviewText","overall",
 "tokens","isProfane","badWords","sentiment","sentimentScore"}
```
`reviewId = reviewerID_asin_unixReviewTime_sha1(text+summary)[:8]`. Key prefix `reviews/` = devset, `cornercase/` = extra test reviews (excluded from results).

## File layout

```
src/
  run.sh                          # provision all resources (idempotent)
  common/config.py                # SSM get(name)
  common/s3_events.py             # parse S3 event, skip s3:TestEvent
  lambdas/preprocess/handler.py   # L1
  lambdas/profanity/handler.py    # L2
  lambdas/sentiment/handler.py    # L3
  lambdas/aggregate/handler.py    # L4
  lambdas/report/handler.py       # L5
  loader.py                       # feed reviews_devset.json into ingest bucket
  tests/test_integration.py
  cornercase/*.json               # engineered test reviews
CONTRACT.md                       # envelope + SSM scheme (frozen reference)
```

---

## Stage 1 — Infrastructure & skeleton 
Build the backbone everyone else plugs into. Verify on day 1 that DynamoDB Streams→Lambda, NLTK/profanityfilter packaging, and atomic counters work on MiniStack.

- **Files:** `run.sh`, `common/config.py`, `common/s3_events.py`, `CONTRACT.md`, all 5 `handler.py` as **stubs** (L4 logic real), `tests/test_integration.py` skeleton, `cornercase/*.json`, `loader.py` stub.
- **In:** the assignment requirements + the dataset.
- **Out:** a deployed chain where dropping one review flows through all 5 stubs, writes a `Reviews` item, atomically increments `Customers`, and bans on the 4th impolite review. Frozen envelope + SSM scheme.
- **Done when:** `bash run.sh` recreates everything from clean; one review traverses all 5 Lambdas; `pytest` green on smoke + ban-boundary; re-delivering a review doesn't double-count.

## Stage 2 — Preprocessing (L1)

- **File:** `lambdas/preprocess/handler.py` (reuse `utils/text_processing.py`).
- **In:** raw review JSON from `reviews-ingest`.
- **Out:** envelope with `tokens` → `reviews-preprocessed`.
- **Do:** combine summary+reviewText, tokenize + stopword-remove (existing util), add **POS-aware WordNet lemmatization** (`running→run`); empty text → `tokens=[]`.
- **Done when:** lemmatization test green; empty-`reviewText` handled.

## Stage 3 — Profanity check (L2)

- **File:** `lambdas/profanity/handler.py`.
- **In:** envelope from `reviews-preprocessed`.
- **Out:** envelope + `isProfane`, `badWords` → `reviews-profanity`.
- **Do:** scan summary+reviewText with `profanityfilter`; record all three fields considered (overall is numeric — noted in report).
- **Done when:** profane corner case → `isProfane=true` + `badWords`; clean review → `false`.

## Stage 4 — Sentiment (L3) + count/ban tests

- **File:** `lambdas/sentiment/handler.py`; ban/count tests in `tests/test_integration.py`.
- **In:** envelope from `reviews-profanity`.
- **Out:** envelope + `sentiment`, `sentimentScore` → `reviews-scored`.
- **Do:** NLTK **VADER** over summary+reviewText, blended with `overall` (weight from SSM); map to positive/neutral/negative via thresholds. Write the integration tests for counting and ban (3 impolite = not banned, 4 = banned, re-delivery doesn't over-count).
- **Done when:** sentiment + counting + ban tests green.

## Stage 5 — Loader, results, report

- **Files:** `lambdas/report/handler.py`, `loader.py`; `report.pdf`, `instructions.pdf`.
- **In:** `Reviews` + `Customers` tables (after full devset run).
- **Out:** results JSON in `results-export`; the report.
- **Do:** real batched/resumable `loader.py` to feed all 78,829 reviews; L5 scans `source="devset"` → #positive/neutral/negative, #failing profanity, banned users. Write `report.pdf` (5 sections + architecture diagram, ≤8pp) and `instructions.pdf` (how to run). Assemble `<groupID>_DIC2026_Assignment_3.zip`.
- **Done when:** full devset processed; 3 result numbers in the report; both PDFs done; `pytest` fully green.

---

**Order:** Stage 1 first (unblocks all). Then 2, 3, 4 in parallel. Then 5 (needs 2–4 for final numbers).
