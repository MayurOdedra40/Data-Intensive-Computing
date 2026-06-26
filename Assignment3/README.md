# Assignment 3 — Serverless Review Pipeline

Event-driven pipeline on MiniStack (AWS emulator): S3 → Lambda chain → DynamoDB.

```
reviews-ingest → L1 preprocess → L2 profanity → L3 sentiment → L4 aggregate → DynamoDB
                                                                              ↑
                                                               L5 report (on-demand)
```

---

## Run Locally

### Prerequisites
- Python 3.11+
- MiniStack installed (`pip install ministack`)

### Steps

**1. Install dependencies**
```bash
pip install -r src/requirements.txt
```

**2. Start MiniStack** (keep this terminal open)
```bash
ministack
```

**3. Provision everything** (run after every MiniStack restart)
```bash
bash src/run.sh
```

This creates all S3 buckets, SSM parameters, DynamoDB tables, and deploys all 5 Lambda functions.

**4. Load reviews**
```bash
# Smoke test — 1 review
python src/loader.py data/reviews_devset.json 1

# Small batch — first 500
python src/loader.py data/reviews_devset.json 500 --batch-size 100 --batch-delay 20

# Full devset — batched to avoid memory errors
python src/loader.py data/reviews_devset.json --batch-size 200 --batch-delay 30
```

**5. Get results**
```bash
python src/generate_results.py
# Writes results.json and prints a summary
```

**6. Run integration tests**
```bash
cd src && pytest tests/test_integration.py -v
```

---

## Run on the LBD Cluster

### Prerequisites
- MiniStack is pre-installed on the cluster — no pip needed for the host environment
- Dataset is available at `hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json`

### Steps

**1. SSH into the cluster and start MiniStack** (keep this terminal open)
```bash
ministack
```

**2. Open a second terminal and run everything with one command**
```bash
bash cluster/run_cluster.sh
```

This automatically:
1. Checks MiniStack is reachable
2. Fetches `reviews_devset.json` from `hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json`
3. Provisions all resources (`run.sh`)
4. Loads all 78,829 reviews in batches of 200 (30s pause between batches)
5. Polls until all reviews appear in DynamoDB
6. Writes `cluster/results.json`

### Overrides

```bash
# Smaller batches if memory errors occur
BATCH_SIZE=100 BATCH_DELAY=60 bash cluster/run_cluster.sh

# Smoke test with fewer reviews
REVIEW_COUNT=500 BATCH_SIZE=100 BATCH_DELAY=10 bash cluster/run_cluster.sh

# Custom MiniStack endpoint
MINISTACK_ENDPOINT=http://localhost:4566 bash cluster/run_cluster.sh
```

---

## Results

After the pipeline finishes, `results.json` contains:

```json
{
  "totalDevsetReviews": 78829,
  "sentiment": {
    "positive": ...,
    "neutral":  ...,
    "negative": ...
  },
  "profanityFailed": ...,
  "bannedUsers": [...]
}
```

Run standalone at any time (MiniStack must be up with data already processed):
```bash
python src/generate_results.py          # local
python cluster/generate_results.py      # on cluster
```

---

## Project Structure

```
Assignment3/
  src/                        # development version
    run.sh                    # provision MiniStack resources
    run_cluster.sh            # end-to-end cluster runner (src version)
    loader.py                 # load reviews into pipeline (batched)
    generate_results.py       # query DynamoDB → results.json
    common/
      config.py               # SSM parameter getter
      s3_events.py            # S3 event parser
    lambdas/
      preprocess/handler.py   # L1: tokenize + POS lemmatize
      profanity/handler.py    # L2: profanity check
      sentiment/handler.py    # L3: VADER sentiment
      aggregate/handler.py    # L4: DynamoDB write + ban logic
      report/handler.py       # L5: on-demand report
    cornercase/               # engineered test reviews
    tests/test_integration.py
  cluster/                    # self-contained LBD cluster package
    run.sh                    # cluster-adapted provisioning
    run_cluster.sh            # cluster end-to-end runner
    loader.py / generate_results.py / common/ / lambdas/ / ...
  data/
    reviews_devset.json       # input dataset (or fetch from HDFS)
    stopwords.txt
```

<!-- ==================================================
  FINAL RESULTS  (devset reviews only)
==================================================
  Combined from segments  : 5
  Total reviews processed : 78827
  Positive                : 67907
  Neutral                 : 1281
  Negative                : 9639
  Failed profanity check  : 6982
  Banned users (8)       :
    - A13QTZ8CIMHHG4  (impolite reviews: 4)
    - A2EDZH51XHFA9B  (impolite reviews: 6)
    - A2HVL790PBWYTU  (impolite reviews: 4)
    - A2OJW07GQRNJUT  (impolite reviews: 5)
    - A320TMDV6KCFU  (impolite reviews: 8)
    - A3LZGLA88K0LA0  (impolite reviews: 4)
    - A3QS1EPDZTLPWS  (impolite reviews: 4)
    - AFVQZQ8PW0L  (impolite reviews: 13) -->

{
  "totalDevsetReviews": 78829,
  "sentiment": {
    "positive": 67908,
    "neutral": 1282,
    "negative": 9639
  },
  "profanityFailed": 6983,
  "bannedUsers": [
    "A13QTZ8CIMHHG4",
    "A2EDZH51XHFA9B",
    "A2HVL790PBWYTU",
    "A2OJW07GQRNJUT",
    "A320TMDV6KCFU",
    "A3LZGLA88K0LA0",
    "A3QS1EPDZTLPWS",
    "AFVQZQ8PW0L"
  ]
}
  

● Confirmed — nothing is missing or stuck. The devset has 2 reviewIds that
  each appear on two lines:

  - A1MACFR0X42D8E_B00466H3MM_1319932800_f2f73a4d ×2
  - A2SB75CW5MXA1P_B005ADNUIG_1372118400_bbad07fd ×2

  So the 78,829 lines contain only 78,827 distinct reviews. Since reviewId is
  the idempotency key
  (reviewerID_asin_unixReviewTime_sha1(reviewText+summary)[:8]), two lines
  sharing all of those are exact-duplicate reviews, and L4's
  ConditionExpression="attribute_not_exists(reviewId)" correctly counts each
  once. 78,827 is the right number — the pipeline did its job.

  Why the re-run does nothing: REVIEW_COUNT defaults to 78829, so REMAINING =
  78829 − 78827 = 2. The loader is then asked to load 2 more reviews, but
  every line's reviewId is already in the processed set, so it uploads zero.
  You'll get that harmless "process 2 / does nothing" dance on every future
  re-run.

  Two things worth doing:

  1. Report it as a finding, not a loss. This is a nice point for the report:
  real-world data had exact duplicates, and the idempotency-key design
  dedupes them transparently. The correct devset total is 78,827.
  1. (Optional) silence the phantom-2 on re-runs so the script reports
  "nothing left to do." Cleanest fix: set the target to distinct reviews
  rather than raw lines. Either run with REVIEW_COUNT=78827, or make
  run_cluster.sh compute the dataset's distinct-id count instead of
  hardcoding 78829.

  Want me to verify the two duplicate pairs are byte-identical records (vs.
  coincidental key collisions), and/or patch run_cluster.sh to derive the
  target from distinct reviewIds?