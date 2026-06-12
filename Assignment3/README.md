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