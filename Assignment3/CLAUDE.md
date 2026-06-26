# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Assignment 3 of a Data-Intensive Computing course (TU Wien, run on the "LBD" cluster at
`lbd.tuwien.ac.at`). We build an **event-driven serverless review-processing pipeline** on
**MiniStack** — a self-hosted, LocalStack-style emulator of AWS (S3 + Lambda + DynamoDB + SSM),
reachable at `http://localhost:4566`. MiniStack is **ephemeral**: every restart wipes all
resources, so `run.sh` must rebuild the whole world and be safe to re-run (idempotent).

The actual requirements live in `Assignment_3_Instructions.pdf`; cluster/AWS-CLI how-tos live in
`Tips_and_Tricks.pdf`. **Submission deadline: 2026-06-30 23:59 (TUWEL).** Deliverables:
`report.pdf` (≤8 pages, 11pt, one column, ≥5 sections + an architecture diagram),
`instructions.pdf`, and `src/` (documented code + integration tests + any cornercase reviews).

The required results (devset only): counts of positive/neutral/negative reviews, number failing
the profanity check, and the list of banned users.

> Two other things live in this folder and are **not** part of the active project:
> `assignment_3_tutorial/` (the LocalStack image-resizer tutorial we started from) and
> `utils/text_processing.py` (leftover Assignment-1 preprocessing). Treat them as separate.

## The pipeline

```
reviews-ingest → L1 preprocess → reviews-preprocessed
               → L2 profanity  → reviews-profanity
               → L3 sentiment  → reviews-scored
               → L4 aggregate  → DynamoDB (Reviews, Customers)
L5 report (on-demand, NOT in the chain)
```

Each chain link is an S3 `s3:ObjectCreated:*` notification wired by `run.sh` step 5. A Lambda's
last act is to **write its output object into the next bucket**, which is itself the trigger for
the next Lambda — there is no orchestrator. A review is "done" once it reaches the `Reviews`
table; the bucket an object currently sits in tells you how far it got.

All five handlers live in [src/lambdas/](src/lambdas/) (`preprocess`, `profanity`, `sentiment`,
`aggregate`, `report`), each a single `handler.py` with `def handler(event, context)`.

### Key design contracts (read [CONTRACT.md](CONTRACT.md) before changing interfaces)

- **Additive envelope.** One review = one JSON object = one S3 object. Each stage only **ADDs**
  keys (`tokens`, `isProfane`/`badWords`, `sentiment`/`sentimentScore`) and never deletes/overwrites
  keys it doesn't own. This is why stages were built independently.
- **`reviewId` has ONE owner: the loader.** `make_review_id` =
  `{reviewerID}_{asin}_{unixReviewTime}_{sha1(reviewText+summary)[:8]}`. It is the idempotency key.
  The same function is duplicated verbatim in [src/loader.py](src/loader.py) — keep them in sync.
- **`source` decides what counts.** The loader stamps `source="devset"` (counts toward results) or
  `"cornercase"` (excluded). L5/results filter on `source`, **not** on the S3 key prefix.
- **Idempotency + ban rule live in L4.** S3→Lambda is at-least-once. `aggregate` inserts into
  `Reviews` with `ConditionExpression="attribute_not_exists(reviewId)"`; only a *newly created*
  row is counted. Profane reviews do an atomic `ADD impoliteCount :one`; ban when the new count is
  **strictly greater than** `ban-threshold` (3) → the 4th impolite review bans. A brand-new
  customer has **no** `banned` attribute (absent = not banned).

### Everything configurable comes from SSM (namespace `/dic-a3/...`)

Nothing is hardcoded in handlers — bucket names, table names, ban threshold, and sentiment
thresholds are read from SSM Parameter Store at runtime. SSM returns **strings**: always cast.
Full table in [CONTRACT.md](CONTRACT.md) §1. The only place to change a bucket/table name or a
tunable is `run.sh` step 2.

### Shared modules (bundled into every Lambda zip)

- [src/common/config.py](src/common/config.py) — `get()` / `get_int()` / `get_float()` SSM wrapper.
- [src/common/s3_events.py](src/common/s3_events.py) — `parse_records(event)` (skips the
  `s3:TestEvent` handshake and any non-S3 record, e.g. a DynamoDB stream record — so L4 can't
  double-count), `read_envelope()`, `write_envelope()`.
- **`STAGE=local`** (set on every Lambda by `run.sh`) makes the in-Lambda boto3 clients point at
  `http://localhost:4566`; without it boto3 would hit real AWS.

### Implementation notes that aren't obvious from one file

- **L1/L3 download NLTK data at cold start** to `/tmp/nltk_data` (`nltk.download(...)`), because the
  Lambda filesystem is read-only elsewhere. This needs network access from the Lambda sandbox at
  runtime — a **known cluster risk** (see below).
- **Profanity uses `better-profanity`, not the hinted `profanityfilter`** — `profanity-filter`
  is broken on PyPI (its `ordered-set-stubs` dependency was removed). Documented in
  [src/requirements.txt](src/requirements.txt).
- **Sentiment blends stars into VADER:** `blended = (1-overall_weight)*vader_compound +
  overall_weight*((overall-3)/2)`, then thresholded by `sentiment-pos`/`sentiment-neg`. This is
  how requirement 4 ("`overall` must be taken into account") is satisfied.
- **`overall: null` guard.** The loader defaults a null star rating to 3.0, and L3 re-guards
  `float(None)` — a regression both the loader and `test_integration_edge.py` cover.

## Common commands

`run.sh` and `run_cluster.sh` both `cd` to their own dir, so call them from anywhere. All AWS CLI
calls target MiniStack via `aws --endpoint-url=http://localhost:4566` with dummy `test`/`test`
creds (the `AWS=` alias at the top of each script bakes this in).

### Provision + run locally

```bash
ministack                                       # terminal 1 — keep open
bash src/run.sh                                 # terminal 2 — provision everything
python src/loader.py data/reviews_devset.json 1 # smoke test: 1 review through the chain
python src/generate_results.py                  # query DynamoDB → results.json (+ S3 upload)
```

`loader.py` is batched to avoid MiniStack OOM: `--batch-size` (default 200), `--batch-delay`
(default 30s), `--skip N`. Full devset: `python src/loader.py data/reviews_devset.json`.

### Run on the LBD cluster (end-to-end, resumable)

```bash
ministack                                       # terminal 1
bash src/run_cluster.sh                          # terminal 2 — fetch dataset, provision, load, checkpoint, finalize
```
Overrides: `REVIEW_COUNT`, `BATCH_SIZE`, `BATCH_DELAY`, `CHECKPOINT_INTERVAL`, `FORCE_FRESH`,
`MINISTACK_ENDPOINT`, `HDFS_DATA_PATH`. Fetches the devset from
`hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json` if not already at `data/reviews_devset.json`.

**Resumable / restart-proof design (the important part).** Each run uses a FRESH (wiped) DynamoDB as
scratch and processes ONLY reviews not already done — the loader skips reviewIds covered by earlier
runs (`--skip-ids-file`). A background `checkpointer.py` snapshots that run's state to its own
`checkpoints/segment_NNN.json` every `CHECKPOINT_INTERVAL`s. Because runs cover **disjoint** reviews,
`finalize_results.py` produces `results.json` by simply **adding** all segments (counts and per-user
impolite counts, so a banned user split across runs still totals correctly). This survives a full
**MiniStack restart** (durable state is the on-disk segments, never the ephemeral tables) and a
driver/SSH death — just re-run `run_cluster.sh`; it adds a segment for whatever is still missing.
`FORCE_FRESH=1` deletes the segments and starts over. Key files: `checkpoints.py` (segment model +
combine), `checkpointer.py`, `finalize_results.py`. `generate_results.py` remains an independent
DynamoDB cross-check (and `test_stress.py` dep).

### Integration tests (require MiniStack up + `run.sh` already applied)

```bash
cd src
pytest tests/test_integration.py -v             # smoke suite
pytest tests/test_integration_edge.py -v        # per-stage edge cases (lemmatize, forceProfane, null overall, idempotency, report shape)
RUN_STRESS=1 pytest tests/test_stress.py -v -s   # OPT-IN full-devset stress (78,829 rows); tune with STRESS_COUNT/STRESS_BATCH_*
pytest tests/test_integration.py::test_smoke_one_review_reaches_reviews_table   # single test
```

### Lambda packaging

`make_lambda` in `run.sh` zips `handler.py` + `config.py` + `s3_events.py` flat (`zip -j`).
Lambdas with third-party deps (`preprocess`, `profanity`, `sentiment`) use `make_lambda_with_deps`,
which also `pip install`s `requirements.txt` into a `package/` dir and bundles it.
**Unzipped Lambda size limit is 250 MB** (per Tips_and_Tricks.pdf); current packages are ~19 MB.

### Poke at MiniStack by hand

```bash
AWS="aws --endpoint-url=http://localhost:4566"
$AWS dynamodb scan --table-name Reviews --select COUNT
$AWS s3 ls s3://reviews-scored/
$AWS lambda list-functions --query 'Functions[].FunctionName'
$AWS ssm get-parameters-by-path --path /dic-a3 --recursive
curl -s http://localhost:4566/_localstack/health   # which services are up
```

## Known issues / risks (current state — June 2026)

This repo is mid-task: the job is to make the full pipeline run on the cluster and produce the
**final devset results**. Watch out for:

- **`results.json` is a stale PARTIAL placeholder (52,920 / 78,829)** — the real numbers come from a
  full `run_cluster.sh` run on the cluster (resumable; see the cluster section above). The old
  `SKIP=52920` hack is gone; resume is now reviewId-based via `checkpoints/segment_*.json`.
- **NLTK data is now PRE-BUNDLED (no runtime download).** `run.sh`'s `make_lambda_with_deps` takes
  an optional 3rd arg listing NLTK packages and downloads+extracts them into `package/nltk_data`,
  which ships in the zip next to `handler.py`. L1/L3 add that dir to `nltk.data.path` and only fall
  back to a `/tmp` download on a `nltk.data.find` miss — so the cluster Lambda needs **no network**.
  Build gotcha: wordnet/omw must be *extracted* (find can't read an un-extracted zip) but
  `vader_lexicon` must stay *zipped* (VADER loads it via its zip-internal path), so the extraction
  step skips `vader_lexicon.zip`. Verified offline (network forced to fail) and end-to-end.
- **Native wheel / Python-version mismatch.** Bundled `regex` ships only
  `_regex.cpython-310-*.so`, but Lambdas are declared `runtime python3.11`. It works locally
  (MiniStack appears to run Lambdas with the host Python), but a different host Python on the
  cluster could break `import nltk`. Re-run `run.sh` *on the cluster* so `pip install` produces
  matching wheels.
- **Build artifacts are committed.** `lambda.zip` and `package/` dirs (~1,343 tracked files) are in
  git despite `*/package/*` in `.gitignore`, so every `run.sh` produces noisy diffs (tqdm version
  bumps, .so churn). Don't hand-edit them; regenerate via `run.sh`. Consider `git rm --cached` them.
- **Stale README/docs.** [README.md](README.md) references a `cluster/` directory that no longer
  exists (everything is under `src/`) and predates the segment-based resumable run. [src/report.md](src/report.md)
  is a stub (intro only); the real `report.pdf` still needs writing.
- **Upload artifact:** `dic_a3_cluster.zip` (built by hand, ~60 KB, source-only) is the thing to
  upload to the cluster; `run.sh` rebuilds `package/`/`lambda.zip` there.
