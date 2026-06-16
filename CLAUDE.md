## Assignment 3 — Serverless review pipeline

Full detail is in [Assignment3/CLAUDE.md](Assignment3/CLAUDE.md). Quick reference:

### Provision + run locally

```bash
ministack                                              # terminal 1 — keep open
bash Assignment3/src/run.sh                           # terminal 2 — provision everything
python Assignment3/src/loader.py Assignment3/data/reviews_devset.json 1  # smoke test
python Assignment3/src/generate_results.py            # write results.json
```

### Run on LBD cluster

```bash
ministack                                              # terminal 1
bash Assignment3/src/run_cluster.sh                   # terminal 2 — end-to-end
```

Overrides: `REVIEW_COUNT`, `BATCH_SIZE`, `BATCH_DELAY`, `MINISTACK_ENDPOINT`.

### Integration tests (require MiniStack already running + `run.sh` applied)

```bash
cd Assignment3/src
pytest tests/test_integration.py -v
pytest tests/test_integration.py::test_smoke_one_review_reaches_reviews_table   # single test
```

### Pipeline shape

```
reviews-ingest → L1 preprocess → reviews-preprocessed
               → L2 profanity  → reviews-profanity
               → L3 sentiment  → reviews-scored
               → L4 aggregate  → DynamoDB (Reviews, Customers)
L5 report (on-demand, not in chain)
```

Chain links are S3→Lambda `ObjectCreated` notifications wired by `run.sh`. Every Lambda reads its config (bucket names, thresholds) exclusively from SSM — nothing is hardcoded. MiniStack is ephemeral; re-run `run.sh` after every restart.

### Key shared modules

- `src/common/config.py` — `get()` / `get_int()` / `get_float()` — SSM Parameter Store wrapper used by all handlers. `STAGE=local` (set by `run.sh` on every Lambda) switches boto3 to `http://localhost:4566`.
- `src/common/s3_events.py` — `parse_records(event)`, `read_envelope()`, `write_envelope()` — S3 event parsing and envelope I/O reused by every handler.
- SSM namespace is `/dic-a3/...`; full parameter table is in [Assignment3/CONTRACT.md](Assignment3/CONTRACT.md).

### Lambda packaging

`make_lambda` in `run.sh` zips `handler.py` + `config.py` + `s3_events.py` flat (`zip -j`). Lambdas with third-party dependencies (`preprocess`, `profanity`, `sentiment`) use `make_lambda_with_deps`, which also `pip install`s `requirements.txt` into a `package/` dir and bundles it.

---

## Shared dataset notes

- `reviews_devset.json`: 78,829 JSON-lines records (one Amazon review per line).
- Local copies live at `Assignment 2/Assignment_1/reviews_devset.json` and `Assignment3/data/reviews_devset.json`.
- On the LBD cluster both assignments fetch it from `hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json`.
