# Part 1 — RDD chi²

**What**: per-category top-75 terms by chi² association, computed with Spark RDDs. Reproduces Assignment 1's `output.txt`.

**Input**: JSON-lines Amazon reviews (`reviewText`, `category`).

**Output**: `outputs/output_rdd.txt` — 23 lines.
- Lines 1–22: `<category> term:chi2 term:chi2 ...` (75 terms per category, sorted by chi² desc, alphabetical tiebreak).
- Line 23: alphabetical union of all surviving terms.

## How it works

1. Tokenize `reviewText` (regex split, casefold, len>1, drop stopwords, **dedupe per document** — chi² counts presence, not frequency).
2. Cache `(category, deduped_tokens)`.
3. Single `flatMap` emits both `("T", term)` and `("TC", term, cat)` keys → one `reduceByKey` produces `n_t` and `n_tc` together.
4. `join` on term to attach `n_t` to each `(term, cat)` row, then map to chi² scores.
5. `aggregateByKey` with a bounded min-heap (size 75) selects top-K per category — single shuffle, no `groupByKey`.
6. Driver-side `collectAsMap` (22 × 75 = tiny) → format and write.

Total: 4 shuffles, 1 cache. `N` and per-category `n_c` count every parsed record (matches Assignment 1).

## Run locally

```bash
src/part1_rdd/run_local.sh
```

## Run on the cluster

```bash
( cd src && zip -r ../common.zip common )

spark-submit --master yarn --deploy-mode cluster \
  --py-files common.zip \
  --files src/common/stopwords.txt \
  src/part1_rdd/chi_square_rdd.py \
  --mode cluster \
  --input hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
  --stopwords stopwords.txt \
  --output output_rdd.txt
```

`--py-files common.zip` ships the `common/` package to executors. `--files stopwords.txt` lands at executor cwd, so `--stopwords` must be the bare filename in cluster mode.
