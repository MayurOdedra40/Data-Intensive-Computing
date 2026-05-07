# Part 2 — DataFrame pipeline (TF-IDF + ChiSqSelector)

**What**: builds a Spark ML `Pipeline` that turns raw `reviewText` into TF-IDF feature vectors, then keeps the 2000 vocabulary terms with the highest chi² association to the `category` label. Writes the selected terms to `outputs/output_ds.txt`.

**Input**: JSON-lines Amazon reviews (`reviewText`, `category`).

**Output**: `outputs/output_ds.txt` — one line, 2000 alphabetical space-separated terms. Format mirrors the merged-dictionary line at the bottom of Assignment 1's `output.txt` so `comm` / set-overlap comparison is direct.

## Pipeline shape

| # | Stage | Notes |
|---|-------|-------|
| 0 | `RegexTokenizer` | Pattern from `common.text_utils.REGEX_TOKENIZER_PATTERN` (locked with Part 1). `gaps=True`, `toLowercase=True`, `minTokenLength=2`. |
| 1 | `StopWordsRemover` | Seeded from `common/stopwords.txt` (596 entries). |
| 2 | `CountVectorizer` | Raw TF. `CountVectorizer` not `HashingTF` — we need the vocabulary back to write `output_ds.txt`. |
| 3 | `IDF` | Standard inverse document frequency. |
| 4 | `StringIndexer` | `category` → numeric `label` (required by ChiSqSelector). |
| 5 | `ChiSqSelector` | `numTopFeatures=2000`. Picks vocabulary indices, looked up in stage-2's vocabulary to recover terms. |

## Factory contract for Part 3

`build_feature_pipeline(num_top_features: int = 2000, stopwords: list[str] | None = None) -> Pipeline` returns the **unfit** pipeline above. Part 3 (Person D) reuses this factory with a smaller `num_top_features` for one arm of the grid search and extends the stage list with `Normalizer(L2)` + `OneVsRest(LinearSVC(...))`. Stage indices are exported as constants (`TOKENIZER_STAGE` ... `SELECTOR_STAGE`) so D can introspect a fitted pipeline without hard-coded numbers.

## Run locally

```bash
src/part2_pipeline/run_local.sh
```

Requires Java 11 + a Python with PySpark 3.5 importable. WSL2 binding errors are pre-handled via `SPARK_LOCAL_IP=127.0.0.1`.

## Run on the cluster

```bash
bash scripts/build_common_zip.sh
spark-submit --master yarn --deploy-mode cluster \
  --py-files common.zip \
  --files src/common/stopwords.txt \
  src/part2_pipeline/pipeline.py \
  --mode cluster \
  --input hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
  --stopwords stopwords.txt \
  --output output_ds.txt
```

`--py-files common.zip` ships the `common/` package to executors. `--files stopwords.txt` lands at executor cwd, so `--stopwords` must be the bare filename in cluster mode. `--num-top-features` is optional (defaults to 2000).

## Comparison vs Part 1 / Assignment 1 (for the report)

Per the spec, results from Part 2 will *not* be identical to Part 1 — the selection criteria differ:

- **Part 1 / Assignment 1**: top 75 chi² terms per category, then union (~1418 terms in A1's merged dictionary).
- **Part 2**: top 2000 chi² terms over a single global ranking computed on TF-IDF vectors with a numeric label.

Other expected sources of small drift:

- `RegexTokenizer(toLowercase=True)` calls Java `String.toLowerCase()`; Part 1 calls Python `str.casefold()`. Diverges only on edge cases like `ß → ss`. Amazon reviews are ASCII-dominant so the impact is near-zero.
- Part 2 has no per-document deduplication of tokens (`CountVectorizer` produces raw counts). Part 1 dedupes per-document for chi² presence semantics.

The notebook's last cell prints the overlap percentage and a sample of terms unique to each side — that's the data point for the report's comparison paragraph.
