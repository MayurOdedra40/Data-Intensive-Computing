# `src/` — Assignment 2 source

Spark implementation of the Amazon Reviews chi² + TF-IDF + SVM pipeline. Three parts share one `common/` module so tokenisation, stopwords, and I/O stay aligned across the RDD job (Part 1), the DataFrame pipeline (Part 2), and the classifier (Part 3).

## Layout

```
src/
├── common/          # shared utilities (tokenizer regex, stopwords, JSON loader, Spark builder)
├── part1_rdd/       # Part 1 — chi² via RDDs → output_rdd.txt
├── part2_pipeline/  # Part 2 — Spark ML pipeline → output_ds.txt
└── part3_classifier/# Part 3 — OneVsRest LinearSVC + grid search
```

Outputs (`output_rdd.txt`, `output_ds.txt`, `grid_search_results.csv`) are written to `../outputs/` at the Assignment 2 root, not inside `src/`. The frozen Assignment 1 reference (`output.txt`) lives at `../Assignment_1/output.txt` and is the single source of truth for any "compare to Assignment 1" report sections.

## Quick start (local)

Part 1 has a one-command runner that handles the Java 11 + WSL2 loopback prereqs:

```bash
src/part1_rdd/run_local.sh
```

It writes `outputs/output_rdd.txt`. The script auto-detects the dev set (checks `Assignment_1/reviews_devset.json` first, then `../Assignment 1/src/Assignment_1_Assets/reviews_devset.json`) and auto-detects Java (Homebrew OpenJDK 17 on macOS, system JDK on Linux). Java 17 is recommended; Java 25+ is not supported by PySpark 3.5.

## Cluster submission (YARN)

Spark needs the `common/` package on every executor. Build a zip once, ship it with `--py-files`, and ship `stopwords.txt` separately with `--files` so it lands at executor cwd:

```bash
# 1. Build common.zip (run from the Assignment 2 root)
( cd src && zip -r ../common.zip common )

# 2. Submit (Part 1 example — Parts 2 & 3 follow the same pattern)
spark-submit --master yarn --deploy-mode cluster \
  --py-files common.zip \
  --files src/common/stopwords.txt \
  src/part1_rdd/chi_square_rdd.py \
  --mode cluster \
  --input hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
  --stopwords stopwords.txt \
  --output output_rdd.txt
```

In `--mode cluster` the `--stopwords` argument is the **bare filename** (`stopwords.txt`), not a path — it resolves against the executor cwd where `--files` deposited it.

## Dataset paths

| Mode    | Path                                                                    |
|---------|-------------------------------------------------------------------------|
| local   | `Assignment_1/reviews_devset.json` (committed, ~23 reviews — smoke-test only) |
| local (full dev) | `../Assignment 1/src/Assignment_1_Assets/reviews_devset.json` |
| cluster | `hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json`            |
| full    | `hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json` (optional)|

> **Note:** `Assignment_1/reviews_devset.json` is a tiny committed copy for quick local smoke-tests only. All real results (including the submitted output files) must be produced from the full dev set — use the HDFS path on the cluster or the Assignment 1 assets path locally.

Per the spec, all submission artefacts must be produced from the **dev set**, including comparisons against Assignment 1. The full combined set is optional.

## JupyterHub etiquette (LBD cluster)

The cluster has a 48h kernel time limit, gracefully extended from the cluster-wide 2h. Burning that quota with idle kernels makes the cluster unusable for everyone. Hard rules from the spec:

- **Stop the SparkContext** at the end of every analysis (`spark.stop()` or restart kernel).
- **Shut down the kernel** when done — closing the browser tab is not enough.
- **Test on a small sample first.** Don't iterate against the full dev set with `take(1)` storms.
- **No `collect()` on unbounded data.** Use `reduceByKey`, `aggregateByKey`, bounded heaps. The same applies to `toPandas()`.
- For long-running jobs, prefer `spark-submit` over interactive notebooks.

## Where to look next

- [common/README.md](common/README.md) — the locked tokenizer regex, stopwords source, and the `casefold` vs `toLowercase` caveat that explains small Part 1 / Part 2 term-set drift.
- [part1_rdd/README.md](part1_rdd/README.md) — Part 1 pipeline shape (4 shuffles, 1 cache) and the bounded-heap top-K trick.
- [../tasks.md](../tasks.md) — task division across the 5 group members.
- [../Assignment_2_Instructions.pdf](../Assignment_2_Instructions.pdf) — official spec.
