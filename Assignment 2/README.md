# Assignment 2: Text Processing and Classification using Apache Spark

TU Wien — Data-Intensive Computing 2026

## What this is

Three-part Spark pipeline over the Amazon Review Dataset:

| Part | What it does | Output |
|------|-------------|--------|
| 1 — RDDs | Chi-square term ranking via Spark RDD API | `outputs/output_rdd.txt` |
| 2 — DataFrame Pipeline | TF-IDF + ChiSqSelector via Spark ML | `outputs/output_ds.txt` |
| 3 — Classifier | OneVsRest LinearSVC + grid search | `outputs/grid_search_results.csv` |

## Prerequisites

**Java** (required by PySpark):
```bash
brew install openjdk@17          # macOS
sudo apt install openjdk-17-jdk  # Ubuntu/Debian
```

**Python dependencies:**
```bash
pip install -r requirements.txt
```

## Run locally

All scripts run from the `Assignment 2/` root and write to `outputs/`.

```bash
src/part1_rdd/run_local.sh       # outputs/output_rdd.txt
src/part2_pipeline/run_local.sh  # outputs/output_ds.txt
```

The local scripts use `Assignment_1/reviews_devset.json` (committed) or fall back to
`../Assignment 1/src/Assignment_1_Assets/reviews_devset.json` if present.

## Run on the TU Wien LBD cluster (YARN)

```bash
# 1. Build zip packages (run once from Assignment 2/ root)
scripts/build_common_zip.sh
( cd src && zip -r ../pipeline.zip part2_pipeline -x 'part2_pipeline/__pycache__/*' )

# 2. Part 1
src/part1_rdd/run_cluster.sh

# 3. Part 2
src/part2_pipeline/run_cluster.sh

# 4. Part 3
src/part3_classifier/run_cluster.sh
```

Output files are fetched from HDFS into `outputs/` by each cluster script.

## Layout

```
Assignment 2/
├── requirements.txt          # Python dependencies
├── src/
│   ├── common/               # shared tokenizer, stopwords, Spark session
│   ├── part1_rdd/            # chi² via RDDs
│   ├── part2_pipeline/       # TF-IDF Spark ML pipeline
│   └── part3_classifier/     # SVM classifier + grid search
├── outputs/                  # generated files (git-ignored)
├── report/                   # report.md / report.pdf
├── Assignment_1/             # reference output.txt from Assignment 1
└── scripts/                  # helper scripts (build_common_zip.sh)
```

See [src/README.md](src/README.md) for detailed per-part instructions.
