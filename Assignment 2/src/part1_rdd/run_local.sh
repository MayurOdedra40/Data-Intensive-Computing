#!/usr/bin/env bash
# Run Part 1 locally against the dev set.
# Java 11 (PySpark 3.5 doesn't support Java 25); SPARK_LOCAL_IP avoids
# WSL2 hostname binding errors.
set -euo pipefail

cd "$(dirname "$0")/../.."

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_IP=127.0.0.1

python3 src/part1_rdd/chi_square_rdd.py \
  --mode local \
  --input "../Assignment 1/src/Assignment_1_Assets/reviews_devset.json" \
  --stopwords src/common/stopwords.txt \
  --output outputs/output_rdd.txt
