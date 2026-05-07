#!/usr/bin/env bash
# Run Part 2 locally against the dev set.
#
# - JAVA_HOME: prefer Java 11 (cluster-canonical, required by PySpark 3.5);
#   fall back to Java 25 if Java 11 isn't installed (works with PySpark 4.x).
# - Python: prefer ../../.venv/bin/python3 if present (where pyspark is pip-installed);
#   fall back to system python3.
# - SPARK_LOCAL_IP avoids WSL2 hostname binding errors.
set -euo pipefail

cd "$(dirname "$0")/../.."

# Search order: cluster-canonical Java 11 → user-local Java 17 (works
# with PySpark 4.x) → /usr/lib system JDKs. Java 25 is intentionally
# skipped — Hadoop's UGI calls Subject.getSubject which is no-op since
# Java 24, breaking spark.read.* I/O.
for cand in \
  /usr/lib/jvm/java-11-openjdk-amd64 \
  /home/andar/.local/jdk17/usr/lib/jvm/java-17-openjdk-amd64 \
  /usr/lib/jvm/java-17-openjdk-amd64 \
  ; do
  if [[ -x "$cand/bin/java" ]]; then
    export JAVA_HOME="$cand"
    break
  fi
done
if [[ -n "${JAVA_HOME:-}" ]]; then
  export PATH="$JAVA_HOME/bin:$PATH"
fi
export SPARK_LOCAL_IP=127.0.0.1

# The .venv is two levels up (sibling of Data-Intensive-Computing/, not
# inside it). Fall back to system python3 if pyspark isn't there.
for cand in \
  ../../.venv/bin/python3 \
  ../.venv/bin/python3 \
  ; do
  if [[ -x "$cand" ]] && "$cand" -c 'import pyspark' 2>/dev/null; then
    PY="$cand"
    break
  fi
done
PY="${PY:-python3}"

# Dev-set search order: local copy under Assignment_1/ (the path the user
# checked in alongside output.txt), then the Assignment 1 assets folder.
for cand in \
  "Assignment_1/reviews_devset.json" \
  "../Assignment 1/src/Assignment_1_Assets/reviews_devset.json" \
  ; do
  if [[ -f "$cand" ]]; then
    INPUT="$cand"
    break
  fi
done
if [[ -z "${INPUT:-}" ]]; then
  echo "run_local.sh: could not find reviews_devset.json" >&2
  exit 1
fi

"$PY" src/part2_pipeline/pipeline.py \
  --mode local \
  --input "$INPUT" \
  --stopwords src/common/stopwords.txt \
  --output outputs/output_ds.txt
