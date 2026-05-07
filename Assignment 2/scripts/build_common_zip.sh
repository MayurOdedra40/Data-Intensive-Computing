#!/usr/bin/env bash
# Build common.zip for spark-submit --py-files.
#
# Resulting common.zip contains src/common/ as a top-level `common/` package
# so executors can `from common.text_utils import ...` after Spark drops it
# on PYTHONPATH. Re-run any time text_utils.py / data_loader.py change.
set -euo pipefail

# Run from the Assignment 2 root regardless of where the user invokes us from.
cd "$(dirname "$0")/.."

if [[ ! -d src/common ]]; then
  echo "build_common_zip.sh: src/common not found from $(pwd)" >&2
  exit 1
fi

rm -f common.zip
( cd src && zip -qr ../common.zip common -x 'common/__pycache__/*' 'common/*.pyc' )

echo "wrote $(pwd)/common.zip ($(du -h common.zip | cut -f1))"
echo "ship to executors with:  spark-submit --py-files common.zip ..."
