#!/usr/bin/env bash
#
# run_cluster.sh -- deploy and run the full pipeline on the LBD cluster.
#
# The LBD cluster has MiniStack pre-installed (no venv / no pip install needed for the
# host environment). This script:
#   1. Checks MiniStack is reachable.
#   2. Fetches reviews_devset.json from HDFS if not already on disk.
#   3. Provisions all AWS resources by calling run.sh.
#   4. Loads the devset reviews into the pipeline via loader.py.
#   5. Polls until all reviews appear in DynamoDB.
#   6. Prints the final result summary.
#
# Prerequisites (do these BEFORE running this script):
#   ministack          # start MiniStack in a separate terminal
#
# Usage (from the repo root or from anywhere):
#   bash src/run_cluster.sh
#
# Overrides (environment variables):
#   REVIEW_COUNT=100   bash src/run_cluster.sh   # load only N reviews (handy for smoke tests)
#   HDFS_DATA_PATH=/some/other/path.json  bash src/run_cluster.sh
#   MINISTACK_ENDPOINT=http://localhost:4566  bash src/run_cluster.sh

set -e
cd "$(dirname "$0")" || exit 1

# ── Credentials (same dummy values as run.sh / MiniStack expects) ─────────────
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_PAGER=""
export MINISTACK_ENDPOINT="${MINISTACK_ENDPOINT:-http://localhost:4566}"

AWS="aws --endpoint-url=${MINISTACK_ENDPOINT}"

# ── 1. MiniStack health check ─────────────────────────────────────────────────
echo "==> [0/4] Checking MiniStack at ${MINISTACK_ENDPOINT} ..."
if ! ${AWS} s3 ls > /dev/null 2>&1; then
    echo ""
    echo "ERROR: MiniStack is not reachable at ${MINISTACK_ENDPOINT}."
    echo "       Start it first in a separate terminal:"
    echo ""
    echo "         ministack"
    echo ""
    exit 1
fi
echo "    MiniStack is up."

# ── 2. Dataset: fetch from HDFS if not already on disk ───────────────────────
DATA_FILE="../data/reviews_devset.json"
# Shared dataset location on the LBD cluster (same as Assignment 2).
HDFS_DATASET="${HDFS_DATA_PATH:-hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json}"

echo "==> [1/4] Dataset"
if [ -f "${DATA_FILE}" ]; then
    echo "    Already on disk: ${DATA_FILE}"
else
    echo "    Fetching from HDFS: ${HDFS_DATASET}"
    mkdir -p "../data"
    if ! hdfs dfs -get "${HDFS_DATASET}" "${DATA_FILE}"; then
        echo ""
        echo "ERROR: Could not fetch ${HDFS_DATASET} from HDFS."
        echo "       Check the path or copy the file manually to ${DATA_FILE}"
        exit 1
    fi
    echo "    Saved to ${DATA_FILE}"
fi

# ── 3. Provision: buckets, SSM params, DynamoDB tables, Lambdas, notifications ─
echo ""
echo "==> [2/4] Provisioning (run.sh)"
# run.sh is idempotent and does its own cd, so calling it here is safe.
bash run.sh

# ── 4. Load reviews into the pipeline (in batches to avoid memory errors) ────
REVIEW_COUNT="${REVIEW_COUNT:-78829}"
BATCH_SIZE="${BATCH_SIZE:-200}"
BATCH_DELAY="${BATCH_DELAY:-30}"
SKIP="${SKIP:-52920}"
echo ""
echo "==> [3/4] Loading ${REVIEW_COUNT} review(s) in batches of ${BATCH_SIZE} (skip=${SKIP}) ..."
echo "    (pausing ${BATCH_DELAY}s between batches to let the pipeline drain)"
python3 loader.py "${DATA_FILE}" "${REVIEW_COUNT}" \
  --batch-size "${BATCH_SIZE}" \
  --batch-delay "${BATCH_DELAY}" \
  --skip "${SKIP}"

# ── 5. Poll until all reviews appear in the Reviews table ────────────────────
echo ""
echo "==> [4/4] Waiting for pipeline to finish ..."
echo "    (each review makes 4 Lambda hops; large batches take several minutes)"
echo ""

REVIEWS_TABLE=$(${AWS} ssm get-parameter \
    --name /dic-a3/tables/reviews \
    --query 'Parameter.Value' --output text)

SCORED_BUCKET=$(${AWS} ssm get-parameter \
    --name /dic-a3/buckets/scored \
    --query 'Parameter.Value' --output text)

TIMEOUT=1200   # 20 minutes -- generous for 78k reviews on a shared cluster
INTERVAL=15
ELAPSED=0

while [ "${ELAPSED}" -lt "${TIMEOUT}" ]; do
    SCORED=$(${AWS} s3 ls "s3://${SCORED_BUCKET}/" --recursive 2>/dev/null \
             | wc -l | tr -d ' ')
    DONE=$(${AWS} dynamodb scan \
             --table-name "${REVIEWS_TABLE}" \
             --filter-expression "#src = :s" \
             --expression-attribute-names '{"#src":"source"}' \
             --expression-attribute-values '{":s":{"S":"devset"}}' \
             --select COUNT \
             --query 'Count' --output text 2>/dev/null || echo 0)
    printf "    scored=%-6s  in DynamoDB=%-6s  (target=%s)\n" \
        "${SCORED}" "${DONE}" "${REVIEW_COUNT}"
    if [ "${DONE}" -ge "${REVIEW_COUNT}" ]; then
        echo ""
        echo "    All reviews processed."
        break
    fi
    sleep "${INTERVAL}"
    ELAPSED=$((ELAPSED + INTERVAL))
done

if [ "${ELAPSED}" -ge "${TIMEOUT}" ]; then
    echo ""
    echo "WARNING: Timeout reached. The pipeline may still be processing."
    echo "         Re-run the results section below manually when it finishes."
fi

# ── 6. Results summary ────────────────────────────────────────────────────────
echo ""
echo "=========================================="
echo "  RESULTS  (source=devset only)"
echo "=========================================="

CUSTOMERS_TABLE=$(${AWS} ssm get-parameter \
    --name /dic-a3/tables/customers \
    --query 'Parameter.Value' --output text)

# sentiment counts
POS=$(${AWS} dynamodb scan \
        --table-name "${REVIEWS_TABLE}" \
        --filter-expression "#src = :s AND sentiment = :v" \
        --expression-attribute-names '{"#src":"source"}' \
        --expression-attribute-values '{":s":{"S":"devset"},":v":{"S":"positive"}}' \
        --select COUNT --query 'Count' --output text)

NEU=$(${AWS} dynamodb scan \
        --table-name "${REVIEWS_TABLE}" \
        --filter-expression "#src = :s AND sentiment = :v" \
        --expression-attribute-names '{"#src":"source"}' \
        --expression-attribute-values '{":s":{"S":"devset"},":v":{"S":"neutral"}}' \
        --select COUNT --query 'Count' --output text)

NEG=$(${AWS} dynamodb scan \
        --table-name "${REVIEWS_TABLE}" \
        --filter-expression "#src = :s AND sentiment = :v" \
        --expression-attribute-names '{"#src":"source"}' \
        --expression-attribute-values '{":s":{"S":"devset"},":v":{"S":"negative"}}' \
        --select COUNT --query 'Count' --output text)

# profanity count
PROF=$(${AWS} dynamodb scan \
         --table-name "${REVIEWS_TABLE}" \
         --filter-expression "#src = :s AND isProfane = :v" \
         --expression-attribute-names '{"#src":"source"}' \
         --expression-attribute-values '{":s":{"S":"devset"},":v":{"BOOL":true}}' \
         --select COUNT --query 'Count' --output text)

# banned customers
BANNED=$(${AWS} dynamodb scan \
           --table-name "${CUSTOMERS_TABLE}" \
           --filter-expression "banned = :v" \
           --expression-attribute-values '{":v":{"BOOL":true}}' \
           --select COUNT --query 'Count' --output text)

TOTAL=$((POS + NEU + NEG))
echo "  Total reviews in DynamoDB : ${TOTAL}"
echo "  Positive                  : ${POS}"
echo "  Neutral                   : ${NEU}"
echo "  Negative                  : ${NEG}"
echo "  Failing profanity check   : ${PROF}"
echo "  Banned customers          : ${BANNED}"
echo "=========================================="