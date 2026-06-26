#!/usr/bin/env bash
#
# run_cluster.sh -- deploy and run the full pipeline on the LBD cluster, RESUMABLY.
#
# The long full-devset run (78,829 reviews, ~20+ min on a shared cluster) can be interrupted, and
# MiniStack itself is ephemeral (a restart wipes its S3/DynamoDB). To survive BOTH, every run:
#   * uses a FRESH, empty DynamoDB as scratch (run.sh wipes the tables);
#   * processes ONLY the reviews not already covered by earlier runs (the loader skips their
#     reviewIds, read from earlier segment files);
#   * has a background checkpointer that snapshots this run's state to its OWN segment file on disk
#     (checkpoints/segment_NNN.json) every few seconds.
# Because the runs cover disjoint reviews, the final results are just the sum of all segments
# (finalize_results.py). Re-running after ANY interruption -- driver death OR a MiniStack restart --
# simply adds another segment for whatever is still missing. See checkpoints.py for the full model.
#
# Prerequisites (do this BEFORE running this script):
#   ministack          # start MiniStack in a separate terminal
#
# Usage (from the repo root or from anywhere):
#   bash src/run_cluster.sh
#
# Overrides (environment variables):
#   REVIEW_COUNT=78829          total devset reviews to end up with
#   BATCH_SIZE=200              upload this many, then pause
#   BATCH_DELAY=30             seconds to pause between batches (let the chain drain)
#   CHECKPOINT_INTERVAL=30     seconds between snapshots of this run's segment file
#   MINISTACK_ENDPOINT=http://localhost:4566
#   HDFS_DATA_PATH=/some/other/path.json
#   FORCE_FRESH=1              delete all segments and start completely over

cd "$(dirname "$0")" || exit 1

# ── Credentials (same dummy values as run.sh / MiniStack expects) ─────────────
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_PAGER=""
export MINISTACK_ENDPOINT="${MINISTACK_ENDPOINT:-http://localhost:4566}"

AWS="aws --endpoint-url=${MINISTACK_ENDPOINT}"

# Persistent state + results live one level up from src/ (= Assignment3/), on the host disk so they
# survive a MiniStack restart.
RESULTS_DIR="$(cd .. && pwd)"
CHECKPOINTS_DIR="${RESULTS_DIR}/checkpoints"
RESULTS_FILE="${RESULTS_DIR}/results.json"

# Tunables
REVIEW_COUNT="${REVIEW_COUNT:-78829}"
BATCH_SIZE="${BATCH_SIZE:-200}"
BATCH_DELAY="${BATCH_DELAY:-30}"
CHECKPOINT_INTERVAL="${CHECKPOINT_INTERVAL:-30}"
WARMUP_DELAY="${WARMUP_DELAY:-15}"   # drop 1 review, then wait this long for the Lambdas to cold-start

set -e   # abort on errors during setup (relaxed later, before the polling/cleanup section)

# ── 1. MiniStack health check ─────────────────────────────────────────────────
echo "==> [0/5] Checking MiniStack at ${MINISTACK_ENDPOINT} ..."
if ! ${AWS} s3 ls > /dev/null 2>&1; then
    echo ""
    echo "ERROR: MiniStack is not reachable at ${MINISTACK_ENDPOINT}."
    echo "       Start it first in a separate terminal:   ministack"
    echo ""
    exit 1
fi
echo "    MiniStack is up."

# ── 2. Dataset: fetch from HDFS if not already on disk ───────────────────────
DATA_FILE="../data/reviews_devset.json"
HDFS_DATASET="${HDFS_DATA_PATH:-hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json}"

echo "==> [1/5] Dataset"
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

# ── 3. Work out what's left to do (from existing segments) ───────────────────
echo "==> [2/5] Run mode"
if [ "${FORCE_FRESH:-0}" = "1" ]; then
    echo "    FORCE_FRESH=1 -- deleting all segments in ${CHECKPOINTS_DIR}"
    rm -rf "${CHECKPOINTS_DIR}"
fi
mkdir -p "${CHECKPOINTS_DIR}"

PRIOR=$(python3 checkpoints.py count "${CHECKPOINTS_DIR}")
REMAINING=$(( REVIEW_COUNT - PRIOR ))
[ "${REMAINING}" -lt 0 ] && REMAINING=0
echo "    Already processed by earlier runs : ${PRIOR}"
echo "    Remaining to process this run      : ${REMAINING}  (target total ${REVIEW_COUNT})"

if [ "${REMAINING}" -eq 0 ]; then
    echo "    Nothing left to do -- jumping straight to results."
    python3 finalize_results.py --checkpoints-dir "${CHECKPOINTS_DIR}" --out "${RESULTS_FILE}" \
        --upload --endpoint "${MINISTACK_ENDPOINT}" || \
    python3 finalize_results.py --checkpoints-dir "${CHECKPOINTS_DIR}" --out "${RESULTS_FILE}"
    echo "Done. Results: ${RESULTS_FILE}"
    exit 0
fi

SEG_INDEX=$(python3 checkpoints.py next-index "${CHECKPOINTS_DIR}")
SEG_FILE=$(printf "%s/segment_%03d.json" "${CHECKPOINTS_DIR}" "${SEG_INDEX}")
SKIP_IDS_FILE="${CHECKPOINTS_DIR}/.processed_ids.$$"
python3 checkpoints.py ids "${CHECKPOINTS_DIR}" "${SKIP_IDS_FILE}"
echo "    This run writes segment: ${SEG_FILE}"

# ── 4. Provision a FRESH MiniStack world (wipes the tables -- safe; state is in segments) ─────
echo ""
echo "==> [3/5] Provisioning (run.sh)"
bash run.sh

REVIEWS_TABLE=$(${AWS} ssm get-parameter --name /dic-a3/tables/reviews \
    --query 'Parameter.Value' --output text)

# Everything below is the live run: transient errors (a scan blip, killing the checkpointer) must
# NOT abort us, so relax set -e here.
set +e

# ── 5. Start this run's background checkpointer ──────────────────────────────
echo ""
echo "==> [4/5] Starting checkpointer (every ${CHECKPOINT_INTERVAL}s -> ${SEG_FILE})"
python3 checkpointer.py --interval "${CHECKPOINT_INTERVAL}" \
    --endpoint "${MINISTACK_ENDPOINT}" --out "${SEG_FILE}" &
CKPT_PID=$!
trap 'kill "${CKPT_PID}" 2>/dev/null' EXIT INT TERM

# ── 5b. Warmup: drop a SINGLE review and pause, so each Lambda cold-starts ONCE (loading NLTK /
#        VADER / the profanity word-list / SSM config) BEFORE the main load floods the pipeline.
#        The warmup review is the first not-yet-processed one; the main load below re-includes it,
#        which is harmless -- the aggregate idempotency gate dedups it, so counts are unaffected.
echo ""
echo "==> Warmup: dropping 1 review, then pausing ${WARMUP_DELAY}s for Lambda cold starts ..."
python3 loader.py "${DATA_FILE}" 1 \
    --batch-size 1 \
    --batch-delay 0 \
    --skip-ids-file "${SKIP_IDS_FILE}"
sleep "${WARMUP_DELAY}"

# ── 6. Load the remaining reviews (skipping anything earlier runs already processed) ─────────
echo ""
echo "==> [5/5] Loading ${REMAINING} review(s) in batches of ${BATCH_SIZE} ..."
echo "    (pausing ${BATCH_DELAY}s between batches to let the pipeline drain)"
python3 loader.py "${DATA_FILE}" "${REMAINING}" \
    --batch-size "${BATCH_SIZE}" \
    --batch-delay "${BATCH_DELAY}" \
    --skip-ids-file "${SKIP_IDS_FILE}"

# ── 7. Wait until this run's reviews all appear in the (fresh) Reviews table ──────────────────
echo ""
echo "==> Waiting for this run to finish (target=${REMAINING} new reviews) ..."
echo "    (each review makes 4 Lambda hops; large runs take several minutes)"
INTERVAL=15
STALL_LIMIT=300        # give up if the count makes NO progress for this many seconds
TIMEOUT=3600           # hard ceiling
ELAPSED=0
STALLED=0
LAST_DONE=-1
while [ "${ELAPSED}" -lt "${TIMEOUT}" ]; do
    DONE=$(${AWS} dynamodb scan --table-name "${REVIEWS_TABLE}" \
             --filter-expression "#src = :s" \
             --expression-attribute-names '{"#src":"source"}' \
             --expression-attribute-values '{":s":{"S":"devset"}}' \
             --select COUNT --query 'Count' --output text 2>/dev/null || echo 0)
    printf "    this run in DynamoDB=%-7s  (target=%s)\n" "${DONE}" "${REMAINING}"
    if [ "${DONE:-0}" -ge "${REMAINING}" ]; then
        echo "    This run's reviews are all processed."
        break
    fi
    if [ "${DONE}" -le "${LAST_DONE}" ]; then
        STALLED=$(( STALLED + INTERVAL ))
        if [ "${STALLED}" -ge "${STALL_LIMIT}" ]; then
            echo ""
            echo "    WARNING: no progress for ${STALL_LIMIT}s (stuck at ${DONE}). Stopping the wait."
            echo "             Re-run this script to add another segment for what's still missing."
            break
        fi
    else
        STALLED=0
    fi
    LAST_DONE="${DONE}"
    sleep "${INTERVAL}"
    ELAPSED=$(( ELAPSED + INTERVAL ))
done

# ── 8. Stop the checkpointer, take a final snapshot of this segment, combine all segments ─────
echo ""
echo "==> Stopping checkpointer and finalizing ..."
kill "${CKPT_PID}" 2>/dev/null
wait "${CKPT_PID}" 2>/dev/null
trap - EXIT INT TERM
rm -f "${SKIP_IDS_FILE}"

# One last authoritative snapshot of THIS run's segment now that loading is done.
python3 checkpointer.py --once --endpoint "${MINISTACK_ENDPOINT}" --out "${SEG_FILE}"

# Final results = sum of all segments (also uploaded to the results-export bucket).
python3 finalize_results.py --checkpoints-dir "${CHECKPOINTS_DIR}" --out "${RESULTS_FILE}" \
    --upload --endpoint "${MINISTACK_ENDPOINT}"

echo "Done. Results: ${RESULTS_FILE}   (segments in ${CHECKPOINTS_DIR})"
