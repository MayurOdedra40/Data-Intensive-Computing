#!/usr/bin/env bash
#
# run.sh -- provision EVERYTHING this serverless app needs on MiniStack.
#
# MiniStack is ephemeral: when you stop and restart it, all buckets/tables/lambdas are gone.
# So this script must recreate the whole world, and it must be safe to run again and again
# (idempotent). We achieve that by deleting-before-creating tables and lambdas, using
# `ssm put-parameter --overwrite`, and tolerating "bucket already exists".
#
# Run it (from the repo root or anywhere):   bash src/run.sh
# Disable the optional DynamoDB-stream trigger:   ENABLE_STREAM_TRIGGER=0 bash src/run.sh
#
# NOTE: we deliberately do NOT use `set -e`. On an ephemeral emulator we re-run this all the
# time, and a harmless "already exists" must not abort the whole script.

# Make every relative path below relative to THIS script's folder (src/), no matter where
# the script is called from.
cd "$(dirname "$0")" || exit 1

# ---------------------------------------------------------------------------------------
# 0. Environment: dummy credentials + the MiniStack endpoint. (Same idea as the tutorial.)
# ---------------------------------------------------------------------------------------
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_PAGER=""          # IMPORTANT: stop the AWS CLI from opening a pager that would hang a script
export MINISTACK_ENDPOINT="${MINISTACK_ENDPOINT:-http://localhost:4566}"

# Every AWS command goes through this alias so it talks to MiniStack, not real AWS.
AWS="aws --endpoint-url=${MINISTACK_ENDPOINT}"

ENABLE_STREAM_TRIGGER="${ENABLE_STREAM_TRIGGER:-1}"   # 1 = also wire the DynamoDB-stream trigger

echo "==> Provisioning on ${MINISTACK_ENDPOINT}"

# ---------------------------------------------------------------------------------------
# 1. S3 buckets (one per pipeline stage). `s3 mb` on an existing bucket is a harmless no-op.
# ---------------------------------------------------------------------------------------
echo "==> [1/6] S3 buckets"
for B in reviews-ingest reviews-preprocessed reviews-profanity reviews-scored results-export; do
  if ${AWS} s3 mb "s3://${B}" >/dev/null 2>&1; then
    echo "  created bucket: ${B}"
  else
    echo "  bucket already exists: ${B}"
  fi
done

# ---------------------------------------------------------------------------------------
# 2. SSM parameters. `--overwrite` makes re-runs update in place instead of erroring.
#    These are the ONLY place bucket/table names and tunables are defined.
# ---------------------------------------------------------------------------------------
echo "==> [2/6] SSM parameters"
put_param () {  # $1 = name, $2 = value
  ${AWS} ssm put-parameter --overwrite --type String --name "$1" --value "$2" >/dev/null
  echo "  ${1} = ${2}"
}
put_param /dic-a3/buckets/ingest        reviews-ingest
put_param /dic-a3/buckets/preprocessed  reviews-preprocessed
put_param /dic-a3/buckets/profanity     reviews-profanity
put_param /dic-a3/buckets/scored        reviews-scored
put_param /dic-a3/buckets/export        results-export
put_param /dic-a3/tables/reviews        Reviews
put_param /dic-a3/tables/customers      Customers
put_param /dic-a3/config/ban-threshold  3
put_param /dic-a3/config/sentiment-pos  0.05
put_param /dic-a3/config/sentiment-neg  -0.05
put_param /dic-a3/config/overall-weight 0.3

# ---------------------------------------------------------------------------------------
# 3. DynamoDB tables. Delete-before-create gives us idempotency AND a clean, empty start
#    (so test counts are deterministic). Only the KEY attribute goes in attribute-definitions.
# ---------------------------------------------------------------------------------------
echo "==> [3/6] DynamoDB tables"

# Reviews -- one item per processed review; a stream is enabled for the optional 2nd trigger.
${AWS} dynamodb delete-table --table-name Reviews >/dev/null 2>&1 \
  && ${AWS} dynamodb wait table-not-exists --table-name Reviews 2>/dev/null
${AWS} dynamodb create-table \
  --table-name Reviews \
  --attribute-definitions AttributeName=reviewId,AttributeType=S \
  --key-schema AttributeName=reviewId,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE >/dev/null
${AWS} dynamodb wait table-exists --table-name Reviews 2>/dev/null
echo "  table ready: Reviews (with NEW_IMAGE stream)"

# Customers -- the ban ledger: impoliteCount + banned, per reviewerID.
${AWS} dynamodb delete-table --table-name Customers >/dev/null 2>&1 \
  && ${AWS} dynamodb wait table-not-exists --table-name Customers 2>/dev/null
${AWS} dynamodb create-table \
  --table-name Customers \
  --attribute-definitions AttributeName=reviewerID,AttributeType=S \
  --key-schema AttributeName=reviewerID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST >/dev/null
${AWS} dynamodb wait table-exists --table-name Customers 2>/dev/null
echo "  table ready: Customers"

# ---------------------------------------------------------------------------------------
# 4. Lambda functions. Each zip contains handler.py PLUS the two shared modules, all at the
#    zip ROOT (zip -j) so that `import config` / `import s3_events` work and the handler is
#    found as `handler.handler`. STAGE=local points the in-Lambda boto3 clients at MiniStack.
# ---------------------------------------------------------------------------------------
echo "==> [4/6] Lambda functions"
make_lambda () {  # $1 = function name, $2 = source dir under lambdas/
  local NAME="$1" DIR="$2"
  ( cd "lambdas/${DIR}"
    rm -f lambda.zip
    zip -q lambda.zip handler.py
    zip -qj lambda.zip ../../common/config.py ../../common/s3_events.py )
  ${AWS} lambda delete-function --function-name "${NAME}" >/dev/null 2>&1
  ${AWS} lambda create-function \
    --function-name "${NAME}" \
    --runtime python3.11 \
    --timeout 30 \
    --zip-file "fileb://lambdas/${DIR}/lambda.zip" \
    --handler handler.handler \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --environment '{"Variables":{"STAGE":"local"}}' >/dev/null
  ${AWS} lambda wait function-active --function-name "${NAME}" 2>/dev/null
  echo "  lambda ready: ${NAME}"
}
make_lambda preprocess preprocess
make_lambda profanity  profanity
make_lambda sentiment  sentiment
make_lambda aggregate  aggregate
make_lambda report     report

# ---------------------------------------------------------------------------------------
# 5. S3 -> Lambda notifications: the 4 links that ARE the chain. put-bucket-notification-
#    configuration REPLACES the whole config each call, so it is naturally idempotent.
# ---------------------------------------------------------------------------------------
echo "==> [5/6] S3 -> Lambda notifications"
wire_notification () {  # $1 = source bucket, $2 = lambda name
  local ARN
  ARN=$(${AWS} lambda get-function --function-name "$2" \
          --query 'Configuration.FunctionArn' --output text)
  ${AWS} s3api put-bucket-notification-configuration \
    --bucket "$1" \
    --notification-configuration \
      "{\"LambdaFunctionConfigurations\":[{\"LambdaFunctionArn\":\"${ARN}\",\"Events\":[\"s3:ObjectCreated:*\"]}]}"
  echo "  ${1}  ->  ${2}"
}
wire_notification reviews-ingest        preprocess
wire_notification reviews-preprocessed  profanity
wire_notification reviews-profanity     sentiment
wire_notification reviews-scored        aggregate

# ---------------------------------------------------------------------------------------
# 6. OPTIONAL: DynamoDB stream -> aggregate (a 2nd kind of trigger, for the report).
#    Guarded: if MiniStack can't do it, we print a warning and carry on -- the S3 chain
#    above already satisfies the assignment ("triggered by S3 buckets AND/OR DynamoDB events").
# ---------------------------------------------------------------------------------------
echo "==> [6/6] Optional DynamoDB-stream trigger"
if [ "${ENABLE_STREAM_TRIGGER}" = "1" ]; then
  STREAM_ARN=$(${AWS} dynamodb describe-table --table-name Reviews \
                 --query 'Table.LatestStreamArn' --output text 2>/dev/null)
  if [ -n "${STREAM_ARN}" ] && [ "${STREAM_ARN}" != "None" ]; then
    if ${AWS} lambda create-event-source-mapping \
         --function-name aggregate \
         --event-source-arn "${STREAM_ARN}" \
         --starting-position LATEST \
         --batch-size 1 >/dev/null 2>/tmp/dic_esm_err; then
      echo "  OK: Reviews stream -> aggregate mapping created"
    else
      echo "  WARN: stream mapping failed (chain still works S3-only). Detail:"
      sed 's/^/    /' /tmp/dic_esm_err
    fi
  else
    echo "  WARN: no stream ARN on Reviews; skipping (S3 chain unaffected)."
  fi
else
  echo "  skipped (ENABLE_STREAM_TRIGGER=0)"
fi

# ---------------------------------------------------------------------------------------
echo
echo "==> Done. The pipeline is provisioned."
echo "    Start the chain by dropping a review into reviews-ingest, e.g.:"
echo "      python src/loader.py data/reviews_devset.json 1"
echo "    Then inspect:"
echo "      ${AWS} dynamodb scan --table-name Reviews --select COUNT"
