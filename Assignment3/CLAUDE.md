# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Assignment 3 of a Data-Intensive Computing course (TU Wien, run on the "LBD" cluster at `lbd.tuwien.ac.at`). The work centers on **MiniStack** — a self-hosted, LocalStack-style emulator of AWS services. The starting point under `assignment_3_tutorial/` is a serverless image-resizer adapted from the LocalStack `sample-serverless-image-resizer-s3-lambda` tutorial.

`Assignment_3_Instructions.pdf` holds the actual assignment requirements — read it before implementing anything beyond the tutorial.

## Two unrelated codebases live here

- **`assignment_3_tutorial/`** — the active serverless project (S3 + Lambda + SSM on MiniStack). This is the Assignment 3 material.
- **`utils/text_processing.py`** + **`data/`** (`reviews_devset.json`, `stopwords.txt`) — leftover shared code from a *prior* assignment (Amazon-review unigram preprocessing). It is imported as `from utils.text_processing import ...` and is independent of the tutorial. Treat the two as separate unless the instructions tie them together.

## Running the serverless app (MiniStack)

`run.sh` does the full deploy: creates the two S3 buckets, stores their names in SSM Parameter Store, zips and deploys the three Lambdas, wires the S3→resize bucket notification, and uploads the static frontend to an S3 website bucket.

On the LBD cluster (no venv / no pip needed):
```bash
ministack                 # start MiniStack in one shell
bash ./run.sh             # deploy, from inside assignment_3_tutorial/
```
Web app: `https://lbd.tuwien.ac.at/user/$USER/proxy/4566/webapp/index.html`

Locally:
```bash
python3 -m venv .env && source .env/bin/activate
pip install -r requirements.txt
ministack                 # in a separate shell
bash ./run.sh
```
Web app: `http://localhost:4566/webapp/index.html`

All AWS CLI calls target MiniStack via `--endpoint-url=http://localhost:4566` with dummy `test`/`test` credentials. The `AWS=` alias at the top of `run.sh` bakes this in; reuse that pattern for any manual `aws` commands.

## Tests

Integration tests require MiniStack to be **already running with `run.sh` applied** (the lambdas `presign`, `resize`, `list` must exist — a fixture waits on them). They are not unit tests.
```bash
cd assignment_3_tutorial
pytest tests/                                  # all
pytest tests/test_integration.py::test_s3_resize_integration   # single test
```
`test_s3_resize_integration` uploads `nyan-cat.png` to the images bucket and asserts the resizer Lambda produced a smaller file in the resized bucket.

## Architecture notes that aren't obvious from one file

- **Three Lambdas, one event flow.** `presign` returns an S3 pre-signed POST so the browser uploads *directly* to S3 (bypassing the Lambda). That upload fires an `s3:ObjectCreated:*` notification that triggers `resize` (Pillow thumbnail to max 400×400) writing to the resized bucket. `list` enumerates both buckets and returns pre-signed GET URLs for display. The wiring lives in `run.sh`, not in code.

- **Bucket names come from SSM, never hardcoded in handlers.** `run.sh` writes `/ministack-thumbnail-app/buckets/images` and `/ministack-thumbnail-app/buckets/resized`; every handler reads them via `ssm.get_parameter`. Change a bucket name in `run.sh` only.

- **Dual S3 client / dual-endpoint pattern (the main subtlety).** Each handler builds *two* boto3 S3 clients: `s3_internal` (talks to MiniStack at `http://localhost:4566` for API calls) and `s3_public` (used only to *generate* pre-signed URLs, pointed at `S3_ENDPOINT_URL` so the URLs are reachable from the browser). This exists because on the LBD cluster the browser reaches S3 through the `/user/$USER/proxy/4566` reverse proxy, not localhost. `run.sh` derives `S3_ENDPOINT_URL` / `PUBLIC_BASE_URL` from the JupyterHub env vars; if pre-signed URLs work from the server but 404 in the browser, this is why.

- **`STAGE=local`** switches handlers to the MiniStack endpoint; without it boto3 would hit real AWS.

- **Handlers are dual-mode.** Each `handler` detects HTTP (function-URL) invocations vs direct invokes via `is_http_event` and returns either an API-Gateway-shaped `{statusCode, headers, body}` (with CORS headers driven by `ALLOWED_ORIGINS`) or a raw Python value. Preserve both paths when editing.

- **Resizer packaging.** `resize` needs Pillow as a native wheel, so `run.sh` installs it with `--platform manylinux2014_x86_64 --only-binary=:all:` into a `package/` dir that gets zipped alongside `handler.py`. The other two Lambdas have no dependencies and ship as a bare `handler.py` zip.

- **Frontend** (`website/app.js`) is plain jQuery and rewrites Lambda function URLs between `file:`, localhost, and proxied-cluster forms (`normalizeFunctionUrl` / `getBaseUrl`); it stores the resolved URLs in `localStorage`.
