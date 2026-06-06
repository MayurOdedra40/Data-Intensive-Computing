# NOTES.md — how this thing actually works

Plain notes for myself, written after walking through `src/run.sh` line by line. No prior
AWS knowledge assumed. The goal: understand what we built and why, well enough to explain it
to someone else.

---

## The one-paragraph version

We built a **review-processing pipeline** that runs on **MiniStack** (a fake AWS that runs on
your own machine, listening at `http://localhost:4566`). A review enters as a file, gets passed
through 5 small functions one after another — each cleans it, checks it, scores it, and finally
records the result in a database. Nobody runs the steps by hand: **dropping a file in one place
automatically wakes up the next function**, like dominoes. The whole thing is set up by one
script, `run.sh`.

---

## The mental model (the parts and what they are)

- **S3 bucket** = a named container for files in vthe cloud. Think "a mailbox." A file (an
  "object") lands in it, identified by a key (its name). In our pipeline, each bucket is one
  stage's mailbox.

- **Lambda** = a function that lives in the cloud, dormant. It is **not** a running program — it's
  just stored code (`handler.py`) plus some config. AWS only starts it up when something
  **triggers** it, runs it once, and shuts it down. "Serverless" = you never manage a server.

- **Trigger / event** = the thing that wakes a Lambda. Ours is "a new file appeared in bucket X".

- **DynamoDB** = a NoSQL database (tables of flexible items). You only declare the **key**; every
  other field is whatever you write. We use it to store the final results.

- **SSM Parameter Store** = a little key→value settings store. We keep bucket names, table names,
  and tunable numbers (like the ban threshold) here, so nothing is hardcoded in the functions.

- **MiniStack is ephemeral** = it forgets everything when it stops. That's why `run.sh` rebuilds
  the whole world from scratch and must be safe to run over and over (this is called being
  **idempotent**).

---

## How a Lambda really works

Every Lambda has the **same signature**:

```python
def handler(event, context):
    ...
    return {"statusCode": 200}
```

- `event` = a dict describing *what happened*. For an S3 trigger it contains the bucket name and
  the file key. Its shape depends on who triggered it.
- `context` = runtime info AWS passes in (request id, time left, etc.). We don't use it, but it's
  always passed so the parameter must be there.
- the return value = the result of that run. For our event-driven functions nobody reads it; it's
  just a "success" signal.

The function is dormant until triggered. On a trigger AWS spins up a `python3.11` sandbox,
unzips the code, imports `handler.py`, calls `handler(event, context)`, then tears it down.

**The key trick that chains the stages:** each Lambda's last act is to **write its output file
into the next bucket**. That write is itself a "new file appeared" event — which triggers the
next Lambda. So the output of one stage *is* the trigger of the next. No orchestrator exists; the
bucket notifications do all the work.

---

## How the data actually flows (important — easy to get backwards)

The pipeline **starts at an S3 bucket, and the database is at the END, not the start.**

```
drop a review file into  reviews-ingest      (S3)   ← THE START
      → preprocess   → writes to reviews-preprocessed (S3)
      → profanity    → writes to reviews-profanity    (S3)
      → sentiment    → writes to reviews-scored        (S3)
      → aggregate    → writes into the Reviews table   (DynamoDB) ← THE END
```

A review reaches the database only *after* it has passed through all four stages. The DB is where
finished results land — it is not what kicks the pipeline off.

`report` is a 5th Lambda that exists but is **not** in the chain. It's run on demand to summarize
results; nothing triggers it automatically.

---

## `run.sh` — what each of the 5 steps does

Run it with `bash src/run.sh`. It prints `[1/5] … [5/5]`. The order matters: **create the
resources first, then wire the connections between them** (you can't point a trigger at a Lambda
that doesn't exist yet).

**Step 0 — setup (top of the file).**
- Sets dummy credentials (`test`/`test`) and a region. MiniStack doesn't check them but the AWS
  CLI insists they exist.
- Sets `AWS_PAGER=""` so the CLI doesn't open a pager (`less`) that would hang the script.
- `cd "$(dirname "$0")"` jumps into `src/` so all relative paths work no matter where you ran it.
- `AWS="aws --endpoint-url=http://localhost:4566"` — every command written as `${AWS} ...` is
  redirected to MiniStack instead of real AWS. This is the core trick.
- The script deliberately does **not** use `set -e`, because a harmless "already exists" must not
  abort everything on a re-run.

**Step 1 — S3 buckets.** Creates the 5 buckets (`reviews-ingest`, `reviews-preprocessed`,
`reviews-profanity`, `reviews-scored`, `results-export`). Re-running is fine: if a bucket already
exists, `mb` just fails quietly and we move on. After this step the buckets are empty containers
with names and nothing else.

**Step 2 — SSM parameters.** Stores 12 settings: bucket names, table names, and tunables
(`ban-threshold 3`, sentiment thresholds, etc.). This is the **single source of truth** — the
Lambdas look names up here instead of hardcoding them, so renaming a bucket means changing one
line. `--overwrite` makes re-runs update in place instead of erroring.

**Step 3 — DynamoDB tables.** Creates `Reviews` (key `reviewId`) and `Customers` (key
`reviewerID`). Each is **deleted first, then recreated** — this guarantees a clean, empty start
every run, so test counts are predictable. You only declare the key; everything else is written
freely later. Creates/deletes are asynchronous, so the script `wait`s for each to actually finish.

**Step 4 — Lambda functions.** For each of the 5 functions it:
- zips `handler.py` together with the shared `config.py` and `s3_events.py`, all flattened to the
  zip root (`zip -j` "junks the paths") so `import config` works,
- deletes any old version, then creates the function with `runtime python3.11`, a 30s timeout,
  entry point `handler.handler` (file `handler`, function `handler`), and env var `STAGE=local`
  (the switch that makes the function's own AWS calls point at MiniStack).

**Step 5 — S3 → Lambda notifications (THIS is the pipeline).** Wires the 4 chain links: "when a
new file lands in bucket X, call Lambda Y." It looks up each Lambda's full address (its **ARN**)
and attaches a notification rule to the bucket. The 4 links:
```
reviews-ingest        → preprocess
reviews-preprocessed  → profanity
reviews-profanity     → sentiment
reviews-scored        → aggregate
```
Setting a notification *replaces* the whole config each time, so re-running can't create
duplicates. `results-export` has no trigger (it's the end); `report` has none either.

---

## The three ways `run.sh` stays safe to re-run (idempotency)

Because MiniStack forgets everything on restart, `run.sh` gets run constantly. Three tactics keep
re-runs from breaking:
1. **Tolerate "already exists"** — buckets (step 1).
2. **`--overwrite`** — SSM parameters (step 2).
3. **Delete-before-create** — tables and Lambdas (steps 3 & 4); also gives a clean empty start.

---

## Quick reference: poke at it by hand

All commands need `--endpoint-url=http://localhost:4566`.

```bash
# start one review through the chain
python src/loader.py data/reviews_devset.json 1

# did it land in the DB? (give it a couple seconds — the chain is async)
aws --endpoint-url=http://localhost:4566 dynamodb scan --table-name Reviews --select COUNT

# what's in a bucket / what functions exist / what settings are stored
aws --endpoint-url=http://localhost:4566 s3 ls s3://reviews-scored/
aws --endpoint-url=http://localhost:4566 lambda list-functions --query 'Functions[].FunctionName'
aws --endpoint-url=http://localhost:4566 ssm get-parameters-by-path --path /dic-a3 --recursive
```

If nothing works after MiniStack restarted: it forgot everything — just run `bash src/run.sh`
again.
