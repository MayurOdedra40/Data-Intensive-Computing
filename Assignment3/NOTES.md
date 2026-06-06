# NOTES.md — understanding this project from zero

This file is written **for you** (the developer), in plain language, assuming **no prior
experience** with AWS or serverless. It explains *what* we built in Stage 1, *why*, and *how
to run it*. Read it top to bottom once; after that it works as a reference.

---

## 1. The 30-second picture

We are building a system that takes **customer reviews** and, one by one, automatically:
1. cleans up the text (preprocessing),
2. checks for bad words (profanity check),
3. figures out if the review is positive / neutral / negative (sentiment),
4. counts how many rude reviews each customer wrote, and
5. **bans** a customer once they post more than 3 rude reviews.

The twist: we do it **"serverless"** and **"event-driven"**. There is no long-running program.
Instead, *dropping a file somewhere* automatically *wakes up* a small function, which when it
finishes *wakes up* the next one, like a chain of dominoes.

---

## 2. The vocabulary (the only concepts you need)

Think of **AWS** as a giant rental service for computing. We use 4 of its "services":

- **S3 (Simple Storage Service)** = a cloud hard drive made of **buckets**. A *bucket* is like a
  folder. You put **objects** (files) in it, each identified by a **key** (its path/name, e.g.
  `reviews/abc123.json`). That's it — buckets hold files.

- **Lambda** = a **function in the cloud**. You upload a `.py` file with a function called
  `handler(event, context)`. AWS runs it *only when something triggers it*, then shuts it down.
  You don't manage a server — hence "**serverless**". You pay (on real AWS) only for the
  milliseconds it runs.

- **A trigger / event** = the thing that wakes a Lambda up. Our trigger is: **"a new object was
  created in bucket X"** (`s3:ObjectCreated`). When that happens, AWS calls the Lambda you
  attached to bucket X and hands it an `event` describing which object appeared.

- **DynamoDB** = a **NoSQL database** made of **tables**. Each table stores **items** (rows).
  Every item has a **partition key** (its unique id). Unlike SQL, you don't define columns up
  front — each item can just have whatever attributes you write. We use it to remember results.

- **SSM Parameter Store** = a tiny **key→value settings store**. We keep our bucket names, table
  names, and tunable numbers (like the ban threshold) here, so nothing is hard-coded inside the
  functions. A function asks SSM "what's the name of the ingest bucket?" at runtime.

- **MiniStack** = a program that **pretends to be AWS on your own machine** (or on the TU Wien
  cluster). It speaks the same language as real AWS, listening at `http://localhost:4566`. This
  lets us build and test the whole thing for free, locally. (It's the course's version of a tool
  called *LocalStack*.)

- **"Ephemeral"** = MiniStack **forgets everything when it stops.** Buckets, tables, functions —
  all gone on restart. That's why we have a script (`run.sh`) that **recreates the whole world**
  from scratch, and why that script must be safe to run over and over.

---

## 3. How the data flows (the heart of it)

We pass around one JSON object per review and call it **"the envelope"**. Picture an actual
envelope traveling through an office: each desk it visits **stamps something new on it** and
passes it along, but **never erases** earlier stamps.

```
   you / loader.py
        │  drop one review JSON into the first bucket
        ▼
  ┌──────────────────┐
  │ reviews-ingest   │  (S3 bucket)
  └──────────────────┘
        │  "new object!" → triggers …
        ▼
  ┌──────────────────┐   L1 preprocess   adds: tokens=[…]
  │  Lambda          │ ───────────────────────────────────►  writes to reviews-preprocessed
  └──────────────────┘
        │  "new object!" → triggers …
        ▼
  ┌──────────────────┐   L2 profanity    adds: isProfane, badWords
  │  Lambda          │ ───────────────────────────────────►  writes to reviews-profanity
  └──────────────────┘
        │  triggers …
        ▼
  ┌──────────────────┐   L3 sentiment    adds: sentiment, sentimentScore
  │  Lambda          │ ───────────────────────────────────►  writes to reviews-scored
  └──────────────────┘
        │  triggers …
        ▼
  ┌──────────────────┐   L4 aggregate    writes results into the database
  │  Lambda          │ ──────────────►  DynamoDB: Reviews table + Customers table (+ban)
  └──────────────────┘

  (separately, on demand)
  L5 report  ── reads the database ──►  writes a summary file to results-export
```

So the **chain is just 4 buckets wired to 4 Lambdas**, plus a database at the end. Each Lambda
reads the envelope from "its" bucket, adds its piece, and writes the envelope to the next bucket
— which automatically triggers the next Lambda. Nobody orchestrates this; the bucket
notifications do.

> Why a chain of buckets instead of one function calling the next directly? Because S3→Lambda is
> the trigger we *know* works on MiniStack (the course tutorial proves it). Building on the one
> reliable mechanism is the safe choice.

---

## 4. What each file is and why it exists

Everything lives under `src/` (plus two docs at the repo root). Here's the map:

| File | What it is |
|---|---|
| `CONTRACT.md` (root) | The **rulebook**. Defines the SSM names, the envelope's keys, the id formula. Every file obeys it. Read this if you're ever unsure what a field means. |
| `NOTES.md` (root) | This file. |
| `src/run.sh` | The **setup script**. Creates every bucket, table, parameter, Lambda, and trigger on MiniStack. Run it once after starting MiniStack. |
| `src/common/config.py` | A 3-line helper every Lambda uses to **read settings from SSM**. |
| `src/common/s3_events.py` | Helpers to **read the S3 event**, **load** the envelope from a bucket, and **save** it to the next bucket. Shared so the handlers stay tiny. |
| `src/lambdas/preprocess/handler.py` | **L1.** Stage-1 *stub* (placeholder). Real text cleaning comes in Stage 2. |
| `src/lambdas/profanity/handler.py` | **L2.** Stage-1 *stub*. Real bad-word detection comes in Stage 3. |
| `src/lambdas/sentiment/handler.py` | **L3.** Stage-1 *stub*. Real sentiment comes in Stage 4. |
| `src/lambdas/aggregate/handler.py` | **L4.** The **one piece of real logic in Stage 1**: writes to the database, counts rude reviews, bans users. |
| `src/lambdas/report/handler.py` | **L5.** Stage-1 *stub*. Real results computation comes in Stage 5. |
| `src/loader.py` | A small script to **drop reviews into the first bucket** so the chain starts. |
| `src/cornercase/*.json` | **Hand-made test reviews** (a profane one, positive/neutral/negative ones, an empty one, an emoji one, and two "ban scenario" users). |
| `src/tests/test_integration.py` | **Automated tests** that drop reviews and check the database reacts correctly. |
| `src/requirements.txt` | The Python packages you need installed locally to run all this. |

> **What's a "stub"?** A placeholder function that has the right *shape* (correct triggers,
> reads/writes the envelope) but fake *content* (e.g. it just writes `tokens=[]` instead of real
> tokens). Stubs let us prove the whole pipeline is wired correctly **now**, and swap in the real
> brains stage by stage **later** without changing any plumbing.

---

## 5. `run.sh`, explained step by step

Run it with `bash src/run.sh`. It prints `[1/6] … [6/6]` as it goes. Here's what each step does:

1. **Environment** — sets dummy credentials (`test`/`test`), the region, and the MiniStack
   address. It also sets `AWS_PAGER=""` — without that, the `aws` command tries to open a text
   pager (`less`) and a script would hang. The line `AWS="aws --endpoint-url=…"` makes every
   command point at MiniStack instead of real AWS.
2. **`[1/6]` Buckets** — creates the 5 S3 buckets. Re-running is fine: a bucket that already
   exists is left as-is.
3. **`[2/6]` SSM parameters** — stores all the names and tunables. Uses `--overwrite` so
   re-running just updates them instead of erroring.
4. **`[3/6]` DynamoDB tables** — deletes-then-recreates `Reviews` and `Customers`. Deleting first
   guarantees a clean, empty start every run (so test counts are predictable). `Reviews` is created
   with a **stream** enabled (explained in step 6).
5. **`[4/6]` Lambdas** — for each of the 5 functions: zips `handler.py` **together with**
   `config.py` and `s3_events.py` (so the imports work), deletes any old version, and creates the
   function. Each gets `STAGE=local`, which is the switch that makes its internal AWS calls go to
   MiniStack.
6. **`[5/6]` Notifications** — wires the 4 chain links: "when a new object lands in bucket X, call
   Lambda Y". **These 4 lines ARE the pipeline.**
7. **`[6/6]` Optional stream trigger** — a *bonus*. It also makes the `Reviews` table notify L4
   whenever a row is written (a "DynamoDB event", a second kind of trigger). It's wrapped so that
   if MiniStack can't do it, you just get a `WARN` and everything else still works. Turn it off
   entirely with `ENABLE_STREAM_TRIGGER=0 bash src/run.sh`.

---

## 6. The clever bit: counting rude reviews safely (L4)

Two things make L4 (`aggregate/handler.py`) tricky, and both are solved with **DynamoDB
features** rather than our own code:

**Problem A — "at-least-once" delivery.** S3 might trigger a Lambda for the *same* object more
than once. If we naively did "+1 rude review" each time, a customer could get banned by accident.

**Solution A — an idempotency gate.** Before counting, L4 inserts the review into the `Reviews`
table with the condition `attribute_not_exists(reviewId)` — meaning "only insert if this id is
new." The first time it succeeds; any repeat **fails** with a special error we catch. We only
count the customer when the insert *actually created* a new row. So each review counts **at most
once**, no matter how many times it's delivered. (Test 2 step 3 proves this.)

**Problem B — two reviews at the same time.** If we did "read count, add 1, write count" and two
ran at once, they could both read 5 and both write 6 — losing one.

**Solution B — an atomic counter.** DynamoDB can do `ADD impoliteCount :one` as a single
indivisible operation on its side. Concurrent updates can't clobber each other. And it returns
the *new* value, so we immediately know if we crossed the line.

**The ban rule.** We compare the new count to the threshold with **strictly greater than**:
`count > 3`. So 3 rude reviews → still fine; the **4th** → banned. That matches the assignment's
wording "more than 3". The threshold `3` comes from SSM, so it's easy to change.

---

## 7. How to run it yourself (local)

You need Python 3 and the ability to install packages. From the repo root:

```bash
# 1. Make an isolated Python environment and install the tools.
python3 -m venv .env
source .env/bin/activate
pip install -r src/requirements.txt

# 2. Start MiniStack. Leave this running in its OWN terminal.
ministack

# 3. In a SECOND terminal (same folder, same venv), build everything:
source .env/bin/activate
bash src/run.sh

# 4. Push one review through the chain:
python src/loader.py data/reviews_devset.json 1

# 5. Check the database reacted (should print a Count of 1):
aws --endpoint-url=http://localhost:4566 dynamodb scan --table-name Reviews --select COUNT

# 6. Run the automated tests:
pytest src/tests/test_integration.py -v
```

On the **TU Wien LBD cluster** it's the same, except MiniStack is already installed (no venv /
pip needed) — just `ministack` in one shell and `bash src/run.sh` in another.

---

## 8. Handy commands to "look inside" MiniStack

All of these start with `aws --endpoint-url=http://localhost:4566` (tip: `alias aws='aws
--endpoint-url=http://localhost:4566'` in your shell to save typing).

```bash
aws ... s3 ls                                  # list buckets
aws ... s3 ls s3://reviews-scored/             # list objects in a bucket
aws ... lambda list-functions --query 'Functions[].FunctionName'   # are the 5 Lambdas there?
aws ... dynamodb scan --table-name Reviews     # dump every processed review
aws ... dynamodb get-item --table-name Customers \
        --key '{"reviewerID":{"S":"CC_BAN_USER"}}'                  # one customer's ban status
aws ... ssm get-parameters-by-path --path /dic-a3 --recursive      # all our settings
```

Want to see a full ban happen by hand? Drop the corner-case files:
```bash
for f in src/cornercase/ban_user_impolite_*.json; do
  aws ... s3 cp "$f" "s3://reviews-ingest/cornercase/$(basename "$f")"
done
# wait a few seconds, then:
aws ... dynamodb get-item --table-name Customers --key '{"reviewerID":{"S":"CC_BAN_USER"}}'
# -> impoliteCount = 4, banned = true
```

---

## 9. Things that will confuse you at first (gotchas)

1. **The chain is asynchronous.** After you drop a review, the database doesn't update
   *instantly* — it takes a moment to flow through 4 Lambdas. That's why the tests **poll** (keep
   checking for a few seconds) instead of checking once. Don't panic if `scan` shows nothing for
   a second.
2. **The `s3:TestEvent`.** The very first message S3 sends a freshly-wired Lambda is a "hello"
   handshake with no review in it. Our code spots it and ignores it. If you ever write a new
   handler, go through `s3_events.parse_records` so this is handled for you.
3. **`STAGE=local`.** If a Lambda's calls seem to hang, it's probably missing this environment
   variable — without it, the function tries to reach the *real* AWS instead of MiniStack.
4. **Zip layout.** A Lambda's `handler.py` must sit at the **top** of its zip, next to
   `config.py` and `s3_events.py`. `run.sh` does this with `zip -j` (junk the folder paths). If
   you see "Unable to import module 'handler'", the zip layout is wrong.
5. **SSM gives you strings.** `ban-threshold` comes back as the text `"3"`, not the number `3`.
   Always cast (`int(...)`), or use the `config.get_int` helper.
6. **A brand-new customer has no `banned` field at all** (not `banned=false`). Treat "missing" as
   "not banned".
7. **MiniStack forgets everything on restart.** If nothing works after you restarted it, you
   probably just need to run `bash src/run.sh` again.

---

## 10. What is real vs. stubbed in Stage 1 (and what's next)

| Piece | Stage 1 status |
|---|---|
| All buckets, tables, SSM, triggers, wiring | **Real & final.** |
| L4 aggregate: DB writes, counting, ban, idempotency | **Real & final.** |
| L1 preprocess (tokenize/lemmatize) | Stub → **Stage 2** |
| L2 profanity (real bad-word detection) | Stub (uses the `forceProfane` flag) → **Stage 3** |
| L3 sentiment (positive/neutral/negative) | Stub (always "neutral") → **Stage 4** |
| L5 report (the 3 result numbers) + full dataset loader | Stub → **Stage 5** |

Stage 1's whole job was to build the **skeleton** so the rest of the team can fill in one box
each without touching the plumbing. The agreement that lets them do that is `CONTRACT.md`.

> **The `forceProfane` trick:** real bad-word detection isn't built until Stage 3, but we wanted
> to test *banning* now. So the stub L2 honors a test-only flag `"forceProfane": true` in a
> review and marks it rude. Our test reviews set that flag; real reviews never do. This is how
> the ban test can pass in Stage 1.
