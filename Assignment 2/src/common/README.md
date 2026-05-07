# `common/` — shared utilities

Single source of truth for tokenization, stopword handling, dataset I/O, and
Spark session construction across Parts 1, 2, and 3.

## The tokenizer is locked

Both Part 1 (RDD path) and Part 2 (DataFrame `RegexTokenizer`) must produce
identical token lists. Two artefacts encode the rule:

- `text_utils.TOKEN_SPLIT_RE` — Python `re` pattern used by Part 1.
- `text_utils.REGEX_TOKENIZER_PATTERN` — string passed to PySpark
  `RegexTokenizer(pattern=..., gaps=True, toLowercase=True)` in Part 2.

Any change to one must be mirrored in the other. The delimiter list comes
straight from the assignment spec and includes non-ASCII characters
(`€`, `§`) plus a backtick (`` ` ``). When updating, smoke-test against a
synthetic string covering every delimiter before declaring done.

## Casefold vs lowercase caveat

Part 1 calls Python's `str.casefold()` (e.g. `ß → ss`). PySpark's
`RegexTokenizer(toLowercase=True)` calls Java `String.toLowerCase()` and does
not apply casefold mappings. Amazon reviews are ASCII-dominant so divergence
is near-zero, but the comparison paragraph in the report should mention
this as a legitimate source of small term-set drift between Part 1 and
Part 2.

## Stopwords

`stopwords.txt` is copied verbatim from Assignment 1
(`Assignment 1/src/Assignment_1_Assets/stopwords.txt`). One word per line,
casefolded on load, blanks ignored. 596 entries.

## Data loader

`data_loader.load_reviews_rdd(sc, path)` parses JSON-lines reviews into an
RDD of dicts; malformed lines are dropped silently. Use
`resolve_path(local, hdfs, mode)` to pick the right URI for the run mode.

## Spark session

`spark_session.build_spark(app_name, mode)` returns a configured
`SparkSession`. `mode="local"` sets `local[*]`; `mode="cluster"` defers to
`spark-submit`. Kryo serializer is enabled.
