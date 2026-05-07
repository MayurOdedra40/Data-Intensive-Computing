"""Tokenizer parity smoke test.

Confirms that `common.text_utils.tokenize` (the Python regex used by Part 1's
RDD job) and PySpark's `RegexTokenizer` (used by Part 2's DataFrame pipeline)
produce identical token lists on a synthetic input that exercises every
delimiter character listed in the assignment spec — whitespace, tabs, digits,
and `()[]{}.!?,;:+=-_"'`~#@&*%€$§\\/`.

Run from the Assignment 2 root::

    python3 scripts/tokenizer_parity.py

Exits 0 on PASS (Python side always; Spark side either matches or is skipped
when PySpark is not importable). Exits 1 if either side produces unexpected
tokens.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make `from common...` imports work when run from any cwd.
_HERE = Path(__file__).resolve().parent
_SRC = _HERE.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from common.text_utils import (  # noqa: E402
    DELIMITER_CHARS,
    REGEX_TOKENIZER_PATTERN,
    tokenize,
)


# Synthetic input touches every delimiter at least once, mixed with digits,
# whitespace, and casing variation. Backtick, €, §, and backslash are the
# delicate ones — they must split tokens, not survive inside them.
SYNTHETIC = (
    "Hello, world!\tThis is a (parenthesized) [bracketed] {braced} test. "
    "End? Yes; right: now.\n"
    "Digits 12345 and €1 §2 with `backticks` ~tilde #hash @at &amp *star "
    "%pct $dol \\back /slash 'quote' \"double\" -dash _under +plus =eq."
)

# Order matches what Python's `re.split` over the delimiter class produces
# after casefold + drop-empty. If you change the delimiter list, regenerate
# this list with `print(tokenize(SYNTHETIC))`.
EXPECTED = [
    "hello", "world", "this", "is", "a", "parenthesized", "bracketed",
    "braced", "test", "end", "yes", "right", "now", "digits", "and",
    "with", "backticks", "tilde", "hash", "at", "amp", "star", "pct",
    "dol", "back", "slash", "quote", "double", "dash", "under", "plus",
    "eq",
]


def check_python_side() -> list[str]:
    py_tokens = tokenize(SYNTHETIC)
    if py_tokens != EXPECTED:
        print("FAIL (python): tokenize() output differs from expected.")
        print(f"  expected: {EXPECTED}")
        print(f"  got     : {py_tokens}")
        sys.exit(1)

    bad = [t for t in py_tokens if any(c in t for c in DELIMITER_CHARS)]
    if bad:
        print(f"FAIL (python): delimiter char survived inside tokens: {bad}")
        sys.exit(1)

    print(f"Python side: PASS ({len(py_tokens)} tokens)")
    return py_tokens


def check_spark_side(py_tokens: list[str]) -> None:
    try:
        from pyspark.ml.feature import RegexTokenizer
        from pyspark.sql import SparkSession
    except ImportError:
        print("Spark side: SKIPPED (pyspark not importable)")
        return

    spark = (
        SparkSession.builder.appName("tokenizer-parity")
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = spark.createDataFrame([(SYNTHETIC,)], ["text"])
        tokenizer = RegexTokenizer(
            inputCol="text",
            outputCol="tokens",
            pattern=REGEX_TOKENIZER_PATTERN,
            gaps=True,
            toLowercase=True,
            minTokenLength=1,  # match Python tokenize() (no length filter)
        )
        spark_tokens = tokenizer.transform(df).select("tokens").first()[0]
    finally:
        spark.stop()

    if spark_tokens != py_tokens:
        print("FAIL (spark): RegexTokenizer output differs from Python.")
        print(f"  python: {py_tokens}")
        print(f"  spark : {spark_tokens}")
        # Show the symmetric difference for easier debugging.
        py_set, sp_set = set(py_tokens), set(spark_tokens)
        print(f"  only-python: {sorted(py_set - sp_set)}")
        print(f"  only-spark : {sorted(sp_set - py_set)}")
        sys.exit(1)

    print(f"Spark side : PASS ({len(spark_tokens)} tokens, identical to Python)")


def main() -> None:
    py_tokens = check_python_side()
    check_spark_side(py_tokens)
    print("OK — tokenizer parity holds for the locked delimiter set.")


if __name__ == "__main__":
    main()
