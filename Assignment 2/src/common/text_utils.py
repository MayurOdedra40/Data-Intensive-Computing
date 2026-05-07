"""Shared text preprocessing for Assignment 2.

This module is the single source of truth for tokenization and stopword
handling across Part 1 (RDD), Part 2 (DataFrame pipeline), and Part 3
(classifier). The regex pattern and preprocessing rules MUST stay aligned
between the Python-side `TOKEN_SPLIT_RE` (used by Part 1's RDD job) and
`REGEX_TOKENIZER_PATTERN` (passed to PySpark `RegexTokenizer` in Part 2).
Any change here must be reviewed against both.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import AbstractSet, Iterable

# The PDF lists these exact characters as delimiters (Assignment 2 spec
# section "Part 2"). Verbatim port from Assignment 1's text_processing.py.
DELIMITER_CHARS = "()[]{}.!?,;:+=-_\"'`~#@&*%€$§\\/"

# Python-side regex used by Part 1 (RDD path). Splits on whitespace, digits,
# and the delimiter character class; Unicode-aware so €/§ work as expected.
TOKEN_SPLIT_RE = re.compile(
    r"[\s\d" + re.escape(DELIMITER_CHARS) + r"]+",
    re.UNICODE,
)

# Equivalent pattern string for PySpark `RegexTokenizer(pattern=...,
# gaps=True, toLowercase=True)` — used by Part 2.
# RegexTokenizer uses Java's java.util.regex which accepts the same character
# class syntax; the only difference is that `re.escape` produces Python's
# escape conventions, so we hand-build the Java string here.
REGEX_TOKENIZER_PATTERN = (
    r"[\s\d\(\)\[\]\{\}\.\!\?\,\;\:\+\=\-\_\"\'`~#@&\*%€\$§\\/]+"
)


@lru_cache(maxsize=None)
def load_stopwords(path: str | Path) -> frozenset[str]:
    """Read a stopword list, casefolded and stripped, into a frozenset."""

    stopword_path = Path(path)
    with stopword_path.open("r", encoding="utf-8") as handle:
        return frozenset(
            line.strip().casefold()
            for line in handle
            if line.strip()
        )


def tokenize(text: object) -> list[str]:
    """Casefold and split raw text into unigram candidates.

    Empty/None input yields an empty list.
    """

    normalized = "" if text is None else str(text).casefold()
    return [tok for tok in TOKEN_SPLIT_RE.split(normalized) if tok]


def preprocess(text: object, stopwords: AbstractSet[str]) -> list[str]:
    """Full Part-1 preprocessing: tokenize, drop short/stopword tokens,
    deduplicate while preserving first-seen order.

    Per-document deduplication is critical for chi² semantics — the
    contingency table counts document presence, not raw term frequency.
    """

    seen: set[str] = set()
    unique: list[str] = []
    for tok in tokenize(text):
        if len(tok) <= 1:
            continue
        if tok in stopwords:
            continue
        if tok in seen:
            continue
        seen.add(tok)
        unique.append(tok)
    return unique


__all__ = [
    "DELIMITER_CHARS",
    "TOKEN_SPLIT_RE",
    "REGEX_TOKENIZER_PATTERN",
    "load_stopwords",
    "tokenize",
    "preprocess",
]
