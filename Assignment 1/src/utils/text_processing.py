#!/usr/bin/env python3
"""Shared text preprocessing for Assignment 1.

The assignment asks us to build document-level unigram features from
``reviewText`` only. This module keeps that logic in one place so every job
uses the exact same preprocessing steps.
"""

from __future__ import annotations

import argparse
import json
from functools import lru_cache
from pathlib import Path
import re
from typing import AbstractSet, Iterator, Mapping

# The PDF lists these exact characters as delimiters.
DELIMITER_CHARS = "()[]{}.!?,;:+=-_\"'`~#@&*%€$§\\/"
TOKEN_SPLIT_RE = re.compile(
    r"[\s\d" + re.escape(DELIMITER_CHARS) + r"]+",
    re.UNICODE,
)

REPORT_PREPROCESSING_EXPLANATION = (
    "Each review is preprocessed from reviewText only. The text is case-folded, "
    "split into unigrams using whitespace, tabs, digits, and the assignment's "
    "delimiter characters, then filtered with stopwords.txt and the length>1 "
    "constraint. The remaining tokens are deduplicated per document while "
    "preserving first-seen order so repeated terms in one review count only once."
)


@lru_cache(maxsize=None)
def load_stopwords(path: str | Path) -> frozenset[str]:
    """Load stopwords once and reuse them across preprocessing calls."""

    stopword_path = Path(path)

    with stopword_path.open("r", encoding="utf-8") as handle:
        return frozenset(
            line.strip().casefold()
            for line in handle
            if line.strip()
        )


def tokenize_review_text(review_text: object) -> list[str]:
    """Case-fold and split ``reviewText`` into unigram candidates."""

    normalized_text = "" if review_text is None else str(review_text).casefold()
    return [token for token in TOKEN_SPLIT_RE.split(normalized_text) if token]


def preprocess_review_text(
    review_text: object,
    stopwords: AbstractSet[str],
) -> list[str]:
    """Apply the full assignment preprocessing pipeline to raw review text."""

    unique_tokens: list[str] = []
    seen_tokens: set[str] = set()

    for token in tokenize_review_text(review_text):
        if len(token) <= 1:
            continue
        if token in stopwords:
            continue
        if token in seen_tokens:
            continue

        seen_tokens.add(token)
        unique_tokens.append(token)

    return unique_tokens


def preprocess_review_record(
    record: Mapping[str, object],
    stopwords: AbstractSet[str],
) -> list[str]:
    """Preprocess a review record using only its ``reviewText`` field."""

    return preprocess_review_text(
        record.get("reviewText", ""),
        stopwords=stopwords,
    )


def extract_category(record: Mapping[str, object]) -> str:
    """Return the review category as a stripped string."""

    category = record.get("category", "")
    return str(category).strip()


def preprocess_review_with_category(
    record: Mapping[str, object],
    stopwords: AbstractSet[str],
) -> tuple[str, list[str]]:
    """Return the review category together with preprocessed ``reviewText``."""

    return extract_category(record), preprocess_review_record(record, stopwords)


def parse_review_line(line: str) -> dict | None:
    """Parse one JSON review line from the dataset."""

    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        return None

    if not isinstance(record, dict):
        return None

    return record


def iter_preprocessed_reviews(
    dataset_path: str | Path,
    stopwords: AbstractSet[str],
) -> Iterator[tuple[dict, list[str]]]:
    """Yield ``(record, tokens)`` pairs for each valid JSON review in the dataset."""

    review_path = Path(dataset_path)

    with review_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped_line = line.strip()
            if not stripped_line:
                continue

            record = parse_review_line(stripped_line)
            if record is None:
                continue

            yield record, preprocess_review_record(record, stopwords=stopwords)


def iter_category_tokens(
    dataset_path: str | Path,
    stopwords: AbstractSet[str],
) -> Iterator[tuple[str, list[str]]]:
    """Yield ``(category, tokens)`` pairs for each review in the dataset."""

    for record, tokens in iter_preprocessed_reviews(dataset_path, stopwords):
        yield extract_category(record), tokens


def preprocess_review_dataset(
    dataset_path: str | Path,
    stopwords_path: str | Path,
) -> Iterator[tuple[dict, list[str]]]:
    """Yield preprocessed reviews for an explicit dataset and stopword file."""

    stopwords = load_stopwords(stopwords_path)
    yield from iter_preprocessed_reviews(dataset_path, stopwords)

def build_argument_parser() -> argparse.ArgumentParser:
    """Create a small CLI for previewing dataset preprocessing."""

    parser = argparse.ArgumentParser(description="Preprocess Assignment 1 reviews.")
    parser.add_argument(
        "--stopwords",
        required=True,
        help="Path to stopwords.txt",
    )
    parser.add_argument(
        "--dataset",
        help="Optional path to a JSON-lines review dataset such as reviews_devset.json",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=3,
        help="How many preprocessed reviews to print when --dataset is provided",
    )
    return parser


def main(
    dataset_path: str | Path,
    stopwords_path: str | Path,
    limit: int | None = None,
) -> list[tuple[str, list[str]]]:
    """Return preprocessed reviews as ``(category, tokens)`` pairs."""

    category_tokens: list[tuple[str, list[str]]] = []
    stopwords = load_stopwords(stopwords_path)

    for index, item in enumerate(iter_category_tokens(dataset_path, stopwords), start=1):
        category_tokens.append(item)
        if limit is not None and index >= limit:
            break

    return category_tokens


def run_cli() -> int:
    """Print preview rows for command-line use."""

    args = build_argument_parser().parse_args()

    if not args.dataset:
        return 0

    for index, (category, tokens) in enumerate(
        main(args.dataset, args.stopwords, args.limit),
        start=1,
    ):
        print(f"{index}\t{category}\t{tokens}")

    return 0


__all__ = [
    "DELIMITER_CHARS",
    "REPORT_PREPROCESSING_EXPLANATION",
    "extract_category",
    "iter_preprocessed_reviews",
    "iter_category_tokens",
    "build_argument_parser",
    "load_stopwords",
    "main",
    "parse_review_line",
    "preprocess_review_dataset",
    "preprocess_review_record",
    "preprocess_review_text",
    "preprocess_review_with_category",
    "run_cli",
    "tokenize_review_text",
]


if __name__ == "__main__":
    raise SystemExit(run_cli())

# to use this codes in your python file, you can import the functions and constants as follows:
# from utils.text_processing import (
#     extract_category,
#     load_stopwords,
#     parse_review_line,
#     preprocess_review_record,
# )
