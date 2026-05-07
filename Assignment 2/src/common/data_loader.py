"""Dataset loading helpers for Assignment 2.

Two access modes are supported:

- "local": read a JSON-lines file from the local filesystem (Assignment 1's
  reviews_devset.json checkout, useful for fast iteration).
- "cluster": read from HDFS via Spark — the path passed in must be an
  ``hdfs://`` URI such as ``hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json``.

The loader returns an RDD of parsed review dicts. Malformed JSON lines and
non-dict payloads are silently dropped — Assignment 1 followed the same
policy.
"""

from __future__ import annotations

import json
from typing import Optional


def _safe_json_loads(line: str) -> Optional[dict]:
    """Parse a single JSON line, returning None on any failure."""

    try:
        record = json.loads(line)
    except (json.JSONDecodeError, TypeError):
        return None
    return record if isinstance(record, dict) else None


def resolve_path(local_path: str, hdfs_path: str, mode: str) -> str:
    """Pick the right input path for the chosen execution mode."""

    if mode == "local":
        return local_path
    if mode == "cluster":
        return hdfs_path
    raise ValueError(f"Unknown mode: {mode!r}")


def load_reviews_rdd(sc, path: str):
    """Read JSON-lines reviews into an RDD of dicts."""

    return (
        sc.textFile(path)
        .map(_safe_json_loads)
        .filter(lambda r: r is not None)
    )


__all__ = ["load_reviews_rdd", "resolve_path"]
