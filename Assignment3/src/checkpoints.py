"""checkpoints.py -- per-run segment files that make the whole pipeline restart-proof.

THE PROBLEM. MiniStack is ephemeral: if it (not just our driver script) restarts mid-run, its
DynamoDB is wiped. A single snapshot file can't simply be "refreshed" from DynamoDB after that --
the fresh DB only holds the reviews processed AFTER the restart, so a refresh would clobber the
earlier numbers.

THE FIX (segments). Every run writes its OWN file: checkpoints/segment_000.json, segment_001.json,
...  A run processes ONLY the reviews that no earlier segment already covered (the loader skips
those reviewIds), so the segments are DISJOINT by construction. That means the final result is a
plain ADD across segments -- including the per-user impolite counts, so a user whose 4 bad reviews
landed 2-in-one-run + 2-in-another still totals 4 and gets banned. Each run starts from a clean,
wiped DynamoDB (its segment reflects only that run), so a MiniStack restart loses nothing that
matters: the earlier segments are already on disk.

This module is the one place that knows the segment layout. It is reused by checkpointer.py
(writes a segment), loader.py (reads the union of processed ids to skip), finalize_results.py and
run_cluster.sh (combine all segments into the final results).

A segment file is exactly what generate_results.compute_state() returns, namely:
    { processedCount, sentiment{...}, profanityFailed, banThreshold,
      userProfaneCounts{reviewerID:int}, bannedUsers[...], processedReviewIds[...], updatedAt }

CLI (used by run_cluster.sh):
    python checkpoints.py count       [DIR]            # distinct reviews across all segments
    python checkpoints.py next-index  [DIR]            # index to use for the next segment file
    python checkpoints.py ids         [DIR] [OUTFILE]  # union of processed reviewIds (newline list)
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Default location: Assignment3/checkpoints/ (one level up from src/), on the host disk so it
# survives MiniStack restarts.
DEFAULT_DIR = str(Path(__file__).parent.parent / "checkpoints")

_SEGMENT_RE = re.compile(r"segment_(\d+)\.json$")


def segment_path(directory: str, index: int) -> str:
    return os.path.join(directory, f"segment_{index:03d}.json")


def segment_files(directory: str) -> list:
    """All segment files in the directory, sorted by their numeric index."""
    files = glob.glob(os.path.join(directory, "segment_*.json"))
    return sorted(files, key=lambda p: int(_SEGMENT_RE.search(p).group(1)))


def next_index(directory: str) -> int:
    """The index to give the next segment (max existing + 1, or 0)."""
    indices = [int(_SEGMENT_RE.search(p).group(1)) for p in segment_files(directory)]
    return (max(indices) + 1) if indices else 0


def load_all(directory: str) -> list:
    """Load every segment dict (skipping any that are unreadable / half-written)."""
    out = []
    for path in segment_files(directory):
        try:
            out.append(json.loads(Path(path).read_text(encoding="utf-8")))
        except (OSError, ValueError):
            # A segment being written right now, or a corrupt file -- skip it; the others stand.
            continue
    return out


def processed_ids(directory: str) -> set:
    """Union of reviewIds already processed across all existing segments.

    The loader skips these so the next run only does new work -- which is exactly what keeps the
    segments disjoint (and therefore safe to add together).
    """
    ids = set()
    for seg in load_all(directory):
        ids.update(seg.get("processedReviewIds", []))
    return ids


def write_atomic(path: str, state: dict) -> None:
    """Write a segment as JSON atomically (temp file + os.replace), stamping updatedAt.

    Atomic rename means a reader -- or a crash -- never sees a half-written file: it is either the
    previous complete segment or the new complete one.
    """
    payload = dict(state)
    payload.setdefault("updatedAt", datetime.now(timezone.utc).isoformat())

    out_dir = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(out_dir, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=out_dir, prefix=".segment.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def combine(segments: list) -> dict:
    """Add disjoint segments into the final results.

    Counts and per-user impolite counts are summed; banned users are then derived from the summed
    counts using the (strictly-greater-than) threshold. We also union the reviewId sets to report
    the distinct total and to DETECT any accidental overlap (which would make the plain sum wrong) --
    if segments overlapped, `overlap` is non-zero and the caller should warn loudly.
    """
    sentiment = {"positive": 0, "neutral": 0, "negative": 0}
    profanity_failed = 0
    ban_threshold = 3
    user_counts: dict = {}
    all_ids: set = set()
    sum_ids = 0

    for seg in segments:
        for k in sentiment:
            sentiment[k] += int(seg.get("sentiment", {}).get(k, 0))
        profanity_failed += int(seg.get("profanityFailed", 0))
        ban_threshold = int(seg.get("banThreshold", ban_threshold))
        for uid, cnt in (seg.get("userProfaneCounts", {}) or {}).items():
            user_counts[uid] = user_counts.get(uid, 0) + int(cnt)
        ids = seg.get("processedReviewIds", [])
        sum_ids += len(ids)
        all_ids.update(ids)

    banned_users = sorted(uid for uid, cnt in user_counts.items() if cnt > ban_threshold)

    return {
        "totalDevsetReviews": sum(sentiment.values()),
        "sentiment": sentiment,
        "profanityFailed": profanity_failed,
        "bannedUsers": banned_users,
        # diagnostics (not part of the required results, but handy + a correctness guard)
        "banThreshold": ban_threshold,
        "userProfaneCounts": user_counts,
        "segments": len(segments),
        "distinctReviews": len(all_ids),
        "overlap": sum_ids - len(all_ids),
    }


# ── tiny CLI so run_cluster.sh can ask simple questions without inline python ─────────────────
def _main(argv) -> int:
    cmd = argv[0] if argv else ""
    directory = argv[1] if len(argv) > 1 else DEFAULT_DIR

    if cmd == "count":
        print(len(processed_ids(directory)))
        return 0
    if cmd == "next-index":
        print(next_index(directory))
        return 0
    if cmd == "ids":
        ids = sorted(processed_ids(directory))
        out = argv[2] if len(argv) > 2 else None
        text = "\n".join(ids)
        if out:
            Path(out).write_text(text, encoding="utf-8")
        else:
            sys.stdout.write(text + ("\n" if ids else ""))
        return 0

    sys.stderr.write("usage: checkpoints.py {count|next-index|ids} [DIR] [OUTFILE]\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
