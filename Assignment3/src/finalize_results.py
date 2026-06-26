"""finalize_results.py -- combine the per-run segment files into the final results.json.

Run this when the pipeline has finished (run_cluster.sh calls it automatically after the run
drains, but it is fine to run by hand any time). It reads ONLY the on-disk segment files in
checkpoints/ -- no DynamoDB / MiniStack needed -- so it still works even if MiniStack was shut down
after the run.

Each run wrote its own segment over a DISJOINT set of reviews (the loader skipped reviewIds already
covered by earlier runs), so combining is a plain ADD across segments -- including per-user impolite
counts, so a user whose bad reviews were split across runs still totals correctly and gets banned.
See checkpoints.py for the model.

It produces the three results the assignment asks for (devset only):
    * number of positive / neutral / negative reviews
    * number of reviews that failed the profanity check
    * users that ended up banned (impolite count strictly greater than the ban threshold)

Usage:
    python finalize_results.py
    python finalize_results.py --checkpoints-dir /path/checkpoints --out /path/results.json
    python finalize_results.py --upload        # also push results.json to the results-export bucket
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import checkpoints

DEFAULT_OUT = str(Path(__file__).parent.parent / "results.json")
LEGACY_FILE = str(Path(__file__).parent.parent / "intermediate_results.json")


def _required_keys(combined: dict) -> dict:
    """The exact result shape the assignment asks for."""
    return {
        "totalDevsetReviews": combined["totalDevsetReviews"],
        "sentiment": combined["sentiment"],
        "profanityFailed": combined["profanityFailed"],
        "bannedUsers": combined["bannedUsers"],
    }


def _combine_legacy(path: str) -> dict:
    """Fallback: derive results from a single legacy intermediate_results.json (no segments)."""
    state = json.loads(Path(path).read_text(encoding="utf-8"))
    return checkpoints.combine([state])


def print_summary(combined: dict) -> None:
    s = combined["sentiment"]
    print()
    print("=" * 50)
    print("  FINAL RESULTS  (devset reviews only)")
    print("=" * 50)
    print(f"  Combined from segments  : {combined.get('segments', 1)}")
    print(f"  Total reviews processed : {combined['totalDevsetReviews']}")
    print(f"  Positive                : {s['positive']}")
    print(f"  Neutral                 : {s['neutral']}")
    print(f"  Negative                : {s['negative']}")
    print(f"  Failed profanity check  : {combined['profanityFailed']}")
    banned = combined["bannedUsers"]
    if banned:
        counts = combined.get("userProfaneCounts", {})
        print(f"  Banned users ({len(banned)})       :")
        for uid in banned:
            print(f"    - {uid}  (impolite reviews: {counts.get(uid, '?')})")
    else:
        print("  Banned users            : none")
    if combined.get("overlap", 0):
        print(f"  WARNING: {combined['overlap']} overlapping reviewId(s) across segments -- "
              f"counts may be inflated! Segments should be disjoint.")
    print("=" * 50)
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description="Combine segment files into the final results.")
    parser.add_argument("--checkpoints-dir", default=checkpoints.DEFAULT_DIR,
                        help="Directory holding segment_*.json files.")
    parser.add_argument("--out", default=DEFAULT_OUT, help="Path to write results.json.")
    parser.add_argument("--upload", action="store_true",
                        help="Also upload results.json to the results-export S3 bucket (needs MiniStack).")
    parser.add_argument("--endpoint", default=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
                        help="MiniStack endpoint (only used with --upload).")
    args = parser.parse_args()

    segments = checkpoints.load_all(args.checkpoints_dir)
    if segments:
        combined = checkpoints.combine(segments)
    elif os.path.exists(LEGACY_FILE):
        print(f"No segments in {args.checkpoints_dir}; using legacy {LEGACY_FILE}.")
        combined = _combine_legacy(LEGACY_FILE)
    else:
        print(f"ERROR: no segment files in {args.checkpoints_dir} and no {LEGACY_FILE} -- "
              f"has the pipeline run yet?", file=sys.stderr)
        return 1

    results = _required_keys(combined)
    out_path = Path(args.out)
    out_path.write_text(json.dumps(results, indent=2))
    print(f"Results written to: {out_path}")
    print_summary(combined)

    if args.upload:
        try:
            import generate_results  # reuse client + SSM helpers
            _ddb, ssm, s3 = generate_results._make_clients(args.endpoint)
            export_bucket = generate_results._param(ssm, "/dic-a3/buckets/export")
            s3.put_object(
                Bucket=export_bucket,
                Key="results.json",
                Body=out_path.read_bytes(),
                ContentType="application/json",
            )
            print(f"Also uploaded to s3://{export_bucket}/results.json")
        except Exception as exc:
            print(f"Warning: S3 upload failed (results.json is still saved locally): {exc}",
                  file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
