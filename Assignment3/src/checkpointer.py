"""checkpointer.py -- continuously snapshot THIS run's pipeline state to its segment file.

Started in the background by run_cluster.sh. Every `--interval` seconds it reads DynamoDB (the
Reviews + Customers tables) and writes the current run's state to a segment file on the HOST
filesystem -- OUTSIDE MiniStack -- so progress survives an interrupted run (SSH drop, timeout, OOM)
AND a full MiniStack restart. Each run writes its OWN segment (checkpoints/segment_NNN.json); the
runs process disjoint reviews, so finalize_results.py just adds the segments up. See checkpoints.py
for the segment model and why this survives a restart.

Why the host (not a Lambda) writes this file: Lambdas can only write to S3/DynamoDB, and many run
concurrently, so a single shared file would race. A single host-side reader is race-free and is the
natural place to persist to local disk.

Usage:
    python checkpointer.py --interval 30 --out checkpoints/segment_000.json   # loop (background)
    python checkpointer.py --once        --out checkpoints/segment_000.json   # one snapshot, exit
"""
from __future__ import annotations

import argparse
import os
import signal
import sys
import time

import checkpoints       # reuse the atomic segment writer
import generate_results  # reuse _make_clients, _scan_all, compute_state (no duplication)

# Default: write this run's own segment file (see checkpoints.py). run_cluster.sh passes an explicit
# --out for the current run; standalone, we just write the next segment.
DEFAULT_OUT = checkpoints.segment_path(checkpoints.DEFAULT_DIR, 0)

_STOP = False


def _handle_stop(signum, _frame):
    """Let run_cluster.sh stop us cleanly (SIGTERM/SIGINT) between ticks."""
    global _STOP
    _STOP = True


def snapshot(ddb, ssm, out_path: str) -> dict:
    """Read THIS run's DynamoDB once and persist it as a segment. Returns the state."""
    state = generate_results.compute_state(ddb, ssm)
    checkpoints.write_atomic(out_path, state)
    return state


def main() -> int:
    parser = argparse.ArgumentParser(description="Snapshot pipeline state to intermediate_results.json.")
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        help="MiniStack endpoint URL (default: http://localhost:4566)",
    )
    parser.add_argument("--interval", type=float, default=30.0,
                        help="Seconds between snapshots (default: 30). Ignored with --once.")
    parser.add_argument("--once", action="store_true",
                        help="Write a single snapshot and exit (use after the run drains).")
    parser.add_argument("--out", default=DEFAULT_OUT,
                        help="Path to the intermediate results file.")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    ddb, ssm, _ = generate_results._make_clients(args.endpoint)

    if args.once:
        try:
            state = snapshot(ddb, ssm, args.out)
        except Exception as exc:
            print(f"checkpointer: ERROR writing final snapshot ({exc})", file=sys.stderr)
            return 1
        print(f"checkpointer: wrote {args.out}  processed={state['processedCount']}  "
              f"banned={len(state['bannedUsers'])}", flush=True)
        return 0

    # Continuous mode: never let a transient DynamoDB error kill the loop -- the whole point is to
    # keep persisting progress for the entire run.
    print(f"checkpointer: snapshotting {args.out} every {args.interval:g}s "
          f"(endpoint={args.endpoint})", flush=True)
    while not _STOP:
        try:
            state = snapshot(ddb, ssm, args.out)
            print(f"checkpointer: processed={state['processedCount']:<6} "
                  f"pos={state['sentiment']['positive']} "
                  f"neu={state['sentiment']['neutral']} "
                  f"neg={state['sentiment']['negative']} "
                  f"profane={state['profanityFailed']} "
                  f"banned={len(state['bannedUsers'])}", flush=True)
        except Exception as exc:
            print(f"checkpointer: snapshot failed, will retry ({exc})", file=sys.stderr, flush=True)

        # Sleep in small slices so SIGTERM is honoured promptly.
        slept = 0.0
        while slept < args.interval and not _STOP:
            time.sleep(min(1.0, args.interval - slept))
            slept += 1.0

    print("checkpointer: stopped.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
