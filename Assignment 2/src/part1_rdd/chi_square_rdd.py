"""Part 1 — chi² ranking via Spark RDDs.

Reproduces Assignment 1's per-category top-K chi² output using the RDD API.
Output schema matches Assignment 1's `output.txt` line-for-line:

    <category> term1:score1 term2:score2 ... term75:score75
    ...
    <alphabetical merged dictionary across all surviving terms>

Run locally::

    spark-submit --master local[*] src/part1_rdd/chi_square_rdd.py \
        --mode local \
        --input "../Assignment 1/src/Assignment_1_Assets/reviews_devset.json" \
        --stopwords src/common/stopwords.txt \
        --output outputs/output_rdd.txt

Run on the cluster (zip ``src/common/`` into ``common.zip`` first)::

    spark-submit --master yarn --deploy-mode cluster \
        --py-files common.zip \
        --files src/common/stopwords.txt \
        src/part1_rdd/chi_square_rdd.py \
        --mode cluster \
        --input hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
        --stopwords stopwords.txt \
        --output output_rdd.txt
"""

from __future__ import annotations

import argparse
import heapq
import sys
import tempfile
import zipfile
from operator import add
from pathlib import Path
from typing import Iterable

# Make `from common...` imports work both in dev (running from
# `Assignment 2/` with `src/` on PYTHONPATH) and in cluster mode (where
# common.zip is shipped via --py-files and lives at the executor cwd).
_HERE = Path(__file__).resolve().parent
_SRC = _HERE.parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from common.data_loader import load_reviews_rdd  # noqa: E402
from common.spark_session import build_spark  # noqa: E402
from common.text_utils import load_stopwords, preprocess  # noqa: E402


# ---------------------------------------------------------------------------
# Numerical core (verbatim port of Assignment 1's job2_chi2.py:107-115)
# ---------------------------------------------------------------------------


def compute_chi2(N: int, n_t: int, n_c: int, n_tc: int) -> float | None:
    """Standard 2x2 contingency chi² statistic.

    A = docs with the term in the category
    B = docs with the term outside the category
    C = docs without the term inside the category
    D = docs without the term outside the category

    Returns None when the denominator is zero (only happens for terms
    appearing in every doc or only in one cell of the table — extremely
    rare on real data, but guarded for safety).
    """

    A = n_tc
    B = n_t - A
    C = n_c - A
    D = N - A - B - C
    denom = (A + B) * (C + D) * (A + C) * (B + D)
    if denom <= 0:
        return None
    return (N * (A * D - B * C) ** 2) / denom


# ---------------------------------------------------------------------------
# Bounded-heap top-K-per-category (avoids groupByKey shuffle of all values)
# ---------------------------------------------------------------------------


class _RevStr:
    """String wrapper with inverted ordering, for heap tie-breaking.

    The final per-category sort uses `(-chi2, term)` ascending — when chi²
    ties, alphabetically smaller terms rank higher. The bounded min-heap
    must therefore evict alphabetically *larger* terms first within a tie.
    Wrapping the term in `_RevStr` flips comparison so the heap's min-tuple
    points to the term we want to evict next.
    """

    def __init__(self, term: str) -> None:
        self.term = term

    def __lt__(self, other: "_RevStr") -> bool:
        return self.term > other.term

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _RevStr) and self.term == other.term

    def __hash__(self) -> int:
        return hash(self.term)

    def __repr__(self) -> str:
        return f"_RevStr({self.term!r})"


def _make_heap_ops(top_k: int):
    """Return (seq_op, comb_op) closures for aggregateByKey.

    The accumulator is a min-heap of ``(chi2, _RevStr(term))`` tuples bounded
    to ``top_k`` elements. ``heappushpop`` evicts the smallest tuple, which
    is the term with smallest chi² (or, on chi² tie, alphabetically largest
    term — exactly the Assignment 1 ordering).
    """

    def seq_op(heap: list, value: tuple[str, float]) -> list:
        term, chi2 = value
        item = (chi2, _RevStr(term))
        if len(heap) < top_k:
            heapq.heappush(heap, item)
        else:
            heapq.heappushpop(heap, item)
        return heap

    def comb_op(left: list, right: list) -> list:
        if len(right) > len(left):
            left, right = right, left
        for item in right:
            if len(left) < top_k:
                heapq.heappush(left, item)
            else:
                heapq.heappushpop(left, item)
        return left

    return seq_op, comb_op


# ---------------------------------------------------------------------------
# Driver-side output writer (verbatim port of Assignment 1's
# finalize_output.py:31-37)
# ---------------------------------------------------------------------------


def format_output(result: dict[str, list[tuple[str, float]]]) -> str:
    """Render the final 23-line output text.

    `result` maps category -> list of `(term, chi2)`. Lines are emitted in
    alphabetical category order, with each category's terms sorted by
    `(-chi2, term)`. The trailing line is the alphabetical union of all
    surviving terms across all categories' top-K lists.
    """

    all_terms: set[str] = set()
    lines: list[str] = []
    for category in sorted(result.keys()):
        terms = sorted(result[category], key=lambda x: (-x[1], x[0]))
        parts = [f"{term}:{score}" for term, score in terms]
        all_terms.update(t for t, _ in terms)
        lines.append(f"{category} " + " ".join(parts))
    lines.append(" ".join(sorted(all_terms)))
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def run(
    input_path: str,
    stopwords_path: str,
    output_path: str,
    mode: str,
    top_k: int = 75,
) -> None:
    spark = build_spark(app_name="Part1-RDD-ChiSquare", mode=mode)
    sc = spark.sparkContext

    # Ship the `common` package to executors. In cluster mode the user is
    # expected to pass `--py-files common.zip` to spark-submit and this block
    # can be skipped. In local mode we build a zip on the fly and ship it so
    # the script is self-contained — `addPyFile(zip)` puts the package on the
    # worker PYTHONPATH, so `from common.text_utils import preprocess`
    # resolves on executors during cloudpickle unpickle.
    if mode == "local":
        common_dir = Path(__file__).resolve().parent.parent / "common"
        zip_path = Path(tempfile.gettempdir()) / "part1_common.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            for py in sorted(common_dir.glob("*.py")):
                zf.write(py, arcname=f"common/{py.name}")
        sc.addPyFile(str(zip_path))

    try:
        records = load_reviews_rdd(sc, input_path)

        # Broadcast stopwords once. The driver loads from local FS — under
        # cluster mode `--files stopwords.txt` puts the file at executor
        # cwd, but only the driver reads it here, then ships the frozenset
        # to executors via broadcast.
        stopwords = load_stopwords(stopwords_path)
        sw_bc = sc.broadcast(stopwords)

        # N counts every parsed record (matches Assignment 1
        # job1_counts.py:55 — the "N" emit is unconditional after parse,
        # before any token-list filter).
        N = records.count()
        if N <= 0:
            raise RuntimeError("No reviews parsed from input.")

        # `cat_records` keeps EVERY parsed record (including ones whose
        # reviewText preprocesses to an empty token list). Assignment 1
        # increments `C:<cat>` unconditionally after parse (job1_counts.py:58),
        # so n_c must include empty-token records too. Fallback to 'Unknown'
        # matches Assignment 1's `record.get('category', 'Unknown')`.
        cat_records = records.map(
            lambda r: (
                str(r.get("category", "Unknown") or "Unknown").strip(),
                preprocess(r.get("reviewText", ""), sw_bc.value),
            )
        )
        cat_records.cache()

        # n_c per category (~22 keys — safe to collect to driver and broadcast).
        n_c_local = (
            cat_records.map(lambda ct: (ct[0], 1))
            .reduceByKey(add)
            .collectAsMap()
        )
        n_c_bc = sc.broadcast(n_c_local)
        N_bc = sc.broadcast(N)

        # For per-term aggregations, drop empty-token records (no tokens to
        # contribute to n_t / n_tc anyway).
        cat_tokens = cat_records.filter(lambda ct: ct[1])

        # Single flatMap emits both count families with tagged keys; one
        # reduceByKey reduces both at once.
        pairs = cat_tokens.flatMap(
            lambda ct: [(("T", t), 1) for t in ct[1]]
            + [(("TC", t, ct[0]), 1) for t in ct[1]]
        )
        counts = pairs.reduceByKey(add)

        n_t_rdd = counts.filter(lambda kv: kv[0][0] == "T").map(
            lambda kv: (kv[0][1], kv[1])
        )
        n_tc_rdd = counts.filter(lambda kv: kv[0][0] == "TC").map(
            lambda kv: (kv[0][1], (kv[0][2], kv[1]))
        )

        # Co-locate by term so each (term, category) row knows its n_t.
        joined = n_tc_rdd.join(n_t_rdd)
        # joined :: (term, ((category, n_tc), n_t))

        scored = (
            joined.map(
                lambda x: (
                    x[1][0][0],  # category
                    (
                        x[0],  # term
                        compute_chi2(
                            N_bc.value,
                            x[1][1],  # n_t
                            n_c_bc.value[x[1][0][0]],  # n_c
                            x[1][0][1],  # n_tc
                        ),
                    ),
                )
            )
            .filter(lambda x: x[1][1] is not None)
        )
        # scored :: (category, (term, chi2))

        seq_op, comb_op = _make_heap_ops(top_k)
        topk = scored.aggregateByKey([], seq_op, comb_op)
        # topk :: (category, [(chi2, term), ...])  (heap order, not sorted)

        # Tiny: 22 categories x top_k tuples. The `tasks.md` ban on
        # `collect()` targets the full term list, not the bounded result.
        result_raw = topk.collectAsMap()
        result = {
            cat: [(rev.term, chi2) for chi2, rev in heap]
            for cat, heap in result_raw.items()
        }

        # Sanity checks — soft warnings keep the script usable on tiny
        # samples for development.
        for category, terms in result.items():
            if len(terms) != top_k:
                print(
                    f"[warn] category {category!r} produced {len(terms)} "
                    f"terms (expected {top_k}); the data may be too small.",
                    file=sys.stderr,
                )

        out_text = format_output(result)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(out_text, encoding="utf-8")

        cat_records.unpersist()
    finally:
        spark.stop()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument(
        "--input",
        required=True,
        help="Path to reviews JSON-lines (local FS path or hdfs:// URI).",
    )
    parser.add_argument(
        "--stopwords",
        required=True,
        help="Path to stopwords.txt (driver-readable).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Where to write the resulting output_rdd.txt.",
    )
    parser.add_argument(
        "--mode",
        choices=("local", "cluster"),
        default="local",
        help="Execution mode. Affects only the SparkSession master setup.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=75,
        help="Top-K terms per category (default 75 to match Assignment 1).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run(
        input_path=args.input,
        stopwords_path=args.stopwords,
        output_path=args.output,
        mode=args.mode,
        top_k=args.top_k,
    )


if __name__ == "__main__":
    main()
