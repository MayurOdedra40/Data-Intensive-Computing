"""Part 2 — TF-IDF + ChiSqSelector via the Spark ML DataFrame pipeline.

Builds the canonical Assignment 2 feature pipeline and writes the 2000
ChiSqSelector-picked vocabulary terms to ``output_ds.txt`` (one alphabetical,
space-separated line — matching the merged-dictionary format on the last
line of Assignment 1's ``output.txt`` so a direct set comparison is trivial).

The ``build_feature_pipeline(num_top_features)`` factory is the contract
Part 3 (Person D) plugs into for grid-search experiments — it returns an
*unfit* :class:`pyspark.ml.Pipeline` so D can vary ``num_top_features`` and
extend the stage list with ``Normalizer`` + ``OneVsRest(LinearSVC)``.

Run locally::

    spark-submit --master local[*] src/part2_pipeline/pipeline.py \
        --mode local \
        --input "../Assignment 1/src/Assignment_1_Assets/reviews_devset.json" \
        --stopwords src/common/stopwords.txt \
        --output outputs/output_ds.txt

Run on the cluster (build common.zip first via scripts/build_common_zip.sh)::

    spark-submit --master yarn --deploy-mode cluster \
        --py-files common.zip \
        --files src/common/stopwords.txt \
        src/part2_pipeline/pipeline.py \
        --mode cluster \
        --input hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
        --stopwords stopwords.txt \
        --output output_ds.txt
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Make `from common...` imports work both in dev (running from
# `Assignment 2/` with `src/` on PYTHONPATH) and in cluster mode (where
# common.zip is shipped via --py-files and lives at the executor cwd).
_HERE = Path(__file__).resolve().parent
_SRC = _HERE.parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from common.data_loader import load_reviews_df  # noqa: E402
from common.spark_session import build_spark  # noqa: E402
from common.text_utils import REGEX_TOKENIZER_PATTERN, load_stopwords  # noqa: E402


# ---------------------------------------------------------------------------
# Pipeline factory — reused by Part 3 with a different num_top_features
# ---------------------------------------------------------------------------


# Stage indices on the fitted pipeline. Kept as constants so Part 3 can
# reach into the same positions when extracting models.
TOKENIZER_STAGE = 0
STOPWORDS_STAGE = 1
COUNTVEC_STAGE = 2
IDF_STAGE = 3
LABEL_STAGE = 4
SELECTOR_STAGE = 5


def build_feature_pipeline(
    num_top_features: int = 2000,
    stopwords: list[str] | None = None,
):
    """Return an unfit :class:`pyspark.ml.Pipeline` for Part 2 / Part 3.

    Stages (in order, indices match the module-level ``*_STAGE`` constants):

    0. ``RegexTokenizer`` — locked delimiter pattern from
       :data:`common.text_utils.REGEX_TOKENIZER_PATTERN`, lowercased,
       ``minTokenLength=2`` (matches Part 1's ``len > 1`` filter).
    1. ``StopWordsRemover`` — uses the supplied stopword list, falls back
       to Spark's English defaults when ``stopwords`` is None.
    2. ``CountVectorizer`` — produces raw term-frequency counts. Chosen
       over ``HashingTF`` because we need to recover the vocabulary to
       write ``output_ds.txt``.
    3. ``IDF`` — multiplies TF by inverse document frequency.
    4. ``StringIndexer`` — turns the string ``category`` column into a
       numeric ``label`` column (required by ``ChiSqSelector``).
    5. ``ChiSqSelector(numTopFeatures=num_top_features)`` — keeps the
       top-K vocabulary indices ranked by chi² against the label.

    The classifier in Part 3 plugs additional stages (Normalizer,
    OneVsRest+LinearSVC) onto this pipeline.
    """

    # Imports kept inside the factory so this module can be inspected on
    # systems without PySpark installed (e.g., the parity test).
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import (
        ChiSqSelector,
        CountVectorizer,
        IDF,
        RegexTokenizer,
        StopWordsRemover,
        StringIndexer,
    )

    tokenizer = RegexTokenizer(
        inputCol="reviewText",
        outputCol="tokens",
        pattern=REGEX_TOKENIZER_PATTERN,
        gaps=True,
        toLowercase=True,
        minTokenLength=2,
    )

    stop_remover = StopWordsRemover(
        inputCol="tokens",
        outputCol="tokens_clean",
    )
    if stopwords is not None:
        stop_remover.setStopWords(stopwords)

    count_vec = CountVectorizer(
        inputCol="tokens_clean",
        outputCol="tf",
    )

    idf = IDF(
        inputCol="tf",
        outputCol="tfidf",
    )

    label_indexer = StringIndexer(
        inputCol="category",
        outputCol="label",
        handleInvalid="keep",
    )

    selector = ChiSqSelector(
        numTopFeatures=num_top_features,
        featuresCol="tfidf",
        outputCol="selected_features",
        labelCol="label",
    )

    return Pipeline(stages=[
        tokenizer, stop_remover, count_vec, idf, label_indexer, selector
    ])


def extract_selected_terms(fitted_pipeline) -> list[str]:
    """Pull the alphabetical vocabulary terms picked by ChiSqSelector.

    Reaches into ``fitted_pipeline.stages`` at the ``COUNTVEC_STAGE`` and
    ``SELECTOR_STAGE`` indices, looks up the selector's chosen feature
    indices in the CountVectorizer's vocabulary, and sorts.
    """

    cv_model = fitted_pipeline.stages[COUNTVEC_STAGE]
    sel_model = fitted_pipeline.stages[SELECTOR_STAGE]
    vocab = cv_model.vocabulary
    return sorted(vocab[i] for i in sel_model.selectedFeatures)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def run(
    input_path: str,
    stopwords_path: str,
    output_path: str,
    mode: str,
    num_top_features: int = 2000,
) -> None:
    spark = build_spark(app_name="Part2-DF-Pipeline", mode=mode)

    # In cluster mode the user passes `--py-files common.zip` to
    # spark-submit, so executors can import `common.*`. In local mode the
    # driver IS the executor (they share the same Python process), so the
    # `sys.path.insert(0, _SRC)` at module top is enough — no addPyFile
    # needed. We deliberately skip the local zip-and-ship dance Part 1
    # does, because addPyFile triggers `Subject.getSubject` deep in
    # Hadoop's UGI which is unsupported on Java 18+.

    try:
        df = load_reviews_df(spark, input_path)

        # Stopwords loaded once on the driver, passed into the pipeline as
        # a plain list (Spark serialises it into the StopWordsRemover stage
        # — no broadcast needed at this size, ~600 entries).
        stopwords = sorted(load_stopwords(stopwords_path))

        pipeline = build_feature_pipeline(
            num_top_features=num_top_features,
            stopwords=stopwords,
        )
        fitted = pipeline.fit(df)

        terms = extract_selected_terms(fitted)
        if len(terms) != num_top_features:
            print(
                f"[warn] ChiSqSelector returned {len(terms)} terms "
                f"(expected {num_top_features}); the dev set may be too "
                "small or the vocabulary smaller than num_top_features.",
                file=sys.stderr,
            )

        out_path = Path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(" ".join(terms) + "\n", encoding="utf-8")
        print(f"wrote {out_path} ({len(terms)} terms)")
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
        help="Where to write the resulting output_ds.txt.",
    )
    parser.add_argument(
        "--mode",
        choices=("local", "cluster"),
        default="local",
        help="Execution mode. Affects only the SparkSession master setup.",
    )
    parser.add_argument(
        "--num-top-features",
        type=int,
        default=2000,
        help="ChiSqSelector top-K (default 2000 per the assignment spec).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run(
        input_path=args.input,
        stopwords_path=args.stopwords,
        output_path=args.output,
        mode=args.mode,
        num_top_features=args.num_top_features,
    )


if __name__ == "__main__":
    main()
