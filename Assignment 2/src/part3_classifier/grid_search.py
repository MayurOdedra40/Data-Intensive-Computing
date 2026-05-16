"""Part 3 — Grid search over TF-IDF + ChiSqSelector + LinearSVC pipeline.

Run on the cluster::

    spark-submit --master yarn --deploy-mode cluster \\
        --py-files common.zip,pipeline.zip \\
        --files src/common/stopwords.txt \\
        src/part3_classifier/grid_search.py \\
        --mode cluster \\
        --input hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \\
        --stopwords stopwords.txt \\
        --output hdfs:///user/e12550218/grid_search_results.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Path fix for local dev mode
_HERE = Path(__file__).resolve().parent
_SRC = _HERE.parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from pyspark.ml import Pipeline
from pyspark.ml.classification import LinearSVC, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import Normalizer
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import SparkSession

from common.text_utils import load_stopwords
from part2_pipeline.pipeline import SELECTOR_STAGE, build_feature_pipeline


def build_spark(mode: str, app_name: str = "Part3-GridSearch") -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if mode == "local":
        builder = builder.master("local[*]")
    # In cluster mode YARN sets the master — do NOT call .master()
    return builder.getOrCreate()


def run(
    input_path: str,
    stopwords_path: str,
    output_path: str,
    mode: str,
) -> None:
    spark = build_spark(mode)

    df = spark.read.json(input_path)
    # Drop rows with null reviewText or category
    df = df.filter(df.reviewText.isNotNull() & df.category.isNotNull())

    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    stopwords = sorted(load_stopwords(stopwords_path))

    # Build base feature pipeline (stages 0-5)
    base_pipeline = build_feature_pipeline(num_top_features=2000, stopwords=stopwords)
    chi_sq_selector = base_pipeline.getStages()[SELECTOR_STAGE]

    normalizer = Normalizer(
        inputCol="selected_features",
        outputCol="normalizedFeatures",
        p=2.0,
    )
    svm = LinearSVC(featuresCol="normalizedFeatures", labelCol="label")
    ovr = OneVsRest(classifier=svm, featuresCol="normalizedFeatures", labelCol="label")

    final_pipeline = Pipeline(stages=base_pipeline.getStages() + [normalizer, ovr])

    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1",
    )

    # 24 combinations: 2 × 3 × 2 × 2
    paramGrid = (
        ParamGridBuilder()
        .addGrid(chi_sq_selector.numTopFeatures, [200, 2000])
        .addGrid(svm.regParam,                   [0.01, 0.1, 1.0])
        .addGrid(svm.standardization,            [True, False])
        .addGrid(svm.maxIter,                    [10, 50])
        .build()
    )

    tvs = TrainValidationSplit(
        estimator=final_pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        trainRatio=0.8,
        parallelism=4,
    )

    print("Starting grid search — 24 combinations ...")
    tvs_model = tvs.fit(train_data)

    predictions = tvs_model.transform(test_data)
    f1_score = evaluator.evaluate(predictions)
    print(f"Final F1 Score on test set: {f1_score:.4f}")

    # Build results as list of strings (no pandas dependency)
    header_parts = ["f1_score", "numTopFeatures", "regParam", "standardization", "maxIter"]
    rows = ["f1_score,numTopFeatures,regParam,standardization,maxIter"]
    for metric, params in sorted(
        zip(tvs_model.validationMetrics, tvs.getEstimatorParamMaps()),
        key=lambda x: -x[0],
    ):
        pmap = {p.name: v for p, v in params.items()}
        rows.append(
            f"{metric:.6f},"
            f"{pmap.get('numTopFeatures', '')},"
            f"{pmap.get('regParam', '')},"
            f"{pmap.get('standardization', '')},"
            f"{pmap.get('maxIter', '')}"
        )

    # Write CSV to HDFS (cluster) or local (local mode)
    if output_path.startswith("hdfs://") or mode == "cluster":
        spark.sparkContext.parallelize(rows, 1).saveAsTextFile(output_path)
        print(f"Results written to HDFS: {output_path}")
    else:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text("\n".join(rows) + "\n", encoding="utf-8")
        print(f"Results written locally: {output_path}")

    spark.stop()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",     required=True)
    parser.add_argument("--stopwords", required=True)
    parser.add_argument("--output",    required=True)
    parser.add_argument("--mode", choices=("local", "cluster"), default="local")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run(
        input_path=args.input,
        stopwords_path=args.stopwords,
        output_path=args.output,
        mode=args.mode,
    )


if __name__ == "__main__":
    main()
