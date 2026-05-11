import sys
import os
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import Normalizer
from pyspark.ml.classification import LinearSVC, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

from src.part2_pipeline.pipeline import build_feature_pipeline


# Add repository root to Python path
repo_root = os.path.abspath("../../")
if repo_root not in sys.path:
    sys.path.append(repo_root)


# Create Spark Session
spark = SparkSession.builder \
    .appName("Task3_GridSearch") \
    .getOrCreate()


# Load dataset
data = spark.read.json(
    "data/reviews.json"
)


# Train/Test split
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)


# Build feature pipeline
base_pipeline = build_feature_pipeline(num_top_features=2000)


# Create normalizer
normalizer = Normalizer(
    inputCol="selected_features",
    outputCol="normalizedFeatures",
    p=2.0
)


# Create SVM
svm = LinearSVC(
    featuresCol="normalizedFeatures",
    labelCol="label"
)


# OneVsRest
ovr = OneVsRest(
    classifier=svm,
    featuresCol="normalizedFeatures",
    labelCol="label"
)


# Final pipeline
final_stages = base_pipeline.getStages() + [normalizer, ovr]

final_pipeline = Pipeline(stages=final_stages)


# Evaluator
evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)


# Parameter Grid (24 combinations)
paramGrid = ParamGridBuilder() \
    .addGrid(
        base_pipeline.getStages()[-1].numTopFeatures,
        [200, 2000]
    ) \
    .addGrid(
        svm.regParam,
        [0.01, 0.1, 1.0]
    ) \
    .addGrid(
        svm.standardization,
        [True, False]
    ) \
    .addGrid(
        svm.maxIter,
        [10, 50]
    ) \
    .build()


# TrainValidationSplit
tvs = TrainValidationSplit(
    estimator=final_pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    trainRatio=0.8,
    parallelism=4
)


# Train model
tvs_model = tvs.fit(train_data)


# Predictions
predictions = tvs_model.transform(test_data)


# Final evaluation
f1_score = evaluator.evaluate(predictions)

print("Final F1 Score:", f1_score)


# Save grid search results
results = []

for metric, params in zip(
    tvs_model.validationMetrics,
    tvs.getEstimatorParamMaps()
):

    row = {
        "f1_score": metric
    }

    for p, v in params.items():
        row[p.name] = v

    results.append(row)


results_df = pd.DataFrame(results)

results_df.to_csv(
    "grid_search_results.csv",
    index=False
)

print("grid_search_results.csv saved successfully!")
