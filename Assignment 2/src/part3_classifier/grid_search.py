import sys
import os
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import Normalizer
from pyspark.ml.classification import LinearSVC, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from src.part2_pipeline.pipeline import build_feature_pipeline, SELECTOR_STAGE
import pandas as pd

# Add the root of the repository to the Python path (same as train_svm)
repo_root = os.path.abspath('../../')
if repo_root not in sys.path:
    sys.path.append(repo_root)


# ---------------------------------------------------------------------------
# Init local Spark (same as train_svm)
# ---------------------------------------------------------------------------

spark = SparkSession.builder.appName("Task3_GridSearch").master("local[*]").getOrCreate()


# ---------------------------------------------------------------------------
# Load dataset and split into Train / Test
# ---------------------------------------------------------------------------

data = spark.read.json("Assignment_1/reviews_devset.json")

train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)


# ---------------------------------------------------------------------------
# Build the feature pipeline (same as train_svm)
# ---------------------------------------------------------------------------

base_pipeline = build_feature_pipeline(num_top_features=2000)

# Grab ChiSqSelector stage directly for the param grid
chi_sq_selector = base_pipeline.getStages()[SELECTOR_STAGE]

# Create the normalizer (same as train_svm)
normalizer = Normalizer(inputCol="selected_features", outputCol="normalizedFeatures", p=2.0)

# Create SVM (same as train_svm, regParam and maxIter will be varied by grid)
svm = LinearSVC(featuresCol="normalizedFeatures", labelCol="label")

# Wrap in OneVsRest for multi-class support (same as train_svm)
ovr = OneVsRest(classifier=svm, featuresCol="normalizedFeatures", labelCol="label")

# Assemble the final pipeline (same as train_svm)
final_stages = base_pipeline.getStages() + [normalizer, ovr]
final_pipeline = Pipeline(stages=final_stages)


# ---------------------------------------------------------------------------
# Evaluator — F1 score
# ---------------------------------------------------------------------------

evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)


# ---------------------------------------------------------------------------
# Parameter Grid — 24 combinations (2 × 3 × 2 × 2)
#   numTopFeatures : [200, 2000]
#   regParam       : [0.01, 0.1, 1.0]
#   standardization: [True, False]
#   maxIter        : [10, 50]
# ---------------------------------------------------------------------------

paramGrid = (
    ParamGridBuilder()
    .addGrid(chi_sq_selector.numTopFeatures, [200, 2000])
    .addGrid(svm.regParam,                   [0.01, 0.1, 1.0])
    .addGrid(svm.standardization,            [True, False])
    .addGrid(svm.maxIter,                    [10, 50])
    .build()
)


# ---------------------------------------------------------------------------
# TrainValidationSplit — 80/20 train/validation split
# ---------------------------------------------------------------------------

tvs = TrainValidationSplit(
    estimator=final_pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    trainRatio=0.8,
    parallelism=4
)


# ---------------------------------------------------------------------------
# Train (grid search over all 24 combinations)
# ---------------------------------------------------------------------------

print("Starting grid search — 24 combinations ...")
tvs_model = tvs.fit(train_data)


# ---------------------------------------------------------------------------
# Evaluate best model on held-out test set
# ---------------------------------------------------------------------------

predictions = tvs_model.transform(test_data)
f1_score = evaluator.evaluate(predictions)
print(f"Final F1 Score on test set: {f1_score:.4f}")


# ---------------------------------------------------------------------------
# Save all grid search results to CSV
# ---------------------------------------------------------------------------

results = []

for metric, params in zip(tvs_model.validationMetrics, tvs.getEstimatorParamMaps()):
    row = {"f1_score": metric}
    for p, v in params.items():
        row[p.name] = v
    results.append(row)

results_df = pd.DataFrame(results)
results_df = results_df.sort_values("f1_score", ascending=False).reset_index(drop=True)

os.makedirs("outputs", exist_ok=True)
results_df.to_csv("outputs/grid_search_results.csv", index=False)

print("grid_search_results.csv saved to outputs/")
print(results_df.to_string())

spark.stop()
