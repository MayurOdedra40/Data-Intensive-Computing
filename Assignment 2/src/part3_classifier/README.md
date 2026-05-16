### Part 3 — Text Classification (SVM + Grid Search)

**What:** Extends the feature extraction pipeline from Part 2 to train a multi-class Support Vector Machine (SVM). Uses an exhaustive hyperparameter grid search to find the optimal model configuration, evaluating based on the F1 measure.
**Input:** Output of Part 2's `build_feature_pipeline` + JSON-lines Amazon reviews (`reviewText`, `category`).
**Output:** `outputs/grid_search_results.csv` — detailing the F1 score, training time, and hyperparameters for all 24 grid combinations.

**How it works:**
* Imports the unfitted base pipeline (`Tokenizer` → `CountVectorizer` → `StringIndexer` → `ChiSqSelector`) from `part2_pipeline`.
* **Normalization:** Appends a `Normalizer` (L2 norm) to scale the feature vectors, as required by the spec.
* **Classifier:** Appends a `LinearSVC` wrapped in a `OneVsRest` estimator to handle the multi-class categorization of the dataset.
**Experiment design:**
* Splits data 80/20 into train/test (`seed=42`); `TrainValidationSplit(trainRatio=0.8, seed=42)` creates a validation set within train.
* Grid search over 24 combinations:
  * `numTopFeatures`: 200, 2000
  * `regParam`: 0.01, 0.1, 1.0
  * `standardization`: True, False
  * `maxIter`: 10, 50
* Evaluates with `MulticlassClassificationEvaluator(metricName="f1")`.

**Run locally** (from `Assignment 2/` root):
```bash
python3 src/part3_classifier/grid_search.py \
  --mode local \
  --input Assignment_1/reviews_devset.json \
  --stopwords src/common/stopwords.txt \
  --output outputs/grid_search_results.csv
```

**Run on the cluster** (from `Assignment 2/` root):
```bash
scripts/build_common_zip.sh
( cd src && zip -r ../pipeline.zip part2_pipeline -x 'part2_pipeline/__pycache__/*' )
src/part3_classifier/run_cluster.sh
```