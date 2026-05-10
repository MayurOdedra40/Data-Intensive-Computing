### Part 3 — Text Classification (SVM + Grid Search)

**What:** Extends the feature extraction pipeline from Part 2 to train a multi-class Support Vector Machine (SVM). Uses an exhaustive hyperparameter grid search to find the optimal model configuration, evaluating based on the F1 measure.
**Input:** Output of Part 2's `build_feature_pipeline` + JSON-lines Amazon reviews (`reviewText`, `category`).
**Output:** `outputs/grid_search_results.csv` — detailing the F1 score, training time, and hyperparameters for all 24 grid combinations.

**How it works:**
* Imports the unfitted base pipeline (`Tokenizer` → `CountVectorizer` → `StringIndexer` → `ChiSqSelector`) from `part2_pipeline`.
* **Normalization:** Appends a `Normalizer` (L2 norm) to scale the feature vectors, as required by the spec.
* **Classifier:** Appends a `LinearSVC` wrapped in a `OneVsRest` estimator to handle the multi-class categorization of the dataset.
**TODO:**
* **Data Prep:** Splits the data into a Train and Test set.
* **Tuning:** Wraps the unified pipeline in a `TrainValidationSplit` (80/20 ratio).
* **Param Grid:** Tests 24 combinations across:
  * Feature filtering: `numTopFeatures` (2000 vs. heavier filtering like 200)
  * Regularization: `regParam` (3 values)
  * Standardization: `standardization` (True/False)
  * Iterations: `maxIter` (10 vs 50)
* Evaluates using `MulticlassClassificationEvaluator` (F1 metric).

**Run locally**
*TODO*

**Run on the cluster**
*TODO*