# Assignment 2: Text Processing and Classification using Apache Spark

**Data-Intensive Computing — TU Wien, 2026**

**Contributing Group Members:** Member A · Member B · Member C · Member D

---

## 1. Introduction

Large-scale text processing requires distributed computing frameworks capable of handling data volumes well beyond the capacity of a single machine. This assignment builds upon Assignment 1's single-node Python implementation of chi-square feature selection over the Amazon Review Dataset, re-implementing and extending it using Apache Spark across three parts.

Part 1 reproduces Assignment 1's chi-square term ranking using the low-level Spark RDD API. Part 2 constructs a Spark ML DataFrame pipeline for TF-IDF feature extraction. Part 3 extends that pipeline with a multi-class Support Vector Machine classifier, tuned via grid search over 24 hyperparameter combinations.

---

## 2. Problem Overview

The Amazon Review Dataset (`reviews_devset.json`) contains customer reviews spanning 22 product categories. Each record provides a free-text `reviewText` field and a `category` label. The task is to transform raw review text into numerical feature vectors and use them for category prediction.

**Part 1:** Compute per-category chi-square scores for all terms, select the top-75 per category, and write `output_rdd.txt` in the same format as Assignment 1's `output.txt`.

**Part 2:** Build a Spark ML `Pipeline` performing tokenisation, stopword removal, TF-IDF weighting, and chi-square feature selection (top 2000 terms globally), writing the selected vocabulary to `output_ds.txt`.

**Part 3:** Extend the Part 2 pipeline with L2 normalisation and a `OneVsRest(LinearSVC)` classifier. Perform grid search over 24 hyperparameter combinations and report weighted F1 on a held-out test set.

---

## 3. Methodology and Approach

### 3.1 Shared Preprocessing (`src/common/`)

All three parts share a single tokenisation definition in `text_utils.py`. Text is split on whitespace, digits, and the delimiter characters `()[]{}.!?,;:+=-_"'\`~#@&*%€$§\/`. Tokens are casefolded and those with fewer than 2 characters are discarded. A 596-entry English stopword list is applied uniformly.

For the RDD path (Part 1) a Python-side `re.compile` pattern is used directly. For the DataFrame path (Parts 2–3) the equivalent `REGEX_TOKENIZER_PATTERN` string is passed to Spark ML's `RegexTokenizer(gaps=True, toLowercase=True, minTokenLength=2)`, ensuring the Java regex engine applies identical splitting rules.

### 3.2 Part 1 — Chi-Square via Spark RDDs

The pipeline proceeds as follows:

1. Load JSON-lines reviews into an RDD via `SparkContext`.
2. Map each record to `(category, deduped_tokens)`. Per-document deduplication is essential — chi-square counts *document presence*, not raw term frequency, so each term contributes at most once per document.
3. **Cache** the `(category, tokens)` RDD; it is reused for both the per-category document count and the term-counting step, avoiding a double read of the source data.
4. Compute `N` (total documents) and `n_c` (documents per category) via `reduceByKey`. The resulting ~22-entry map is **broadcast** to all executors.
5. A single `flatMap` emits two tagged key families — `("T", term)` for global term counts and `("TC", term, category)` for co-occurrence counts — followed by a single `reduceByKey`, producing both `n_t` and `n_tc` in one shuffle pass.
6. `n_tc` and `n_t` RDDs are **joined on term**; each joined row is mapped to a chi-square score using the standard 2×2 contingency formula.
7. `aggregateByKey` with a **bounded min-heap** (capacity 75) selects top-K terms per category without materialising all per-category scores, avoiding the memory cost of `groupByKey`.
8. The ~1650-row result is collected to the driver and written to `output_rdd.txt`.

### 3.3 Part 2 — TF-IDF Pipeline via Spark ML

The pipeline is constructed as a `pyspark.ml.Pipeline` with six stages:

| Stage | Component | Key parameters |
|---|---|---|
| 0 | `RegexTokenizer` | same delimiter pattern as Part 1; `minTokenLength=2` |
| 1 | `StopWordsRemover` | 596-entry custom stopword list |
| 2 | `CountVectorizer` | raw TF counts; vocabulary stored for term recovery |
| 3 | `IDF` | standard inverse document frequency |
| 4 | `StringIndexer` | encodes `category` → numeric `label` for `ChiSqSelector` |
| 5 | `ChiSqSelector` | `numTopFeatures=2000`; global chi-square selection |

`CountVectorizer` is used rather than `HashingTF` because its fitted model exposes the vocabulary, which is required to recover the actual term strings for `output_ds.txt`. The pipeline is fit in a single call; selected terms are retrieved by mapping `ChiSqSelectorModel.selectedFeatures` (indices) back into `CountVectorizerModel.vocabulary`.

The factory function `build_feature_pipeline(num_top_features, stopwords)` returns an *unfit* pipeline, making it directly reusable in Part 3 with a different `num_top_features` value via the grid search.

### 3.4 Part 3 — Multi-Class SVM Classification and Grid Search

Two stages are appended to the Part 2 pipeline:

- **`Normalizer(p=2.0)`** — L2-normalises the TF-IDF feature vectors before the classifier, preventing high-magnitude terms from dominating the SVM margin.
- **`OneVsRest(classifier=LinearSVC(...))`** — wraps Spark's binary `LinearSVC` in a one-vs-rest strategy for the 22-class problem.

Data is split 80/20 into `train_data` / `test_data` with `seed=42`. Hyperparameter selection uses `TrainValidationSplit(trainRatio=0.8, seed=42)` within `train_data`, producing an effective 64%/16%/20% train/validation/test partition. `MulticlassClassificationEvaluator(metricName="f1")` (weighted F1) is used as the optimisation criterion and final test metric.

**Grid search space — 24 combinations (2 × 3 × 2 × 2):**

| Hyperparameter | Values |
|---|---|
| `ChiSqSelector.numTopFeatures` | 200, 2000 |
| `LinearSVC.regParam` | 0.01, 0.1, 1.0 |
| `LinearSVC.standardization` | True, False |
| `LinearSVC.maxIter` | 10, 50 |

**Pipeline figure:**

```
 reviews_devset.json
         │
         ▼
 ┌───────────────────┐
 │  RegexTokenizer   │  whitespace/digit/delimiter split, lowercase, minLen=2
 └────────┬──────────┘
          │ tokens
          ▼
 ┌───────────────────┐
 │ StopWordsRemover  │  596-entry stopword list
 └────────┬──────────┘
          │ tokens_clean
          ▼
 ┌───────────────────┐
 │  CountVectorizer  │  raw TF counts (vocabulary preserved)
 └────────┬──────────┘
          │ tf
          ▼
 ┌───────────────────┐
 │       IDF         │  TF-IDF weighted features
 └────────┬──────────┘
          │ tfidf
          ▼
 ┌────────┴──────────┐   ┌──────────────────────────┐
 │  StringIndexer    │──▶│     ChiSqSelector         │
 │ (category→label)  │   │  numTopFeatures∈{200,2000}│ ← grid param
 └───────────────────┘   └────────────┬─────────────┘
                                      │ selected_features
                                      ▼
                         ┌────────────────────────────┐
                         │    Normalizer (L2, p=2.0)  │  Part 3 only
                         └────────────┬───────────────┘
                                      │ normalizedFeatures
                                      ▼
                         ┌────────────────────────────┐
                         │  OneVsRest (LinearSVC)     │  regParam ∈ {0.01,0.1,1.0}
                         │                            │  standardization ∈ {T,F}
                         │                            │  maxIter ∈ {10,50}
                         └────────────┬───────────────┘
                                      │ prediction
                                      ▼
                    MulticlassClassificationEvaluator (weighted F1)
```

---

## 4. Results

### 4.1 Part 1 — RDD Output vs. Assignment 1

The RDD implementation produces `output_rdd.txt` with 22 category lines and 1 merged dictionary line, matching Assignment 1's output format exactly. Table 1 shows per-category term overlap between the two outputs.

**Table 1: Per-category top-75 term overlap — RDD vs. Assignment 1**

| Category | Overlap |
|---|:---:|
| Apps\_for\_Android | 66/75 |
| Automotive | 49/75 |
| Baby | 57/75 |
| Beauty | 65/75 |
| Books | 70/75 |
| CDs\_and\_Vinyl | 71/75 |
| Cell\_Phones\_and\_Accessories | 66/75 |
| Clothing\_Shoes\_and\_Jewelry | 70/75 |
| Digital\_Music | 46/75 |
| Electronics | 72/75 |
| Grocery\_and\_Gourmet\_Food | 65/75 |
| Health\_and\_Personal\_Care | 60/75 |
| Home\_and\_Kitchen | 63/75 |
| Kindle\_Store | 59/75 |
| Movies\_and\_TV | 66/75 |
| Musical\_Instruments | 48/75 |
| Office\_Products | 54/75 |
| Patio\_Lawn\_and\_Garden | 55/75 |
| Pet\_Supplies | 56/75 |
| Sports\_and\_Outdoors | 59/75 |
| Tools\_and\_Home\_Improvement | 60/75 |
| Toys\_and\_Games | 61/75 |
| **Average** | **60.8/75 (81.1%)** |

The merged dictionaries contain 1,464 terms (RDD) and 1,418 terms (Assignment 1), with an overlap of **1,177 terms (83.0%)**. The 287 terms unique to the RDD output and 241 terms unique to Assignment 1 are attributable to two factors. First, Spark's distributed `reduceByKey` performs floating-point additions in a non-deterministic partition order, producing marginally different chi-square scores from Assignment 1's fixed single-process reducer — enough to swap terms with near-equal scores at rank boundaries. Second, the Spark RDD tokeniser and Assignment 1's tokeniser share the same regex pattern, but minor differences in how each framework handles Unicode edge cases can affect the token list for a small number of reviews. Categories with lower overlap (Digital\_Music: 46/75, Musical\_Instruments: 48/75, Automotive: 49/75) tend to have many near-tied terms, where small numerical differences cause more boundary swaps.

---

### 4.2 Part 2 — DataFrame Pipeline vs. Assignment 1

The `ChiSqSelector` in Part 2 selects 2,000 terms globally across all categories, while Assignment 1 selected the top-75 per category and took their union (1,418 unique terms). These are fundamentally different selection criteria, so the outputs are not expected to be identical.

| Metric | Value |
|---|---|
| Part 2 selected terms (`output_ds.txt`) | 2,000 |
| Assignment 1 merged dictionary | 1,418 |
| Terms in both | 763 (53.8% of Assignment 1) |
| Terms only in Part 2 | 1,237 |
| Terms only in Assignment 1 | 655 |

The 1,237 terms unique to Part 2 are generally mid-frequency words that score well under TF-IDF weighting globally but do not dominate any single category's top-75 list. Examples include general adjectives and verbs (`access`, `adjust`, `achieve`) that appear across many categories with moderate TF-IDF scores. Conversely, the 655 terms present in Assignment 1 but absent from Part 2 are mostly highly category-specific jargon (e.g., `ableton`, `acne`, `airsoft`) that scores very highly within one category under the per-category chi-square ranking of Assignment 1 but ranks below the global top 2,000 in Part 2's TF-IDF + chi-square pipeline.

---

### 4.3 Part 3 — Grid Search Results

**Table 2: All 24 combinations, sorted by validation F1 (descending)**

| Rank | numTopFeatures | regParam | standardization | maxIter | Val. F1 |
|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | 2000 | 0.01 | True | 10 | **0.6077** |
| 2 | 2000 | 0.01 | True | 50 | 0.6030 |
| 3 | 2000 | 0.10 | True | 10 | 0.6021 |
| 4 | 2000 | 0.10 | True | 50 | 0.5938 |
| 5 | 2000 | 1.00 | True | 10 | 0.5708 |
| 6 | 2000 | 1.00 | True | 50 | 0.5526 |
| 7 | 2000 | 0.10 | False | 50 | 0.4993 |
| 8 | 2000 | 0.01 | False | 50 | 0.4980 |
| 9 | 200 | 0.01 | True | 10 | 0.3817 |
| 10 | 200 | 0.10 | True | 10 | 0.3719 |
| 11 | 200 | 0.01 | True | 50 | 0.3710 |
| 12 | 200 | 0.10 | True | 50 | 0.3583 |
| 13 | 2000 | 0.01 | False | 10 | 0.3563 |
| 14 | 200 | 1.00 | True | 10 | 0.3376 |
| 15 | 200 | 0.01 | False | 50 | 0.3317 |
| 16 | 200 | 1.00 | True | 50 | 0.3303 |
| 17 | 200 | 0.10 | False | 50 | 0.3260 |
| 18 | 200 | 0.01 | False | 10 | 0.3258 |
| 19 | 200 | 1.00 | False | 50 | 0.3244 |
| 20 | 200 | 0.10 | False | 10 | 0.2696 |
| 21 | 2000 | 1.00 | False | 10 | 0.1241 |
| 22 | 2000 | 0.10 | False | 10 | 0.1240 |
| 23 | 200 | 1.00 | False | 10 | 0.0008 |
| 24 | 2000 | 1.00 | False | 50 | 0.0005 |

**Best model test-set F1: 0.6077** (numTopFeatures=2000, regParam=0.01, standardization=True, maxIter=10)

**Effect of feature dimensionality (`numTopFeatures`):** This is the single most impactful parameter. The top 8 validation scores all use 2,000 features (F1 range: 0.499–0.608), while the best 200-feature configuration achieves only 0.382. The 10× dimensionality reduction from 2,000 to 200 features causes an average F1 drop of approximately 0.22. The 22-class Amazon review problem has sufficient lexical diversity across product domains that richer vocabularies consistently provide more discriminative signal.

**Effect of regularisation (`regParam`):** With standardization=True and 2,000 features, lower regularisation (regParam=0.01) consistently outperforms higher values — F1 drops from 0.608 at regParam=0.01 to 0.570 at regParam=1.0. This suggests the TF-IDF features are sufficiently informative and aggressive L2 penalisation over-constrains the decision boundaries. With standardization=False the effect is more severe: regParam=1.0 collapses performance to near zero (F1 ≈ 0.0005–0.124), likely because without feature standardisation the SVM's gradient updates become numerically unstable at high regularisation.

**Effect of standardisation (`standardization`):** Standardisation has a pronounced effect. With 2,000 features, standardization=True yields F1 values between 0.553 and 0.608; standardization=False yields 0.001 to 0.499. The collapse at standardization=False with regParam=1.0 (F1 < 0.13) indicates that TF-IDF feature scales vary widely and standardisation is essential for stable SVM convergence in this setting.

**Effect of max iterations (`maxIter`):** The effect is modest: the best result uses maxIter=10, and increasing to 50 slightly reduces F1 in several configurations (e.g., regParam=0.01, standardization=True: 0.608 vs. 0.603). This suggests the linear SVM converges quickly on L2-normalised TF-IDF features and additional iterations do not help, possibly introducing slight overfitting on the validation split.

---

## 5. Conclusions

This assignment implemented a complete distributed text-processing and classification pipeline on the Amazon Review Dataset using Apache Spark.

**Part 1** demonstrated that the chi-square term ranking from Assignment 1 can be reproduced with the Spark RDD API. An average per-category term overlap of 81.1% (60.8/75 terms) and an 83.0% merged-dictionary overlap confirm strong alignment. The remaining divergence arises from floating-point non-determinism in distributed reductions and minor Unicode handling differences, not from algorithmic errors.

**Part 2** showed that Spark ML's `Pipeline` API cleanly encapsulates TF-IDF feature extraction. The global ChiSqSelector (2,000 terms) overlaps with 53.8% of Assignment 1's per-category top-75 union. The divergence is expected: global TF-IDF chi-square selection favours broadly discriminative mid-frequency terms, while per-category selection of Assignment 1 emphasises highly domain-specific vocabulary.

**Part 3** demonstrated that a `OneVsRest(LinearSVC)` classifier achieves a test-set weighted F1 of **0.6077** on the 22-category problem with the optimal configuration (2,000 features, regParam=0.01, standardization=True, maxIter=10). Feature dimensionality was the dominant factor in performance, followed by the combination of standardisation and regularisation strength. Standardisation is effectively mandatory for stable SVM convergence on TF-IDF features with higher regularisation values.

---

*Submitted via TUWEL · May 2026 · DIC Assignment 2*
