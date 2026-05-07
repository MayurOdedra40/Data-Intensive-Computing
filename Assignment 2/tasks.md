Assignment 2 - Task Division

Repo structure 
Assignment_2/
├── src/
│   ├── README.md                       # B
│   ├── common/                         # B
│   │   ├── __init__.py
│   │   ├── text_utils.py
│   │   ├── data_loader.py
│   │   ├── spark_session.py
│   │   └── stopwords.txt
│   ├── part1_rdd/                      # A
│   │   ├── chi_square_rdd.ipynb
│   │   └── chi_square_rdd.py
│   ├── part2_pipeline/                 # C
│   │   ├── pipeline.ipynb
│   │   └── pipeline.py
│   └── part3_classifier/               # D
│       ├── train_svm.ipynb
│       └── train_svm.py
├── outputs/
│   ├── output_rdd.txt                  # A
│   ├── output_ds.txt                   # C
│   └── grid_search_results.csv         # D
├── report/
│   ├── report.pdf                      # E
│   ├── report.tex (or .docx)           # E
│   └── pipeline_figure.png             # E
└── Assignment_1/
    └── output.txt                      # Already done, get it from the zip folder (committed Day 1)

Person A — Part 1 (RDDs) Lead
Owns: RDD chi² implementation and output_rdd.txt.
Delivers:
src/part1_rdd/chi_square_rdd.ipynb and exported .py
outputs/output_rdd.txt
A 1-paragraph comparison vs. Assignment 1 for the report
Receives from B: shared tokeniser function, stopword broadcast, JSON loader. 
Receives from E: Assignment 1's output.txt checked into the repo for comparison. 
Hands to others: category and term frequency counts that C can use as a sanity check for Part 2.
Watch out for:
Don't collect() the full term list. Use reduceByKey and top(n, key=...) per category.
Cache the (category, tokens) RDD before computing per-term and per-category aggregates.
Make sure tokenisation matches B's regex exactly.

Person B — Shared Utilities + Cluster Plumbing
Owns: the src/common/ module that everyone depends on.
Delivers:
text_utils.py with the exact RegexTokenizer pattern matching the spec, plus a Python-side regex equivalent for Part 1 RDD use
stopwords.txt (use Spark's English list; document the source)
data_loader.py with helpers to read the dev set from either local path or HDFS based on a flag
spark_session.py with a configured builder (executor memory, dynamic allocation, etc.)
A README.md in src/ explaining how to run each part
Receives from everyone: agreement on the tokeniser regex by Day 3. Lock it down before A and C build on top. 
Hands to others: a single source of truth for tokenisation, stopwords, and I/O. 
This is the most important shared dependency.
Watch out for:
The spec's delimiter list contains € and § (non-ASCII) and a backtick (`). Test the regex on a synthetic string containing every delimiter before declaring done.
Suggested PySpark RegexTokenizer pattern: [\\s\\d()\\[\\]{}.!?,;:+=\\-_"'\u0060~#@&*%€$§\\\\/]+ with gaps=True, toLowercase=True.

Person C — Part 2 (DataFrame Pipeline) Lead
Owns: Spark ML pipeline and output_ds.txt.
Delivers:
src/part2_pipeline/pipeline.ipynb and exported .py
A reusable function build_feature_pipeline(num_top_features=2000) returning an unfit Pipeline (so D can plug it in)
outputs/output_ds.txt — the 2000 selected terms, sorted
A 1-paragraph comparison vs. Part 1 / Assignment 1 for the report
Receives from B: tokenizer regex, stopwords, data loader. 
Receives from A: sanity-check counts (top 5 most frequent terms per category, total vocabulary size). 
Hands to D: build_feature_pipeline parameterized by num_top_features so the grid search can swap 2000 for a smaller value.
Watch out for:
Use CountVectorizer, not HashingTF — you need to recover the vocabulary for output_ds.txt.
ChiSqSelector needs a numeric label column → use StringIndexer on category.
Don't fit the pipeline twice — fit once, reuse.
The selected terms come from the fitted ChiSqSelectorModel.selectedFeatures (indices) → look them up in the CountVectorizerModel.vocabulary.
The spec explicitly warns: results from Part 2 will not be identical to Part 1 / Assignment 1, and intermediate outputs are not directly comparable. The comparison paragraph should describe overlap and divergence, not flag differences as bugs.

Person D — Part 3 (SVM + Grid Search) Lead
Owns: classification pipeline, experiments, results table.
Delivers:
src/part3_classifier/train_svm.ipynb and .py (run this one with spark-submit, not interactively)
outputs/grid_search_results.csv with one row per parameter combination: F1, train time, hyperparameters
The best model's F1 on the held-out test set
2–3 paragraphs interpreting the results for the report
Receives from C: build_feature_pipeline(num_top_features). 
Receives from B: data loader, Spark session config. 
Hands to E: results CSV, best hyperparameters, interpretation text.
Grid search dimensions (24 combinations total):
Parameter
Values
Feature filtering
(a) ChiSqSelector top 2000 (default), (b) a much heavier filter — see Spark ML docs for options (e.g. ChiSqSelector with 100–200 features, or percentile/fpr/fdr/fwe selector types)
Regularization
3 values (e.g., 0.001, 0.01, 0.1)
Standardization
True, False
Max iterations
2 values (e.g., 10, 50)

Watch out for:
Use OneVsRest(classifier=LinearSVC(...)) — multi-class strategy required by the spec.
Use TrainValidationSplit (single split) rather than CrossValidator — the spec asks for train/val/test, not k-fold, and CV multiplies runtime by k.
Set seed=42 on randomSplit, LinearSVC, and TrainValidationSplit.
For the "heavier filter" comparison, pick something like 200 features so the dimensionality difference is meaningful.
Downsample the dev set if cluster time is tight (the spec explicitly allows it).
Run via spark-submit, not in JupyterHub interactively.

Person E — Report, Pipeline Figure, Coordination, Submission
Owns: report.pdf, the pipeline diagram, the final zip, and code documentation review.
Delivers:
report/report.pdf — ≤8 pages, 11pt, one-column, 5 required sections, contributing members listed at the top
A clean pipeline figure (draw.io, Excalidraw, or TikZ — readable, ≤1 page)
The final <groupID>_DIC2026_Assignment_2.zip correctly named
Code documentation review across everyone's notebooks (10pts on the line)
Day 1: a report skeleton committed to the repo so A, C, D can fill in their sections directly
Receives from A, C, D: a 2–3 sentence methods description and a results paragraph from each. 
Receives from A: output_rdd.txt and Assignment 1 comparison. 
Receives from C: output_ds.txt and Part 2 comparison. 
Receives from D: grid search results CSV and interpretation.
Required report sections:
Introduction
Problem Overview
Methodology and Approach (with the pipeline figure, max 1 page)
Results — must include performance indicators across all parameter settings explored, with interpretation
Conclusions
Format constraints (from spec): ≤8 pages A4, 11pt font, one-column layout.
Final QA responsibility: after the zip is built, unzip it on the cluster and re-run one notebook end-to-end to confirm the submission actually works.

