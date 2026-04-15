# Job 1: Feature Counting Module Guide

This document explains the MapReduce logic implemented in `job1_counts.py`. This job is the first step in the MapReduce pipeline for calculating Chi-Square values for text classification.

## Purpose

Job 1 is responsible for the "Observation Phase," transforming the Amazon Review dataset into a summarized set of aggregated counts.

## Class Overview: `MRJob1Counts`

### `configure_args()`
Defines the command-line arguments for the job.
* **`--stopwords`**: Accepts the path to `stopwords.txt`. This file is distributed to all worker nodes via the Hadoop distributed cache.

### `mapper_init()`
Runs once per worker node before any records are processed.
* **Function**: Loads the stopword list into memory.
* **Efficiency**: Avoids the overhead of repeatedly opening the stopword file for millions of individual review records.

### `mapper(_, line)`
The primary processing unit for every JSON review record in the dataset.
1. **Parsing**: Safely converts the raw JSON string into a Python dictionary.
2. **Feature Extraction**: Extracts the `reviewText` for tokenization and the `category` for classification.
3. **Preprocessing**: Applies the mandatory delimiters, converts text to lowercase, and filters out stopwords.
4. **Filtering**: Removes any tokens consisting of only one character.
5. **Deduplication**: Converts the list of tokens into a `set` to ensure counts represent document presence rather than frequency.
6. **Emissions**:
    * **`"N"`**: Emits a count of 1 for total reviews in the dataset (N).
    * **`"C:<category>"`**: Emits a count of 1 for the specific category (C).
    * **`"T:<term>"`**: Emits a count of 1 for the global presence of a term (T).
    * **`"TC:<term>:<category>"`**: Emits a count of 1 for the joint presence of a term within a specific category (TC).

### `combiner(key, counts)`
Runs locally on the Map node to perform partial aggregation.
* **Logic**: Sums the `1`s for each key locally before the data is shuffled across the network.
* **Efficiency**: Significantly reduces network I/O, which is crucial for processing the full 56GB dataset within the given timeframe.

### `reducer(key, counts)`
The final stage of the MapReduce job.
* **Function**: Receives partial sums from all combiners and performs the final global summation.
* **Output**: Produces the definitive counts used as input for the subsequent Chi-Square calculation.

---

## Usage Instructions

### Local Development (Devset)
Use the provided 0.1% sample (`reviews_devset.json`) to verify logic and output format.
```bash
python src/job1_counts.py --stopwords Assignment_1_Assets/stopwords.txt Assignment_1_Assets/reviews_devset.json > local_counts.txt
```

### Cluster evaluation

```bash
python3 src/job1_counts.py -r hadoop \
  --files Assignment_1_Assets/stopwords.txt,src/utils/text_processing.py \
  --stopwords stopwords.txt \
  <path_to_reviews_file>
```