# Preprocessing Module Guide

This document explains the preprocessing logic implemented in `text_processing.py`.
The module is responsible for reading review text, cleaning it, tokenizing it,
and returning document-level tokens that can be reused by the MapReduce jobs.

## Purpose

The preprocessing module does four main things:

- reads stopwords from `stopwords.txt`
- tokenizes only the `reviewText` field
- removes stopwords and one-character tokens
- deduplicates tokens per document so counts represent document presence, not frequency

## Function Overview

### `load_stopwords(path)`

Loads the stopword list from a file and returns it as a set-like collection.
The result is cached so the same stopword file is not loaded repeatedly.

### `tokenize_review_text(review_text)`

Converts the input text to lowercase using case folding and splits it into raw
token candidates using:

- whitespace
- tabs
- digits
- the assignment delimiter characters

This function only splits text. It does not remove stopwords and does not deduplicate.

### `preprocess_review_text(review_text, stopwords)`

Runs the full preprocessing pipeline on raw review text.

It performs these steps in order:

1. calls `tokenize_review_text(review_text)`
2. removes tokens of length 1
3. removes tokens found in the stopword set
4. removes repeated tokens inside the same document

The final result is a list of unique tokens for one review.

### `preprocess_review_record(record, stopwords)`

Extracts `record["reviewText"]` and sends it to `preprocess_review_text(...)`.
This guarantees that only `reviewText` is used for text preprocessing.

### `extract_category(record)`

Reads the `category` field from the review record and returns it as a stripped string.

### `preprocess_review_with_category(record, stopwords)`

Returns both pieces needed by later jobs:

- the review category
- the preprocessed tokens from `reviewText`

Output shape:

```python
(category, tokens)
```

### `parse_review_line(line)`

Safely parses one JSON line from the dataset.

If the line is malformed or not a JSON object, it returns `None`.
This is useful in MapReduce jobs where invalid lines should be skipped safely.

### `iter_preprocessed_reviews(dataset_path, stopwords)`

Reads the dataset line by line and yields:

```python
(record, tokens)
```

For each line, it:

1. reads one line from the JSON-lines dataset
2. calls `parse_review_line(line)`
3. skips invalid records
4. calls `preprocess_review_record(record, stopwords)`
5. yields the original record and its cleaned tokens

### `iter_category_tokens(dataset_path, stopwords)`

Builds on top of `iter_preprocessed_reviews(...)` and yields:

```python
(category, tokens)
```

This is usually the most useful iterator for the MapReduce jobs.

### `preprocess_review_dataset(dataset_path, stopwords_path)`

This is a convenience wrapper.

It first calls `load_stopwords(stopwords_path)` and then calls
`iter_preprocessed_reviews(dataset_path, stopwords)`.

Use this when you have file paths and want the module to handle stopword loading for you.

### `build_argument_parser()`

Creates the command-line argument parser for running the preprocessing module directly.

Supported arguments:

- `--stopwords`
- `--dataset`
- `--limit`

### `main(dataset_path, stopwords_path, limit=None)`

This is the main importable helper for other Python files.

It returns:

```python
[(category, tokens), ...]
```

Internally it:

1. calls `load_stopwords(stopwords_path)`
2. calls `iter_category_tokens(dataset_path, stopwords)`
3. collects the results into a list
4. optionally stops early if `limit` is given

### `run_cli()`

This is the command-line entry point used when the file is run directly.

It:

1. parses CLI arguments using `build_argument_parser()`
2. checks whether a dataset path was provided
3. calls `main(dataset_path, stopwords_path, limit)`
4. prints the returned `(category, tokens)` rows

## Call Flow

## Flow 1: Another Python file imports preprocessing

If another file does:

```python
from utils.text_processing import main

rows = main(dataset_path, stopwords_path)
```

the call sequence is:

1. `main(...)`
2. `load_stopwords(...)`
3. `iter_category_tokens(...)`
4. `iter_preprocessed_reviews(...)`
5. `parse_review_line(...)`
6. `preprocess_review_record(...)`
7. `preprocess_review_text(...)`
8. `tokenize_review_text(...)`

Then the processed result comes back upward as:

1. tokens return to `preprocess_review_text(...)`
2. tokens return to `preprocess_review_record(...)`
3. category and tokens return from `iter_category_tokens(...)`
4. `main(...)` collects everything into a list

## Flow 2: `job_01_counts.py` uses preprocessing

`job_01_counts.py` uses the module in this order:

1. `mapper_init()` calls `load_stopwords(...)`
2. `mapper()` calls `parse_review_line(...)`
3. `mapper()` calls `extract_category(...)`
4. `mapper()` calls `preprocess_review_record(...)`
5. `preprocess_review_record(...)` calls `preprocess_review_text(...)`
6. `preprocess_review_text(...)` calls `tokenize_review_text(...)`
7. the mapper emits document-level keys using the final deduplicated token list

This is why Job 1 correctly counts document presence rather than raw word frequency.

## Flow 3: Running the file directly

If you run:

```bash
python3 "Assignment 1/src/utils/text_processing.py" \
  --stopwords "Assignment 1/Assignment_1_Assets/stopwords.txt" \
  --dataset "Assignment 1/Assignment_1_Assets/reviews_devset.json"
```

the call sequence is:

1. `run_cli()`
2. `build_argument_parser()`
3. `main(...)`
4. `load_stopwords(...)`
5. `iter_category_tokens(...)`
6. `iter_preprocessed_reviews(...)`
7. `parse_review_line(...)`
8. `preprocess_review_record(...)`
9. `preprocess_review_text(...)`
10. `tokenize_review_text(...)`

## Summary

The most important functions are:

- `load_stopwords(...)` for stopword loading
- `preprocess_review_text(...)` for the actual cleaning logic
- `preprocess_review_record(...)` for enforcing `reviewText`-only processing
- `iter_category_tokens(...)` for streaming `(category, tokens)` pairs
- `main(...)` for importing preprocessing from other Python files

Together, these functions keep preprocessing consistent across the full assignment pipeline.
