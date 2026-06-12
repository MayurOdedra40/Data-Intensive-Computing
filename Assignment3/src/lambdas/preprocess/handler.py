"""L1 preprocess  (STAGE-2 real implementation).

Reads the review envelope from reviews-ingest, tokenizes the combined
summary+reviewText with POS-aware WordNet lemmatization, and writes the
enriched envelope (with `tokens`) to reviews-preprocessed.

Trigger:  new object in `reviews-ingest`.
Writes:   the same envelope + `tokens` into `reviews-preprocessed`.
"""
import os
import re

import nltk

# Download NLTK data to /tmp on cold start (Lambda filesystem is read-only elsewhere).
_NLTK_DIR = "/tmp/nltk_data"
nltk.data.path.insert(0, _NLTK_DIR)
for _pkg in ("averaged_perceptron_tagger", "wordnet", "stopwords", "omw-1.4"):
    nltk.download(_pkg, download_dir=_NLTK_DIR, quiet=True)

from nltk.corpus import stopwords as _nltk_sw
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer

import config
import s3_events

# ── tokenizer ──────────────────────────────────────────────────────────────
# Same delimiter set as utils/text_processing.py so preprocessing is consistent
# with Assignment-1 results.
_DELIMITERS = "()[]{}.!?,;:+=-_\"'`~#@&*%€$§\\/"
_SPLIT_RE = re.compile(r"[\s\d" + re.escape(_DELIMITERS) + r"]+", re.UNICODE)

# Load English stopwords once at cold start (cached by NLTK internally).
_STOPWORDS = frozenset(_nltk_sw.words("english"))

_lemmatizer = WordNetLemmatizer()


def _penn_to_wn(tag: str):
    """Map a Penn Treebank POS tag to the WordNet equivalent."""
    if tag.startswith("J"):
        return wordnet.ADJ
    if tag.startswith("V"):
        return wordnet.VERB
    if tag.startswith("R"):
        return wordnet.ADV
    return wordnet.NOUN  # default covers N* and unknowns


def _preprocess(text: str) -> list:
    """Full pipeline: tokenize → POS-tag → lemmatize → stopword-filter → dedup."""
    if not text or not text.strip():
        return []

    # Case-fold and split on whitespace / digits / delimiters.
    raw = [t for t in _SPLIT_RE.split(text.casefold()) if t]
    if not raw:
        return []

    # POS-aware lemmatization.
    tagged = nltk.pos_tag(raw)
    lemmatized = [_lemmatizer.lemmatize(w, _penn_to_wn(tag)) for w, tag in tagged]

    # Filter: min length 2, not a stopword, deduplicate preserving first-seen order.
    seen: set = set()
    result = []
    for token in lemmatized:
        if len(token) <= 1 or token in _STOPWORDS or token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


def handler(event, context):
    target = config.get("/dic-a3/buckets/preprocessed")
    for bucket, key in s3_events.parse_records(event):
        envelope = s3_events.read_envelope(bucket, key)
        summary = envelope.get("summary") or ""
        review_text = envelope.get("reviewText") or ""
        envelope["tokens"] = _preprocess(f"{summary} {review_text}")
        s3_events.write_envelope(target, key, envelope)
    return {"statusCode": 200}
