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

# NLTK data resolution. We PRE-BUNDLE the corpora into the zip (run.sh downloads them into
# package/nltk_data, which lands next to this handler in the deployed Lambda) so the function
# needs NO network at runtime -- important on the cluster, where the Lambda sandbox may have no
# outbound internet. We still keep a /tmp download fallback for local dev where the data wasn't
# bundled. `nltk.data.find` is offline; only `nltk.download` touches the network, and we call it
# strictly on a miss.
_BUNDLED_NLTK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nltk_data")
_TMP_NLTK = "/tmp/nltk_data"
for _p in (_BUNDLED_NLTK, _TMP_NLTK):
    if _p not in nltk.data.path:
        nltk.data.path.insert(0, _p)

# (resource path used by nltk.data.find, download package name)
_REQUIRED = [
    ("taggers/averaged_perceptron_tagger_eng", "averaged_perceptron_tagger_eng"),
    ("taggers/averaged_perceptron_tagger",     "averaged_perceptron_tagger"),
    ("corpora/wordnet",                        "wordnet"),
    ("corpora/omw-1.4",                        "omw-1.4"),
    ("corpora/stopwords",                      "stopwords"),
]
for _res, _pkg in _REQUIRED:
    try:
        nltk.data.find(_res)
    except LookupError:
        nltk.download(_pkg, download_dir=_TMP_NLTK, quiet=True)

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


# Read the output-bucket name from SSM ONCE at cold start and reuse it across warm invocations,
# instead of calling SSM on every invocation (~1 call per review on the hot path). Same pattern as
# aggregate/handler.py and the sentiment thresholds.
TARGET_BUCKET = config.get("/dic-a3/buckets/preprocessed")


def handler(event, context):
    for bucket, key in s3_events.parse_records(event):
        envelope = s3_events.read_envelope(bucket, key)
        summary = envelope.get("summary") or ""
        review_text = envelope.get("reviewText") or ""
        envelope["tokens"] = _preprocess(f"{summary} {review_text}")
        s3_events.write_envelope(TARGET_BUCKET, key, envelope)
    return {"statusCode": 200}
