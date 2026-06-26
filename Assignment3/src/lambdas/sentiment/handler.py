"""L3 sentiment-analysis  (STAGE-4 real implementation).

Reads the profanity-checked envelope, scores summary+reviewText with NLTK VADER,
blends the compound score with the normalised star rating, and maps the result to
positive / neutral / negative using thresholds from SSM.

Blending formula (overall normalised from [1,5] → [-1,+1]):
    blended = (1 - overall_weight) * vader_compound
              + overall_weight * ((overall - 3) / 2)

Thresholds (SSM):
    blended >= sentiment-pos  → "positive"
    blended <= sentiment-neg  → "negative"
    otherwise                 → "neutral"

Trigger:  new object in `reviews-profanity`.
Writes:   envelope + `sentiment` + `sentimentScore` → `reviews-scored`.
"""
import os

import nltk

# NLTK data resolution: prefer the corpora pre-bundled into the zip by run.sh (so the Lambda needs
# no network at runtime -- the cluster sandbox may have none), fall back to a /tmp download for
# local dev. `nltk.data.find` is offline; `nltk.download` runs only on a miss. See the longer
# comment in lambdas/preprocess/handler.py.
_BUNDLED_NLTK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nltk_data")
_TMP_NLTK = "/tmp/nltk_data"
for _p in (_BUNDLED_NLTK, _TMP_NLTK):
    if _p not in nltk.data.path:
        nltk.data.path.insert(0, _p)

# Check the exact resource VADER loads (it reads the lexicon from INSIDE the bundled zip via
# this zip-internal path), so a bundled vader_lexicon.zip satisfies the check with no network.
try:
    nltk.data.find("sentiment/vader_lexicon.zip/vader_lexicon/vader_lexicon.txt")
except LookupError:
    nltk.download("vader_lexicon", download_dir=_TMP_NLTK, quiet=True)

from nltk.sentiment.vader import SentimentIntensityAnalyzer

import config
import s3_events

_sia = SentimentIntensityAnalyzer()

POS_THRESH  = config.get_float("/dic-a3/config/sentiment-pos")
NEG_THRESH  = config.get_float("/dic-a3/config/sentiment-neg")
OVR_WEIGHT  = config.get_float("/dic-a3/config/overall-weight")
# Output bucket read once at cold start (reused across warm invocations), not per invocation.
TARGET_BUCKET = config.get("/dic-a3/buckets/scored")


def _score(summary: str, review_text: str, overall: float) -> tuple:
    text = f"{summary} {review_text}".strip()
    vader = _sia.polarity_scores(text)["compound"] if text else 0.0
    normalised_overall = (overall - 3.0) / 2.0
    blended = (1.0 - OVR_WEIGHT) * vader + OVR_WEIGHT * normalised_overall
    if blended >= POS_THRESH:
        label = "positive"
    elif blended <= NEG_THRESH:
        label = "negative"
    else:
        label = "neutral"
    return label, round(blended, 6)


def handler(event, context):
    for bucket, key in s3_events.parse_records(event):
        envelope = s3_events.read_envelope(bucket, key)
        summary = envelope.get("summary") or ""
        review_text = envelope.get("reviewText") or ""
        # `overall` may be missing OR present-but-null (the loader emits null when a source
        # row has no star rating). `.get(..., 3.0)` only covers the missing case, so a null
        # would reach float(None) and crash the whole stage -- fall back to neutral (3.0).
        raw_overall = envelope.get("overall")
        overall = float(raw_overall) if raw_overall is not None else 3.0
        sentiment, score = _score(summary, review_text, overall)
        envelope["sentiment"] = sentiment
        envelope["sentimentScore"] = score
        s3_events.write_envelope(TARGET_BUCKET, key, envelope)
    return {"statusCode": 200}
