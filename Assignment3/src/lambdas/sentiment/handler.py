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
import nltk

_NLTK_DIR = "/tmp/nltk_data"
nltk.data.path.insert(0, _NLTK_DIR)
nltk.download("vader_lexicon", download_dir=_NLTK_DIR, quiet=True)

from nltk.sentiment.vader import SentimentIntensityAnalyzer

import config
import s3_events

_sia = SentimentIntensityAnalyzer()

POS_THRESH  = config.get_float("/dic-a3/config/sentiment-pos")
NEG_THRESH  = config.get_float("/dic-a3/config/sentiment-neg")
OVR_WEIGHT  = config.get_float("/dic-a3/config/overall-weight")


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
    target = config.get("/dic-a3/buckets/scored")
    for bucket, key in s3_events.parse_records(event):
        envelope = s3_events.read_envelope(bucket, key)
        summary = envelope.get("summary") or ""
        review_text = envelope.get("reviewText") or ""
        overall = float(envelope.get("overall", 3.0))
        sentiment, score = _score(summary, review_text, overall)
        envelope["sentiment"] = sentiment
        envelope["sentimentScore"] = score
        s3_events.write_envelope(target, key, envelope)
    return {"statusCode": 200}
