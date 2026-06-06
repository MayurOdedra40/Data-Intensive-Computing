"""L3 sentiment-analysis  (STAGE-1 STUB).

Real version (Stage 4) will run NLTK VADER over summary+reviewText and blend in the star
rating. For now it just writes a neutral placeholder so the chain reaches L4.

Trigger:  new object in `reviews-profanity`.
Writes:   the envelope (+ sentiment, sentimentScore) into `reviews-scored`.
"""
import config
import s3_events


def handler(event, context):
    target = config.get("/dic-a3/buckets/scored")
    for bucket, key in s3_events.parse_records(event):
        envelope = s3_events.read_envelope(bucket, key)
        envelope.setdefault("sentiment", "neutral")   # Stage 4 replaces with the VADER result
        envelope.setdefault("sentimentScore", 0.0)
        s3_events.write_envelope(target, key, envelope)
    return {"statusCode": 200}
