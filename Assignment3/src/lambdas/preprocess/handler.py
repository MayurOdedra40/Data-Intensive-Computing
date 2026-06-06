"""L1 preprocess  (STAGE-1 STUB).

Real version (Stage 2) will tokenize + remove stopwords + lemmatize summary+reviewText
using utils/text_processing.py. For now it just proves the relay works: read the review
from the ingest bucket, add an empty `tokens` list, write it to the preprocessed bucket.

Trigger:  new object in `reviews-ingest`.
Writes:   the same object (key unchanged) into `reviews-preprocessed`.
"""
import config
import s3_events


def handler(event, context):
    target = config.get("/dic-a3/buckets/preprocessed")
    for bucket, key in s3_events.parse_records(event):
        envelope = s3_events.read_envelope(bucket, key)
        envelope.setdefault("tokens", [])  # Stage 2 replaces this with real tokens
        s3_events.write_envelope(target, key, envelope)
    return {"statusCode": 200}
