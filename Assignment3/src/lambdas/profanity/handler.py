"""L2 profanity-check  (STAGE-1 STUB).

Real version (Stage 3) will scan summary+reviewText with a profanity library. For now it
honors the `forceProfane` test hook (see CONTRACT.md) so the ban logic can be tested end to
end before real detection exists.

Trigger:  new object in `reviews-preprocessed`.
Writes:   the envelope (+ isProfane, badWords) into `reviews-profanity`.
"""
import config
import s3_events


def handler(event, context):
    target = config.get("/dic-a3/buckets/profanity")
    for bucket, key in s3_events.parse_records(event):
        envelope = s3_events.read_envelope(bucket, key)
        # Stage 3 replaces this line with a real profanityfilter scan of summary+reviewText.
        envelope["isProfane"] = bool(envelope.get("forceProfane", False))
        envelope["badWords"] = ["<forced>"] if envelope["isProfane"] else []
        s3_events.write_envelope(target, key, envelope)
    return {"statusCode": 200}
