"""L2 profanity-check  (STAGE-3 real implementation).

Reads the preprocessed envelope, scans summary+reviewText with profanityfilter,
adds isProfane (bool) and badWords (list[str]), and writes to reviews-profanity.

Note: `overall` is a numeric star-rating so it cannot contain bad words;
only the two text fields are scanned (documented in the report).

Trigger:  new object in `reviews-preprocessed`.
Writes:   the envelope + `isProfane` + `badWords` into `reviews-profanity`.
"""
from better_profanity import profanity as _pf

import config
import s3_events

# Initialise once at cold start — loading the word-list is expensive.
_pf.load_censor_words()


def _check(text: str) -> tuple:
    """Return (is_profane: bool, bad_words: list[str]) for the given text."""
    is_profane = _pf.contains_profanity(text)

    bad_words = []
    if is_profane:
        seen: set = set()
        for word in text.lower().split():
            # Strip surrounding punctuation before testing the bare word.
            clean = word.strip(".,!?;:\"'()[]{}")
            if clean and clean not in seen and _pf.contains_profanity(clean):
                seen.add(clean)
                bad_words.append(clean)

    return is_profane, bad_words


def handler(event, context):
    target = config.get("/dic-a3/buckets/profanity")
    for bucket, key in s3_events.parse_records(event):
        envelope = s3_events.read_envelope(bucket, key)
        if envelope.get("forceProfane"):
            # Test-fixture escape hatch: mark as profane without running the filter.
            envelope["isProfane"] = True
            envelope["badWords"] = []
        else:
            summary = envelope.get("summary") or ""
            review_text = envelope.get("reviewText") or ""
            is_profane, bad_words = _check(f"{summary} {review_text}")
            envelope["isProfane"] = is_profane
            envelope["badWords"] = bad_words
        s3_events.write_envelope(target, key, envelope)
    return {"statusCode": 200}
