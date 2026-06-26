"""Duplicate reviewId checker for reviews_devset.json.

Run from the data/ directory:   python dup.py

Verifies that the NEW make_review_id (a SHA-256 of the ENTIRE review object) gives every row a
unique id unless two rows are byte-for-byte identical. Prints, for contrast, how many rows the OLD
id collided. Exits non-zero if any two DIFFERENT objects share a new id (which must never happen).
"""
import hashlib
import json
import sys
from collections import defaultdict


def make_review_id(rec: dict) -> str:
    """EXACT copy of src/loader.py make_review_id -- keep in sync."""
    canonical = json.dumps(rec, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def old_make_review_id(rec: dict) -> str:
    """The previous formula, kept only to show how many ids it collided."""
    text = (rec.get("reviewText") or "") + (rec.get("summary") or "")
    h = hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]
    return f'{rec["reviewerID"]}_{rec["asin"]}_{rec["unixReviewTime"]}_{h}'


with open("reviews_devset.json", encoding="utf-8") as f:
    data = [json.loads(line) for line in f if line.strip()]

# Group rows by NEW id.
groups = defaultdict(list)
for obj in data:
    groups[make_review_id(obj)].append(obj)

bad_collisions = 0   # same new id but the objects DIFFER  -> would be a real bug
true_duplicate_rows = 0  # same new id AND identical objects -> legitimate duplicate rows
for key, objs in groups.items():
    if len(objs) > 1:
        distinct = {json.dumps(o, sort_keys=True) for o in objs}
        if len(distinct) == 1:
            true_duplicate_rows += len(objs) - 1
        else:
            bad_collisions += 1
            print(f"BAD COLLISION on {key}:")
            for o in objs:
                print("   ", json.dumps(o, sort_keys=True))

# Old id collisions, for contrast.
old_groups = defaultdict(list)
for obj in data:
    old_groups[old_make_review_id(obj)].append(obj)
old_collided_rows = sum(len(v) - 1 for v in old_groups.values() if len(v) > 1)

print("-" * 60)
print(f"rows in dataset                         : {len(data)}")
print(f"distinct NEW ids                        : {len(groups)}")
print(f"NEW collisions between DIFFERENT objects : {bad_collisions}   <-- must be 0")
print(f"true duplicate rows (identical objects)  : {true_duplicate_rows}")
print(f"rows that collided under the OLD id      : {old_collided_rows}")
print("-" * 60)
print("OK -- no duplicate ids for differing reviews." if bad_collisions == 0
      else "FAIL -- different reviews share an id (see above).")
sys.exit(1 if bad_collisions else 0)
