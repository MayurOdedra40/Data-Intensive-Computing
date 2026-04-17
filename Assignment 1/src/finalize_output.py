# Final Output Formatter
# Author: Ali Hassan

import json
from collections import defaultdict
import sys

category_terms = defaultdict(list)
all_terms = set()

for line in sys.stdin:
    try:
        key_str, value_str = line.split("\t", 1)
        category = json.loads(key_str)
        term, score = json.loads(value_str)

        category_terms[category].append((term, float(score)))
        all_terms.add(term)
    except Exception:
        continue

for category in sorted(category_terms.keys()):
    terms = sorted(category_terms[category], key=lambda x: (-x[1], x[0]))
    parts = [f"{term}:{score}" for term, score in terms]
    print(f"{category} " + " ".join(parts))

print(" ".join(sorted(all_terms)))
