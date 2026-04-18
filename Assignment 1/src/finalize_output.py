# Final Output Formatter
#
# Formats Job 3's top-K output into the assignment's required shape:
# one line per category (alphabetical) with `term:chi2` pairs in
# descending chi-square order, followed by a final line listing every
# surviving term alphabetically. Runs locally — Job 3's output is small
# enough that a MapReduce pass would just add scheduling overhead.

import json
from collections import defaultdict
import sys

# Per-category (term, chi2) lists and the merged vocabulary.
category_terms = defaultdict(list)
all_terms = set()

# Parse Job 3's tab-separated lines.
for line in sys.stdin:
    try:
        key_str, value_str = line.split("\t", 1)
        category = json.loads(key_str)
        term, score = json.loads(value_str)

        category_terms[category].append((term, float(score)))
        all_terms.add(term)
    except Exception:
        # Skip malformed lines rather than fail the whole formatting pass.
        continue

# Emit one line per category; sort key (-chi2, term) matches Job 3's ordering.
for category in sorted(category_terms.keys()):
    terms = sorted(category_terms[category], key=lambda x: (-x[1], x[0]))
    parts = [f"{term}:{score}" for term, score in terms]
    print(f"{category} " + " ".join(parts))

# Final line: the merged alphabetical dictionary of every surviving term.
print(" ".join(sorted(all_terms)))
