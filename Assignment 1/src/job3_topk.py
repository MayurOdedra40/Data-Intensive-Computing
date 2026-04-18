from mrjob.job import MRJob
import json

class Job3TopK(MRJob):
    """For each category, keep the 75 terms with the highest chi-square score.

    Input is Job 2's raw text output (``"<jcat>\\t<jval>"`` per line); the
    reducer's per-category sort is why no combiner is used here.
    """

    def mapper(self, _, line):
        """Parse ``<category>\\t[term, chi2]`` lines and re-emit keyed by category.

        Malformed lines are silently skipped so one bad row can't fail the job.
        """
        try:
            key_str, value_str = line.split("\t", 1)
            category = json.loads(key_str)
            term, score = json.loads(value_str)
            yield category, (term, float(score))
        except Exception:
            pass

    def reducer(self, category, values):
        """Emit the top-75 (term, chi2) pairs for one category.

        Sort key is ``(-chi2, term)`` so ties break alphabetically, making the
        output deterministic across runs and workers.
        """
        terms = list(values)

        # Sort: chi-square descending, term alphabetical
        terms_sorted = sorted(terms, key=lambda x: (-x[1], x[0]))

        # Top 75
        for term, score in terms_sorted[:75]:
            yield category, (term, score)

if __name__ == "__main__":
    Job3TopK.run()
