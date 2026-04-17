from mrjob.job import MRJob
import json

class Job4TopK(MRJob):

    def mapper(self, _, line):
        try:
            key_str, value_str = line.split("\t", 1)
            category = json.loads(key_str)
            term, score = json.loads(value_str)
            yield category, (term, float(score))
        except Exception:
            pass

    def reducer(self, category, values):
        terms = list(values)

        # Sort: chi-square descending, term alphabetical
        terms_sorted = sorted(terms, key=lambda x: (-x[1], x[0]))

        # Top 75
        for term, score in terms_sorted[:75]:
            yield category, (term, score)

if __name__ == "__main__":
    Job4TopK.run()
