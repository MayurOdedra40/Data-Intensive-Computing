from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
import os
import json
from utils.text_processing import load_stopwords, parse_review_line, preprocess_review_text

class MRJob1Counts(MRJob):
    # Use RawValueProtocol to read the JSON lines as raw strings
    INPUT_PROTOCOL = RawValueProtocol

    JOBCONF = {
        'mapreduce.job.reduces': 24,
    }

    def configure_args(self):
        super(MRJob1Counts, self).configure_args()
        self.add_file_arg('--stopwords', help='Path to stopwords.txt')

    def mapper_init(self):
        # Load stopwords 
        stop_path = self.options.stopwords
        self.stopwords = load_stopwords(stop_path) if stop_path else set()

    def mapper(self, _, line):
        # Parse json
        record = parse_review_line(line)
        if not record:
            return

        # Get category and review text
        category = record.get('category', 'Unknown').strip()
        text = record.get('reviewText', '')
        
        # Preprocess text
        raw_tokens = preprocess_review_text(text, self.stopwords)
        
        # Get unique tokens
        unique_tokens = {t for t in raw_tokens if len(t) > 1}

        # Total Docs
        yield "N", 1
        
        # Category docs 
        yield f"C:{category}", 1
        
        for token in unique_tokens:
            # Term docs 
            yield f"T:{token}", 1
            # Term-Category docs 
            yield f"TC:{token}:{category}", 1

    def combiner(self, key, counts):
        # Sum counts locally on the worker node
        yield key, sum(counts)

    def reducer(self, key, counts):
        # Sum all local counts
        yield key, sum(counts)

if __name__ == '__main__':
    MRJob1Counts.run()