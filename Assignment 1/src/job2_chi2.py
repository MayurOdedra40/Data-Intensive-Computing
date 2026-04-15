from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol


class MRJob2ChiSquare(MRJob):
    INPUT_PROTOCOL = JSONProtocol

    def configure_args(self):
        super(MRJob2ChiSquare, self).configure_args()
        self.add_file_arg('--globals',
                          help='Path to globals.tsv containing N and C:* rows '
                               'extracted from Job 1 output')

    def _load_globals(self):
        self.N = 0
        self.cat_docs = {}  # category name -> n_c

        path = self.options.globals
        if not path:
            return

        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.rstrip('\n')
                if not line:
                    continue
                # Line is: <json-key>\t<int>
                key_str, val_str = line.split('\t', 1)
                # Strip the surrounding JSON quotes, e.g. '"C:Books"' -> 'C:Books'
                key = key_str.strip().strip('"')
                val = int(val_str)
                if key == 'N':
                    self.N = val
                elif key.startswith('C:'):
                    self.cat_docs[key[2:]] = val

    def mapper_init(self):
        self._load_globals()

    def reducer_init(self):
        self._load_globals()

    def mapper(self, key, value):
        if not isinstance(key, str):
            return

        if key.startswith('T:'):
            # "T:<term>" - emit the per-term document count
            term = key[2:]
            yield term, ('T', value)

        elif key.startswith('TC:'):
            _, term, category = key.split(':', 2)
            yield term, ('TC', category, value)

        # "N" and "C:..." rows are intentionally dropped - already in globals.

    def reducer(self, term, values):
        n_t = 0
        tc_rows = []  # list of (category, n_tc)

        for v in values:
            # mrjob serializes tuples as JSON lists, so v is a list here.
            tag = v[0]
            if tag == 'T':
                n_t = v[1]
            elif tag == 'TC':
                tc_rows.append((v[1], v[2]))

        # Defensive: if for some reason we never saw a T row, there is no
        # meaningful chi^2 to compute for this term.
        if n_t == 0:
            return

        N = self.N
        for category, n_tc in tc_rows:
            n_c = self.cat_docs.get(category, 0)
            if n_c == 0:
                continue

            A = n_tc
            B = n_t - A
            C = n_c - A
            D = N - A - B - C

            denom = (A + B) * (C + D) * (A + C) * (B + D)
            if denom <= 0:
                continue
            chi2 = (N * (A * D - B * C) ** 2) / denom

            yield category, (term, chi2)


if __name__ == '__main__':
    MRJob2ChiSquare.run()
