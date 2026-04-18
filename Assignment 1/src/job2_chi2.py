from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol


class MRJob2ChiSquare(MRJob):
    """Map-side broadcast join that turns Job 1 counts into chi-square scores.

    ``globals.tsv`` (N and the 22 n_c values) is broadcast to every worker so
    the join happens in the reducer without a second shuffle for corpus-level
    counts. The mapper re-keys by term so each reducer call sees one ``T`` row
    plus one ``TC`` row per category the term appears in.
    """

    INPUT_PROTOCOL = JSONProtocol

    def configure_args(self):
        """Register ``--globals`` so mrjob ships the broadcast file with the job."""
        super(MRJob2ChiSquare, self).configure_args()
        self.add_file_arg('--globals',
                          help='Path to globals.tsv containing N and C:* rows '
                               'extracted from Job 1 output')

    def _load_globals(self):
        """Parse ``globals.tsv`` into ``self.N`` and ``self.cat_docs`` lookup tables.

        Called from both mapper_init and reducer_init so the broadcast values
        are available in either phase regardless of how mrjob schedules tasks.
        """
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
        """Populate broadcast globals on each mapper task."""
        self._load_globals()

    def reducer_init(self):
        """Populate broadcast globals on each reducer task."""
        self._load_globals()

    def mapper(self, key, value):
        """Re-key Job 1 output by term so ``T`` and ``TC`` rows meet in one reducer.

        ``N`` and ``C:*`` rows are discarded because the same values are already
        loaded in memory from the broadcast file.
        """
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
        """Compute chi-square for one term against every category it appears in.

        Output is keyed by category — exactly the grouping Job 3 needs — so no
        reshuffle happens between Job 2 and Job 3.
        """
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
