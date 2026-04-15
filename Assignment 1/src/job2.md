# Job 2 — Chi-Square Computation

## Goal

Job 2 turns Job 1's flat count table into per-category chi-square scores.
For every unigram term `t` and every product category `c` that contains it,
the job emits `(c, (t, chi2(t, c)))`, where `chi2(t, c)` is the standard
one-degree-of-freedom chi-square statistic. The resulting stream feeds Job 3,
which groups by category and selects the top 75 most discriminative terms.

## The Join Problem

Computing `chi2(t, c)` requires four numbers:

- `N`   — total number of documents in the corpus
- `n_c` — number of documents in category `c`
- `n_t` — number of documents containing term `t`
- `n_tc` — number of documents in category `c` that contain term `t`

Job 1 produced all four, but scattered them across its output: `N` is a
single row, `n_c` appears on 22 rows (one per category), `n_t` and `n_tc`
together make up the bulk of the file. Job 2's only real task is to bring
the right counts together at the right reducer, at minimum cost.

the designexploits the
fact that `N` and the 22 category counts are tiny (23 integers in total).
They are extracted from Job 1's output into a small side file,
`globals.tsv`, and shipped to every worker via mrjob's `--globals`
argument. Each mapper and reducer task loads this file once at
initialisation and keeps `N` and the `n_c` table in memory. The remaining
rows — `T:<term>` and `TC:<term>:<category>` — are re-keyed by **term**
during the map phase, so that the shuffle places `n_t` and every relevant
`n_tc` for a given term in the same reduce group. This is a textbook
"map-side broadcast + reduce-side join": the small side of the join is
broadcast, the large side is partitioned.

## Contingency Table and Formula

The 2×2 contingency table for `(t, c)` counts documents, not term
occurrences (which is why Job 1 deduplicates tokens per document). Its
cells are derived directly from the four counts above:

```
A = n_tc                       (in c, contains t)
B = n_t - A                    (not in c, contains t)
C = n_c - A                    (in c, does not contain t)
D = N - A - B - C              (not in c, does not contain t)
```

The chi-square score is then

```
chi2 = N * (A*D - B*C)^2 / ((A+B)(C+D)(A+C)(B+D))
```

A larger score indicates a stronger association — either strongly present
or strongly absent — between the term and the category, relative to the
independence baseline.

## <k, v> Design

| Stage        | Key                                  | Value                                           |
|--------------|--------------------------------------|-------------------------------------------------|
| mapper in    | `str` — `"T:<term>"` or `"TC:<term>:<category>"` | `int` — document count from Job 1               |
| mapper out   | `term` (`str`)                       | `("T", n_t)` or `("TC", category, n_tc)`        |
| reducer out  | `category` (`str`)                   | `(term, chi2)`                                  |

Rows of type `"N"` and `"C:..."` are dropped at map time because the same
information is already available from the broadcast side file; re-shuffling
them would be wasted bandwidth. The reducer output is keyed by **category**
rather than by term so that the subsequent top-75-per-category step in
Job 3 receives exactly the grouping it needs, with no further re-shuffling.

## Edge Cases

A term that never co-occurs with a category produces no `TC` row in Job 1's
output, so the reducer emits nothing for that `(t, c)` pair. This is the
desired behaviour: such pairs have `chi2 = 0` and would never make the
per-category top-75 anyway. The reducer also guards against a degenerate
denominator (zero when a term is either absent from every document or
present in all of them, which can occur at very small scales); these cases
are skipped rather than yielding an undefined score. Python's
arbitrary-precision integers are used for the numerator, so even at the
full 142-million-review scale no overflow is possible.

## Efficiency Notes

Job 2 does not use a combiner. Combiners are useful when a mapper emits
many rows sharing the same key — "the" appearing in millions of documents,
for example — but Job 1's reducer already aggregated those duplicates, so
every `(key, value)` pair produced by Job 2's mapper is unique and a
combiner would have nothing to collapse.

Because mapper and reducer tasks run in separate processes (potentially on
different cluster nodes), the globals table is loaded in both `mapper_init`
and `reducer_init`. They share the load logic through a single
`_load_globals` helper to guarantee identical state on both sides.

The per-term reduce group is tightly bounded: at most one `T` row and one
`TC` row per category, i.e. at most 23 values per term. There is no skew
risk at the reduce stage, and the whole computation for a term is linear
in the number of categories it appears in.
