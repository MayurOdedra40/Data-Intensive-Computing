Assignment 1 – Task Breakdown
Person 1 (MAYUR) — Preprocessing
Goal: Clean and tokenize text correctly
Tasks:
Implement preprocessing function:
lowercase (case folding)
tokenize using required delimiters
remove stopwords (from stopwords.txt)
remove tokens of length 1
Ensure ONLY reviewText is used
Deduplicate tokens per document (important!)
Create 10 test examples (input → expected tokens)
Files:
src/utils/text_processing.py
Output:
Final preprocessing function used by everyone
Short explanation for report

Person 2 (Dominik) — Counting Job (Job 1)
Goal: Generate all required document-level counts
Tasks:
Build MapReduce Job 1:
parse JSON safely
use preprocessing
emit:
total docs → ("*N*", "docs")
category docs → ("*C*", category)
term docs → ("*T*", term)
term-category docs → ("*TC*", term, category)
Use combiner for efficiency
Ensure document presence (NOT frequency)
Files:
src/job1_counts.py
Output:
Correct aggregated counts
Key-value explanation for report

Person 3 (Your Name) — Chi-Square Computation
Goal: Compute χ² scores correctly
Tasks:
Build:
job2_prepare_stats.py → assemble stats
job3_chi_square.py → compute χ²
Compute:
A, B, C, D, N correctly
Handle edge cases (division by zero)
Formula:
χ2=N(AD−BC)2(A+B)(C+D)(A+C)(B+D)\chi^2 = \frac{N(AD - BC)^2}{(A+B)(C+D)(A+C)(B+D)}χ2=(A+B)(C+D)(A+C)(B+D)N(AD−BC)2​
Files:
src/job2_prepare_stats.py
src/job3_chi_square.py
Output:
(category, term) → chi-square score
Math explanation for report

Person 4 (Anda) — Ranking + Output
Goal: Produce final formatted results
Tasks:
Build:
job4_topk.py → top 75 per category
finalize_output.py → final output.txt
Sort:
terms by χ² (descending)
tie → alphabetical
Keep exactly 75 terms per category
Merge all terms → unique dictionary (alphabetical)
Files:
src/job4_topk.py
src/finalize_output.py
Output:
Final output.txt (correct format)

Person 5 (Paisie) — Integration + Report
Goal: Combine everything + submission
Tasks:
Create pipeline script:
src/run_all.sh
Run full pipeline (devset first)
Prepare report:
Introduction
Problem Overview
Methodology (WITH pipeline diagram)
Conclusion
Ensure:
paths are parameterized
code runs end-to-end
all files included for submission
Files:
src/run_all.sh
report.pdf
Output:
Final zip submission
Pipeline diagram + documentation

