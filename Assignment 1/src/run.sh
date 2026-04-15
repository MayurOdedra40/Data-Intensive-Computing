export PYTHONPATH="$(pwd):$PYTHONPATH"
python3 job1_counts.py -r local --stopwords ../Assignment_1_Assets/stopwords.txt ../Assignment_1_Assets/reviews_devset.json > ../job1_output.txt
grep -E '^"N"|^"C:' ../job1_output.txt > ../globals.tsv
python3 job2_chi2.py -r local --globals ../globals.tsv ../job1_output.txt > ../job2_output.txt
