export PYTHONPATH="$(pwd):$PYTHONPATH"
INPUT="${1:-../Assignment_1_Assets/reviews_devset.json}"
python3 job1_counts.py -r local --stopwords ../Assignment_1_Assets/stopwords.txt "$INPUT" > ../job1_output.txt
grep -E '^"N"|^"C:' ../job1_output.txt > ../globals.tsv
python3 job2_chi2.py -r local --globals ../globals.tsv ../job1_output.txt > ../job2_output.txt
python3 job4_topk.py -r local ../job2_output.txt > ../job4_output.txt
python3 finalize_output.py < ../job4_output.txt > ../output.txt
