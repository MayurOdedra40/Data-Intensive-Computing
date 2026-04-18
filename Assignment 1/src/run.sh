cd "$(dirname "$0")"

STREAMING_JAR=/usr/lib/hadoop/tools/lib/hadoop-streaming.jar

export PYTHONPATH=".:$PYTHONPATH"

cp Assignment_1_Assets/stopwords.txt stopwords.txt

INPUT="${1:-hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json}"

zip -qr utils.zip utils/

python3 job1_counts.py -r hadoop \
  --hadoop-streaming-jar "$STREAMING_JAR" \
  --files stopwords.txt \
  --py-files utils.zip \
  --stopwords stopwords.txt \
  "$INPUT" > results/job1_output.txt

grep -E '^"N"|^"C:' results/job1_output.txt > results/globals.tsv

python3 job2_chi2.py -r hadoop \
  --hadoop-streaming-jar "$STREAMING_JAR" \
  --files results/globals.tsv \
  --py-files utils.zip \
  --globals results/globals.tsv \
  results/job1_output.txt > results/job2_output.txt

python3 job3_topk.py -r hadoop \
  --hadoop-streaming-jar "$STREAMING_JAR" \
  results/job2_output.txt > results/job3_output.txt

python3 finalize_output.py < results/job3_output.txt > results/output.txt
