SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SRC_DIR/.." && pwd)"
STOPWORDS_SRC="$ROOT_DIR/Assignment_1_Assets/stopwords.txt"
STREAMING_JAR=/usr/lib/hadoop/tools/lib/hadoop-streaming.jar

export PYTHONPATH="$SRC_DIR:$PYTHONPATH"
cd "$SRC_DIR"

cp "$STOPWORDS_SRC" stopwords.txt

INPUT="${1:-hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json}"

zip -qr utils.zip utils/

python3 job1_counts.py -r hadoop \
  --hadoop-streaming-jar "$STREAMING_JAR" \
  --files stopwords.txt \
  --py-files utils.zip \
  --stopwords stopwords.txt \
  "$INPUT" > "$ROOT_DIR/job1_output.txt"

grep -E '^"N"|^"C:' "$ROOT_DIR/job1_output.txt" > globals.tsv

python3 job2_chi2.py -r hadoop \
  --hadoop-streaming-jar "$STREAMING_JAR" \
  --files globals.tsv \
  --py-files utils.zip \
  --globals globals.tsv \
  "$ROOT_DIR/job1_output.txt" > "$ROOT_DIR/job2_output.txt"

python3 job4_topk.py -r hadoop \
  --hadoop-streaming-jar "$STREAMING_JAR" \
  "$ROOT_DIR/job2_output.txt" > "$ROOT_DIR/job4_output.txt"

python3 finalize_output.py < "$ROOT_DIR/job4_output.txt" > "$ROOT_DIR/output.txt"
