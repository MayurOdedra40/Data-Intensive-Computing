echo "=============================="
echo " Running Part 1 — RDD Chi2"
echo "=============================="
hdfs dfs -rm -r -f "${HDFS_HOME}/output_rdd.txt"

spark-submit --master yarn --deploy-mode cluster \
    --py-files common.zip \
    --files src/common/stopwords.txt \
    src/part1_rdd/chi_square_rdd.py \
    --mode cluster \
    --input hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
    --stopwords stopwords.txt \
    --output "${HDFS_HOME}/output_rdd.txt"

echo "Part 1 done. Fetching output..."
mkdir -p outputs
hdfs dfs -getmerge "${HDFS_HOME}/output_rdd.txt" outputs/output_rdd.txt
echo "Output: outputs/output_rdd.txt"