echo "=============================="
echo " Running Part 2 — DF Pipeline"
echo "=============================="
hdfs dfs -rm -r -f "${HDFS_HOME}/output_ds.txt"

spark-submit --master yarn --deploy-mode cluster \
    --py-files common.zip \
    --files src/common/stopwords.txt \
    src/part2_pipeline/pipeline.py \
    --mode cluster \
    --input hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
    --stopwords stopwords.txt \
    --output "${HDFS_HOME}/output_ds.txt"

echo "Part 2 done. Fetching output..."
mkdir -p outputs
hdfs dfs -getmerge "${HDFS_HOME}/output_ds.txt" outputs/output_ds.txt
echo "Output: outputs/output_ds.txt"