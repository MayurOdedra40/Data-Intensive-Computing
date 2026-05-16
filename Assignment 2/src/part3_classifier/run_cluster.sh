echo "=============================="
echo " Running Part 3 — SVM and Grid Search"
echo "=============================="

USER=$(whoami)
HDFS_HOME="hdfs:///user/${USER}"

hdfs dfs -rm -r -f "${HDFS_HOME}/grid_search_results.csv"

# Build pipeline.zip if not present
if [ ! -f pipeline.zip ]; then
    echo "Building pipeline.zip..."
    zip pipeline.zip src/part2_pipeline/pipeline.py src/part2_pipeline/__init__.py
fi

spark-submit --master yarn --deploy-mode cluster \
    --py-files common.zip,pipeline.zip \
    --files src/common/stopwords.txt \
    src/part3_classifier/grid_search.py \
    --mode cluster \
    --input hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
    --stopwords stopwords.txt \
    --output "${HDFS_HOME}/grid_search_results.csv"

echo "Part 3 done. Fetching output..."
mkdir -p outputs
hdfs dfs -getmerge "${HDFS_HOME}/grid_search_results.csv" outputs/grid_search_results.csv
echo "Output: outputs/grid_search_results.csv"