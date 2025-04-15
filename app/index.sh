#!/bin/bash

INPUT_PATH=${1:-/index/data}
HDFS_TMP_PATH=/tmp/index

if [[ "$INPUT_PATH" == "/index/data" ]]; then
    echo "[LOGS]: Using HDFS path directly: $INPUT_PATH"
else
    echo "[LOGS]: Copying local file to HDFS..."
    hdfs dfs -put -f "$INPUT_PATH" /tmp/local_data
    INPUT_PATH="hdfs:///tmp/local_data"
fi

# Clean old outputs
hadoop fs -rm -r "$HDFS_TMP_PATH/step1"
hadoop fs -rm -r "$HDFS_TMP_PATH/step2"

# Step 1: Tokenization & term frequency
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
  -input "$INPUT_PATH" \
  -output "$HDFS_TMP_PATH/step1" \
  -mapper mapreduce/mapper1.py \
  -reducer mapreduce/reducer1.py \
  -file mapreduce/mapper1.py \
  -file mapreduce/reducer1.py

# Step 2: Build index and stats
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
  -input "$HDFS_TMP_PATH/step1" \
  -output "$HDFS_TMP_PATH/step2" \
  -mapper mapreduce/mapper2.py \
  -reducer mapreduce/reducer2.py \
  -file mapreduce/mapper2.py \
  -file mapreduce/reducer2.py

# Step 3: Store in Cassandra
source .venv/bin/activate
python3 app.py "$HDFS_TMP_PATH/step2"
