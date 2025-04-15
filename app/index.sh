#!/bin/bash

# Enhanced index.sh with better resource management

# Configuration
INPUT_PATH=${1:-"/index/data"}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="/tmp/mapreduce_logs"
mkdir -p $LOG_DIR

# Cleanup previous outputs
hdfs dfs -rm -r -f /tmp/index/term_doc_pos 2>/dev/null
hdfs dfs -rm -r -f /tmp/index/term_freq 2>/dev/null

# Phase 1: Document Processing
echo "Starting Phase 1: Document Processing"
MAP_MEM=1024
REDUCE_MEM=1024
JAVA_OPTS=800

# Phase 1 with minimal resources
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="LowMemIndexer_Phase1" \
    -D mapreduce.task.timeout=3600000 \
    -D mapreduce.map.memory.mb=$MAP_MEM \
    -D mapreduce.reduce.memory.mb=$REDUCE_MEM \
    -D mapreduce.map.java.opts=-Xmx${JAVA_OPTS}m \
    -D mapreduce.reduce.java.opts=-Xmx${JAVA_OPTS}m \
    -D stream.non.zero.exit.is.failure=false \
    -input "$INPUT_PATH" \
    -output /tmp/index/term_doc_pos \
    -mapper "python3 /app/mapreduce/mapper1.py" \
    -reducer "python3 /app/mapreduce/reducer1.py" \
    -file /app/mapreduce/mapper1.py \
    -file /app/mapreduce/reducer1.py

if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "Phase 1 failed. Check $LOG_DIR/phase1_$TIMESTAMP.log"
    exit 1
fi

# Phase 2: Term Frequency Calculation
echo "Starting Phase 2: Term Frequency Calculation"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="DocIndexer_Phase2_$TIMESTAMP" \
    -D mapreduce.task.timeout=1200000 \
    -input /tmp/index/term_doc_pos \
    -output /tmp/index/term_freq \
    -mapper "python3 /app/mapreduce/mapper2.py" \
    -reducer "python3 /app/mapreduce/reducer2.py" \
    -file /app/mapreduce/mapper2.py \
    -file /app/mapreduce/reducer2.py \
    -cmdenv PYTHONUNBUFFERED=1 \
    2>&1 | tee $LOG_DIR/phase2_$TIMESTAMP.log

if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "Phase 2 failed. Check $LOG_DIR/phase2_$TIMESTAMP.log"
    exit 1
fi

echo "Indexing completed successfully. Logs available in $LOG_DIR"