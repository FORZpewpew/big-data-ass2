#!/bin/bash

source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 



# DOWNLOAD a.parquet or any parquet file before you run this

hdfs dfs -put -f a.parquet /  && \
    spark-submit prepare_data.py && \
    echo "[LOGS] Putting data to hdfs" && \
    hdfs dfs -put data / && \
    spark-submit prepare_index_data.py && \
    hdfs dfs -count /data | awk '{print "[LOGS] Number of files in /data:", $2}' && \
    hdfs dfs -ls /index/data && \
    echo "[LOGS] done data preparation!"