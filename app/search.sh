#!/bin/bash
echo "[LOGS] Running search query: $1"

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit --master yarn --archives /app/.venv.tar.gz#.venv query.py  $1 2>&1 | grep "\[LOGS\]"