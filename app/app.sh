#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

python3 cassandra_init.py
if [ $? -ne 0 ]; then
  echo "[LOGS] Failed to set up Cassandra. Exiting."
  exit 1
fi

echo "[LOGS] STARTING TO PREPARE DATA"


# Collect data
bash prepare_data.sh | grep "[LOGS]"

# Run the indexer
bash index.sh /index/data | grep "[LOGS]"

# Run the ranker
# bash search.sh "this is a query!" | grep "[LOGS]"

echo "[LOGS] Конец!"

bash