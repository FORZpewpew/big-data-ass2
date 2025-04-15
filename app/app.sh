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

echo "[LOGS] STARTING TO PREPARE DATA"

# Collect data
bash prepare_data.sh | grep "[LOGS]"

# Run the indexer
bash index.sh | grep "[LOGS]"

# Run the ranker
#bash search.sh "this is a query!" | grep "[LOGS]"

echo "[LOGS] The end!"

bash