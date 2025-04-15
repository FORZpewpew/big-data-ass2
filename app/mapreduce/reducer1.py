#!/usr/bin/env python3
import sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time

# Connect to Cassandra only when needed
def get_cassandra_session():
    return Cluster(
        ['cassandra'],
        connect_timeout=60
    ).connect('search_engine')

def process_batch(batch):
    session = None
    try:
        session = get_cassandra_session()
        
        # Prepare statements
        doc_stmt = session.prepare("""
            INSERT INTO documents (doc_id, doc_path, doc_length)
            VALUES (?, ?, ?) IF NOT EXISTS
        """)
        
        index_stmt = session.prepare("""
            INSERT INTO inverted_index (term_text, doc_id, term_frequency, positions)
            VALUES (?, ?, ?, ?)
        """)
        
        # Process batch
        for term, doc_id, positions in batch.items():
            session.execute(index_stmt, (
                term,
                doc_id['doc_id'],
                len(positions),
                sorted(positions)
            ))
            
    finally:
        if session:
            session.cluster.shutdown()

current_term = None
batch = {}
BATCH_SIZE = 100  # Process every 100 terms

for line in sys.stdin:
    try:
        term, doc_id, position, doc_path, doc_length = line.strip().split('\t')
        position = int(position)
        doc_length = int(doc_length)
        
        if term not in batch:
            batch[term] = {
                'doc_id': doc_id,
                'positions': []
            }
        batch[term]['positions'].append(position)
        
        # Process batch when size limit reached
        if len(batch) >= BATCH_SIZE:
            process_batch(batch)
            batch = {}
            
    except Exception as e:
        sys.stderr.write(f"ERROR: {str(e)}\n")
        continue

# Process remaining items
if batch:
    process_batch(batch)