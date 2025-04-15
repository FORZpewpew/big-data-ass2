#!/usr/bin/env python
import sys
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra'])
session = cluster.connect()
session.set_keyspace('search_engine')

current_term = None
doc_freq = 0

for line in sys.stdin:
    term, count = line.strip().split('\t')
    count = int(count)
    
    if term == current_term:
        doc_freq += count
    else:
        if current_term:
            # Update document frequency for the term
            session.execute("""
                INSERT INTO terms (term_text, document_frequency)
                VALUES (%s, %s)
            """, (current_term, doc_freq))
        
        current_term = term
        doc_freq = count

# Last term
if current_term:
    session.execute("""
        INSERT INTO terms (term_text, document_frequency)
        VALUES (%s, %s)
    """, (current_term, doc_freq))

# Calculate and store global statistics
total_docs = session.execute("SELECT COUNT(*) FROM documents").one()[0]
avg_doc_length = session.execute("SELECT AVG(doc_length) FROM documents").one()[0]

session.execute("""
    INSERT INTO collection_stats (stat_name, stat_value)
    VALUES (%s, %s)
""", ("total_documents", float(total_docs)))

session.execute("""
    INSERT INTO collection_stats (stat_name, stat_value)
    VALUES (%s, %s)
""", ("avg_document_length", avg_doc_length))