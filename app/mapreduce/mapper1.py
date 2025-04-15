#!/usr/bin/env python3
import sys
import re
import os
from uuid import uuid5, NAMESPACE_URL
import time

# Disable Cassandra for mapper - we'll batch process later
USE_CASSANDRA = False

def process_document(doc_path, content):
    doc_id = str(uuid5(NAMESPACE_URL, doc_path))
    tokens = re.findall(r'\w{3,}', content.lower())  # Only words with 3+ chars
    for position, term in enumerate(tokens):
        print(f"{term}\t{doc_id}\t{position}\t{doc_path}\t{len(tokens)}")
    return True

for line in sys.stdin:
    try:
        parts = line.strip().split('\t', 1)
        if len(parts) == 2:
            doc_path, content = parts
            process_document(doc_path, content)
    except Exception as e:
        sys.stderr.write(f"ERROR: {str(e)}\n")
        continue