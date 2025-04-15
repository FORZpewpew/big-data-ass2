#!/usr/bin/env python3
import sys
from collections import defaultdict

doc_lengths = {}
term_postings = defaultdict(list)

for line in sys.stdin:
    parts = line.strip().split('\t')
    if parts[0] == 'DOCLEN':
        _, doc_id, doc_len, title = parts
        doc_lengths[doc_id] = (int(doc_len), title)
    elif parts[0] == 'TERM':
        _, term, doc_id, tf = parts
        term_postings[term].append((doc_id, int(tf)))

for term, postings in term_postings.items():
    df = len(postings)
    print(f"VOCAB\t{term}\t{df}")
    for doc_id, tf in postings:
        print(f"INDEX\t{term}\t{doc_id}\t{tf}")

for doc_id, (length, title) in doc_lengths.items():
    print(f"DOCLEN\t{doc_id}\t{length}\t{title}")
