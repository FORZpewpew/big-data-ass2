#!/usr/bin/env python3
import sys
from collections import defaultdict

doc_lengths = defaultdict(int)
doc_terms = defaultdict(list)
doc_titles = {}

for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) < 4:
        continue
    doc_id, term, count, title = parts
    count = int(count)
    doc_lengths[doc_id] += count
    doc_terms[doc_id].append((term, count))
    doc_titles[doc_id] = title

for doc_id, terms in doc_terms.items():
    doc_len = doc_lengths[doc_id]
    title = doc_titles[doc_id].replace("\t", " ").replace("\n", " ")
    print(f"DOCLEN\t{doc_id}\t{doc_len}\t{title}")
    for term, tf in terms:
        print(f"TERM\t{term}\t{doc_id}\t{tf}")
