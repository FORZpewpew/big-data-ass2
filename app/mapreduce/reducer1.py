#!/usr/bin/env python3
import sys
from collections import defaultdict

current_doc = None
tf_counts = defaultdict(int)
current_title = ""

for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) < 4:
        continue
    doc_id, word, count, title = parts

    if current_doc != doc_id and current_doc is not None:
        for w, c in tf_counts.items():
            print(f"{current_doc}\t{w}\t{c}\t{current_title}")
        tf_counts.clear()

    current_doc = doc_id
    current_title = title
    tf_counts[word] += int(count)

if current_doc:
    for w, c in tf_counts.items():
        print(f"{current_doc}\t{w}\t{c}\t{current_title}")
