#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) != 3:
        continue
    doc_id, doc_title, doc_text = parts
    words = re.findall(r'\w+', doc_text.lower())
    for word in words:
        print(f"{doc_id}\t{word}\t1\t{doc_title}")
