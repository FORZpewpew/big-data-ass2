#!/usr/bin/env python
import sys

for line in sys.stdin:
    term, doc_id, position = line.strip().split('\t')
    print(f"{term}\t1")