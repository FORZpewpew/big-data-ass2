#!/usr/bin/env python3
import subprocess
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

def fetch_hdfs_output(hdfs_path):
    cmd = ["hdfs", "dfs", "-cat", f"{hdfs_path}/part-*"]
    output = subprocess.check_output(cmd).decode("utf-8")
    return output.strip().split("\n")

def connect_cassandra():
    cluster = Cluster(["cassandra-server"])
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)

    session.set_keyspace("search")
    return session

def ensure_tables(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary (
            term TEXT PRIMARY KEY,
            df INT
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            term TEXT,
            doc_id TEXT,
            tf INT,
            PRIMARY KEY (term, doc_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            doc_id TEXT PRIMARY KEY,
            length INT,
            title TEXT
        )
    """)

def parse_and_insert(lines, session):
    insert_vocab = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
    insert_index = session.prepare("INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)")
    insert_doc = session.prepare("INSERT INTO documents (doc_id, length, title) VALUES (?, ?, ?)")


    vocab_batch = BatchStatement()
    index_batch = BatchStatement()
    doc_batch = BatchStatement()

    batch_limit = 250
    vb_count = ib_count = db_count = 0

    for line in lines:
        parts = line.strip().split("\t")
        if len(parts) < 2:
            continue

        record_type = parts[0]

        if record_type == "VOCAB":
            term, df = parts[1], int(parts[2])
            vocab_batch.add(insert_vocab, (term, df))
            vb_count += 1
            if vb_count >= batch_limit:
                session.execute(vocab_batch)
                vocab_batch = BatchStatement()
                vb_count = 0

        elif record_type == "INDEX":
            term, doc_id, tf = parts[1], parts[2], int(parts[3])
            index_batch.add(insert_index, (term, doc_id, tf))
            ib_count += 1
            if ib_count >= batch_limit:
                session.execute(index_batch)
                index_batch = BatchStatement()
                ib_count = 0

        elif record_type == "DOCLEN":
            doc_id, length = parts[1], int(parts[2])
            title = parts[3] if len(parts) > 3 else ""
            doc_batch.add(insert_doc, (doc_id, length, title))
            db_count += 1
            if db_count >= batch_limit:
                session.execute(doc_batch)
                doc_batch = BatchStatement()
                db_count = 0

    if vb_count > 0:
        session.execute(vocab_batch)
    if ib_count > 0:
        session.execute(index_batch)
    if db_count > 0:
        session.execute(doc_batch)


def main():
    import sys
    if len(sys.argv) != 2:
        sys.exit(1)

    hdfs_output_path = sys.argv[1]
    lines = fetch_hdfs_output(hdfs_output_path)

    session = connect_cassandra()

    ensure_tables(session)

    parse_and_insert(lines, session)


if __name__ == "__main__":
    main()
