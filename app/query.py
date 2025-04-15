from pyspark import SparkContext, SparkConf
import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sys
import math

k1 = 1.5
b = 0.75

def fetch_cassandra_data():
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search')

    vocab_rows = session.execute("SELECT term, df FROM vocabulary")
    index_rows = session.execute("SELECT term, doc_id, tf FROM inverted_index")
    doc_rows = session.execute("SELECT doc_id, title, length FROM documents")

    vocab = {row.term: row.df for row in vocab_rows}
    index = [(row.term, row.doc_id, row.tf) for row in index_rows]
    docs = {row.doc_id: {'title': row.title, 'length': row.length} for row in doc_rows}

    return vocab, index, docs

def compute_bm25(query_terms, vocab, index_rdd, docs, N, avg_doc_len):
    query_terms_set = set(query_terms)

    filtered_rdd = index_rdd.filter(lambda x: x[0] in query_terms_set)

    def bm25_score(record):
        term, doc_id, tf = record
        df = vocab.get(term, 0)
        idf = math.log(1 + (N - df + 0.5) / (df + 0.5))

        doc_len = docs[doc_id]['length']
        denom = tf + k1 * (1 - b + b * doc_len / avg_doc_len)
        score = idf * tf * (k1 + 1) / denom
        return (doc_id, score)

    scored = filtered_rdd.map(bm25_score)

    return scored.reduceByKey(lambda a, b: a + b)

def main():
    if len(sys.argv) < 2:
        print("[LOGS] Query not provided.")
        sys.exit(1)

    query = sys.argv[1]
    query_terms = query.strip().lower().split()

    conf = SparkConf().setAppName("BM25Search")
    sc = SparkContext(conf=conf)

    vocab, index_data, docs = fetch_cassandra_data()
    N = len(docs)
    avg_doc_len = sum(d['length'] for d in docs.values()) / N if N > 0 else 1

    index_rdd = sc.parallelize(index_data)

    scored_docs = compute_bm25(query_terms, vocab, index_rdd, docs, N, avg_doc_len)

    top_10 = scored_docs.takeOrdered(10, key=lambda x: -x[1])

    print("\n[LOGS] Top 10 Results for Query:", query)
    for doc_id, score in top_10:
        title = docs[doc_id].get("title", "N/A")
        print(f"[LOGS] {doc_id}\t{title}\tScore: {score:.4f}")

    sc.stop()

if __name__ == "__main__":
    main()
