import sys
import math
from pyspark import SparkContext, SparkConf
from cassandra.cluster import Cluster

# BM25 constants
k1 = 1.5
b = 0.75

def fetch_cassandra_data():
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search')

    # Load vocabulary
    vocab_rows = session.execute("SELECT term, df FROM vocabulary")
    vocab = {row.term: row.df for row in vocab_rows}

    # Load inverted index
    index_rows = session.execute("SELECT term, doc_id, tf FROM inverted_index")
    index = [(row.term, row.doc_id, row.tf) for row in index_rows]

    # Load document stats
    doc_rows = session.execute("SELECT doc_id, length FROM documents")
    doc_lengths = {}
    total_length = 0
    for row in doc_rows:
        doc_lengths[row.doc_id] = row.length
        total_length += row.length

    avg_dl = total_length / len(doc_lengths) if doc_lengths else 1
    N = len(doc_lengths)

    return vocab, index, doc_lengths, avg_dl, N

def compute_bm25(sc, query_terms, vocab, index, doc_lengths, avg_dl, N):
    # Broadcast variables
    bc_vocab = sc.broadcast(vocab)
    bc_doc_lengths = sc.broadcast(doc_lengths)

    # Convert index to RDD
    index_rdd = sc.parallelize(index)

    # Filter index to only query terms
    filtered = index_rdd.filter(lambda x: x[0] in query_terms)

    # Compute BM25 for each (term, doc_id)
    def score(row):
        term, doc_id, tf = row
        df = bc_vocab.value.get(term, 0)
        idf = math.log((N - df + 0.5) / (df + 0.5) + 1)
        dl = bc_doc_lengths.value.get(doc_id, 0)
        numerator = tf * (k1 + 1)
        denominator = tf + k1 * (1 - b + b * dl / avg_dl)
        score = idf * (numerator / denominator)
        return (doc_id, score)

    scored = filtered.map(score)

    # Sum BM25 scores per doc_id
    doc_scores = scored.reduceByKey(lambda a, b: a + b)

    # Get top 10
    top10 = doc_scores.takeOrdered(10, key=lambda x: -x[1])

    return top10

def main():
    if len(sys.argv) < 2:
        print("[LOGS] Please provide a query string.")
        sys.exit(1)

    query = sys.argv[1].strip().lower()
    query_terms = query.split()

    conf = SparkConf().setAppName("BM25Query")
    sc = SparkContext(conf=conf)

    print("[LOGS] Fetching index data from Cassandra...")
    vocab, index, doc_lengths, avg_dl, N = fetch_cassandra_data()

    print(f"[LOGS] Total docs: {N}, Avg doc len: {avg_dl:.2f}")
    top_docs = compute_bm25(sc, query_terms, vocab, index, doc_lengths, avg_dl, N)

    print("[LOGS] Top 10 Documents:")
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search')
    for doc_id, score in top_docs:
        title_result = session.execute("SELECT title FROM documents WHERE doc_id = %s", (doc_id,))
        title = title_result.one()
        title_text = title.title if title else "(No title)"
        print(f"[LOGS]Document ID: {doc_id}\tTitle: {title_text}\tBM25 score: {score:.4f}")

    sc.stop()

if __name__ == "__main__":
    main()
