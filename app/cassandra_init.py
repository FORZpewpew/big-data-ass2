from cassandra.cluster import Cluster
import time

CASSANDRA_HOSTS = ['cassandra-server']
KEYSPACE = 'search'
TABLE = 'inverted_index'

# Retry connection
for attempt in range(2):
    try:
        cluster = Cluster(CASSANDRA_HOSTS)
        session = cluster.connect()
        break
    except Exception as e:
        print(f"[LOGS] Cassandra not ready yet... attempt {attempt+1}/10")
        time.sleep(5)
else:
    print("[LOGS] Could not connect to Cassandra.")
    exit(1)

# Create keyspace
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
""")

session.set_keyspace(KEYSPACE)

# Create table
session.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        word TEXT PRIMARY KEY,
        documents MAP<TEXT, INT>
    )
""")

