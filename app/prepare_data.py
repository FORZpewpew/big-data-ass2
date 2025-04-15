from pyspark.sql import SparkSession
from pathvalidate import sanitize_filename
import subprocess
import os

# Create and configure Spark session
spark = SparkSession.builder \
    .appName("data_preparation") \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()

sc = spark.sparkContext

# Read parquet file


df = spark.read.parquet("/a.parquet")
n = 1000
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)
os.makedirs("data", exist_ok=True)

def create_doc(row):
    # Generate the sanitized filename
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    
    # Write the content to the file
    with open(filename, "w") as f:
        f.write(row['text'])

df.foreach(create_doc)
