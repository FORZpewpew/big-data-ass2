from pyspark.sql import SparkSession
from pathvalidate import sanitize_filename
import subprocess
import os

spark = SparkSession.builder \
    .appName("data_preparation_rdd") \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext

data_rdd = sc.wholeTextFiles("hdfs:///data").cache()

# Transform the RDD to extract id, title, and content
# Assuming the file name format is id_title.txt
transformed_rdd = data_rdd.map(lambda file: (
    os.path.basename(file[0]).split("_")[0],  # Extract id from the file name
    "_".join(os.path.basename(file[0]).split("_")[1:]).replace(".txt", ""),  # Extract title from the file name
    file[1]  # Content of the file
)).map(lambda row: f"{row[0]}\t{row[1]}\t{row[2]}")  # Format as tab-separated string

output_dir = "hdfs:///index/data"
try:
    subprocess.run(["hdfs", "dfs", "-rm", "-r", output_dir], check=True)
    print(f"[LOGS] Removed existing output directory: {output_dir}")
except subprocess.CalledProcessError:
    print(f"[LOGS] Output directory {output_dir} does not exist or could not be removed.")

# Save the transformed RDD as a single partition to /index/data in HDFS
transformed_rdd = transformed_rdd.coalesce(1)

transformed_rdd.saveAsTextFile("hdfs:///index/data/")

#transformed_rdd = transformed_rdd.repartition(2)
#transformed_rdd.saveAsTextFile("hdfs:///index/data/")

print("[LOGS] Transformed RDD saved to HDFS at /index/data")