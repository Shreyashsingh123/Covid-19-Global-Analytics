from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, broadcast

# Spark Session
spark = SparkSession.builder \
    .appName("Task11_ExecutionPlan") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Read Data (From Staging Layer)

full_grouped = spark.read.parquet(
    "hdfs://localhost:9000/data/covid/staging/Covid_dataset/full_grouped"
)

worldometer = spark.read.parquet(
    "hdfs://localhost:9000/data/covid/staging/Covid_dataset/worldometer_data"
)
print("\n================ QUERY 1: GROUP BY ================\n")

query1 = full_grouped.groupBy("Country_Region").agg(
    _sum("Confirmed").alias("Total_Confirmed")
)

query1.explain("extended")

print("\n================ QUERY 2: BROADCAST JOIN ================\n")

query2 = full_grouped.join(
    broadcast(worldometer),
    "Country_Region",
    "inner"
)

query2.explain("extended")

print("\n================ QUERY 3: SORT MERGE JOIN ================\n")

query3 = full_grouped.join(
    worldometer,
    "Country_Region",
    "inner"
)

query3.explain("extended")

spark.stop()