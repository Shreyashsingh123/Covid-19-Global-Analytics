import time
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("ComparePerformance").getOrCreate()


spark.catalog.clearCache()
start_time = time.time()

csv_df = spark.read.csv("hdfs://localhost:9000/data/covid/raw/Covid_dataset/covid_19_clean_complete.csv", header=True)

csv_df.collect()

csv_time=time.time() - start_time

spark.catalog.clearCache()
start_time = time.time()
parquet_df = spark.read.parquet("hdfs://localhost:9000/data/covid/staging/Covid_dataset/full_grouped.parquet")
parquet_df.collect()
parquet_time = time.time() - start_time



print("Comparing Reading time.")
print("CSV time: " + str(csv_time))
print("Parquet time: " + str(parquet_time))


print("Comparing Execution Plan")
print(csv_df.explain(True))
print(parquet_df.explain(True))