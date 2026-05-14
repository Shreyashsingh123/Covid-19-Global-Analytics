from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, broadcast, rand, floor, desc
)
from pyspark import StorageLevel

# Spark Session

spark = SparkSession.builder \
    .appName("Task10_11_Performance_Optimization") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Read Data

df = spark.read.parquet(
    "hdfs://localhost:9000/data/covid/staging/Covid_dataset/full_grouped"
)

worldometer = spark.read.parquet(
    "hdfs://localhost:9000/data/covid/staging/Covid_dataset/worldometer_data"
)
# 1️ Partition Strategy

# Repartition by Date
df_date_part = df.repartition("Date")

df_date_part.write.mode("overwrite") \
    .partitionBy("Date") \
    .parquet("hdfs://localhost:9000/data/covid/optimized/by_date")

# Repartition by Country
df_country_part = df.repartition("Country_Region")

df_country_part.write.mode("overwrite") \
    .partitionBy("Country_Region") \
    .parquet("hdfs://localhost:9000/data/covid/optimized/by_country")

# 2️⃣ Data Skew Detection

skew_check = df.groupBy("Country_Region") \
    .count() \
    .orderBy(desc("count"))

skew_check.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/covid/analytics/skew_check"
)

# 
# 3️ Salting Technique for Skew Handling

salted_df = df.withColumn("salt", floor(rand() * 10))

salted_df = salted_df.repartition("Country_Region", "salt")


# 4️ Broadcast Join Optimization

joined_df = df.join(
    broadcast(worldometer),
    "Country_Region",
    "inner"
)

joined_df.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/covid/analytics/broadcast_join_output"
)

# Verify execution plan
print("===== Broadcast Join Execution Plan =====")
joined_df.explain("formatted")


# 5️ Shuffle Optimization Example


filtered_df = df.filter(col("Confirmed") > 1000) \
                .filter(col("Deaths") > 100)

aggregated_df = filtered_df.groupBy("Country_Region") \
    .sum("Confirmed", "Deaths")

aggregated_df.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/covid/analytics/shuffle_optimized"
)

print("===== Aggregation Execution Plan =====")
aggregated_df.explain("extended")

# 6️ Caching Strategy


df.persist(StorageLevel.MEMORY_AND_DISK)

# Reuse cached dataframe
cached_query = df.groupBy("Country_Region") \
    .sum("Confirmed")

cached_query.show(5)


# 7️ Execution Plan Analysis Queries


# Query 1: GroupBy
query1 = df.groupBy("Country_Region").sum("Confirmed")
print("===== Query 1 Plan =====")
query1.explain("extended")

# Query 2: Broadcast Join
query2 = df.join(broadcast(worldometer), "Country_Region")
print("===== Query 2 Plan =====")
query2.explain("extended")

# Query 3: Normal Join (No Broadcast)
query3 = df.join(worldometer, "Country_Region")
print("===== Query 3 Plan =====")
query3.explain("extended")