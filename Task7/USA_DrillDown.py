from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as _sum,
    desc,
    count,
    avg,
    stddev,
    max as _max
)

# Spark Session

spark = SparkSession.builder \
    .appName("USA_Drilldown_Analysis") \
    .getOrCreate()

# Enable Adaptive Query Execution (Good Practice)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Paths

input_path = "hdfs://localhost:9000/data/covid/staging/Covid_dataset/usa_county_wise"
output_path = "hdfs://localhost:9000/data/covid/analytics/Covid_dataset/usa_drilldown/"

# Read Data
df = spark.read.parquet(input_path)

# 1️ Aggregate County Data → State Level

state_df = df.groupBy("Province_State").agg(
    _sum("Confirmed").alias("Total_Confirmed"),
    _sum("Deaths").alias("Total_Deaths")
)

state_df.write.mode("overwrite").parquet(
    output_path + "state_level_aggregation"
)
# 2️ Top 10 Affected States (By Confirmed Cases)

top10_states = state_df.orderBy(
    desc("Total_Confirmed")
).limit(10)

top10_states.write.mode("overwrite").parquet(
    output_path + "top10_states_by_confirmed"
)

# 3️ Detect Data Skew Across States

# Record count per state
record_count = df.groupBy("Province_State").agg(
    count("*").alias("Record_Count")
).orderBy(desc("Record_Count"))

record_count.write.mode("overwrite").parquet(
    output_path + "state_record_count_distribution"
)

# Statistical Skew Analysis
state_stats = state_df.agg(
    avg("Total_Confirmed").alias("Average_Confirmed"),
    stddev("Total_Confirmed").alias("StdDev_Confirmed"),
    _max("Total_Confirmed").alias("Max_Confirmed")
)

state_stats.write.mode("overwrite").parquet(
    output_path + "skew_statistics"
)

# Calculate Skew Ratio
stats = state_stats.collect()[0]
skew_ratio = stats["Max_Confirmed"] / stats["Average_Confirmed"] \
    if stats["Average_Confirmed"] != 0 else 0

# Stop Spark
spark.stop()