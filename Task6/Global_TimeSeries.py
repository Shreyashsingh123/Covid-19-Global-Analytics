from pyspark.sql import SparkSession
from pyspark.sql.functions import (avg,round,mean,stddev,col,abs as _abs,desc,lag,sum as _sum,year,month)
from pyspark.sql.window import Window

spark=SparkSession.builder.appName("GlobalTimeSeries").getOrCreate()

df=spark.read.parquet("hdfs://localhost:9000/data/covid/staging/Covid_dataset/day_wise")

#Global daily average new cases
global_avg_new_cases = df.agg(
    round(avg("New_cases"), 2).alias("Global_Avg_New_Cases")
)

global_avg_new_cases.show()


stats = df.agg(
    mean("New_cases").alias("mean_cases"),
    stddev("New_cases").alias("std_cases")
).collect()[0]

mean_cases = stats["mean_cases"]
std_cases = stats["std_cases"]

df_z = df.withColumn(
    "Z_score",
    (col("New_cases") - mean_cases) / std_cases
)

spike_days = df_z.filter(_abs(col("Z_score")) > 2)

spike_days.select("Date", "New_cases", "Z_score").show()




peak_death = df.orderBy(desc("Deaths")).limit(1)

peak_death.select("Date", "Deaths").show()


monthly_deaths = df.withColumn("Year", year("Date")) \
    .withColumn("Month", month("Date")) \
    .groupBy("Year", "Month") \
    .agg(_sum("Deaths").alias("Monthly_Deaths")) \
    .orderBy("Year", "Month")

windowSpec = Window.orderBy("Year", "Month")

monthly_growth = monthly_deaths.withColumn(
    "Prev_Month_Deaths",
    lag("Monthly_Deaths").over(windowSpec)
).withColumn(
    "MoM_Growth_Rate",
    round(
        ((col("Monthly_Deaths") - col("Prev_Month_Deaths")) /
         col("Prev_Month_Deaths")) * 100,
        2
    )
)

monthly_growth.show()