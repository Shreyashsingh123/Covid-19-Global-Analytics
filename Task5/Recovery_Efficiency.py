from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, round, when,
    row_number, desc, avg,lag
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("RecoveryEfficiency").getOrCreate()

# Read parquet
df = spark.read.parquet(
    "hdfs://localhost:9000/data/covid/staging/Covid_dataset/full_grouped"
)
df1=spark.read.parquet("hdfs://localhost:9000/data/covid/staging/Covid_dataset/worldometer_data")


#Recovery Percentage per country
recovery_df=df.withColumn(
    "Recovery_percentage",
    round(
        when(col("Confirmed")!=0,
             (col("Recovered")/col("Confirmed"))*100
        ).otherwise(0),
        2
    )
)

recovery_df.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/covid/analytics/Covid_dataset/recovery_percentage_per_country"
)


# 7 day rolling revovery average (Window Function)
window_spec=Window.partitionBy("Country_Region").orderBy("Date").rowsBetween(-
6,0)
rolling_recovery=recovery_df.withColumn(
    "7_day_rolling_recovery",
    round(avg("Recovery_percentage").over(window_spec),2)
)
rolling_recovery.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/covid/analytics/Covid_dataset/7_day_rolling_recovery_percentage"
)


# Country with fastest recovery growth.
growth_window=Window.partitionBy("Country_Region").orderBy("Date")

growth_window = Window.partitionBy("Country_Region").orderBy("Date")

growth_df = recovery_df.withColumn(
    "Previous_Percentage",
    lag("Recovery_percentage", 1).over(growth_window)
)

growth_df = growth_df.withColumn(
    "Recovery_Growth",
    round(
        col("Recovery_percentage") - col("Previous_Percentage"),
        2
    )
)

fastest_growth_df=growth_df.orderBy(
    col("Recovery_Growth").desc()
)

fastest_growth_df.show(10)

fastest_growth_df.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/covid/analytics/Covid_dataset/fastest_recovery")


#Peak Recovery Day
peak_window = Window.partitionBy("Country").orderBy(
    col("Recovery_percentage").desc()
)

peak_recovery_df = recovery_df.withColumn(
    "rank",
    row_number().over(peak_window)
).filter(
    col("rank") == 1
).drop("rank")

peak_recovery_df.show()

peak_recovery_df.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/covid/analytics/Covid_dataset/peak_recovery_day_per_country"
)
