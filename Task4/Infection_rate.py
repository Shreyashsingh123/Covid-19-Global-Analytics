from pyspark.sql import SparkSession
from pyspark.sql.functions import col,round,when,sum as _sum,desc

spark = SparkSession.builder.appName("Task4-InfectionRateAnalytics").getOrCreate()


worldometer=spark.read.parquet("hdfs://localhost:9000/data/covid/staging/Covid_dataset/worldometer_data.parquet")

# confirm case per thousand population
# worldometer.show(1)

infection_df = worldometer.withColumn(
    "Confirmed_per_1000",
    round(
        when(col("Population") != 0,
             (col("TotalCases") / col("Population")) * 1000
        ).otherwise(0),
        4
    )
)

infection_df.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/covid/analytics/Covid_dataset/confirmed_per_1000"
)


# Active case per thousand population
infect_df=worldometer.withColumn(
    "Active_per_1000",
    round(
        when(col("Population") != 0,
             (col("ActiveCases") / col("Population")) * 1000
        ).otherwise(0),
        4
    )
)
infect_df.write.mode("overwrite").parquet("hdfs://localhost:9000/data/covid/analytics/Covid_dataset/active_per_1000")
infect_df.show(10)



#Top 10 countries by infection rate
top_infection_rate=infect_df.orderBy(
    desc("Active_per_1000")
).limit(10)
top_infection_rate.write.mode("overwrite").parquet("hdfs://localhost:9000/data/covid/analytics/Covid_dataset/top10_infection_rate")
top_infection_rate.show(10)

#who region infectin ranking
who_infection=infect_df.groupBy("WHO Region").agg(
    _sum("TotalCases").alias("Total_Confirmed"),
    _sum("Population").alias("Total_Population")
).withColumn(
    "Infection_per_1000",
    round(
        when(col("Total_Population") != 0,
                (col("Total_Confirmed") / col("Total_Population")) * 100
        ).otherwise(0),
        4
    )
).orderBy(desc("Infection_per_1000"))
who_infection.write.mode("overwrite").parquet("hdfs://localhost:9000/data/covid/analytics/Covid_dataset/who_region_infection_ranking")
who_infection.show(10)