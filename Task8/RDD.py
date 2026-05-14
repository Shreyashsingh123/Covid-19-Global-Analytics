from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("RDD").getOrCreate()

sc=spark.sparkContext

input_path = "hdfs://localhost:9000/data/covid/staging/Covid_dataset/full_grouped"
output_path = "hdfs://localhost:9000/data/covid/analytics/Covid_dataset/rdd_analysis/"


df=spark.read.parquet(input_path)

confirm_rdd=df.select(
    "Country_Region",
    "Confirmed",
    "Deaths",
    "Recovered"
).rdd


# 1 Total Confirmed per Country
confirmed_rdd=confirm_rdd.map(lambda x:(x[0],x[1])).reduceByKey(lambda a,b:a+b)

confirmed_rdd.saveAsTextFile(output_path+"confirmed_cases_by_country")


# 2 Total Deaths per Country
deaths_rdd = confirm_rdd.map(
    lambda x: (x[0], x[2])
).reduceByKey(
    lambda a, b: a + b
)

deaths_rdd.saveAsTextFile(
    output_path + "total_deaths_per_country"
)


# 3 Total Recovered per Country
combined_rdd = confirm_rdd.map(
    lambda x: (x[0], (x[1], x[2]))
)

# Reduce by country
aggregated_rdd = combined_rdd.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)


death_percentage_rdd = aggregated_rdd.mapValues(
    lambda x: (x[1] / x[0] * 100) if x[0] != 0 else 0
)

death_percentage_rdd.saveAsTextFile(
    output_path + "death_percentage_per_country"
)

spark.stop()