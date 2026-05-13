from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("CovidAnalysis").getOrCreate()
df=spark.read.csv("hdfs://localhost:9000/data/covid/raw/Covid_dataset/country_wise_latest.csv",header=True,inferSchema=True)
df.show(5)
df.printSchema()

df1=spark.read.csv("hdfs://localhost:9000/data/covid/raw/Covid_dataset/covid_19_clean_complete.csv",header=True,inferSchema=True)
df1.show(5)
df1.printSchema()

df2=spark.read.csv("hdfs://localhost:9000/data/covid/raw/Covid_dataset/day_wise.csv",header=True,inferSchema=True)
df2.show(5)
df2.printSchema()


df3=spark.read.csv("hdfs://localhost:9000/data/covid/raw/Covid_dataset/full_grouped.csv",header=True,inferSchema=True)
df3.show(5)
df3.printSchema()


df4=spark.read.csv("hdfs://localhost:9000/data/covid/raw/Covid_dataset/usa_county_wise.csv",header=True,inferSchema=True)
df4.show(5)
df4.printSchema()

df5=spark.read.csv("hdfs://localhost:9000/data/covid/raw/Covid_dataset/worldometer_data.csv",header=True,inferSchema=True)
df5.show(5)
df5.printSchema()



spark.stop()