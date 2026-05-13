from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("CovidIngestion").getOrCreate()

raw_path="hdfs://localhost:9000/data/covid/raw/Covid_dataset"
staging_path="hdfs://localhost:9000/data/covid/staging/Covid_dataset"

schema=StructType([
    StructField("Date", StringType(), True),
    StructField("Country/Region", StringType(), True),
    StructField("Confirmed", IntegerType(), True),
    StructField("Deaths", IntegerType(), True),
    StructField("Recovered", IntegerType(), True),
    StructField("Active", IntegerType(), True),
    StructField("New Cases", IntegerType(), True),
    StructField("New Deaths", IntegerType(), True),
    StructField("New Recovered", IntegerType(), True),
])
df_full=spark.read.csv(raw_path+"full_grouped.csv",header=True,schema=schema)

df_full=df_full.fillna(0)

df_full.write.mode("overwrite").parquet(staging_path+"full_grouped.parquet")

files=[
    "day_wise.csv",
    "country_wise_latest.csv",
    "covid_19_clean_complete.csv",
    "worldometer_data.csv",
    "usa_county_wise.csv"
]

for file in files:
    df=spark.read.csv(raw_path+file,header=True,inferSchema=True)
    df=df.fillna(0)

    folder_name=file.replace(".csv","")
    df.write.mode("overwrite").parquet(staging_path+folder_name+".parquet")
    print("completed")

spark.stop()