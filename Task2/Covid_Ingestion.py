from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_date

spark=SparkSession.builder.appName("CovidIngestion").getOrCreate()

raw_path="hdfs://localhost:9000/data/covid/raw/Covid_dataset/"
staging_path="hdfs://localhost:9000/data/covid/staging/Covid_dataset/"

schemas = {
    "country_wise_latest.csv": StructType(
        [
            StructField("Country/Region", StringType(), True),
            StructField("Confirmed", IntegerType(), True),
            StructField("Deaths", IntegerType(), True),
            StructField("Recovered", IntegerType(), True),
            StructField("Active", IntegerType(), True),
            StructField("New cases", IntegerType(), True),
            StructField("New deaths", IntegerType(), True),
            StructField("New recovered", IntegerType(), True),
            StructField("Deaths / 100 Cases", DoubleType(), True),
            StructField("Recovered / 100 Cases", DoubleType(), True),
            StructField("Deaths / 100 Recovered", DoubleType(), True),
            StructField("Confirmed last week", IntegerType(), True),
            StructField("1 week change", IntegerType(), True),
            StructField("1 week % increase", DoubleType(), True),
            StructField("WHO Region", StringType(), True),
        ]
    ),
    "covid_19_clean_complete.csv": StructType(
        [
            StructField("Province/State", StringType(), True),
            StructField("Country/Region", StringType(), True),
            StructField("Lat", DoubleType(), True),
            StructField("Long", DoubleType(), True),
            StructField("Date", StringType(), True),
            StructField("Confirmed", IntegerType(), True),
            StructField("Deaths", IntegerType(), True),
            StructField("Recovered", IntegerType(), True),
            StructField("Active", IntegerType(), True),
            StructField("WHO Region", StringType(), True),
        ]
    ),
    "day_wise.csv": StructType(
        [
            StructField("Date", StringType(), True),
            StructField("Confirmed", IntegerType(), True),
            StructField("Deaths", IntegerType(), True),
            StructField("Recovered", IntegerType(), True),
            StructField("Active", IntegerType(), True),
            StructField("New cases", IntegerType(), True),
            StructField("New deaths", IntegerType(), True),
            StructField("New recovered", IntegerType(), True),
            StructField("Deaths / 100 Cases", DoubleType(), True),
            StructField("Recovered / 100 Cases", DoubleType(), True),
            StructField("Deaths / 100 Recovered", DoubleType(), True),
            StructField("No. of countries", IntegerType(), True),
        ]
    ),
    "full_grouped.csv": StructType(
        [
            StructField("Date", StringType(), True),
            StructField("Country/Region", StringType(), True),
            StructField("Confirmed", IntegerType(), True),
            StructField("Deaths", IntegerType(), True),
            StructField("Recovered", IntegerType(), True),
            StructField("Active", IntegerType(), True),
            StructField("New cases", IntegerType(), True),
            StructField("New deaths", IntegerType(), True),
            StructField("New recovered", IntegerType(), True),
            StructField("WHO Region", StringType(), True),
        ]
    ),
    "usa_county_wise.csv": StructType(
        [
            StructField("UID", LongType(), True),
            StructField("iso2", StringType(), True),
            StructField("iso3", StringType(), True),
            StructField("code3", IntegerType(), True),
            StructField("FIPS", IntegerType(), True),
            StructField("Admin2", StringType(), True),
            StructField("Province_State", StringType(), True),
            StructField("Country_Region", StringType(), True),
            StructField("Lat", DoubleType(), True),
            StructField("Long_", DoubleType(), True),
            StructField("Combined_Key", StringType(), True),
            StructField("Date", StringType(), True),
            StructField("Confirmed", IntegerType(), True),
            StructField("Deaths", IntegerType(), True),
        ]
    ),
    "worldometer_data.csv": StructType(
        [
            StructField("Country/Region", StringType(), True),
            StructField("Continent", StringType(), True),
            StructField("Population", LongType(), True),
            StructField("TotalCases", LongType(), True),
            StructField("NewCases", IntegerType(), True),
            StructField("TotalDeaths", IntegerType(), True),
            StructField("NewDeaths", IntegerType(), True),
            StructField("TotalRecovered", IntegerType(), True),
            StructField("NewRecovered", IntegerType(), True),
            StructField("ActiveCases", IntegerType(), True),
            StructField("Serious,Critical", IntegerType(), True),
            StructField("Tot Cases/1M pop", DoubleType(), True),
            StructField("Deaths/1M pop", DoubleType(), True),
            StructField("TotalTests", LongType(), True),
            StructField("Tests/1M pop", DoubleType(), True),
            StructField("WHO Region", StringType(), True),
        ]
    ),
}

def read_data(file_name,schema)-> DataFrame:
    return spark.read.csv(raw_path+file_name,header=True,schema=schema)

def clean_name(df):
    for col in df.columns:
        new_col=(
            col.strip()
            .replace(" ","_")
            .replace("/","_")
            .replace(",","_")
            .replace(".","")
            .replace("-", "_")
        )
        df=df.withColumnRenamed(col,new_col)
    return df

def data_preprocessing(df:DataFrame,file_name) -> DataFrame:
    numeric=[]
    cols=[]
    for field in df.schema.fields:
         if isinstance(field.dataType,(IntegerType, DoubleType, LongType),):
            numeric.append(field.name)
         elif isinstance(field.dataType, StringType):
            cols.append(field.name)
    
    if numeric:
        df = df.fillna(0, subset=numeric)

    if cols:
        df = df.fillna("Unknown", subset=cols)
    
    if "Date" in df.columns:
        if file_name == "usa_county_wise.csv":
            df = df.withColumn("Date", to_date("Date", "M/d/yy"))

        else:
            df = df.withColumn("Date", to_date("Date", "yyyy-MM-dd"))

        df = df.dropna(subset=["Date"])

    return df



def save_data(df: DataFrame, file_name):
    df.write.mode("overwrite").parquet(staging_path + file_name)
    print("Data saved to: " + staging_path + file_name)


def main():
    for file_name, schema in schemas.items():
        print("Processing file: " + file_name)
        df = read_data(file_name, schema)
        df = clean_name(df)
        df = data_preprocessing(df, file_name)
        df.printSchema()
        save_data(df, file_name[:-4])

if __name__ == "__main__":
    main()
    spark.stop()
    