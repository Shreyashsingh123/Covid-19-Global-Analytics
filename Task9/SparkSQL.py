from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("SparkSQL_Covid_Analysis") \
    .getOrCreate()


# Load Data
df = spark.read.parquet(
    "hdfs://localhost:9000/data/covid/staging/Covid_dataset/full_grouped"
)
# 1️ Create Temporary View
df.createOrReplaceTempView("covid_data")

# 2️ Top 10 Infection Countries

top_infection = spark.sql("""
    SELECT Country_Region,
           SUM(Confirmed) AS Total_Confirmed
    FROM covid_data
    GROUP BY Country_Region
    ORDER BY Total_Confirmed DESC
    LIMIT 10
""")

top_infection.show()

# 3️ Death Percentage Ranking

death_percentage = spark.sql("""
    SELECT Country_Region,
           SUM(Deaths) AS Total_Deaths,
           SUM(Confirmed) AS Total_Confirmed,
           ROUND(
               (SUM(Deaths)/SUM(Confirmed))*100, 2
           ) AS Death_Percentage
    FROM covid_data
    GROUP BY Country_Region
    ORDER BY Death_Percentage DESC
""")

death_percentage.show()

# 4️ Rolling 7-Day Average (Window Function)

rolling_avg = spark.sql("""
    SELECT
        Country_Region,
        Date,
        ROUND(
            AVG(Recovered)
            OVER (
                PARTITION BY Country_Region
                ORDER BY Date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2
        ) AS Rolling_7Day_Recovery_Avg
    FROM covid_data
""")

rolling_avg.show()

# 5️ Compare Physical Plans

print("=== SQL Physical Plan ===")
top_infection.explain(True)

# DataFrame API equivalent
df_api = df.groupBy("Country_Region") \
           .sum("Confirmed") \
           .orderBy(col("sum(Confirmed)").desc()) \
           .limit(10)

print("=== DataFrame Physical Plan ===")
df_api.explain(True)

spark.stop()