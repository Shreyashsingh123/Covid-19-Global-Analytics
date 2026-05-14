from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum as _sum, desc, when


spark = SparkSession.builder.appName("Task3-DeathPercentageAnalytics").getOrCreate()

# Read Parquet Files (from staging)

full_grouped = spark.read.parquet(
    "hdfs://localhost:9000/data/covid/staging/Covid_dataset/full_grouped"
)

worldometer = spark.read.parquet(
    "hdfs://localhost:9000/data/covid/staging/Covid_dataset/worldometer_data"
)


# 1️⃣ Daily Death Percentage per Country
# Formula: (Deaths / Confirmed) * 100

daily_country = full_grouped.withColumn(
    "Death_Percentage",
    round(
        when(col("Confirmed") != 0,
             (col("Deaths") / col("Confirmed")) * 100
        ).otherwise(0),
        2
    )
)

daily_country.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/covid/analytics/Covid_dataset/daily_country_death_percentage"
)


# 2️ Global Daily Death Percentage

global_daily = full_grouped.groupBy("Date").agg(
    _sum("Confirmed").alias("Total_Confirmed"),
    _sum("Deaths").alias("Total_Deaths")
).withColumn(
    "Global_Death_Percentage",
    round(
        when(col("Total_Confirmed") != 0,
             (col("Total_Deaths") / col("Total_Confirmed")) * 100
        ).otherwise(0),
        2
    )
)

global_daily.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/covid/analytics/Covid_dataset/global_daily_death_percentage"
)


# 3️ Continent-wise Death Percentage

# Use alias to avoid ambiguous column error
fg = full_grouped.alias("fg")
wm = worldometer.alias("wm")

continent_df = fg.join(
    wm,
    col("fg.`Country_Region`") == col("wm.`Country_Region`"),
    "left"
)


continent_death = continent_df.groupBy(
    col("wm.Continent")
).agg(
    _sum(col("fg.Confirmed")).alias("Total_Confirmed"),
    _sum(col("fg.Deaths")).alias("Total_Deaths")
).withColumn(
    "Continent_Death_Percentage",
    round(
        when(col("Total_Confirmed") != 0,
             (col("Total_Deaths") / col("Total_Confirmed")) * 100
        ).otherwise(0),
        2
    )
)

continent_death.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/covid/analytics/Covid_dataset/continent_death_percentage"
)


# 4️ Country with Highest Overall Death Percentage

country_highest = full_grouped.groupBy("Country_Region").agg(
    _sum("Deaths").alias("Total_Deaths"),
    _sum("Confirmed").alias("Total_Confirmed")
).withColumn(
    "Death_Percentage",
    round(
        when(col("Total_Confirmed") != 0,
             (col("Total_Deaths") / col("Total_Confirmed")) * 100
        ).otherwise(0),
        2
    )
).orderBy(desc("Death_Percentage")).limit(1)

country_highest.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/covid/analytics/Covid_dataset/highest_death_percentage_country"
)

# 5️ Top 10 Countries by Deaths Per Capita
# Formula: Total Deaths / Population

country_totals = continent_df.groupBy(
    col("fg.`Country_Region`"),
    col("wm.Population")
).agg(
    _sum(col("fg.Deaths")).alias("Total_Deaths")
)

top10_per_capita = country_totals.withColumn(
    "Deaths_Per_Capita",
    round(
        when(col("Population") != 0,
             col("Total_Deaths") / col("Population")
        ).otherwise(0),
        6
    )
).orderBy(desc("Deaths_Per_Capita")).limit(10)

top10_per_capita.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/covid/analytics/Covid_dataset/top10_deaths_per_capita"
)

# Stop Spark
spark.stop()