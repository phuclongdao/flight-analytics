import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, when, month, round
from pyspark.sql.functions import sum as spark_sum, unix_timestamp

spark = (
    SparkSession.builder
    .appName("Flight Analytics Local")
    .master("local[1]")
    .config("spark.driver.memory", "512m")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

DATA_PATH = (
    "file:///D:/Long/Uni/Big Data/"
    "flight-analytics/hdfs_yarn_setup/data/merged_flights.parquet"
)

start = time.time()

print("SparkSession created")

df = spark.read.parquet(DATA_PATH)

print("Read parquet OK")

total = df.count()
print("Total flights:", total)



df_valid_duration = (
    df
    .filter(
        col("first_seen").isNotNull() &
        col("last_seen").isNotNull() &
        col("flight_id").isNotNull() &
        (col("last_seen") > col("first_seen"))
        
    )
    .withColumn(
        "flight_duration_sec",
        unix_timestamp(col("last_seen")) - unix_timestamp(col("first_seen"))
    )
    .filter(col("flight_duration_sec") <= 16 * 3600)
)
valid_count = df_valid_duration.count()
print("Valid-duration flights:", valid_count)
df_valid_duration \
    .select(
        "icao24",
        "flight_id",
        "first_seen",
        "last_seen",
        "flight_duration_sec"
    ) \
    .orderBy(col("flight_duration_sec").desc()) \
    .show(10, truncate=False)

df_type_duration = (
    df_valid_duration
    .filter(col("typecode").isNotNull())
    .groupBy("typecode")
    .agg(
        spark_sum("flight_duration_sec").alias("total_flight_duration_sec")
    )
    .orderBy(col("total_flight_duration_sec").desc())
)

df_type_duration.show(10, truncate=False)

df_avg_duration_by_type = (
    df_valid_duration
    .filter(col("typecode").isNotNull())
    .groupBy("typecode")
    .agg(
        round(avg("flight_duration_sec"), 2).alias("average_duration_sec")
    )
    .orderBy(col("average_duration_sec").desc())
)
df_avg_duration_by_type.show(10, truncate=False)


spark.stop()

print("Done in", time.time() - start, "seconds")
