# analytics_local.py — KHÔNG DÙNG Spark writer (tránh NativeIO Windows)
# CHỈ GHI CÁC BẢNG AGGREGATE NHỎ BẰNG pandas (AN TOÀN BỘ NHỚ)

import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, month,
    sum as spark_sum, unix_timestamp
)

# ================== SPARK ==================
spark = (
    SparkSession.builder
    .appName("Flight Analytics Local")
    .master("local[1]")
    .config("spark.driver.memory", "512m")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.hadoop.io.native.lib.available", "false")
    .getOrCreate()
)

DATA_PATH = (
    "file:///D:/Long/Uni/Big Data/"
    "flight-analytics/hdfs_yarn_setup/data/merged_flights.parquet"
)

OUT_DIR = "D:/Long/Uni/Big Data/flight-analytics/output"
os.makedirs(OUT_DIR, exist_ok=True)

start = time.time()

df = spark.read.parquet(DATA_PATH)

# ================== ANALYTICS ==================

total = df.count()

df_month = (
    df.filter(col("dof").isNotNull())
      .withColumn("month", month(col("dof")))
      .groupBy("month")
      .agg(count("*").alias("count"))
      .orderBy("month")
)

df_adep = (
    df.filter(col("adep_p").isNotNull())
      .groupBy("adep_p")
      .agg(count("*").alias("count"))
      .orderBy(col("count").desc())
)

df_ades = (
    df.filter(col("ades_p").isNotNull())
      .groupBy("ades_p")
      .agg(count("*").alias("count"))
      .orderBy(col("count").desc())
)

df_type = (
    df.filter(col("typecode").isNotNull())
      .groupBy("typecode")
      .agg(count("*").alias("count"))
      .orderBy(col("count").desc())
)

df_valid_duration = (
    df.filter(
        col("first_seen").isNotNull() &
        col("last_seen").isNotNull() &
        (col("last_seen") > col("first_seen"))
    )
    .withColumn(
        "flight_duration_sec",
        unix_timestamp(col("last_seen")) - unix_timestamp(col("first_seen"))
    )
    .filter(col("flight_duration_sec") <= 16 * 3600)
)

valid_count = df_valid_duration.count()

df_type_duration = (
    df_valid_duration
    .filter(col("typecode").isNotNull())
    .groupBy("typecode")
    .agg(
        spark_sum("flight_duration_sec")
        .alias("total_flight_duration_sec")
    )
    .orderBy(col("total_flight_duration_sec").desc())
)

df_avg_duration_by_type = (
    df_valid_duration
    .filter(col("typecode").isNotNull())
    .groupBy("typecode")
    .agg(
        avg("flight_duration_sec")
        .alias("average_duration_sec")
    )
    .orderBy(col("average_duration_sec").desc())
)

# ================== SAVE (PANDAS – SAFE) ==================
df_month.toPandas().to_parquet(f"{OUT_DIR}/flights_per_month.parquet", index=False)
df_adep.limit(500).toPandas().to_parquet(f"{OUT_DIR}/flights_per_adep.parquet", index=False)
df_ades.limit(500).toPandas().to_parquet(f"{OUT_DIR}/flights_per_ades.parquet", index=False)
df_type.limit(500).toPandas().to_parquet(f"{OUT_DIR}/flights_per_typecode.parquet", index=False)
df_type_duration.limit(500).toPandas().to_parquet(f"{OUT_DIR}/total_duration_by_type.parquet", index=False)
df_avg_duration_by_type.limit(500).toPandas().to_parquet(f"{OUT_DIR}/avg_duration_by_type.parquet", index=False)

spark.stop()

print("Total flights:", total)
print("Valid-duration flights:", valid_count)
print("Done in", time.time() - start, "seconds")
