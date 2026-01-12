from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# ===============================
# 1️⃣ Tạo SparkSession
# ===============================
spark = SparkSession.builder \
    .appName("Process Flights Batch") \
    .getOrCreate()

# ===============================
# 2️⃣ Đọc tất cả file Parquet trên HDFS
# ===============================
hdfs_path = "hdfs://namenode:9000/user/root/processed/*.parquet"
flights_df = spark.read.parquet(hdfs_path)

# ===============================
# 3️⃣ Kiểm tra schema
# ===============================
flights_df.printSchema()
# Cột có thể: "icao24","flight_id","first_seen","last_seen","dof",
# "adep_p","ades_p","registration","model","typecode"

# ===============================
# 4️⃣ Thống kê cơ bản
# ===============================
total_rows = flights_df.count()
print(f"Total flights: {total_rows}")

# Số chuyến bay theo departure airport
dep_counts = flights_df.groupBy("adep_p").count().orderBy(col("count").desc())
dep_counts.show(10, truncate=False)

# Số chuyến bay theo typecode
typecode_counts = flights_df.groupBy("typecode").count().orderBy(col("count").desc())
typecode_counts.show(10, truncate=False)

# ===============================
# 5️⃣ Xử lý null/UNKNOWN (nếu còn)
# ===============================
flights_df = flights_df.fillna("UNKNOWN", subset=["flight_id","adep_p","ades_p","registration","model","typecode"])

# ===============================
# 6️⃣ Ví dụ filter dữ liệu
# ===============================
# Lấy các chuyến bay từ sân bay "JFK" (hoặc các giá trị cụ thể)
jfk_flights = flights_df.filter(col("adep_p") == "JFK")
print("Flights from JFK:", jfk_flights.count())

# ===============================
# 7️⃣ Lưu kết quả gộp lại
# ===============================
output_hdfs = "hdfs://namenode:9000/user/root/final/merged_flights.parquet"
flights_df.write.mode("overwrite").parquet(output_hdfs)

print(f"✅ Final merged DataFrame saved to {output_hdfs}")

# ===============================
# 8️⃣ Dừng SparkSession
# ===============================
spark.stop()
