import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# --- CẤU HÌNH ---
KAFKA_BOOTSTRAP_SERVERS = "kafka1:29092,kafka2:29092"
KAFKA_TOPIC = "smart-meter-data"
HDFS_PATH = "hdfs://namenode:8020/data/smart_meter_cleaned" # Nơi lưu dữ liệu sạch
CHECKPOINT_PATH = "hdfs://namenode:8020/checkpoints/ingest_job" # Bắt buộc cho Streaming

# Thiết lập logging để dễ theo dõi
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    # 1. Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("SmartMeterIngestion") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.cores.max", "1") \
        .config("spark.executor.cores", "1") \
        .getOrCreate()

    logger.info("Spark Session started successfully")

    # 2. Định nghĩa Schema (Khớp 100% với JSON từ generator.py)
    # Cấu trúc lồng nhau (Nested Structure)
    json_schema = StructType([
        StructField("meter_id", StringType(), True),
        StructField("timestamp", StringType(), True), 
        StructField("location", StructType([
            StructField("city", StringType(), True),
            StructField("district", StringType(), True),
            StructField("gps", StringType(), True)
        ])),
        StructField("measurements", StructType([
            StructField("voltage_v", DoubleType(), True),
            StructField("current_a", DoubleType(), True),
            StructField("active_power_kw", DoubleType(), True),
            StructField("power_factor", DoubleType(), True),
            StructField("frequency_hz", DoubleType(), True),
            StructField("total_energy_kwh", DoubleType(), True)
        ])),
        StructField("status", StructType([
            StructField("code", IntegerType(), True),
            StructField("message", StringType(), True),
            StructField("battery_level", IntegerType(), True)
        ]))
    ])

    # 3. Đọc dữ liệu từ Kafka (ReadStream)
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 4. Parse JSON & Flatten (Làm phẳng dữ liệu)
    # Chuyển cột 'value' (binary) thành String rồi parse JSON
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), json_schema).alias("data")
    ).select("data.*") # Bung các trường ra

    # 5. TIỀN XỬ LÝ (PRE-PROCESSING / CLEANING) 
    cleaned_stream = parsed_stream \
        .withColumn("event_time", col("timestamp").cast(TimestampType())) \
        .withColumn("date", to_date(col("event_time"))) \
        .select(
            col("meter_id"),
            col("event_time"),
            col("date"), # Dùng để partition
            col("location.district"),
            col("measurements.voltage_v").alias("voltage"),
            col("measurements.current_a").alias("current"),
            col("measurements.active_power_kw").alias("power"),
            col("measurements.power_factor").alias("power_factor"),
            col("measurements.frequency_hz").alias("frequency"),
            col("measurements.total_energy_kwh").alias("energy"),
            col("status.code").alias("status_code")
        )

    # --- BỘ LỌC (FILTERING RULES) ---
    # Rule 1: Loại bỏ dữ liệu Null ở các trường quan trọng (Meter ID, Time)
    filtered_stream = cleaned_stream.filter(col("meter_id").isNotNull() & col("event_time").isNotNull())
    
    # Rule 2: Logic lọc giá trị rác
    # Lưu ý: Generator có thể tắt metric (gửi null). 
    # Dùng coalesce(col, 0) để coi như bằng 0 nếu null, tránh drop mất dòng data
    filtered_stream = filtered_stream.filter(
        (coalesce(col("voltage"), lit(1)) > 0) & 
        (coalesce(col("power"), lit(0)) >= 0)
    )

    # 6. Ghi xuống HDFS (WriteStream)
    # Định dạng Parquet: Tối ưu cho lưu trữ và truy vấn sau này
    query = filtered_stream.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", HDFS_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("date") \
        .trigger(processingTime="1 minute") \
        .start()

    logger.info(f"Streaming to HDFS started. Path: {HDFS_PATH}")
    logger.info(">>> Partitioning by 'date'. Trigger interval: 1 minute.")
    query.awaitTermination()

if __name__ == "__main__":
    main()