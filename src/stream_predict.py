import logging
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# --- CẤU HÌNH ---
KAFKA_BOOTSTRAP_SERVERS = "kafka1:29092,kafka2:29092"
KAFKA_TOPIC = "smart-meter-data"
MODEL_PATH = "hdfs://namenode:8020/models/consumption_model"
PREDICTIONS_PATH = "hdfs://namenode:8020/data/predictions"
CHECKPOINT_PATH = "hdfs://namenode:8020/checkpoints/predict_job"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \
        .appName("SmartMeterPrediction") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.cores.max", "1") \
        .config("spark.executor.cores", "1") \
        .getOrCreate()

    logger.info("Prediction Job Started...")

    # 1. Load the trained model from HDFS
    try:
        model = PipelineModel.load(MODEL_PATH)
        logger.info(f"Model loaded successfully from {MODEL_PATH}")
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        spark.stop()
        return

    # 2. Define the same schema as the ingestion job
    json_schema = StructType([
        StructField("meter_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("measurements", StructType([
            StructField("voltage_v", DoubleType(), True),
            StructField("current_a", DoubleType(), True),
            StructField("power_factor", DoubleType(), True),
            StructField("frequency_hz", DoubleType(), True),
            StructField("active_power_kw", DoubleType(), True) # The real value
        ]))
    ])

    # 3. Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # 4. Parse and select the features needed for the model
    parsed_stream = raw_stream.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")

    feature_stream = parsed_stream.select(
        col("meter_id"),
        col("timestamp").cast(TimestampType()).alias("event_time"),
        col("measurements.voltage_v").alias("voltage"),
        col("measurements.current_a").alias("current"),
        col("measurements.power_factor").alias("power_factor"),
        col("measurements.frequency_hz").alias("frequency"),
        col("measurements.active_power_kw").alias("actual_power")
    ).dropna()

    # 5. Use the model to make predictions on the stream
    # The model is a Pipeline, so it will automatically assemble the features
    predictions = model.transform(feature_stream)

    # 6. Select final output and write to a sink (e.g., another HDFS path or Kafka topic)
    output_stream = predictions.select(
        col("meter_id"),
        col("event_time"),
        col("actual_power"),
        col("prediction").alias("predicted_power")
    )

    query = output_stream.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", PREDICTIONS_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="15 seconds") \
        .start()

    logger.info(f"Streaming predictions to {PREDICTIONS_PATH}")
    query.awaitTermination()

if __name__ == "__main__":
    main()