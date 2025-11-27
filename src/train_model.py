import logging
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

# --- CẤU HÌNH ---
# Lưu ý: Dùng port 8020 cho kết nối RPC (như trong docker-compose)
HDFS_INPUT_PATH = "hdfs://namenode:8020/data/smart_meter_cleaned"
MODEL_OUTPUT_PATH = "hdfs://namenode:8020/models/consumption_model"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # 1. Khởi tạo Spark Session (Batch Mode)
    spark = SparkSession.builder \
        .appName("SmartMeterTraining") \
        .getOrCreate()
    
    logger.info("🚀 Training Job Started...")

    # 2. Load dữ liệu từ HDFS (Parquet)
    try:
        # Đọc toàn bộ dữ liệu hiện có trong Data Lake
        df = spark.read.parquet(HDFS_INPUT_PATH)
        
        # In ra số lượng bản ghi để kiểm tra
        record_count = df.count()
        logger.info(f"📊 Found {record_count} records for training.")
        
        if record_count == 0:
            logger.warning("⚠️ No data found! Please wait for ingest_data.py to run for a while.")
            return
            
    except Exception as e:
        logger.error(f"❌ Error reading data from HDFS: {e}")
        logger.info("Tip: Ensure ingest_data.py is running and generating parquet files.")
        return

    # 3. Chuẩn bị dữ liệu (Feature Engineering)
    # Cần sửa lại logic model ở đây, hiện đang implement Model đơn giản: Dự đoán Công suất (Power) dựa trên Dòng điện (Current) và Điện áp (Voltage) 
    # Chọn các cột input (Features) và cột target (Label)
    feature_cols = ["voltage", "current", "power_factor", "frequency"]
    label_col = "power" # active_power_kw

    # Loại bỏ các dòng có giá trị null để tránh lỗi training
    train_data = df.select(feature_cols + [label_col]).dropna()

    # VectorAssembler: Gom các cột features thành 1 vector duy nhất (Spark ML yêu cầu)
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # 4. Định nghĩa Model (Linear Regression)
    lr = LinearRegression(featuresCol="features", labelCol=label_col)

    # Tạo Pipeline: Gom bước xử lý vector và bước training lại
    pipeline = Pipeline(stages=[assembler, lr])

    # 5. Huấn luyện Model
    logger.info("🏋️ Training model...")
    model = pipeline.fit(train_data)

    # In ra các hệ số của model (Coefficients) để xem nó học được gì
    lr_model = model.stages[-1]
    logger.info(f"✅ Model Trained! Coefficients: {lr_model.coefficients} Intercept: {lr_model.intercept}")

    # 6. Lưu Model xuống HDFS
    # Cho phép ghi đè (overwrite) để cập nhật model mới nhất
    try:
        model.write().overwrite().save(MODEL_OUTPUT_PATH)
        logger.info(f"💾 Model saved successfully to: {MODEL_OUTPUT_PATH}")
    except Exception as e:
        logger.error(f"❌ Failed to save model: {e}")

    spark.stop()

if __name__ == "__main__":
    main()