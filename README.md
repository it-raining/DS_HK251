# DS\_HK251: IoT Smart Meter Big Data Analytics

**Project:** Phân tích dữ liệu lớn cho công tơ điện/nước IoT.
**Goal:** Phân tích và dự đoán dữ liệu nhà máy điện sử dụng kiến trúc Lakehouse.

## Tổng quan kiến trúc (Architecture)

  * **Ingestion (Thu thập):** Kafka (2 Brokers)
  * **Storage (Lưu trữ):** HDFS (Data Lake)
  * **Processing (Xử lý):** Apache Spark (Streaming & Batch)
  * **Client:** Python Generator (giả lập dữ liệu) & Streamlit Dashboard (hiển thị)

-----

## 1\. Khởi tạo hạ tầng (Infrastructure Setup)

Bước đầu tiên là dựng cluster và cài đặt các thư viện phụ thuộc cần thiết.

```bash
# 1. Build image và khởi chạy các container dưới nền (detached mode)
docker-compose up -d --build
# --scale client-app=3 nghĩa là chạy 3 container cho service này
docker-compose up -d --scale client-app=3

# 2. [QUAN TRỌNG] Cài đặt numpy cho Spark Master và Worker
# Spark MLlib cần numpy để tính toán ma trận, ta dùng -u 0 (user root) để có quyền cài đặt
docker-compose exec -u 0 spark-master pip install numpy
docker-compose exec -u 0 spark-worker pip install numpy
```

## 2\. Cấu hình Kafka (Kafka Configuration)

Sau khi container chạy, cần tạo Topic để chứa dữ liệu từ Smart Meter gửi về.

```bash
# Tạo topic tên 'smart-meter-data'
# --partitions 1: Chia dữ liệu vào 1 phân vùng
# --replication-factor 1: Chỉ lưu 1 bản sao (do chạy local cluster nhỏ)
docker-compose exec kafka1 kafka-topics --create --topic smart-meter-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Tao topic plugin data de gui data tu plugin neu co
docker-compose exec kafka1 kafka-topics --create --topic plugins-data --bootstrap-server kafka1:29092 --partitions 1 --replication-factor 1

# (Tùy chọn) Kiểm tra xem topic đã được tạo thành công chưa
docker-compose exec kafka1 kafka-topics --list --bootstrap-server localhost:9092
```

## 3\. Vận hành luồng dữ liệu (Data Pipeline Execution)

Quy trình chuẩn: Tạo dữ liệu giả lập -\> Gửi vào Kafka -\> Spark đọc từ Kafka và ghi xuống HDFS.

### Bước 3.1: Chạy bộ sinh dữ liệu (Producer)

Giả lập các thiết bị IoT gửi số liệu về.

```bash
# Truy cập vào container client-app
docker-compose exec client-app bash

# --- BÊN TRONG CONTAINER ---
# Chạy script sinh dữ liệu (giữ terminal này chạy để đẩy data liên tục)
python src/generator.py
```

### Bước 3.2: Xử lý dữ liệu Streaming (Ingestion)

Dùng Spark để đọc dữ liệu từ Kafka, làm sạch và lưu trữ vào HDFS.

```bash
# Submit job ingest_data.py lên Spark Master
# --packages: Tải thư viện tích hợp Spark-Kafka (vì Spark mặc định không có sẵn)
# --conf: Cấu hình trỏ hệ thống file mặc định về HDFS namenode
docker-compose exec -u 0 spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:8020" \
  /app/src/ingest_data.py
```

## 4\. Machine Learning & Dự đoán (Analytics)

Sau khi đã có dữ liệu trong HDFS, tiến hành huấn luyện mô hình và chạy dự đoán thời gian thực.

### Bước 4.1: Huấn luyện mô hình (Model Training - Batch)

Job này đọc dữ liệu lịch sử từ HDFS để train model.

```bash
# Submit job train_model.py
docker-compose exec -u 0 spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/src/train_model.py
```

### Bước 4.2: Dự đoán luồng (Stream Prediction)

Sử dụng model đã train để dự đoán dữ liệu mới đang đổ về từ Kafka.

```bash
# Submit job stream_predict.py
# Cần nạp lại package kafka và cấu hình HDFS như bước Ingest
docker-compose exec -u 0 spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:8020" \
  /app/src/stream_predict.py
```

## 5\. Hiển thị Dashboard (Visualization)

Cài đặt thư viện và khởi chạy giao diện người dùng.

```bash
# 1. Cài đặt các thư viện cần thiết cho Dashboard (pyarrow để đọc parquet, hdfs để kết nối data lake)
docker-compose exec client-app pip install pyarrow pandas streamlit hdfs

# 2. Khởi chạy Streamlit Dashboard
# Sau lệnh này, truy cập browser theo port đã cấu hình (thường là http://localhost:8501)
docker-compose exec client-app streamlit run src/dashboard.py
```

## 6\. Công cụ Monitoring & Debugging

Sử dụng khi hệ thống gặp lỗi hoặc cần kiểm tra dòng chảy dữ liệu.

**Kiểm tra Kafka:**
Xem dữ liệu thô có thực sự đang vào Kafka không.

```bash
docker-compose exec kafka1 kafka-console-consumer --bootstrap-server localhost:9092 --topic smart-meter-data --from-beginning
```

**Kiểm tra sức khỏe HDFS:**
Đảm bảo NameNode và DataNode đang kết nối tốt.

```bash
# Kiểm tra phiên bản/trạng thái NameNode
docker exec namenode cat /hadoop/dfs/name/current/VERSION

# Kiểm tra DataNode (Nếu lỗi, có thể do xung đột ClusterID khi reset môi trường không sạch)
docker exec datanode cat /hadoop/dfs/data/current/VERSION
```

## 7\. Dọn dẹp hệ thống (Cleanup)

```bash
# Tắt toàn bộ container và xóa Volumes (-v)
# LƯU Ý: Lệnh này sẽ xóa sạch dữ liệu trong HDFS và Kafka
docker-compose down -v
```

docker-compose exec etcd etcdctl get /config

docker-compose exec etcd etcdctl put /config "$(cat config.json)"

docker system prune -a --volumes
