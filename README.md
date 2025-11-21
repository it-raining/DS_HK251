# DS_HK251: IoT Smart Meter Big Data Analytics

**Project:** Big data analytics for IoT-based electricity/water meters.
**Goal:** Power Plant Data Analysis and Prediction using a Lakehouse architecture.

## Architecture Overview
* **Ingestion:** Kafka (2 Brokers)
* **Storage:** HDFS (Data Lake)
* **Processing:** Apache Spark (Streaming & Batch)
* **Client:** Python Generator & Streamlit Dashboard

---

## 1. Infrastructure Setup
Start the Docker containers and install necessary dependencies.

```bash
# Build and start the cluster in detached mode
docker-compose up -d --build

# [CRITICAL] Install numpy for Spark Master and Worker (Required for MLlib)
docker-compose exec -u 0 spark-master pip install numpy
docker-compose exec -u 0 spark-worker pip install numpy
```

## 2\. Kafka Configuration

Once the containers are running, you need to initialize the Kafka topic for the smart meter data.

```bash
# Create topic for smart meter data (raw data)
docker-compose exec kafka1 kafka-topics --create --topic smart-meter-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# (Optional) Verify that the topic was created successfully
docker-compose exec kafka1 kafka-topics --list --bootstrap-server localhost:9092
```

## 3\. Data Generation (Producer)

Run the Python script to simulate IoT smart meters sending data to Kafka.

```bash
# Enter the client-app container
docker-compose exec client-app bash

# --- INSIDE THE CONTAINER ---
# Run the generator script:
python src/generator.py
```

*(Note: Keep this terminal open to keep generating data, or run it in the background).*

## 4\. Data Processing (Spark Streaming)

Submit the Spark job to consume data from Kafka, clean it, and write it to HDFS.

```bash
# Submit the ingest_data.py job to the Spark Master
docker-compose exec -u 0 spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:8020" /app/src/ingest_data.py
```

## 5\. Model Training (Spark Batch)

Once sufficient data has been ingested into HDFS, run the training job to generate the Machine Learning model.

```bash
# Submit the train_model.py job
docker-compose exec -u 0 spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/src/train_model.py
```

## 6\. Monitoring & Debugging

Tools to check if data is flowing correctly and services are healthy.

**Check Kafka Stream:**

```bash
# Check raw data arriving in Kafka
docker-compose exec kafka1 kafka-console-consumer --bootstrap-server localhost:9092 --topic smart-meter-data --from-beginning
```

**Check HDFS Status (DataNode Connectivity):**

```bash
# Check Namenode version/status
docker exec namenode cat /hadoop/dfs/name/current/VERSION

# Check Datanode version/status (If this fails, Datanode might have crashed due to ClusterID mismatch)
docker exec datanode cat /hadoop/dfs/data/current/VERSION
```

```
```