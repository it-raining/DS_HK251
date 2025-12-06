# --- CONFIGURATION ---
$PROJECT_NAME = "ds_hk251"

# --- HELPER FUNCTIONS ---
function Show-Header {
    Clear-Host
    Write-Host "======================================================" -ForegroundColor Cyan
    Write-Host "       SMART METER SYSTEM - CONTROL CENTER            " -ForegroundColor Cyan
    Write-Host "======================================================" -ForegroundColor Cyan
    Write-Host "Instructions: Open multiple terminals, select one option per terminal." -ForegroundColor Yellow
    Write-Host ""
}

# --- MAIN MENU ---
Show-Header
Write-Host "1. " -NoNewline; Write-Host "[Infrastructure] " -ForegroundColor Green -NoNewline; Write-Host "Start System (Docker Compose & Scale)"
Write-Host "2. " -NoNewline; Write-Host "[Generator] " -ForegroundColor Green -NoNewline; Write-Host "Activate Generator ON EACH CONTAINER"
Write-Host "3. " -NoNewline; Write-Host "[Spark Job] " -ForegroundColor Green -NoNewline; Write-Host "Submit Spark Streaming (Data Processing)"
Write-Host "4. " -NoNewline; Write-Host "[Dashboard] " -ForegroundColor Green -NoNewline; Write-Host "Launch Streamlit Dashboard"
Write-Host "5. " -NoNewline; Write-Host "[Train Model] " -ForegroundColor Red -NoNewline; Write-Host "Train Prediction Model (Runs on Spark Master)"
Write-Host "6. " -NoNewline; Write-Host "[Predict] " -ForegroundColor Red -NoNewline; Write-Host "Predict Power Consumption (Runs on Spark Master)"
Write-Host "7. " -NoNewline; Write-Host "[Stop All] " -ForegroundColor Red -NoNewline; Write-Host "Stop and Remove Entire System"
Write-Host "8. " -NoNewline; Write-Host "[Generator] " -ForegroundColor Yellow -NoNewline; Write-Host "Activate LARGE SCALE Generator (4 Meters/Node)"
Write-Host "0. Exit"
Write-Host ""

$option = Read-Host "Select option (0-7)"

switch ($option) {
    "1" {
            Write-Host "--- Starting Infrastructure ---" -ForegroundColor Cyan
            Write-Host "Command: docker-compose up -d --build --scale client-app=3"
            docker-compose up -d --scale client-app=3

            Write-Host "--- Waiting for Kafka to be ready... ---" -ForegroundColor Yellow
            
            # Vòng lặp kiểm tra trạng thái Kafka (tối đa 60 giây)
            $maxRetries = 12
            $retryCount = 0
            $kafkaReady = $false

            while (-not $kafkaReady -and $retryCount -lt $maxRetries) {
                try {
                    # Thử liệt kê topic để kiểm tra kết nối (chuyển hướng lỗi vào null để không in ra màn hình)
                    $null = docker-compose exec kafka1 kafka-topics --list --bootstrap-server localhost:9092 2>&1
                    if ($LASTEXITCODE -eq 0) {
                        $kafkaReady = $true
                        Write-Host "Kafka is UP!" -ForegroundColor Green
                    } else {
                        throw "Kafka not ready"
                    }
                } catch {
                    Write-Host "Kafka not ready yet, waiting 5s... ($($retryCount+1)/$maxRetries)" -ForegroundColor DarkGray
                    Start-Sleep -Seconds 5
                    $retryCount++
                }
            }

            if ($kafkaReady) {
                Write-Host "--- Creating Kafka Topics ---" -ForegroundColor Cyan
                docker-compose exec kafka1 kafka-topics --create --topic smart-meter-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>$null
                docker-compose exec kafka1 kafka-topics --create --topic plugins-data --bootstrap-server kafka1:29092 --partitions 1 --replication-factor 1 2>$null

                Write-Host "Current Kafka Topics:" -ForegroundColor Yellow
                docker-compose exec kafka1 kafka-topics --list --bootstrap-server localhost:9092
            } else {
                Write-Host "Error: Kafka failed to start within timeout." -ForegroundColor Red
            }
            
            Write-Host "--- Setting up Environment ---" -ForegroundColor Cyan
            docker-compose exec -u 0 spark-master pip install numpy
            docker-compose exec -u 0 spark-worker pip install numpy
        }
    "2" {
        Write-Host "--- Activating Generators on Containers ---" -ForegroundColor Cyan
        
        # Get container IDs for 'client-app' service
        $containers = docker-compose ps -q client-app

        if (-not $containers) {
            Write-Host "Error: No containers running. Run step 1 first!" -ForegroundColor Red
        } else {
            foreach ($container in $containers) {
                $rawName = docker inspect --format="{{.Name}}" $container
                $name = $rawName.TrimStart('/')
                
                Write-Host "Starting generator on: $name" -ForegroundColor Yellow
                
                # Execute python script in background inside container
                docker exec -d $container sh -c "python3 -u src/generator.py > /proc/1/fd/1 2>&1"
            }
            Write-Host " Generators activated on all nodes." -ForegroundColor Green
            Write-Host "Tip: Use 'docker-compose logs -f client-app' to view data stream." -ForegroundColor Cyan
        }
    }
    "3" {
        Write-Host "--- Submitting Spark Streaming Job (Ingestion) ---" -ForegroundColor Cyan
        docker-compose exec -u 0 spark-master /opt/spark/bin/spark-submit `
          --master spark://spark-master:7077 `
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
          --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:8020" `
          /app/src/ingest_data.py
    }
    "4" {
        Write-Host "--- Launching Dashboard ---" -ForegroundColor Cyan
        Write-Host "Rnd URL: http://localhost:8501 (or 8502/8503)" -ForegroundColor Green
        
        # Check if specific container is running to attach dashboard to it
        $isRunning = docker ps | Select-String "${PROJECT_NAME}-client-app-1" -Quiet
        # Write-Host $isRunning
        
        if ($isRunning) {
            docker-compose exec client-app streamlit run src/dashboard.py
        } else {
            # Fallback to index 1 if named container not found
            docker-compose exec --index=1 client-app streamlit run src/dashboard.py
        }
    }
    "5" {
        Write-Host "--- Training Prediction Model ---" -ForegroundColor Cyan
        docker-compose exec -u 0 spark-master /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        /app/src/train_model.py
    }
    "6" {
        Write-Host "--- Running Real-time Prediction ---" -ForegroundColor Cyan
        docker-compose exec -u 0 spark-master /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
        --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:8020" `
        /app/src/stream_predict.py
    }
    "7" {
        Write-Host "--- Stopping System ---" -ForegroundColor Red
        docker-compose down -v
        Write-Host "Success. System stopped." -ForegroundColor Green
    }
    "8" {
        Write-Host "--- Activating LARGE SCALE Generators ---" -ForegroundColor Yellow
        $containers = docker-compose ps -q client-app
        
        if (-not $containers) {
            Write-Host "Error: No containers running. Run step 1 first!" -ForegroundColor Red
        } else {
            foreach ($container in $containers) {
                $rawName = docker inspect --format="{{.Name}}" $container
                $name = $rawName.TrimStart('/')
                Write-Host "Injecting 4 virtual meters into: $name" -ForegroundColor Yellow
                
                # Truyền tham số --count 4
                docker exec -d $container sh -c "python3 -u src/generator.py --count 4 > /proc/1/fd/1 2>&1"
            }
            Write-Host "DONE! Total system capacity: $(($containers.Count) * 4) Meters." -ForegroundColor Green
        }
    }
    "0" {
        Write-Host "Good luck."
        exit
    }
    Default {
        Write-Host "Invalid option." -ForegroundColor Red
    }
}