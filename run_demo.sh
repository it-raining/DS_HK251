#!/bin/bash

# --- MÀU SẮC ---
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# --- CẤU HÌNH ---
PROJECT_NAME="ds_hk251" 

# --- HÀM HỖ TRỢ ---
function show_header() {
    clear
    echo -e "${BLUE}======================================================${NC}"
    echo -e "${BLUE}       HỆ THỐNG SMART METER - CONTROL CENTER          ${NC}"
    echo -e "${BLUE}======================================================${NC}"
    echo -e "${YELLOW}Hướng dẫn: Mở nhiều Terminal, mỗi Terminal chọn 1 mục.${NC}"
    echo ""
}

# --- MENU CHÍNH ---
show_header
echo -e "1. ${GREEN}[Infrastructure]${NC} Khởi động hệ thống (Docker Compose & Scale)"
echo -e "2. ${GREEN}[Generator]${NC}      Kích hoạt Generator TRÊN TỪNG CONTAINER"
echo -e "3. ${GREEN}[Spark Job]${NC}      Submit Spark Streaming (Xử lý dữ liệu)"
echo -e "4. ${GREEN}[Dashboard]${NC}      Bật giao diện Streamlit"
echo -e "5. ${RED}[Train Model]${NC}      Huấn luyện mô hình dự báo (Chạy trên Spark Master)"
echo -e "6. ${RED}[Predict]${NC}           Dự báo tiêu thụ điện (Chạy trên Spark Master)"
echo -e "7. ${RED}[Large scale generator]${NC}     4 Cong to dien tren 1 container"
echo -e "8. ${RED}[Stop All]${NC}       Dừng và xóa toàn bộ hệ thống"
echo -e "0. Thoát"
echo ""
read -p "Chọn chức năng (0-8): " option
case $option in
    1)
        echo -e "${BLUE}--- Đang khởi động Infrastructure ---${NC}"
        echo -e "Lệnh: docker-compose up -d --scale client-app=5"
        sudo lsof -t -i:8080 -i:7077 -i:9092 -i:9870 -i:8020 -i:2379 -i:8081 | xargs sudo kill -9
        docker-compose up -d --scale client-app=5
        
        echo -e "${BLUE}--- Đang theo dõi log Kafka (Ctrl+C để thoát log) ---${NC}"
        docker-compose exec -u 0 spark-master pip install numpy
        docker-compose exec -u 0 spark-worker pip install numpy

        docker-compose exec kafka1 kafka-topics --create --topic smart-meter-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
        docker-compose exec kafka1 kafka-topics --create --topic plugins-data --bootstrap-server kafka1:29092 --partitions 1 --replication-factor 1

        docker-compose exec kafka1 kafka-topics --list --bootstrap-server localhost:9092
        ;;
    2)
        echo -e "${BLUE}--- Đang kích hoạt Generator trên các Container ---${NC}"
        
        # Lấy danh sách ID của tất cả container thuộc service 'client-app'
        CONTAINERS=$(docker-compose ps -q client-app)

        if [ -z "$CONTAINERS" ]; then
            echo -e "${RED}Lỗi: Chưa có container nào chạy. Hãy chạy bước 1 trước!${NC}"
        else
            # Đếm số container và gán index
            CONTAINER_INDEX=1
            TOTAL_CONTAINERS=$(echo "$CONTAINERS" | wc -l)
            
            echo -e "${GREEN}Tìm thấy $TOTAL_CONTAINERS containers${NC}"
            
            for container in $CONTAINERS; do
                # Lấy tên container để hiển thị cho đẹp
                NAME=$(docker inspect --format="{{.Name}}" $container | cut -c 2-)
                
                echo -e "${YELLOW}Kích hoạt generator trên: $NAME (Index: $CONTAINER_INDEX/$TOTAL_CONTAINERS)${NC}"
                
                # Exec vào container với CONTAINER_INDEX đúng
                docker exec -d $container sh -c "export CONTAINER_INDEX=$CONTAINER_INDEX && python3 -u src/generator.py --total-containers $TOTAL_CONTAINERS --mode normal > /proc/1/fd/1 2>&1"
                
                # Tăng index cho container tiếp theo
                CONTAINER_INDEX=$((CONTAINER_INDEX + 1))
            done
            
            echo -e "${GREEN}✓ Đã kích hoạt xong trên tất cả $TOTAL_CONTAINERS containers.${NC}"
            echo -e "${BLUE}Tip: Dùng lệnh 'docker-compose logs -f client-app' để xem chúng gửi tin.${NC}"
        fi
        ;;
    3)
        echo -e "${BLUE}--- Đang Submit Spark Streaming Job ---${NC}"
        docker-compose exec -u 0 spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
          --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:8020" \
          /app/src/ingest_data.py
        ;;
    4)
        echo -e "${BLUE}--- Đang bật Dashboard ---${NC}"
        echo -e "Truy cập: ${GREEN}http://localhost:8502${NC}"
        
        if docker ps | grep -q "${PROJECT_NAME}-client-app-1"; then
             docker-compose exec client-app streamlit run src/dashboard.py
        else
             docker-compose exec --index=1 client-app streamlit run src/dashboard.py
        fi
        ;;
    5)
        echo -e "${BLUE}--- Đang huấn luyện mô hình dự báo ---${NC}"
        docker-compose exec -u 0 spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /app/src/train_model.py
        ;;
    6)
        docker-compose exec -u 0 spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:8020" \
        /app/src/stream_predict.py
        ;;
    7)
        echo -e "${BLUE}--- Kích hoạt Large Scale Generator trên các Container ---${NC}"
        
        # Lấy danh sách ID của tất cả container thuộc service 'client-app'
        CONTAINERS=$(docker-compose ps -q client-app)

        if [ -z "$CONTAINERS" ]; then
            echo -e "${RED}Lỗi: Chưa có container nào chạy. Hãy chạy bước 1 trước!${NC}"
        else
            # Đếm số container và gán index
            CONTAINER_INDEX=1
            TOTAL_CONTAINERS=$(echo "$CONTAINERS" | wc -l)
            
            echo -e "${GREEN}Tìm thấy $TOTAL_CONTAINERS containers${NC}"
            
            for container in $CONTAINERS; do
                # Lấy tên container để hiển thị cho đẹp
                NAME=$(docker inspect --format="{{.Name}}" $container | cut -c 2-)
                
                echo -e "${YELLOW}Kích hoạt large scale generator trên: $NAME (Index: $CONTAINER_INDEX/$TOTAL_CONTAINERS)${NC}"
                
                # Exec vào container với mode large-scale
                docker exec -d $container sh -c "export CONTAINER_INDEX=$CONTAINER_INDEX && python3 -u src/generator.py --total-containers $TOTAL_CONTAINERS --mode large-scale > /proc/1/fd/1 2>&1"
                
                # Tăng index cho container tiếp theo
                CONTAINER_INDEX=$((CONTAINER_INDEX + 1))
            done
            
            echo -e "${GREEN}✓ Đã kích hoạt xong large scale generator trên tất cả $TOTAL_CONTAINERS containers.${NC}"
            echo -e "${BLUE}Tip: Dùng lệnh 'docker-compose logs -f client-app' để xem chúng gửi tin.${NC}"
        fi
        ;;
    8)
        echo -e "${RED}--- Đang dừng hệ thống ---${NC}"
        docker-compose down
        echo -e "${GREEN}Thành công${NC}"
        ;;
    0)
        echo "Goodd luckk =))."
        exit 0
        ;;
    *)
        echo -e "${RED}Lựa chọn không hợp lệ.${NC}"
        ;;
esac