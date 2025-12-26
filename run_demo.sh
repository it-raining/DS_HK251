#!/bin/bash

# --- MÀU SẮC ---
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
CYAN='\033[0;36m'
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

# --- HÀM KIỂM TRA GENERATOR STATUS ---
function check_generator_status() {
    echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║           TRẠNG THÁI GENERATOR                         ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"
    
    CONTAINERS=$(docker compose ps -q client-app)
    
    if [ -z "$CONTAINERS" ]; then
        echo -e "${RED}❌ Không có container nào đang chạy${NC}"
        return
    fi
    
    INDEX=1
    for container in $CONTAINERS; do
        NAME=$(docker inspect --format="{{.Name}}" $container | cut -c 2-)
        
        # Kiểm tra process generator.py
        RUNNING=$(docker exec $container pgrep -f "generator.py" 2>/dev/null)
        
        if [ -n "$RUNNING" ]; then
            echo -e "${GREEN}✓${NC} Container $INDEX ($NAME): ${GREEN}ĐANG CHẠY${NC} (PID: $RUNNING)"
        else
            echo -e "${RED}✗${NC} Container $INDEX ($NAME): ${RED}KHÔNG HOẠT ĐỘNG${NC}"
        fi
        
        INDEX=$((INDEX + 1))
    done
    echo ""
}

# --- HÀM STOP GENERATOR ---
function stop_generator() {
    local CONTAINER_ID=$1
    local NAME=$(docker inspect --format="{{.Name}}" $CONTAINER_ID | cut -c 2-)
    
    # Kill tất cả process generator.py
    docker exec $CONTAINER_ID pkill -f "generator.py" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓${NC} Đã dừng generator trên $NAME"
    else
        echo -e "${YELLOW}⚠${NC} Không tìm thấy generator đang chạy trên $NAME"
    fi
}

# --- HÀM START GENERATOR ---
function start_generator() {
    local CONTAINER_ID=$1
    local CONTAINER_INDEX=$2
    local TOTAL_CONTAINERS=$3
    local MODE=$4  # "normal" hoặc "large-scale"
    
    local NAME=$(docker inspect --format="{{.Name}}" $CONTAINER_ID | cut -c 2-)
    
    # Kiểm tra nếu đã chạy rồi
    RUNNING=$(docker exec $CONTAINER_ID pgrep -f "generator.py" 2>/dev/null)
    if [ -n "$RUNNING" ]; then
        echo -e "${YELLOW}⚠${NC} Generator trên $NAME đã chạy rồi (PID: $RUNNING)"
        return
    fi
    
    # Start generator
    docker exec -d $CONTAINER_ID sh -c "export CONTAINER_INDEX=$CONTAINER_INDEX && python3 -u src/generator.py --total-containers $TOTAL_CONTAINERS --mode $MODE > /proc/1/fd/1 2>&1"
    
    sleep 1
    
    # Verify
    RUNNING=$(docker exec $CONTAINER_ID pgrep -f "generator.py" 2>/dev/null)
    if [ -n "$RUNNING" ]; then
        echo -e "${GREEN}✓${NC} Đã khởi động generator trên $NAME (PID: $RUNNING)"
    else
        echo -e "${RED}✗${NC} Lỗi: Không thể khởi động generator trên $NAME"
    fi
}

# --- MENU QUẢN LÝ GENERATOR ---
function generator_management_menu() {
    while true; do
        clear
        echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
        echo -e "${CYAN}║           QUẢN LÝ GENERATOR                            ║${NC}"
        echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"
        echo ""
        
        check_generator_status
        
        echo -e "1. ${GREEN}Bật TẤT CẢ Generators (Normal Mode)${NC}"
        echo -e "2. ${GREEN}Bật TẤT CẢ Generators (Large Scale - 4 meters/container)${NC}"
        echo -e "3. ${YELLOW}Bật/Tắt TỪNG Generator${NC}"
        echo -e "4. ${RED}Tắt TẤT CẢ Generators${NC}"
        echo -e "5. ${BLUE}Xem Logs Realtime${NC}"
        echo -e "6. ${BLUE}Refresh Status${NC}"
        echo -e "0. Quay lại Menu Chính"
        echo ""
        read -p "Chọn chức năng (0-6): " gen_option
        
        case $gen_option in
            1)
                echo -e "${BLUE}--- Đang bật TẤT CẢ Generators (Normal Mode) ---${NC}"
                CONTAINERS=$(docker compose ps -q client-app)
                TOTAL=$(echo "$CONTAINERS" | wc -l)
                INDEX=1
                
                for container in $CONTAINERS; do
                    start_generator "$container" "$INDEX" "$TOTAL" "normal"
                    INDEX=$((INDEX + 1))
                done
                
                echo -e "${GREEN}✓ Hoàn tất!${NC}"
                read -p "Nhấn Enter để tiếp tục..."
                ;;
            2)
                echo -e "${BLUE}--- Đang bật TẤT CẢ Generators (Large Scale) ---${NC}"
                CONTAINERS=$(docker compose ps -q client-app)
                TOTAL=$(echo "$CONTAINERS" | wc -l)
                INDEX=1
                
                for container in $CONTAINERS; do
                    start_generator "$container" "$INDEX" "$TOTAL" "large-scale"
                    INDEX=$((INDEX + 1))
                done
                
                echo -e "${GREEN}✓ Hoàn tất!${NC}"
                read -p "Nhấn Enter để tiếp tục..."
                ;;
            3)
                echo -e "${BLUE}--- Quản lý TỪNG Container ---${NC}"
                CONTAINERS=$(docker compose ps -q client-app)
                TOTAL=$(echo "$CONTAINERS" | wc -l)
                
                # Hiển thị danh sách
                INDEX=1
                for container in $CONTAINERS; do
                    NAME=$(docker inspect --format="{{.Name}}" $container | cut -c 2-)
                    RUNNING=$(docker exec $container pgrep -f "generator.py" 2>/dev/null)
                    
                    if [ -n "$RUNNING" ]; then
                        echo -e "$INDEX. $NAME ${GREEN}[ĐANG CHẠY]${NC}"
                    else
                        echo -e "$INDEX. $NAME ${RED}[KHÔNG HOẠT ĐỘNG]${NC}"
                    fi
                    INDEX=$((INDEX + 1))
                done
                
                echo ""
                read -p "Chọn container (1-$TOTAL) hoặc 0 để hủy: " choice
                
                if [ "$choice" -eq 0 ]; then
                    continue
                fi
                
                if [ "$choice" -ge 1 ] && [ "$choice" -le "$TOTAL" ]; then
                    # Lấy container tương ứng
                    TARGET=$(echo "$CONTAINERS" | sed -n "${choice}p")
                    NAME=$(docker inspect --format="{{.Name}}" $TARGET | cut -c 2-)
                    RUNNING=$(docker exec $TARGET pgrep -f "generator.py" 2>/dev/null)
                    
                    if [ -n "$RUNNING" ]; then
                        read -p "Generator đang chạy. Bạn muốn [S]top hay [R]estart? (s/r): " action
                        case $action in
                            s|S)
                                stop_generator "$TARGET"
                                ;;
                            r|R)
                                stop_generator "$TARGET"
                                sleep 1
                                read -p "Chọn mode: [1] Normal, [2] Large Scale: " mode_choice
                                if [ "$mode_choice" -eq 2 ]; then
                                    start_generator "$TARGET" "$choice" "$TOTAL" "large-scale"
                                else
                                    start_generator "$TARGET" "$choice" "$TOTAL" "normal"
                                fi
                                ;;
                        esac
                    else
                        read -p "Chọn mode: [1] Normal, [2] Large Scale: " mode_choice
                        if [ "$mode_choice" -eq 2 ]; then
                            start_generator "$TARGET" "$choice" "$TOTAL" "large-scale"
                        else
                            start_generator "$TARGET" "$choice" "$TOTAL" "normal"
                        fi
                    fi
                fi
                
                read -p "Nhấn Enter để tiếp tục..."
                ;;
            4)
                echo -e "${RED}--- Đang tắt TẤT CẢ Generators ---${NC}"
                CONTAINERS=$(docker compose ps -q client-app)
                
                for container in $CONTAINERS; do
                    stop_generator "$container"
                done
                
                echo -e "${GREEN}✓ Đã tắt tất cả!${NC}"
                read -p "Nhấn Enter để tiếp tục..."
                ;;
            5)
                echo -e "${BLUE}--- Xem Logs Realtime (Ctrl+C để thoát) ---${NC}"
                docker compose logs -f client-app
                ;;
            6)
                # Refresh (loop lại menu)
                continue
                ;;
            0)
                return
                ;;
            *)
                echo -e "${RED}Lựa chọn không hợp lệ.${NC}"
                read -p "Nhấn Enter để tiếp tục..."
                ;;
        esac
    done
}

# --- MENU CHÍNH ---
while true; do
    show_header
    echo -e "1. ${GREEN}[Infrastructure]${NC} Khởi động hệ thống (Docker Compose & Scale)"
    echo -e "2. ${CYAN}[Generator Manager]${NC} Quản lý Generator (Bật/Tắt/Status)"
    echo -e "3. ${GREEN}[Spark Job]${NC}      Submit Spark Streaming (Xử lý dữ liệu)"
    echo -e "4. ${GREEN}[Dashboard]${NC}      Bật giao diện Streamlit"
    echo -e "5. ${RED}[Train Model]${NC}      Huấn luyện mô hình dự báo (Chạy trên Spark Master)"
    echo -e "6. ${RED}[Predict]${NC}           Dự báo tiêu thụ điện (Chạy trên Spark Master)"
    echo -e "7. ${RED}[Stop All]${NC}       Dừng và xóa toàn bộ hệ thống"
    echo -e "0. Thoát"
    echo ""
    read -p "Chọn chức năng (0-7): " option
    
    case $option in
        1)
            echo -e "${BLUE}--- Đang khởi động Infrastructure ---${NC}"
            
            # Kill processes chiếm port
            sudo lsof -t -i:8080 -i:7077 -i:9092 -i:9093 -i:9870 -i:8020 -i:2379 -i:8081 -i:8082 -i:8083 -i:8084 -i:8085 | xargs sudo kill -9 2>/dev/null
            
            # Cleanup Zookeeper state
            echo -e "${YELLOW}Dọn dẹp Zookeeper state cũ...${NC}"
            docker compose down
            docker volume rm ds_hk251_zookeeper_data ds_hk251_kafka1_data ds_hk251_kafka2_data 2>/dev/null || true
            
            # Start
            docker compose up -d --scale client-app=5
            
            # Wait for Zookeeper
            echo -e "${BLUE}--- Chờ Zookeeper khởi động ---${NC}"
            MAX_WAIT=30
            ELAPSED=0
            while [ $ELAPSED -lt $MAX_WAIT ]; do
                if docker compose exec zookeeper nc -z localhost 2181 &>/dev/null; then
                    echo -e "${GREEN}✓ Zookeeper sẵn sàng!${NC}"
                    break
                fi
                echo -e "${YELLOW}Đang chờ... ($ELAPSED/$MAX_WAIT giây)${NC}"
                sleep 2
                ELAPSED=$((ELAPSED + 2))
            done
            
            # Wait for Kafka
            echo -e "${BLUE}--- Chờ Kafka khởi động ---${NC}"
            MAX_WAIT=60
            ELAPSED=0
            while [ $ELAPSED -lt $MAX_WAIT ]; do
                if docker compose exec kafka1 kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
                    if docker compose exec kafka2 kafka-broker-api-versions --bootstrap-server localhost:9093 &>/dev/null; then
                        echo -e "${GREEN}✓ Cả 2 Kafka brokers sẵn sàng!${NC}"
                        break
                    fi
                fi
                
                # Check zombie broker
                if docker compose logs kafka1 --tail=5 | grep -q "NodeExistsException"; then
                    echo -e "${RED}⚠ Phát hiện zombie broker! Đang cleanup...${NC}"
                    docker compose restart zookeeper
                    sleep 5
                    docker compose restart kafka1 kafka2
                    ELAPSED=0
                fi
                
                echo -e "${YELLOW}Đang chờ Kafka... ($ELAPSED/$MAX_WAIT giây)${NC}"
                sleep 3
                ELAPSED=$((ELAPSED + 3))
            done

            echo -e "${BLUE}--- Cài đặt thư viện cho Spark Cluster ---${NC}"
            docker compose exec -u 0 spark-master pip install numpy
            
            for i in {1..4}; do
                echo -e "Installing numpy on ${GREEN}spark-worker-$i${NC}..."
                docker compose exec -u 0 spark-worker-$i pip install numpy
            done

            echo -e "${BLUE}--- Tạo Kafka Topics ---${NC}"
            docker compose exec kafka1 kafka-topics --create --topic smart-meter-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
            docker compose exec kafka1 kafka-topics --create --topic plugins-data --bootstrap-server kafka1:29092 --partitions 1 --replication-factor 1 --if-not-exists

            echo -e "${GREEN}✓ Topics đã tạo:${NC}"
            docker compose exec kafka1 kafka-topics --list --bootstrap-server localhost:9092
            
            echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
            echo -e "${GREEN}║   ✓ HỆ THỐNG ĐÃ SẴN SÀNG              ║${NC}"
            echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
            read -p "Nhấn Enter để tiếp tục..."
            ;;
        2)
            generator_management_menu
            ;;
        3)
            echo -e "${BLUE}--- Đang Submit Spark Streaming Job ---${NC}"
            docker compose exec -u 0 spark-master /opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 \
              --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
              --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:8020" \
              /app/src/ingest_data.py
            read -p "Nhấn Enter để tiếp tục..."
            ;;
        4)
            echo -e "${BLUE}--- Đang bật Dashboard ---${NC}"
            PUBLIC_IP=$(curl -s ifconfig.me || echo "localhost")
            echo -e "Truy cập: ${GREEN}http://${PUBLIC_IP}:8501${NC} (Hoặc các port 8502-8505)"
            
            FIRST_CONTAINER=$(docker compose ps -q client-app | head -n 1)
            if [ -n "$FIRST_CONTAINER" ]; then
                 docker exec $FIRST_CONTAINER streamlit run src/dashboard.py
            else
                 echo -e "${RED}Không tìm thấy client-app nào đang chạy.${NC}"
            fi
            ;;
        5)
            echo -e "${BLUE}--- Đang huấn luyện mô hình dự báo ---${NC}"
            docker compose exec -u 0 spark-master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            /app/src/train_model.py
            read -p "Nhấn Enter để tiếp tục..."
            ;;
        6)
            docker compose exec -u 0 spark-master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:8020" \
            /app/src/stream_predict.py
            read -p "Nhấn Enter để tiếp tục..."
            ;;
        7)
            echo -e "${RED}--- Đang dừng hệ thống ---${NC}"
            docker compose down
            echo -e "${GREEN}Thành công${NC}"
            read -p "Nhấn Enter để tiếp tục..."
            ;;
        0)
            echo "Good luck!"
            exit 0
            ;;
        *)
            echo -e "${RED}Lựa chọn không hợp lệ.${NC}"
            read -p "Nhấn Enter để tiếp tục..."
            ;;
    esac
done