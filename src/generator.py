import time
import json
import random
import datetime
from kafka import KafkaProducer

# --- CẤU HÌNH ---
KAFKA_BOOTSTRAP_SERVERS = ['kafka1:29092', 'kafka2:29092'] # Địa chỉ trong mạng Docker
KAFKA_TOPIC = 'smart-meter-data'
NUM_METERS = 10  # Số lượng công tơ giả lập
SLEEP_TIME = 2   # Thời gian nghỉ giữa các lần gửi (giây)

# Định nghĩa các hồ sơ tiêu thụ điện (để dữ liệu trông thật hơn)
PROFILES = {
    "Residential": {"voltage_base": 220, "current_range": (0.5, 15.0), "p_factor_range": (0.85, 0.99)},
    "Commercial":  {"voltage_base": 220, "current_range": (10.0, 50.0), "p_factor_range": (0.80, 0.95)},
    "Industrial":  {"voltage_base": 380, "current_range": (50.0, 200.0), "p_factor_range": (0.85, 0.98)}
}

LOCATIONS = [
    {"city": "Ho Chi Minh City", "district": "Quan 1", "base_gps": (10.7769, 106.7009)},
    {"city": "Ho Chi Minh City", "district": "Quan 3", "base_gps": (10.7843, 106.6844)},
    {"city": "Ho Chi Minh City", "district": "Thu Duc", "base_gps": (10.8499, 106.7519)},
]

# --- KHỞI TẠO TRẠNG THÁI CÔNG TƠ ---
# Lưu trữ trạng thái tích lũy (kwh) để nó tăng dần theo thời gian
meter_states = {}

def init_meters():
    for i in range(1, NUM_METERS + 1):
        meter_id = f"MT_{1000 + i}"
        c_type = random.choice(["Residential", "Commercial", "Industrial"])
        loc = random.choice(LOCATIONS)
        
        meter_states[meter_id] = {
            "type": c_type,
            "location": loc,
            "total_energy_kwh": random.uniform(1000.0, 50000.0), # Số điện ban đầu ngẫu nhiên
            "status_code": 0
        }
    print(f"Đã khởi tạo {NUM_METERS} công tơ ảo.")

# --- HÀM SINH DỮ LIỆU ---
def generate_reading(meter_id):
    state = meter_states[meter_id]
    profile = PROFILES[state["type"]]
    
    # 1. Sinh số liệu điện áp & dòng điện (có nhiễu ngẫu nhiên)
    voltage = profile["voltage_base"] + random.uniform(-5.0, 5.0) # Dao động +/- 5V
    current = random.uniform(*profile["current_range"])
    power_factor = random.uniform(*profile["p_factor_range"])
    frequency = random.uniform(49.8, 50.2)
    
    # 2. Tính công suất (kW) = (U * I * CosPhi) / 1000
    # Nếu là công nghiệp 3 pha thì nhân thêm căn 3 (~1.73), ở đây làm đơn giản
    multiplier = 1.73 if state["type"] == "Industrial" else 1.0
    active_power_kw = (voltage * current * power_factor * multiplier) / 1000
    
    # 3. Tính điện năng tích lũy (Energy kWh)
    # Giả sử hàm này chạy mỗi SLEEP_TIME giây -> cộng thêm lượng điện tiêu thụ trong thời gian đó
    # kWh = kW * (giây / 3600)
    energy_consumed = active_power_kw * (SLEEP_TIME / 3600.0)
    state["total_energy_kwh"] += energy_consumed

    # 4. Logic Status (98% là bình thường, 2% lỗi)
    rand_status = random.random()
    if rand_status > 0.99:
        status_code = 1
        status_msg = "Low Volt"
        voltage = voltage * 0.8 # Giả lập sụt áp
    elif rand_status > 0.98:
        status_code = 2
        status_msg = "Overload"
        current = current * 1.5 # Giả lập quá dòng
    else:
        status_code = 0
        status_msg = "Normal"

    # 5. Format JSON theo mẫu yêu cầu
    data = {
        "meter_id": meter_id,
        "timestamp": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z",
        "location": {
            "city": state["location"]["city"],
            "district": state["location"]["district"],
            "gps": f"{state['location']['base_gps'][0]}, {state['location']['base_gps'][1]}"
        },
        "measurements": {
            "voltage_v": round(voltage, 1),
            "current_a": round(current, 2),
            "active_power_kw": round(active_power_kw, 3),
            "power_factor": round(power_factor, 2),
            "frequency_hz": round(frequency, 2),
            "total_energy_kwh": round(state["total_energy_kwh"], 2)
        },
        "status": {
            "code": status_code,
            "message": status_msg,
            "battery_level": random.randint(80, 100)
        }
    }
    return data

# --- HÀM MAIN ---
def main():
    # 1. Kết nối Kafka
    print("Đang kết nối tới Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        print("Đã kết nối Kafka thành công!")
    except Exception as e:
        print(f"Lỗi kết nối Kafka: {e}")
        return

    # 2. Khởi tạo công tơ
    init_meters()

    # 3. Vòng lặp vô tận để gửi dữ liệu
    try:
        while True:
            for meter_id in meter_states:
                record = generate_reading(meter_id)
                
                # Gửi vào topic
                future = producer.send(KAFKA_TOPIC, value=record)

                # Callback khi gửi thành công
                def on_send_success(metadata, r=record, m=meter_id):
                    print(f"Sent {m} | P={r['measurements']['active_power_kw']}kW | Status={r['status']['message']}")
                    print(f"Topic: {metadata.topic}, Partition: {metadata.partition}, Offset: {metadata.offset}")
                
                # Callback khi gửi thất bại
                def on_send_error(exc, m=meter_id):
                    print(f"Failed to send {m}: {exc}")
                
                future.add_callback(on_send_success)
                future.add_errback(on_send_error)
                
                # In ra màn hình để debug (chỉ in 1 dòng cho gọn)
            
            producer.flush()
            print(f"--- Batch sent. Sleeping {SLEEP_TIME}s ---")
            time.sleep(SLEEP_TIME)
            
    except KeyboardInterrupt:
        print("Dừng generator.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()