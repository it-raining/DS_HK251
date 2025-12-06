import time
import json
import os
import random
import datetime
import importlib
from kafka import KafkaProducer
import etcd3 
import argparse # Configurable via command-line args

# --- CẤU HÌNH KẾT NỐI ---
ETCD_HOST = 'etcd'
ETCD_PORT = 2379
KAFKA_BOOTSTRAP_SERVERS = ['kafka1:29092', 'kafka2:29092']
KAFKA_METRIC_TOPIC = 'smart-meter-data'
KAFKA_ALERT_TOPIC = 'plugins-data'

# Key chứa toàn bộ cấu hình JSON trên etcd
CONFIG_KEY = '/config'

# Cấu hình mặc định (Fallback)
DEFAULT_CONFIG = {
    "interval": 2,
    "enabled_metrics": [
        "voltage_v", "current_a", "active_power_kw", 
        "power_factor", "frequency_hz", "total_energy_kwh"
    ],
    "plugins": []
}

# --- THIẾT LẬP MÔ PHỎNG ---
DEVICE_ID = os.getenv('HOSTNAME', 'unknown-meter')
NUM_METERS = 1

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

# Biến toàn cục lưu trạng thái các công tơ
meter_states = {}

# --- HELPER FUNCTIONS ---
def on_send_success(record_metadata):
    """Callback khi gửi thành công"""
    print(f"✓ Sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

def on_send_error(excp):
    """Callback khi gửi thất bại"""
    print(f"✗ Failed to send message: {excp}")

def get_etcd_client():
    """Tạo kết nối an toàn tới etcd"""
    try:
        return etcd3.client(host=ETCD_HOST, port=ETCD_PORT)
    except Exception as e:
        print(f"Error creating etcd client: {e}")
        return None

def get_app_config(etcd_client):
    """Lấy config tổng từ etcd. Nếu không có hoặc lỗi, trả về mặc định."""
    if not etcd_client:
        return DEFAULT_CONFIG

    try:
        value, _ = etcd_client.get(CONFIG_KEY)
        if value:
            # Parse JSON từ etcd
            return json.loads(value.decode('utf-8'))
        else:
            # Nếu key chưa tồn tại, tạo mặc định lên etcd
            print(f"Config key '{CONFIG_KEY}' not found. Uploading defaults...")
            etcd_client.put(CONFIG_KEY, json.dumps(DEFAULT_CONFIG))
            return DEFAULT_CONFIG
    except Exception as e:
        print(f"Error reading config from etcd: {e}")
        return DEFAULT_CONFIG

def load_plugins_dynamic(plugin_names):
    """Import và khởi tạo danh sách plugin"""
    loaded_instances = []
    print(f"Loading plugins: {plugin_names}")
    
    for full_name in plugin_names:
        try:
            # Phân tách module và class (VD: plugins.guard.GuardPlugin)
            if '.' not in full_name:
                print(f"Invalid plugin format: {full_name}")
                continue
                
            module_name, class_name = full_name.rsplit('.', 1)
            
            # Import module
            mod = importlib.import_module(module_name)
            # Lấy class
            clazz = getattr(mod, class_name)
            
            # Khởi tạo instance
            instance = clazz()
            instance.initialize() # Gọi hàm initialize của plugin
            
            loaded_instances.append(instance)
            print(f"Plugin loaded: {class_name}")
            
        except ImportError:
            print(f"Plugin module not found: {full_name}")
        except AttributeError:
            print(f"Plugin class not found in module: {full_name}")
        except Exception as e:
            print(f"Error loading {full_name}: {e}")
            
    return loaded_instances

def init_meters(num_meters=NUM_METERS):
    """Khởi tạo trạng thái ban đầu cho NHIỀU công tơ ảo"""
    print(f"Initializing {num_meters} virtual meters on this node...")
    
    for i in range(num_meters):
        # Nếu chạy nhiều hơn 1 meter, thêm hậu tố _01, _02 để phân biệt
        suffix = f"_{i+1:02d}" if num_meters > 1 else ""
        meter_id = f"MT_{DEVICE_ID}{suffix}"
        
        c_type = random.choice(["Residential", "Commercial", "Industrial"])
        loc = random.choice(LOCATIONS)
        
        meter_states[meter_id] = {
            "type": c_type,
            "location": loc,
            "total_energy_kwh": random.uniform(1000.0, 50000.0)
        }
        print(f"  + Created: {meter_id} ({c_type})")

def generate_reading(meter_id, interval, enabled_metrics):
    """Sinh dữ liệu giả lập dựa trên profile và metrics được bật"""
    state = meter_states[meter_id]
    profile = PROFILES[state["type"]]
    
    # 1. Sinh số liệu ngẫu nhiên theo profile
    voltage = profile["voltage_base"] + random.uniform(-5.0, 5.0)
    current = random.uniform(*profile["current_range"])
    power_factor = random.uniform(*profile["p_factor_range"])
    frequency = random.uniform(49.8, 50.2)
    
    # 2. Tính toán công suất
    multiplier = 1.73 if state["type"] == "Industrial" else 1.0
    active_power_kw = (voltage * current * power_factor * multiplier) / 1000
    
    # 3. Tính điện năng tiêu thụ trong khoảng thời gian interval
    # P (kW) * t (hours) = kWh
    energy_consumed = active_power_kw * (interval / 3600.0)
    state["total_energy_kwh"] += energy_consumed

    # 4. Logic mô phỏng sự cố (Status)
    rand_status = random.random()
    if rand_status > 0.99:
        status = {"code": 1, "message": "Low Voltage", "battery": 95}
        voltage *= 0.8
    elif rand_status > 0.98:
        status = {"code": 2, "message": "Overload", "battery": 90}
        current *= 1.5
    else:
        status = {"code": 0, "message": "Normal", "battery": 100}

    # 5. Tập hợp tất cả metrics có thể có
    full_data = {
        "voltage_v": round(voltage, 1),
        "current_a": round(current, 2),
        "active_power_kw": round(active_power_kw, 3),
        "power_factor": round(power_factor, 2),
        "frequency_hz": round(frequency, 2),
        "total_energy_kwh": round(state["total_energy_kwh"], 2)
    }
    
    # 6. Lọc chỉ lấy những metric được config
    measurements = {k: v for k, v in full_data.items() if k in enabled_metrics}

    # 7. Đóng gói JSON cuối cùng
    record = {
        "meter_id": meter_id,
        "timestamp": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z",
        "location": {
            "city": state["location"]["city"],
            "district": state["location"]["district"],
            "gps": f"{state['location']['base_gps'][0]}, {state['location']['base_gps'][1]}"
        },
        "measurements": measurements,
        "status": {
            "code": status["code"],
            "message": status["message"],
            "battery_level": status["battery"]
        }
    }
    return record

# --- MAIN LOOP ---

def main():
    parser = argparse.ArgumentParser(description='Smart Meter Generator')
    parser.add_argument('--count', type=int, default=1, help='Number of virtual meters per container')
    args = parser.parse_args()
    
    NUM_METERS = args.count # Lấy số lượng từ lệnh chạy

    print(f"Starting Generator [{DEVICE_ID}] with {NUM_METERS} meters...")
    
    # 1. Kết nối Etcd
    etcd_client = get_etcd_client()
    
    # 2. Kết nối Kafka
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("Connected to Kafka")
        except Exception as e:
            print(f"Waiting for Kafka... ({e})")
            time.sleep(5)

    # 3. Khởi tạo công tơ
    init_meters(NUM_METERS)

    # Biến quản lý trạng thái Plugins (để tránh reload liên tục)
    active_plugins = []
    current_plugin_names = []

    try:
        while True:
            # --- BƯỚC 1: ĐỌC CẤU HÌNH ---
            config = get_app_config(etcd_client)
            
            # Lấy tham số (dùng default nếu config thiếu field)
            interval = config.get("interval", DEFAULT_CONFIG["interval"])
            enabled_metrics = config.get("enabled_metrics", DEFAULT_CONFIG["enabled_metrics"])
            new_plugin_names = config.get("plugins", [])

            # --- BƯỚC 2: QUẢN LÝ PLUGIN (RELOAD NẾU CẦN) ---
            # So sánh danh sách tên plugin (Set comparison để không quan tâm thứ tự)
            if set(new_plugin_names) != set(current_plugin_names):
                print(f"Config changed. Reloading plugins...")
                
                # Dọn dẹp plugin cũ
                for p in active_plugins:
                    if hasattr(p, 'finalize'):
                        try: p.finalize()
                        except: pass
                
                # Load plugin mới
                active_plugins = load_plugins_dynamic(new_plugin_names)
                current_plugin_names = new_plugin_names

            # --- BƯỚC 3: CHẠY PLUGIN ---
            for plugin in active_plugins:
                try:
                    # Giả sử plugin có hàm run() trả về data hoặc None
                    plugin_data = plugin.run()
                    if plugin_data:
                        # Gửi data từ plugin vào topic riêng
                        producer.send(KAFKA_ALERT_TOPIC, value=plugin_data)
                        print(f"Plugin Alert Sent: {plugin_data}")
                except Exception as e:
                    print(f"Plugin runtime error: {e}")

            # --- BƯỚC 4: SINH DỮ LIỆU CÔNG TƠ ---
            for meter_id in meter_states:
                record = generate_reading(meter_id, interval, enabled_metrics)
                
                # Gửi Kafka
                future = producer.send(KAFKA_METRIC_TOPIC, value=record)
                future.add_callback(on_send_success)
                future.add_errback(on_send_error)

                metrics_count = len(record['measurements'])
                print(f"[{meter_id}] Sent data ({metrics_count} metrics) | Status: {record['status']['message']}")

            # Gửi thực sự
            producer.flush()
            
            # Nghỉ theo interval cấu hình
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nStopping Generator...")
        # Dọn dẹp
        for p in active_plugins:
            if hasattr(p, 'finalize'): p.finalize()
        producer.close()

if __name__ == "__main__":
    main()