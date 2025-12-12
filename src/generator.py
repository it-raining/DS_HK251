import time
import json
import os
import random
import datetime
import importlib
import pandas as pd
from kafka import KafkaProducer
import etcd3 
import argparse
import hashlib

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

# Cache data CSV
csv_data = None
current_row_index = {}  # Theo dõi vị trí đọc của từng meter

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
            return json.loads(value.decode('utf-8'))
        else:
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
            if '.' not in full_name:
                print(f"Invalid plugin format: {full_name}")
                continue
                
            module_name, class_name = full_name.rsplit('.', 1)
            mod = importlib.import_module(module_name)
            clazz = getattr(mod, class_name)
            instance = clazz()
            instance.initialize()
            loaded_instances.append(instance)
            print(f"Plugin loaded: {class_name}")
            
        except ImportError:
            print(f"Plugin module not found: {full_name}")
        except AttributeError:
            print(f"Plugin class not found in module: {full_name}")
        except Exception as e:
            print(f"Error loading {full_name}: {e}")
            
    return loaded_instances

def load_csv_data(csv_file, total_containers, demo_mode):
    """Load CSV file và phân chia meter_id theo container"""
    global csv_data
    try:
        print(f"Loading CSV from: {csv_file}")
        csv_data = pd.read_csv(csv_file)
        csv_data['timestamp'] = pd.to_datetime(csv_data['timestamp'])
        
        all_meters = sorted(csv_data['meter_id'].unique().tolist())
        
        container_index_env = os.getenv('CONTAINER_INDEX')
        
        if container_index_env:
            # Đã set sẵn index từ docker-compose
            container_index = int(container_index_env) - 1  # Convert 1-based to 0-based
            print(f"✓ Using CONTAINER_INDEX from environment: {container_index}")
        else:
            # Fallback: dùng hash
            container_id = os.getenv('HOSTNAME', 'default')
            hash_value = int(hashlib.md5(container_id.encode()).hexdigest(), 16)
            container_index = hash_value % total_containers
            print(f"⚠ Using hash-based index from hostname: {container_id}")
        
        print(f"Container Index: {container_index} / {total_containers}")
        
        # Chia đều meters cho các containers
        if demo_mode == 'large-scale':
            # Large Scale: 5 containers, mỗi container 2 meters
            assigned_meters = [m for i, m in enumerate(all_meters) if i % total_containers == container_index]
        else:
            # Normal: 5 containers, mỗi container 1 meter
            assigned_meters = [m for i, m in enumerate(all_meters[:5]) if i % total_containers == container_index]
        
        csv_data = csv_data[csv_data['meter_id'].isin(assigned_meters)]
        
        return assigned_meters
    except Exception as e:
        print(f"Error loading CSV: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_next_record(meter_id, enabled_metrics):
    """Lấy dòng tiếp theo từ CSV cho meter_id"""
    global csv_data, current_row_index
    
    # Filter data của meter này
    meter_data = csv_data[csv_data['meter_id'] == meter_id]
    
    if meter_data.empty:
        print(f"No data for {meter_id}")
        return None
    
    # Lấy index hiện tại (hoặc bắt đầu từ 0)
    if meter_id not in current_row_index:
        current_row_index[meter_id] = 0
    
    idx = current_row_index[meter_id]
    
    # Nếu hết data, loop lại từ đầu
    if idx >= len(meter_data):
        current_row_index[meter_id] = 0
        idx = 0
    
    # Lấy row hiện tại
    row = meter_data.iloc[idx]
    current_row_index[meter_id] += 1
    
    # Map CSV columns -> measurements
    full_data = {
        "voltage_v": float(row.get('measurements_voltage_v', 0)),
        "current_a": float(row.get('measurements_current_a', 0)),
        "active_power_kw": float(row.get('measurements_active_power_kw', 0)),
        "power_factor": float(row.get('measurements_power_factor', 0)),
        "frequency_hz": float(row.get('measurements_frequency_hz', 0)),
        "total_energy_kwh": float(row.get('measurements_total_energy_kwh', 0))
    }
    
    # Filter theo enabled_metrics
    measurements = {k: v for k, v in full_data.items() if k in enabled_metrics}
    
    # Tạo record JSON
    record = {
        "meter_id": str(row['meter_id']),
        "timestamp": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z",
        "location": {
            "city": str(row.get('location_city', 'Unknown')),
            "district": str(row.get('location_district', 'Unknown')),
            "gps": str(row.get('location_gps', '0.0,0.0'))
        },
        "measurements": measurements,
        "status": {
            "code": int(row.get('status_code', 0)),
            "message": str(row.get('status_message', 'OK')),
            "battery_level": int(row.get('status_battery_level', 100))
        }
    }
    
    return record

# --- MAIN LOOP ---
def main():
    parser = argparse.ArgumentParser(description='Smart Meter Generator (CSV Mode)')
    parser.add_argument('--csv', type=str, default='/app/data/smart_meters_linear_noise_5pct.csv', 
                        help='Path to CSV file')
    parser.add_argument('--total-containers', type=int, default=5, 
                        help='Total number of containers (for meter distribution)')
    parser.add_argument('--mode', type=str, choices=['normal', 'large-scale'], default='normal',
                        help='Demo mode: normal (1 meter/container) or large-scale (2 meters/container)')
    
    args = parser.parse_args()

    print(f"Starting Generator [{DEVICE_ID}] in CSV mode...")
    print(f"Arguments: csv={args.csv}, total_containers={args.total_containers}, mode={args.mode}")
    
    # 1. Load CSV với args
    meter_ids = load_csv_data(args.csv, args.total_containers, args.mode)
    if not meter_ids:
        print("No meters found in CSV. Exiting...")
        return
    
    # 2. Kết nối Etcd
    etcd_client = get_etcd_client()
    
    # 3. Kết nối Kafka
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                retries=3
            )
            print("✓ Connected to Kafka")
        except Exception as e:
            print(f"Waiting for Kafka... ({e})")
            time.sleep(5)

    # 4. Quản lý Plugins
    active_plugins = []
    current_plugin_names = []

    try:
        while True:
            # --- BƯỚC 1: ĐỌC CẤU HÌNH ---
            config = get_app_config(etcd_client)
            
            interval = config.get("interval", DEFAULT_CONFIG["interval"])
            enabled_metrics = config.get("enabled_metrics", DEFAULT_CONFIG["enabled_metrics"])
            new_plugin_names = config.get("plugins", [])

            # --- BƯỚC 2: QUẢN LÝ PLUGIN ---
            if set(new_plugin_names) != set(current_plugin_names):
                print(f"Config changed. Reloading plugins...")
                
                for p in active_plugins:
                    if hasattr(p, 'finalize'):
                        try: p.finalize()
                        except: pass
                
                active_plugins = load_plugins_dynamic(new_plugin_names)
                current_plugin_names = new_plugin_names

            # --- BƯỚC 3: CHẠY PLUGIN ---
            for plugin in active_plugins:
                try:
                    plugin_data = plugin.run()
                    if plugin_data:
                        producer.send(KAFKA_ALERT_TOPIC, value=plugin_data)
                        print(f"Plugin Alert Sent: {plugin_data}")
                except Exception as e:
                    print(f"Plugin runtime error: {e}")

            # --- BƯỚC 4: GỬI DỮ LIỆU TỪ CSV ---
            for meter_id in meter_ids:
                record = get_next_record(meter_id, enabled_metrics)
                
                if record:
                    future = producer.send(KAFKA_METRIC_TOPIC, value=record)
                    future.add_callback(on_send_success)
                    future.add_errback(on_send_error)
                    
                    metrics_count = len(record['measurements'])
                    print(f"[{meter_id}] Row {current_row_index[meter_id]} sent ({metrics_count} metrics)")

            producer.flush()
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nStopping Generator...")
        for p in active_plugins:
            if hasattr(p, 'finalize'): p.finalize()
        producer.close()

if __name__ == "__main__":
    main()