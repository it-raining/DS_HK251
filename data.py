import csv
import random
import math
from datetime import datetime, timedelta

# Cấu hình
NUM_RECORDS = 1000
OUTPUT_FILE = 'smart_meters_linear_noise_5pct.csv'

households = [
    {"id": "LCL-001", "dist": "Camden", "gps": "51.536,-0.142"},
    {"id": "LCL-002", "dist": "Westminster", "gps": "51.497,-0.135"},
    {"id": "LCL-003", "dist": "Greenwich", "gps": "51.482,0.007"},
    {"id": "LCL-004", "dist": "Hackney", "gps": "51.545,-0.055"},
    {"id": "LCL-005", "dist": "Islington", "gps": "51.538,-0.103"},
    {"id": "LCL-006", "dist": "Chelsea", "gps": "51.487,-0.168"},
    {"id": "LCL-007", "dist": "Kensington", "gps": "51.501,-0.193"},
    {"id": "LCL-008", "dist": "Hammersmith", "gps": "51.492,-0.223"},
    {"id": "LCL-009", "dist": "Fulham", "gps": "51.478,-0.196"},
    {"id": "LCL-010", "dist": "Brixton", "gps": "51.462,-0.116"}
]

# Khởi tạo năng lượng tích lũy
energy_counters = {h["id"]: random.uniform(1000, 5000) for h in households}
start_time = datetime(2012, 1, 1, 0, 0, 0)

def get_current_by_hour(hour):
    """
    Hàm tạo dòng điện (A) giả lập theo hành vi con người (Hourly Trend).
    - Đêm (0-5h): Thấp
    - Sáng (6-9h): Cao
    - Ngày (10-16h): Trung bình
    - Tối (17-22h): Rất cao (Peak)
    """
    base_noise = random.uniform(-0.5, 0.5) # Nhiễu tự nhiên của hành vi
    if 0 <= hour <= 5:
        return max(0.1, random.uniform(0.2, 0.8) + base_noise)
    elif 6 <= hour <= 9:
        return random.uniform(2.0, 5.0) + base_noise
    elif 17 <= hour <= 22:
        return random.uniform(3.0, 8.0) + base_noise
    else:
        return random.uniform(1.0, 3.0) + base_noise

def generate_linear_data():
    headers = [
        "meter_id", "timestamp", 
        "location_city", "location_district", "location_gps",
        "measurements_voltage_v", "measurements_current_a", 
        "measurements_active_power_kw", "measurements_power_factor", 
        "measurements_frequency_hz", "measurements_total_energy_kwh",
        "status_code", "status_message", "status_battery_level"
    ]
    
    data = []
    current_time = start_time
    records_created = 0
    
    # Voltage cố định mức chuẩn để đảm bảo tính tuyến tính rõ ràng cho demo
    # (Bạn có thể cho dao động nhẹ nếu muốn model khó hơn)
    FIXED_VOLTAGE = 230.0 
    FIXED_PF = 0.95

    while records_created < NUM_RECORDS:
        for house in households:
            if records_created >= NUM_RECORDS: break
            
            # 1. Sinh biến độc lập (Feature X): Current
            current_a = round(get_current_by_hour(current_time.hour), 3)
            
            # 2. Tính giá trị lý tưởng (Ground Truth Y)
            # Power = (V * I * PF) / 1000
            ideal_power_kw = (FIXED_VOLTAGE * current_a * FIXED_PF) / 1000
            
            # 3. Bơm nhiễu (Noise Injection) +- 5%
            # Random từ 0.95 đến 1.05
            noise_factor = random.uniform(0.95, 1.05)
            measured_power_kw = ideal_power_kw * noise_factor
            
            # Làm tròn
            measured_power_kw = round(measured_power_kw, 4)

            # Cộng dồn năng lượng (Integrate Power over time)
            # 30 phút = 0.5 giờ
            energy_counters[house["id"]] += measured_power_kw * 0.5
            total_energy = round(energy_counters[house["id"]], 3)
            
            # Các chỉ số phụ khác
            freq = round(random.uniform(49.98, 50.02), 2)
            
            record = [
                house["id"],
                current_time.strftime("%Y-%m-%d %H:%M:%S"),
                "London", house["dist"], house["gps"],
                FIXED_VOLTAGE,          # Feature cố định
                current_a,              # Feature chính (X)
                measured_power_kw,      # Target (Y) = X*Const + Noise
                FIXED_PF,
                freq,
                total_energy,
                0, "OK", 100
            ]
            data.append(record)
            records_created += 1
            
        current_time += timedelta(minutes=30)
        
    return headers, data

# Xuất file
headers, rows = generate_linear_data()
with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    writer.writerows(rows)

print(f"Xong! File '{OUTPUT_FILE}' chứa dữ liệu tuyến tính với nhiễu +-5%.")