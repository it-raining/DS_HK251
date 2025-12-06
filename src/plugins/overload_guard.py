import random
import json
from base import BasePlugin

class OverloadGuardPlugin(BasePlugin):
    def initialize(self):
        print("[Guard] System Guard Active")
        # Giả sử dùng chung Kafka producer hoặc init cái mới
    
    def run(self):
        # Giả lập kiểm tra điện áp
        current_voltage = random.uniform(210, 250) 
        
        # Logic phát hiện bất thường
        if current_voltage > 240:
            alert_msg = {
                "type": "CRITICAL",
                "message": f"Over-voltage detected: {current_voltage:.2f}V",
                "action": "Cut-off triggered"
            }
            print(f"[Guard] NGUY HIỂM! {json.dumps(alert_msg)}")
            # Gửi tin này vào Kafka topic 'alerts' thay vì 'data'
            return alert_msg
        else:
            print(f"[Guard] Voltage normal: {current_voltage:.2f}V")
            return None
    
    def finalize(self):
        print("[Guard] Stopped")