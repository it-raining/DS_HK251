import streamlit as st
import pandas as pd
import time
import json
import io
from hdfs import InsecureClient
import etcd3
from datetime import datetime, timedelta

ETCD_HOST = 'etcd'
ETCD_PORT = 2379

# --- CẤU HÌNH ---
HDFS_URL = 'http://namenode:9870'
HDFS_USER = 'root'
CLEANED_PATH = '/data/smart_meter_cleaned'
PREDICTION_PATH = '/data/predictions'

st.set_page_config(page_title="Smart Meter Lakehouse", layout="wide")
st.title("Smart Meter Lakehouse Monitor")

CONFIG_KEY = '/config'
DEFAULT_CONFIG = {
    "interval": 2,
    "enabled_metrics": [
        "voltage_v", "current_a", "active_power_kw", 
        "power_factor", "frequency_hz", "total_energy_kwh"
    ],
    "plugins": []
}

# --- HÀM HỖ TRỢ ---
@st.cache_resource
def get_hdfs_client():
    try:
        return InsecureClient(HDFS_URL, user=HDFS_USER)
    except Exception as e:
        st.error(f"Lỗi kết nối HDFS: {e}")
        return None

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

def save_config_to_etcd(interval, enabled_metrics, plugins):
    """Lưu config vào etcd"""
    try:
        etcd_client = get_etcd_client()
        if not etcd_client:
            st.error("Không thể kết nối tới etcd")
            return False

        new_config = {
            "interval": interval,
            "enabled_metrics": enabled_metrics,
            "plugins": plugins
        }
        etcd_client.put(CONFIG_KEY, json.dumps(new_config))
        return True
    except Exception as e:
        st.error(f"Lỗi lưu config: {e}")
        return False

@st.cache_data(ttl=30)
def read_parquet_from_hdfs_cached(hdfs_url, user, path, limit_hours=1):
    """Đọc file Parquet từ HDFS - Version tối ưu"""
    try:
        client = InsecureClient(hdfs_url, user=user)
        
        try:
            items = client.list(path)
        except Exception:
            return pd.DataFrame()

        date_folders = [f for f in items if f.startswith('date=')]
        
        if not date_folders:
            parquet_files = [f for f in items if f.endswith('.parquet')]
            full_path = path
        else:
            date_folders.sort(reverse=True)
            latest_date_folder = date_folders[0]
            full_path = f"{path}/{latest_date_folder}"
            files_in_date = client.list(full_path)
            parquet_files = [f for f in files_in_date if f.endswith('.parquet')]

        parquet_files = sorted(parquet_files, reverse=True)[:5]
        
        all_dfs = []
        
        for filename in parquet_files:
            file_path = f"{full_path}/{filename}"
            with client.read(file_path) as reader:
                content = reader.read()
                if len(content) > 0:
                    try:
                        df_part = pd.read_parquet(io.BytesIO(content))
                        
                        if 'event_time' in df_part.columns:
                            df_part['event_time'] = pd.to_datetime(df_part['event_time'])
                            all_dfs.append(df_part)
                    except Exception as e:
                        print(f"Skip file lỗi {filename}: {e}")
                        continue
        
        if all_dfs:
            df = pd.concat(all_dfs, ignore_index=True)
            df = df.sort_values('event_time', ascending=False)
            return df.head(500)
        
        return pd.DataFrame()

    except Exception as e:
        print(f"Error reading parquet HDFS: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=30)
def read_json_from_hdfs_cached(hdfs_url, user, path, limit_hours=1):
    """Đọc file JSON từ HDFS - Version tối ưu"""
    try:
        client = InsecureClient(hdfs_url, user=user)
        files = client.list(path)
        json_files = sorted([f for f in files if f.startswith('part-') and f.endswith('.json')], reverse=True)
        
        json_files = json_files[:3]
        
        all_records = []
        cutoff_time = pd.Timestamp.now() - pd.Timedelta(hours=limit_hours)
        
        for filename in json_files:
            with client.read(f"{path}/{filename}") as reader:
                content = reader.read().decode('utf-8')
                for line in content.strip().split('\n'):
                    if line:
                        try:
                            record = json.loads(line)
                            if 'event_time' in record:
                                event_time = pd.to_datetime(record['event_time']).tz_localize(None)
                                if event_time >= cutoff_time:
                                    all_records.append(record)
                        except json.JSONDecodeError:
                            continue
        
        if all_records:
            df = pd.DataFrame(all_records)
            df['event_time'] = pd.to_datetime(df['event_time']).dt.tz_localize(None)
            df = df.sort_values('event_time', ascending=False)
            return df.head(500)
        
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading json: {e}")
        return pd.DataFrame()

# --- GIAO DIỆN ---
client = get_hdfs_client()

if client:
    st.sidebar.success("HDFS Connected")
else:
    st.sidebar.error("HDFS Disconnected")

st.sidebar.header("Configuration")
current_config = get_app_config(get_etcd_client())

st.sidebar.subheader("Generator Settings")
new_interval = st.sidebar.number_input(
    "Sampling Interval (giây)",
    min_value=1,
    max_value=60,
    value=current_config.get('interval', 2),
    help="Tần suất gửi dữ liệu từ generator"
)

st.sidebar.subheader("Enabled Metrics")
all_metrics = [
    "voltage_v",
    "current_a",
    "active_power_kw",
    "power_factor",
    "frequency_hz",
    "total_energy_kwh"
]

current_metrics = current_config.get('enabled_metrics', all_metrics)
new_metrics = []
for metric in all_metrics:
    if st.sidebar.checkbox(
        metric.replace('_', ' ').title(),
        value=metric in current_metrics,
        key=f"metric_{metric}"
    ):
        new_metrics.append(metric)

st.sidebar.subheader("Plugins")
available_plugins = [
    "plugins.overload_guard.OverloadGuardPlugin",
]

current_plugins = current_config.get('plugins', [])
new_plugins = []
for plugin in available_plugins:
    plugin_name = plugin.split('.')[-1].replace('Plugin', '')
    if st.sidebar.checkbox(
        plugin_name,
        value=plugin in current_plugins,
        key=f"plugin_{plugin}"
    ):
        new_plugins.append(plugin)

if st.sidebar.button("Lưu cấu hình", type="primary", use_container_width=True):
    if save_config_to_etcd(new_interval, new_metrics, new_plugins):
        st.sidebar.success("Đã lưu!")
        st.cache_data.clear()
    else:
        st.sidebar.error("Lỗi khi lưu")

# Main Dashboard
st.header("Smart Meter Real-time Monitor")

refresh_interval = st.selectbox("Tốc độ refresh (giây)", [1, 5, 10, 30, 60], index=0)

# Tạo placeholders cho nội dung động
info_placeholder = st.empty()
col_left, col_right = st.columns(2)

left_header = col_left.empty()
left_metrics = col_left.empty()
left_chart = col_left.empty()
left_expander = col_left.empty()

right_header = col_right.empty()
right_metrics = col_right.empty()
right_chart = col_right.empty()
right_expander = col_right.empty()

if client:
    while True:
        # Đọc data
        df_clean = read_parquet_from_hdfs_cached(HDFS_URL, HDFS_USER, CLEANED_PATH, limit_hours=1)
        df_pred = read_json_from_hdfs_cached(HDFS_URL, HDFS_USER, PREDICTION_PATH, limit_hours=1)
        
        info_placeholder.info(f"Historical: {len(df_clean)} records | Predictions: {len(df_pred)} records")
        
        # LEFT COLUMN
        left_header.subheader("Historical Data (HDFS)")
        
        if not df_clean.empty:
            latest_time = df_clean['event_time'].max().strftime('%H:%M:%S')
            
            with left_metrics.container():
                m1, m2, m3 = st.columns(3)
                m1.metric("Bản ghi", f"{len(df_clean)}")
                
                if 'power' in df_clean.columns:
                    m2.metric("Công suất TB", f"{df_clean['power'].mean():.2f} kW")
                
                m3.metric("Cập nhật", latest_time)

            chart_data = df_clean.head(100).sort_values('event_time')
            if 'power' in chart_data.columns:
                left_chart.line_chart(chart_data.set_index('event_time')['power'], use_container_width=True)
            
            with left_expander.expander("Xem chi tiết (20 records)"):
                st.dataframe(df_clean.head(20), use_container_width=True)
        else:
            left_metrics.warning("Chưa có dữ liệu Historical")
        
        # RIGHT COLUMN
        right_header.subheader("AI Predictions")
        
        if not df_pred.empty:
            last_row = df_pred.iloc[0]
            
            if 'actual_power' in last_row and 'predicted_power' in last_row:
                error = abs(last_row['actual_power'] - last_row['predicted_power'])
                error_pct = (error / last_row['actual_power'] * 100) if last_row['actual_power'] > 0 else 0
                
                with right_metrics.container():
                    p1, p2, p3 = st.columns(3)
                    p1.metric("Thực tế", f"{last_row['actual_power']:.2f} kW")
                    p2.metric("Dự báo", f"{last_row['predicted_power']:.2f} kW")
                    p3.metric("Sai số", f"{error_pct:.1f}%")

                chart_data = df_pred.head(100).sort_values('event_time')
                right_chart.line_chart(
                    chart_data.set_index('event_time')[['actual_power', 'predicted_power']], 
                    use_container_width=True
                )
            
            with right_expander.expander("Xem chi tiết (20 records)"):
                st.dataframe(df_pred.head(20), use_container_width=True)
        else:
            right_metrics.info("Đang chờ dữ liệu Predictions")
        
        time.sleep(refresh_interval)
else:
    st.error("Không thể kết nối tới HDFS")