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
# st.header("Smart Meter Real-time Monitor")

# refresh_interval = st.selectbox("Tốc độ refresh (giây)", [1, 5, 10, 30, 60], index=0)

# # Tạo placeholders cho nội dung động
# info_placeholder = st.empty()
# col_left, col_right = st.columns(2)

# left_header = col_left.empty()
# left_metrics = col_left.empty()
# left_chart = col_left.empty()
# left_expander = col_left.empty()

# right_header = col_right.empty()
# right_metrics = col_right.empty()
# right_chart = col_right.empty()
# right_expander = col_right.empty()

# if client:
#     while True:
#         # Đọc data
#         df_clean = read_parquet_from_hdfs_cached(HDFS_URL, HDFS_USER, CLEANED_PATH, limit_hours=1)
#         df_pred = read_json_from_hdfs_cached(HDFS_URL, HDFS_USER, PREDICTION_PATH, limit_hours=1)
        
#         info_placeholder.info(f"Historical: {len(df_clean)} records | Predictions: {len(df_pred)} records")
        
#         # LEFT COLUMN
#         left_header.subheader("Historical Data (HDFS)")
        
#         if not df_clean.empty:
#             latest_time = df_clean['event_time'].max().strftime('%H:%M:%S')
            
#             with left_metrics.container():
#                 m1, m2, m3 = st.columns(3)
#                 m1.metric("Bản ghi", f"{len(df_clean)}")
                
#                 if 'power' in df_clean.columns:
#                     m2.metric("Công suất TB", f"{df_clean['power'].mean():.2f} kW")
                
#                 m3.metric("Cập nhật", latest_time)

#             chart_data = df_clean.head(100).sort_values('event_time')
#             if 'power' in chart_data.columns:
#                 left_chart.line_chart(chart_data.set_index('event_time')['power'], use_container_width=True)
            
#             with left_expander.expander("Xem chi tiết (20 records)"):
#                 st.dataframe(df_clean.head(20), use_container_width=True)
#         else:
#             left_metrics.warning("Chưa có dữ liệu Historical")
        
#         # RIGHT COLUMN
#         right_header.subheader("AI Predictions")
        
#         if not df_pred.empty:
#             last_row = df_pred.iloc[0]
            
#             if 'actual_power' in last_row and 'predicted_power' in last_row:
#                 error = abs(last_row['actual_power'] - last_row['predicted_power'])
#                 error_pct = (error / last_row['actual_power'] * 100) if last_row['actual_power'] > 0 else 0
                
#                 with right_metrics.container():
#                     p1, p2, p3 = st.columns(3)
#                     p1.metric("Thực tế", f"{last_row['actual_power']:.2f} kW")
#                     p2.metric("Dự báo", f"{last_row['predicted_power']:.2f} kW")
#                     p3.metric("Sai số", f"{error_pct:.1f}%")

#                 chart_data = df_pred.head(100).sort_values('event_time')
#                 right_chart.line_chart(
#                     chart_data.set_index('event_time')[['actual_power', 'predicted_power']], 
#                     use_container_width=True
#                 )
            
#             with right_expander.expander("Xem chi tiết (20 records)"):
#                 st.dataframe(df_pred.head(20), use_container_width=True)
#         else:
#             right_metrics.info("Đang chờ dữ liệu Predictions")
        
#         time.sleep(refresh_interval)
# else:
#     st.error("Không thể kết nối tới HDFS")

# --- MAIN DASHBOARD INTERFACE ---

st.title("Smart Meter Lakehouse | Central Monitoring System")

# Layout Container for smoother refreshing
dashboard_placeholder = st.empty()

# Sidebar: Operational Controls
st.sidebar.markdown("---")
st.sidebar.subheader("Dashboard Controls")
refresh_interval = st.sidebar.selectbox("Refresh Rate", options=[1, 5, 10, 30], index=1, format_func=lambda x: f"{x} seconds")

if client:
    while True:
        # 1. Ingest Data from HDFS (Data Lake)
        df_clean = read_parquet_from_hdfs_cached(HDFS_URL, HDFS_USER, CLEANED_PATH, limit_hours=1)
        df_pred = read_json_from_hdfs_cached(HDFS_URL, HDFS_USER, PREDICTION_PATH, limit_hours=1)
        
        with dashboard_placeholder.container(): # Clear previous frame
            
            # ==========================================
            # LAYER 1: SYSTEM KPIs (High-level Metrics)
            # ==========================================
            st.markdown("### 1. Grid Overview")
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            
            active_nodes = 0
            avg_voltage = 0.0
            total_load_kw = 0.0
            grid_status = "OFFLINE"

            if not df_clean.empty:
                # Get the absolute latest snapshot for every unique meter
                snapshot = df_clean.sort_values('event_time').groupby('meter_id').tail(1)
                
                active_nodes = len(snapshot)
                avg_voltage = snapshot['voltage'].mean() if 'voltage' in snapshot else 0
                total_load_kw = snapshot['power'].sum() if 'power' in snapshot else 0
                
                # Simple logic for status: Nominal voltage is usually 220V +/- 10%
                if 200 <= avg_voltage <= 240:
                    grid_status = "NOMINAL"
                else:
                    grid_status = "UNSTABLE"

            kpi1.metric("Active Nodes", f"{active_nodes}", delta="Connected")
            kpi2.metric("Avg Grid Voltage", f"{avg_voltage:.1f} V", delta_color="normal" if grid_status == "NOMINAL" else "inverse")
            kpi3.metric("Real-time Load", f"{total_load_kw:.2f} kW", help="Aggregated active power across all nodes")
            kpi4.metric("Grid Status", grid_status)

            st.markdown("---")

            # ==========================================
            # LAYER 2: TELEMETRY & CONSUMPTION (Per Spec)
            # ==========================================

            st.markdown("### 2. Telemetry Analysis")
            
            if not df_clean.empty and 'meter_id' in df_clean.columns:
                col_telemetry, col_consumption = st.columns(2)
                
                with col_telemetry:
                    st.subheader("Voltage Stability Monitor")
                    # Pivot data to show multi-series lines (one line per meter)
                    # This allows detecting if a specific node is undervoltage
                    df_voltage = df_clean.sort_values('event_time').tail(100)
                    if 'voltage' in df_voltage.columns:
                        pivot_volt = df_voltage.pivot_table(index='event_time', columns='meter_id', values='voltage', aggfunc='first')
                        pivot_volt = pivot_volt.ffill().bfill()
                        st.line_chart(pivot_volt, use_container_width=True, height=300)
                    st.caption("Real-time voltage fluctuation per node (V).")

                with col_consumption:
                    st.subheader("Energy Consumption Distribution")
                    # Aggregating total energy usage per meter
                    if 'energy' in df_clean.columns:
                        # Taking max() because energy is a cumulative counter in smart meters
                        df_usage = df_clean.groupby('meter_id')['energy'].max().sort_values(ascending=False)
                        st.bar_chart(df_usage, use_container_width=True, height=300)
                    st.caption("Cumulative energy consumption per node (kWh).")

            st.markdown("---")

            # ==========================================
            # LAYER 3: PREDICTIVE ANALYTICS (Spark ML)
            # ==========================================
            st.markdown("### 3. AI Inference (Spark Streaming)")
            
            col_pred_chart, col_raw_data = st.columns([2, 1])

            with col_pred_chart:
                st.subheader("Load Forecasting: Actual vs Predicted")
                if not df_pred.empty:
                    # Sort data
                    df_vis = df_pred.sort_values('event_time').tail(60)
                    last_row = df_vis.iloc[-1]
                    
                    act = last_row.get('actual_power', 0)
                    pred = last_row.get('predicted_power', 0)
                    err_val = abs(act - pred)
                    err_pct = (err_val / act * 100) if act > 0 else 0
                    
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Actual Load", f"{act:.2f} kW")
                    m2.metric("Predicted", f"{pred:.2f} kW")
                    m3.metric("Error Rate", f"{err_pct:.1f}%", delta=f"{err_val:.2f} kW", delta_color="inverse")
                    
                    st.line_chart(
                        df_vis.set_index('event_time')[['actual_power', 'predicted_power']], 
                        use_container_width=True,
                        color=["#00CC96", "#EF553B"] 
                    )
                else:
                    st.warning("Model is initializing... Waiting for Spark Streaming output.")

            with col_raw_data:
                st.subheader("Data Lake Logs")
                if not df_clean.empty:
                    display_cols = ['event_time', 'meter_id', 'voltage', 'power']
                    valid_cols = [c for c in display_cols if c in df_clean.columns]
                    st.dataframe(
                        df_clean.sort_values('event_time', ascending=False)[valid_cols].head(10), 
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.write("No data in HDFS.")

        time.sleep(refresh_interval)

else:
    st.error(f"CRITICAL: Unable to connect to HDFS NameNode at {HDFS_URL}. Check Docker network.")