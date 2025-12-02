import streamlit as st
import pandas as pd
import time
import json
import io
from hdfs import InsecureClient

# --- CẤU HÌNH ---
HDFS_URL = 'http://namenode:9870'
HDFS_USER = 'root'
CLEANED_PATH = '/data/smart_meter_cleaned'  # Dữ liệu từ ingest_data.py (Parquet)
PREDICTION_PATH = '/data/predictions'       # Dữ liệu từ stream_predict.py (JSON)

st.set_page_config(page_title="Smart Meter Lakehouse", page_icon="⚡", layout="wide")
st.title("Smart Meter Lakehouse Monitor")

# --- HÀM HỖ TRỢ ---
@st.cache_resource
def get_hdfs_client():
    try:
        return InsecureClient(HDFS_URL, user=HDFS_USER)
    except Exception as e:
        st.error(f"Lỗi kết nối HDFS: {e}")
        return None

def read_parquet_from_hdfs(client, path, limit_hours=1):
    """Đọc file Parquet từ HDFS theo khoảng thời gian"""
    all_dfs = []
    try:
        files = client.list(path)
        parquet_files = [f for f in files if f.endswith('.parquet')]
        
        # Đọc tất cả files
        for filename in parquet_files:
            with client.read(f"{path}/{filename}") as reader:
                content = reader.read()
                df_part = pd.read_parquet(io.BytesIO(content))
                all_dfs.append(df_part)
    except Exception:
        return pd.DataFrame()
    
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        
        # Filter theo thời gian (chỉ lấy N giờ gần nhất)
        if 'event_time' in df.columns:
            df['event_time'] = pd.to_datetime(df['event_time'])
            cutoff_time = pd.Timestamp.now() - pd.Timedelta(hours=limit_hours)
            df = df[df['event_time'] >= cutoff_time]
            df = df.sort_values('event_time', ascending=False)
        
        return df
    return pd.DataFrame()

def read_json_from_hdfs(client, path, limit_hours=1):
    """Đọc file JSON từ HDFS theo khoảng thời gian"""
    all_records = []
    try:
        files = client.list(path)
        json_files = [f for f in files if f.startswith('part-') and f.endswith('.json')]
        
        for filename in json_files:
            with client.read(f"{path}/{filename}") as reader:
                content = reader.read().decode('utf-8')
                for line in content.strip().split('\n'):
                    if line:
                        try:
                            all_records.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
    except Exception:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    
    # Filter theo thời gian
    if not df.empty and 'event_time' in df.columns:
        df['event_time'] = pd.to_datetime(df['event_time'])
        # Đồng bộ timezone: loại bỏ timezone hoặc thêm timezone cho cutoff_time
        if df['event_time'].dt.tz is not None:
            cutoff_time = pd.Timestamp.now(tz='UTC') - pd.Timedelta(hours=limit_hours)
        else:
            cutoff_time = pd.Timestamp.now() - pd.Timedelta(hours=limit_hours)
        df = df[df['event_time'] >= cutoff_time]
    
    return df

# --- GIAO DIỆN ---
client = get_hdfs_client()

st.header("Smart Meter Real-time Monitor")

# Refresh interval control
refresh_interval = st.selectbox("Tốc độ refresh (giây)", [1, 3, 5, 10], index=1)

# Create two columns for Historical and Predictions
col_left, col_right = st.columns(2)

# Placeholders for dynamic content
placeholder_historical = col_left.empty()
placeholder_predictions = col_right.empty()

# Main loop
# Main loop
while True:
    if client:
        # Read data
        df_clean = read_parquet_from_hdfs(client, CLEANED_PATH, limit_hours=1)
        df_pred = read_json_from_hdfs(client, PREDICTION_PATH, limit_hours=1)
        
        # LEFT COLUMN: Historical Data
        with placeholder_historical.container():
            st.subheader("Historical Data (HDFS)")
            
            if not df_clean.empty:
                if 'event_time' in df_clean.columns:
                    df_clean['event_time'] = pd.to_datetime(df_clean['event_time'])
                    df_clean = df_clean.sort_values('event_time', ascending=False)

                # Metrics
                latest_time = df_clean['event_time'].max().strftime('%H:%M:%S')
                m1, m2, m3 = st.columns(3)
                m1.metric("Bản ghi", f"{len(df_clean)}")
                m2.metric("Công suất TB", f"{df_clean['power'].mean():.2f} kW")
                m3.metric("Cập nhật", latest_time)

                # Chart - Hiển thị 100 records gần nhất
                chart_data_hist = df_clean.head(100).sort_values('event_time')
                st.line_chart(chart_data_hist.set_index('event_time')['power'], use_container_width=True)
                
                with st.expander("Xem chi tiết dữ liệu"):
                    st.dataframe(df_clean.head(50), use_container_width=True)
            else:
                st.warning("Chưa có dữ liệu Historical")
        
        # RIGHT COLUMN: Predictions
        with placeholder_predictions.container():
            st.subheader("AI Predictions (Real-time)")
            
            if not df_pred.empty:
                if 'event_time' in df_pred.columns:
                    df_pred['event_time'] = pd.to_datetime(df_pred['event_time'])
                    df_pred = df_pred.sort_values('event_time', ascending=False)

                # Metrics
                last_row = df_pred.iloc[0]
                latest_pred_time = df_pred['event_time'].max().strftime('%H:%M:%S')
                error = abs(last_row['actual_power'] - last_row['predicted_power'])
                error_pct = (error / last_row['actual_power'] * 100) if last_row['actual_power'] > 0 else 0
                
                p1, p2, p3 = st.columns(3)
                p1.metric("Thực tế", f"{last_row['actual_power']:.2f} kW")
                p2.metric("Dự báo", f"{last_row['predicted_power']:.2f} kW")
                p3.metric("Sai số", f"{error:.2f} kW ({error_pct:.1f}%)")

                # Chart - Hiển thị 100 records gần nhất
                chart_data_pred = df_pred.head(100).sort_values('event_time')
                st.line_chart(
                    chart_data_pred.set_index('event_time')[['actual_power', 'predicted_power']], 
                    use_container_width=True
                )
                
                with st.expander("Xem chi tiết dữ liệu"):
                    st.dataframe(df_pred.head(50), use_container_width=True)
            else:
                st.info("Đang chờ dữ liệu Predictions")
    
    time.sleep(refresh_interval)