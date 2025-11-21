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
st.title("⚡ Smart Meter Lakehouse Monitor")

# --- HÀM HỖ TRỢ ---
@st.cache_resource
def get_hdfs_client():
    try:
        return InsecureClient(HDFS_URL, user=HDFS_USER)
    except Exception as e:
        st.error(f"❌ Lỗi kết nối HDFS: {e}")
        return None

def read_parquet_from_hdfs(client, path, limit=5):
    """Đọc file Parquet từ HDFS vào Pandas DataFrame"""
    all_dfs = []
    try:
        files = client.list(path)
        parquet_files = [f for f in files if f.endswith('.parquet')]
        
        # Lấy các file mới nhất
        for filename in sorted(parquet_files, reverse=True)[:limit]:
            with client.read(f"{path}/{filename}") as reader:
                content = reader.read()
                # Dùng io.BytesIO để đọc bytes thành file-like object cho pandas
                df_part = pd.read_parquet(io.BytesIO(content))
                all_dfs.append(df_part)
    except Exception:
        return pd.DataFrame()
    
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()

def read_json_from_hdfs(client, path, limit=5):
    """Đọc file JSON từ HDFS (cho Predictions)"""
    all_records = []
    try:
        files = client.list(path)
        json_files = [f for f in files if f.startswith('part-') and f.endswith('.json')]
        
        for filename in sorted(json_files, reverse=True)[:limit]:
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

    return pd.DataFrame(all_records)

# --- GIAO DIỆN ---
tab1, tab2 = st.tabs(["📊 Historical Data (Ingestion)", "🔮 AI Predictions (Streaming)"])

client = get_hdfs_client()

# TAB 1: Dữ liệu đã làm sạch (Parquet)
with tab1:
    st.header("Dữ liệu trong Data Lake (HDFS)")
    if st.button("🔄 Làm mới dữ liệu Ingestion"):
        st.cache_data.clear()
    
    if client:
        df_clean = read_parquet_from_hdfs(client, CLEANED_PATH)
        if not df_clean.empty:
            if 'event_time' in df_clean.columns:
                df_clean['event_time'] = pd.to_datetime(df_clean['event_time'])
                df_clean = df_clean.sort_values('event_time', ascending=False)

            # Metrics
            col1, col2, col3 = st.columns(3)
            col1.metric("Tổng bản ghi", len(df_clean))
            col2.metric("Điện áp TB", f"{df_clean['voltage'].mean():.1f} V")
            col3.metric("Công suất TB", f"{df_clean['power'].mean():.2f} kW")

            # Chart
            st.subheader("Biểu đồ tiêu thụ điện (Dữ liệu Training)")
            st.line_chart(df_clean.set_index('event_time')[['power', 'voltage']])
            
            with st.expander("Xem dữ liệu thô"):
                st.dataframe(df_clean.head(50))
        else:
            st.warning("Chưa tìm thấy dữ liệu Parquet. Hãy chạy ingest_data.py trước.")

# TAB 2: Dự báo Real-time (JSON)
with tab2:
    st.header("So sánh Thực tế vs Dự báo")
    placeholder = st.empty()
    
    # Auto-refresh logic cho Tab 2
    run_streaming = st.checkbox("Bật chế độ Real-time Update", value=False)
    
    while run_streaming:
        if client:
            df_pred = read_json_from_hdfs(client, PREDICTION_PATH)
            
            with placeholder.container():
                if not df_pred.empty:
                    if 'event_time' in df_pred.columns:
                        df_pred['event_time'] = pd.to_datetime(df_pred['event_time'])
                        df_pred = df_pred.sort_values('event_time')

                    last_row = df_pred.iloc[-1]
                    m1, m2 = st.columns(2)
                    m1.metric("Thực tế", f"{last_row['actual_power']:.3f} kW")
                    m2.metric("Dự báo AI", f"{last_row['predicted_power']:.3f} kW", 
                             delta=f"{last_row['actual_power'] - last_row['predicted_power']:.3f}")

                    st.line_chart(df_pred.set_index('event_time')[['actual_power', 'predicted_power']])
                else:
                    st.info("⏳ Đang chờ dữ liệu dự báo từ stream_predict.py...")
        
        time.sleep(3)