"""
Forest Lakehouse Dashboard - Main App
Streamlit-based visualization for forest health monitoring.
"""
import streamlit as st

st.set_page_config(
    page_title="Forest Health Lakehouse",
    page_icon="🌲",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🌲 Forest Health Lakehouse - Forestry 4.0")
st.markdown("---")

st.markdown("""
### Hệ thống Giám sát Sức khỏe Rừng Keo

**Quy mô:** 1 hectare | **25 plots** (5×5 grid) | **75 sensors**

**Tọa độ:** 15.3350°N, 108.2528°E

---

#### Trang Dashboard

- **🗺️ Forest Map** — Bản đồ NDVI, anomaly overlay, time slider
- **📊 Health Dashboard** — Forest Health Index, sensor time series, drill-down
- **📦 Feature Store** — Query feature table, export CSV/Parquet

---

#### Kiến trúc Lakehouse

| Layer | Description |
|-------|-------------|
| **Bronze** | Raw data (sensor streaming, weather, NDVI, OCR, images) |
| **Silver** | Cleaned, standardized, anomaly-detected |
| **Gold** | Feature store, Forest Health Index, plot summary |

#### Data Sources

| Source | Type | Frequency |
|--------|------|-----------|
| Sensor IoT | Streaming (Kafka) | Every 10s |
| Weather | Batch (Open-Meteo API) | Hourly |
| Satellite NDVI | Batch | 5-10 days |
| OCR Notes | Batch | 2-5/week |
| Field Images | Batch | 2-5/week |
""")

# Sidebar info
st.sidebar.markdown("### System Info")
st.sidebar.info(
    "MinIO: http://localhost:9001\n\n"
    "Kafka: localhost:9092\n\n"
    "Dashboard: localhost:8501"
)
