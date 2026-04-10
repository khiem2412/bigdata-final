"""
Feature Store Viewer - Query, filter, and export feature table.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from deltalake import DeltaTable
import os
import io

st.set_page_config(page_title="Feature Store", page_icon="📦", layout="wide")
st.title("📦 Feature Store Viewer")

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "AWS_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}


@st.cache_data(ttl=300)
def load_features():
    try:
        dt = DeltaTable("s3://lakehouse/gold/feature_store", storage_options=STORAGE_OPTIONS)
        return dt.to_pandas()
    except Exception as e:
        st.error(f"Cannot load feature store: {e}")
        return pd.DataFrame()


features = load_features()

if features.empty:
    st.warning("Feature store is empty. Run the pipeline first.")
    st.stop()

features["date"] = pd.to_datetime(features["date"])

# Sidebar filters
st.sidebar.markdown("### Filters")

plots = sorted(features["plot_id"].unique())
selected_plots = st.sidebar.multiselect("Plots", plots, default=plots[:5])

date_range = st.sidebar.date_input(
    "Date Range",
    value=(features["date"].min().date(), features["date"].max().date()),
)

flag_filter = st.sidebar.multiselect(
    "Flag Filters",
    ["has_disease_flag", "has_drought_flag", "has_pest_flag", "has_damage_flag"],
)

# Apply filters
filtered = features[features["plot_id"].isin(selected_plots)]
if len(date_range) == 2:
    filtered = filtered[
        (filtered["date"].dt.date >= date_range[0])
        & (filtered["date"].dt.date <= date_range[1])
    ]

for flag in flag_filter:
    if flag in filtered.columns:
        filtered = filtered[filtered[flag] == 1]

# === Schema Info ===
with st.expander("📋 Feature Schema"):
    schema_data = {
        "Feature": [
            "plot_id", "date",
            "temperature_avg", "humidity_avg", "soil_moisture_avg",
            "temperature_max", "temperature_min",
            "temperature_7d_avg", "soil_moisture_7d_avg",
            "anomaly_count", "anomaly_count_7d",
            "ndvi", "ndvi_7d_avg", "ndvi_30d_avg",
            "ndvi_7d_slope", "ndvi_30d_slope",
            "rainfall_daily", "rainfall_7d_sum",
            "has_disease_flag", "has_drought_flag",
            "has_pest_flag", "has_damage_flag",
            "report_count",
        ],
        "Type": [
            "string", "date",
            "float", "float", "float",
            "float", "float",
            "float", "float",
            "int", "int",
            "float", "float", "float",
            "float", "float",
            "float", "float",
            "int", "int",
            "int", "int",
            "int",
        ],
        "Description": [
            "Plot identifier (PK)", "Date (PK)",
            "Daily avg temperature (°C)", "Daily avg humidity (%)", "Daily avg soil moisture",
            "Daily max temperature", "Daily min temperature",
            "7-day rolling avg temperature", "7-day rolling avg soil moisture",
            "Daily anomaly count", "7-day rolling anomaly count",
            "NDVI value (sparse)", "7-day NDVI average", "30-day NDVI average",
            "7-day NDVI slope", "30-day NDVI slope",
            "Daily rainfall (mm)", "7-day rainfall sum (mm)",
            "Disease flag", "Drought flag",
            "Pest flag", "Damage flag",
            "Number of field reports",
        ],
    }
    st.dataframe(pd.DataFrame(schema_data), use_container_width=True, hide_index=True)

# === Stats ===
st.markdown("### Summary Statistics")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Records", f"{len(filtered):,}")
with col2:
    st.metric("Plots", len(filtered["plot_id"].unique()))
with col3:
    st.metric("Date Range", f"{filtered['date'].min().date()} — {filtered['date'].max().date()}" if not filtered.empty else "N/A")
with col4:
    if "ndvi" in filtered.columns:
        ndvi_mean = filtered["ndvi"].dropna().mean()
        st.metric("Avg NDVI", f"{ndvi_mean:.3f}" if pd.notna(ndvi_mean) else "N/A")

# === Feature Distribution ===
st.markdown("### Feature Distribution")
numeric_cols = [
    c for c in filtered.columns
    if filtered[c].dtype in ["float64", "float32", "int64", "int32"]
    and c not in ["latitude", "longitude"]
]

selected_feature = st.selectbox("Select Feature", numeric_cols, index=0)
if selected_feature:
    col1, col2 = st.columns(2)
    with col1:
        fig_hist = px.histogram(
            filtered, x=selected_feature,
            title=f"Distribution: {selected_feature}",
            nbins=50,
        )
        st.plotly_chart(fig_hist, use_container_width=True)
    with col2:
        fig_box = px.box(
            filtered, x="plot_id", y=selected_feature,
            title=f"By Plot: {selected_feature}",
        )
        fig_box.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_box, use_container_width=True)

# === Data Table ===
st.markdown("### Data Table")
display_cols = st.multiselect(
    "Columns to display",
    filtered.columns.tolist(),
    default=[
        "plot_id", "date", "ndvi", "temperature_avg",
        "soil_moisture_avg", "anomaly_count_7d",
        "has_disease_flag", "has_drought_flag",
    ],
)

if display_cols:
    st.dataframe(
        filtered[display_cols].sort_values(["plot_id", "date"]),
        use_container_width=True,
        height=400,
    )

# === Export ===
st.markdown("### Export")
col1, col2 = st.columns(2)

with col1:
    csv_data = filtered.to_csv(index=False)
    st.download_button(
        label="📥 Download CSV",
        data=csv_data,
        file_name="feature_store.csv",
        mime="text/csv",
    )

with col2:
    parquet_buffer = io.BytesIO()
    filtered.to_parquet(parquet_buffer, index=False)
    st.download_button(
        label="📥 Download Parquet",
        data=parquet_buffer.getvalue(),
        file_name="feature_store.parquet",
        mime="application/octet-stream",
    )
