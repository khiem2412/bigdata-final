"""
Health Dashboard - FHI trends, sensor drill-down, NDVI analysis.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from deltalake import DeltaTable
import os

st.set_page_config(page_title="Health Dashboard", page_icon="📊", layout="wide")
st.title("📊 Forest Health Dashboard")

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "AWS_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}


@st.cache_data(ttl=300)
def load_data(table_path):
    try:
        dt = DeltaTable(f"s3://lakehouse/{table_path}", storage_options=STORAGE_OPTIONS)
        return dt.to_pandas()
    except Exception as e:
        st.error(f"Cannot load {table_path}: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_sensor_for_plot(plot_id):
    """Load sensor data lazily — only for the selected plot (avoids loading 7M+ rows)."""
    try:
        dt = DeltaTable("s3://lakehouse/silver/sensor_clean", storage_options=STORAGE_OPTIONS)
        df = dt.to_pandas(
            filters=[("plot_id", "=", plot_id)],
        )
        return df
    except Exception as e:
        return pd.DataFrame()


fhi_df = load_data("gold/forest_health_index")
feature_df = load_data("gold/feature_store")
ndvi_df = load_data("silver/satellite_clean")
ocr_df = load_data("silver/ocr_processed")
image_df = load_data("silver/image_processed")

if fhi_df.empty:
    st.warning("No FHI data available. Run the pipeline first.")
    st.stop()

fhi_df["date"] = pd.to_datetime(fhi_df["date"])

# Sidebar: Plot selector
st.sidebar.markdown("### Filter")
plots = sorted(fhi_df["plot_id"].unique())
selected_plot = st.sidebar.selectbox("Select Plot", plots)

date_range = st.sidebar.date_input(
    "Date Range",
    value=(fhi_df["date"].min().date(), fhi_df["date"].max().date()),
    min_value=fhi_df["date"].min().date(),
    max_value=fhi_df["date"].max().date(),
)

# Filter data
if len(date_range) == 2:
    mask = (fhi_df["date"].dt.date >= date_range[0]) & (fhi_df["date"].dt.date <= date_range[1])
    fhi_filtered = fhi_df[mask]
else:
    fhi_filtered = fhi_df

# === FHI Overview (all plots) ===
st.markdown("### Forest Health Index — All Plots")

# Heatmap: FHI over time per plot
monthly_fhi = (
    fhi_filtered
    .assign(month=fhi_filtered["date"].dt.to_period("M").astype(str))
    .groupby(["plot_id", "month"])["fhi"]
    .mean()
    .reset_index()
)

if not monthly_fhi.empty:
    pivot = monthly_fhi.pivot(index="plot_id", columns="month", values="fhi")
    fig_heat = px.imshow(
        pivot.values,
        labels=dict(x="Month", y="Plot", color="FHI"),
        x=list(pivot.columns),
        y=list(pivot.index),
        color_continuous_scale="RdYlGn",
        zmin=20,
        zmax=100,
        aspect="auto",
    )
    fig_heat.update_layout(title="Monthly Average FHI by Plot", height=600)
    st.plotly_chart(fig_heat, use_container_width=True)

# === Selected Plot Drill-Down ===
st.markdown(f"### Drill-Down: {selected_plot}")
plot_fhi = fhi_filtered[fhi_filtered["plot_id"] == selected_plot].sort_values("date")

# FHI time series
if not plot_fhi.empty:
    fig_fhi = go.Figure()
    fig_fhi.add_trace(go.Scatter(
        x=plot_fhi["date"], y=plot_fhi["fhi"],
        mode="lines+markers", name="FHI",
        line=dict(color="green", width=2),
        marker=dict(size=3),
    ))
    fig_fhi.add_hline(y=60, line_dash="dash", line_color="orange", annotation_text="Moderate")
    fig_fhi.add_hline(y=40, line_dash="dash", line_color="red", annotation_text="Stressed")
    fig_fhi.update_layout(
        title=f"FHI Time Series — {selected_plot}",
        yaxis_title="Forest Health Index",
        yaxis_range=[0, 105],
        height=400,
    )
    st.plotly_chart(fig_fhi, use_container_width=True)

# Sensor time series
tab1, tab2, tab3 = st.tabs(["🌡️ Sensor Data", "🛰️ NDVI", "📝 Notes & Images"])

with tab1:
    plot_sensors = load_sensor_for_plot(selected_plot)
    if not plot_sensors.empty:
        plot_sensors["date"] = pd.to_datetime(plot_sensors["date"])

        if len(date_range) == 2:
            plot_sensors = plot_sensors[
                (plot_sensors["date"].dt.date >= date_range[0])
                & (plot_sensors["date"].dt.date <= date_range[1])
            ]

        if not plot_sensors.empty:
            # Daily averages for cleaner visualization
            daily = (
                plot_sensors
                .groupby("date")
                .agg(
                    temperature=("temperature", "mean"),
                    humidity=("humidity", "mean"),
                    soil_moisture=("soil_moisture", "mean"),
                    anomaly_count=("is_anomaly", "sum"),
                )
                .reset_index()
            )

            col1, col2 = st.columns(2)
            with col1:
                fig_temp = px.line(
                    daily, x="date", y="temperature",
                    title="Temperature (Daily Avg)",
                    color_discrete_sequence=["#FF6B6B"],
                )
                fig_temp.update_layout(height=300)
                st.plotly_chart(fig_temp, use_container_width=True)

                fig_hum = px.line(
                    daily, x="date", y="humidity",
                    title="Humidity (Daily Avg)",
                    color_discrete_sequence=["#4ECDC4"],
                )
                fig_hum.update_layout(height=300)
                st.plotly_chart(fig_hum, use_container_width=True)

            with col2:
                fig_sm = px.line(
                    daily, x="date", y="soil_moisture",
                    title="Soil Moisture (Daily Avg)",
                    color_discrete_sequence=["#45B7D1"],
                )
                fig_sm.update_layout(height=300)
                st.plotly_chart(fig_sm, use_container_width=True)

                fig_anom = px.bar(
                    daily, x="date", y="anomaly_count",
                    title="Anomaly Count (Daily)",
                    color_discrete_sequence=["#FF4757"],
                )
                fig_anom.update_layout(height=300)
                st.plotly_chart(fig_anom, use_container_width=True)
        else:
            st.info(f"No sensor data for {selected_plot} in selected range.")

with tab2:
    if not ndvi_df.empty:
        ndvi_df["date"] = pd.to_datetime(ndvi_df["date"])
        plot_ndvi = ndvi_df[ndvi_df["plot_id"] == selected_plot].sort_values("date")

        if not plot_ndvi.empty:
            fig_ndvi = go.Figure()
            fig_ndvi.add_trace(go.Scatter(
                x=plot_ndvi["date"], y=plot_ndvi["ndvi"],
                mode="lines+markers", name="NDVI",
                line=dict(color="green", width=2),
            ))
            if "ndvi_30d_avg" in plot_ndvi.columns:
                fig_ndvi.add_trace(go.Scatter(
                    x=plot_ndvi["date"], y=plot_ndvi["ndvi_30d_avg"],
                    mode="lines", name="30-day Avg",
                    line=dict(color="gray", dash="dash"),
                ))

            # Mark anomalies
            if "is_ndvi_anomaly" in plot_ndvi.columns:
                anomalies = plot_ndvi[plot_ndvi["is_ndvi_anomaly"] == True]
                if not anomalies.empty:
                    fig_ndvi.add_trace(go.Scatter(
                        x=anomalies["date"], y=anomalies["ndvi"],
                        mode="markers", name="Anomaly",
                        marker=dict(color="red", size=10, symbol="x"),
                    ))

            fig_ndvi.update_layout(
                title=f"NDVI Time Series — {selected_plot}",
                yaxis_title="NDVI",
                yaxis_range=[0, 1],
                height=400,
            )
            st.plotly_chart(fig_ndvi, use_container_width=True)
        else:
            st.info(f"No NDVI data for {selected_plot}.")

with tab3:
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 📝 Field Notes (OCR)")
        if not ocr_df.empty:
            plot_ocr = ocr_df[ocr_df["plot_id"] == selected_plot].sort_values(
                "timestamp", ascending=False
            )
            if not plot_ocr.empty:
                for _, note in plot_ocr.head(10).iterrows():
                    with st.expander(
                        f"{note.get('timestamp', 'N/A')} — {note.get('note_type', 'N/A')}"
                    ):
                        st.write(note.get("text", ""))
                        st.caption(f"Author: {note.get('author', 'N/A')} | Keywords: {note.get('keywords', '')}")
            else:
                st.info(f"No notes for {selected_plot}.")

    with col2:
        st.markdown("#### 📷 Field Images")
        if not image_df.empty:
            plot_images = image_df[image_df["plot_id"] == selected_plot].sort_values(
                "timestamp", ascending=False
            )
            if not plot_images.empty:
                for _, img in plot_images.head(10).iterrows():
                    with st.expander(
                        f"{img.get('timestamp', 'N/A')} — {img.get('filename', 'N/A')}"
                    ):
                        st.write(img.get("description", ""))
                        st.caption(f"Tags: {img.get('tags', '')}")
            else:
                st.info(f"No images for {selected_plot}.")
