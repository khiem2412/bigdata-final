"""
Forest Map - NDVI visualization with time slider and overlays.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
from deltalake import DeltaTable
import os

st.set_page_config(page_title="Forest Map", page_icon="🗺️", layout="wide")
st.title("🗺️ Forest Map — NDVI & Anomaly Overlay")

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "AWS_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

FOREST_CENTER = [15.3350, 108.2528]


@st.cache_data(ttl=300)
def load_ndvi():
    try:
        dt = DeltaTable("s3://lakehouse/silver/satellite_clean", storage_options=STORAGE_OPTIONS)
        return dt.to_pandas()
    except Exception as e:
        st.error(f"Cannot load NDVI data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_fhi():
    try:
        dt = DeltaTable("s3://lakehouse/gold/forest_health_index", storage_options=STORAGE_OPTIONS)
        return dt.to_pandas()
    except Exception as e:
        st.error(f"Cannot load FHI data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_ocr():
    try:
        dt = DeltaTable("s3://lakehouse/silver/ocr_processed", storage_options=STORAGE_OPTIONS)
        return dt.to_pandas()
    except Exception as e:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_plot_summary():
    try:
        dt = DeltaTable("s3://lakehouse/gold/plot_summary", storage_options=STORAGE_OPTIONS)
        return dt.to_pandas()
    except Exception as e:
        st.error(f"Cannot load plot summary: {e}")
        return pd.DataFrame()


# Load data
ndvi_df = load_ndvi()
fhi_df = load_fhi()
plot_summary = load_plot_summary()

if ndvi_df.empty:
    st.warning("No NDVI data available. Run the data pipeline first.")
    st.stop()

# Time slider
ndvi_df["date"] = pd.to_datetime(ndvi_df["date"])
dates = sorted(ndvi_df["date"].dt.date.unique())

col1, col2 = st.columns([3, 1])

with col2:
    st.markdown("### Controls")
    overlay = st.multiselect(
        "Overlay Layers",
        ["Disease", "Drought", "Anomaly"],
        default=["Anomaly"],
    )

with col1:
    selected_date_idx = st.slider(
        "Select Date",
        min_value=0,
        max_value=len(dates) - 1,
        value=len(dates) - 1,
        format="",
    )
    selected_date = dates[selected_date_idx]
    st.markdown(f"**Selected: {selected_date}**")

# Filter NDVI for selected date
ndvi_day = ndvi_df[ndvi_df["date"].dt.date == selected_date].copy()

if ndvi_day.empty:
    # Find nearest date
    nearest_idx = min(range(len(dates)), key=lambda i: abs((dates[i] - selected_date).days))
    selected_date = dates[nearest_idx]
    ndvi_day = ndvi_df[ndvi_df["date"].dt.date == selected_date].copy()
    st.info(f"No data for selected date. Showing nearest: {selected_date}")

# Plot NDVI heatmap using Plotly
st.markdown("### NDVI Map")

tab1, tab2 = st.tabs(["Grid Map", "Interactive Map"])

with tab1:
    if not ndvi_day.empty:
        # Extract row/col from plot_id
        ndvi_day["row"] = ndvi_day["plot_id"].str.extract(r"plot_(\d+)_").astype(int)
        ndvi_day["col"] = ndvi_day["plot_id"].str.extract(r"plot_\d+_(\d+)").astype(int)

        # Pivot for heatmap
        pivot = ndvi_day.pivot_table(values="ndvi", index="row", columns="col", aggfunc="mean")

        fig = px.imshow(
            pivot.values,
            labels=dict(x="Column", y="Row", color="NDVI"),
            x=[f"C{c}" for c in pivot.columns],
            y=[f"R{r}" for r in pivot.index],
            color_continuous_scale="RdYlGn",
            zmin=0.2,
            zmax=0.85,
            aspect="equal",
        )
        fig.update_layout(
            title=f"NDVI Grid — {selected_date}",
            height=500,
        )

        # Add anomaly markers
        if "Anomaly" in overlay and "is_ndvi_anomaly" in ndvi_day.columns:
            anomalies = ndvi_day[ndvi_day["is_ndvi_anomaly"] == True]
            if not anomalies.empty:
                fig.add_trace(go.Scatter(
                    x=[f"C{c}" for c in anomalies["col"]],
                    y=[f"R{r}" for r in anomalies["row"]],
                    mode="markers",
                    marker=dict(symbol="x", size=20, color="red", line=dict(width=2)),
                    name="NDVI Anomaly",
                ))

        st.plotly_chart(fig, use_container_width=True)

with tab2:
    # m = folium.Map(location=FOREST_CENTER, zoom_start=18, tiles="OpenStreetMap")
    m = folium.Map(location=FOREST_CENTER, zoom_start=18)

    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri",
        name="Esri Satellite",
    ).add_to(m)
    for _, row in ndvi_day.iterrows():
        ndvi_val = row["ndvi"]
        # Color: green (healthy) → red (stressed)
        if ndvi_val >= 0.6:
            color = "green"
        elif ndvi_val >= 0.4:
            color = "orange"
        else:
            color = "red"

        popup_text = f"""
        <b>{row['plot_id']}</b><br>
        NDVI: {ndvi_val:.3f}<br>
        Date: {selected_date}
        """

        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=12,
            popup=folium.Popup(popup_text, max_width=200),
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
        ).add_to(m)

    st_folium(m, width=700, height=500)

# FHI Summary
st.markdown("### Forest Health Summary")
if not plot_summary.empty:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        avg_fhi = plot_summary["avg_fhi"].mean()
        st.metric("Average FHI", f"{avg_fhi:.1f}")
    with col2:
        healthy = (plot_summary["latest_status"] == "Healthy").sum()
        st.metric("Healthy Plots", f"{healthy}/25")
    with col3:
        stressed = (plot_summary["latest_status"].isin(["Stressed", "Critical", "Severe"])).sum()
        st.metric("Stressed Plots", stressed)
    with col4:
        total_disease = plot_summary["total_disease_reports"].sum()
        st.metric("Disease Reports", int(total_disease))

    # Status distribution
    fig_status = px.bar(
        plot_summary.groupby("latest_status").size().reset_index(name="count"),
        x="latest_status",
        y="count",
        color="latest_status",
        color_discrete_map={
            "Healthy": "green",
            "Moderate": "yellowgreen",
            "Stressed": "orange",
            "Critical": "red",
            "Severe": "darkred",
        },
        title="Plot Health Status Distribution",
    )
    st.plotly_chart(fig_status, use_container_width=True)
