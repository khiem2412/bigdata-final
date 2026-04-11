"""
Satellite View — 5×5 grid with NDVI false-color imagery and live sensor overlay.
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from deltalake import DeltaTable
import os

st.set_page_config(page_title="Satellite View", page_icon="🛰️", layout="wide")
st.title("🛰️ Satellite View — 5×5 Plot Grid")

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "AWS_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

NROWS, NCOLS = 5, 5


# ── data loaders ──────────────────────────────────────────────
@st.cache_data(ttl=120)
def load_table(path):
    try:
        dt = DeltaTable(f"s3://lakehouse/{path}", storage_options=STORAGE_OPTIONS)
        return dt.to_pandas()
    except Exception as e:
        st.error(f"Cannot load {path}: {e}")
        return pd.DataFrame()


ndvi_df = load_table("silver/satellite_clean")
fhi_df = load_table("gold/forest_health_index")
feature_df = load_table("gold/feature_store")
plot_summary_df = load_table("gold/plot_summary")

if ndvi_df.empty:
    st.warning("No NDVI data. Run the pipeline first.")
    st.stop()

ndvi_df["date"] = pd.to_datetime(ndvi_df["date"])
dates = sorted(ndvi_df["date"].dt.date.unique())


# ── sidebar ───────────────────────────────────────────────────
st.sidebar.markdown("### 🛰️ Satellite Controls")

selected_idx = st.sidebar.slider(
    "Observation date", 0, len(dates) - 1, len(dates) - 1
)
selected_date = dates[selected_idx]

color_by = st.sidebar.radio(
    "Color layer",
    ["NDVI (Satellite)", "Forest Health Index"],
)

show_sensors = st.sidebar.checkbox("Show sensor overlay", True)
show_anomalies = st.sidebar.checkbox("Highlight anomalies", True)


# ── helpers ───────────────────────────────────────────────────
def _parse_rc(plot_id: str):
    parts = plot_id.replace("plot_", "").split("_")
    return int(parts[0]) - 1, int(parts[1]) - 1


# ── build grid ────────────────────────────────────────────────
ndvi_day = ndvi_df[ndvi_df["date"].dt.date == selected_date]
if ndvi_day.empty:
    nearest = min(dates, key=lambda d: abs((d - selected_date).days))
    ndvi_day = ndvi_df[ndvi_df["date"].dt.date == nearest]
    selected_date = nearest

ndvi_grid = np.full((NROWS, NCOLS), np.nan)
for _, row in ndvi_day.iterrows():
    r, c = _parse_rc(row["plot_id"])
    ndvi_grid[r][c] = row["ndvi"]

fhi_grid = np.full((NROWS, NCOLS), np.nan)
if not fhi_df.empty:
    fhi_df["date"] = pd.to_datetime(fhi_df["date"])
    fhi_near = fhi_df[fhi_df["date"].dt.date == selected_date]
    if fhi_near.empty:
        fhi_near = fhi_df.loc[fhi_df.groupby("plot_id")["date"].idxmax()]

    for _, row in fhi_near.iterrows():
        r, c = _parse_rc(row["plot_id"])
        fhi_grid[r][c] = row["fhi"]


# ── sensor latest ─────────────────────────────────────────────
sensor_info = {}
if show_sensors and not feature_df.empty:
    feature_df["date"] = pd.to_datetime(feature_df["date"])
    day_features = feature_df[feature_df["date"].dt.date == selected_date]

    if day_features.empty:
        latest_date = feature_df["date"].dt.date.max()
        day_features = feature_df[feature_df["date"].dt.date == latest_date]

    for _, row in day_features.iterrows():
        sensor_info[row["plot_id"]] = {
            "temp": row.get("temperature_avg", 0),
            "hum": row.get("humidity_avg", 0),
            "sm": row.get("soil_moisture_avg", 0),
            "anom": row.get("anomaly_count", 0),
            "readings": row.get("anomaly_count_7d", 0),
        }


# ── heatmap ───────────────────────────────────────────────────
st.markdown("### False-Color Satellite Image")

use_fhi = color_by == "Forest Health Index"
grid_data = fhi_grid if use_fhi else ndvi_grid
zmin, zmax = (0, 100) if use_fhi else (0.2, 0.85)
label = "FHI" if use_fhi else "NDVI"

fig_sat = go.Figure(
    data=go.Heatmap(
        z=grid_data,
        colorscale="RdYlGn",
        zmin=zmin,
        zmax=zmax,
    )
)

st.plotly_chart(fig_sat, use_container_width=True)


# ── sensor cards ──────────────────────────────────────────────
st.markdown("### Sensor Grid")

for r in range(NROWS):
    cols = st.columns(NCOLS)
    for c in range(NCOLS):
        pid = f"plot_{r+1:02d}_{c+1:02d}"
        ndvi_val = ndvi_grid[r][c]
        fhi_val = fhi_grid[r][c]

        with cols[c]:
            st.markdown(f"""
            **{pid}**  
            NDVI: {ndvi_val:.2f}  
            FHI: {fhi_val:.0f}
            """)


# ── drill down ────────────────────────────────────────────────
st.markdown("### Plot Drill-Down")

sel_plot = st.selectbox(
    "Select plot",
    [f"plot_{r+1:02d}_{c+1:02d}" for r in range(NROWS) for c in range(NCOLS)]
)

tab1, tab2, tab3 = st.tabs(["NDVI", "Sensors", "Features"])


# TAB 2 FIXED
with tab2:

    @st.cache_data(ttl=300)
    def _load_sensor_plot(pid):
        try:
            dt = DeltaTable(
                "s3://lakehouse/silver/sensor_clean",
                storage_options=STORAGE_OPTIONS
            )
            return dt.to_pandas(filters=[("plot_id", "=", pid)])
        except Exception:
            return pd.DataFrame()

    ps = _load_sensor_plot(sel_plot)

    if not ps.empty:
        ps["date"] = pd.to_datetime(ps["date"])

        daily = (
            ps.groupby(ps["date"].dt.date)
            .agg(
                temperature=("temperature", "mean"),
                humidity=("humidity", "mean"),
                soil_moisture=("soil_moisture", "mean"),
            )
            .reset_index()
        )

        daily.rename(columns={"date": "day"}, inplace=True)

        fig_s = make_subplots(
            rows=3, cols=1, shared_xaxes=True,
            subplot_titles=[
                "Temperature",
                "Humidity",
                "Soil Moisture"
            ],
        )

        fig_s.add_trace(
            go.Scatter(x=daily["day"], y=daily["temperature"]),
            row=1, col=1
        )
        fig_s.add_trace(
            go.Scatter(x=daily["day"], y=daily["humidity"]),
            row=2, col=1
        )
        fig_s.add_trace(
            go.Scatter(x=daily["day"], y=daily["soil_moisture"]),
            row=3, col=1
        )

        st.plotly_chart(fig_s, use_container_width=True)

    else:
        st.info("No sensor data")