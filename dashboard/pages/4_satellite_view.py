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
sensor_df = load_table("silver/sensor_clean")
fhi_df = load_table("gold/forest_health_index")
feature_df = load_table("gold/feature_store")
plot_summary_df = load_table("gold/plot_summary")

if ndvi_df.empty:
    st.warning("No NDVI data. Run the pipeline first.")
    st.stop()

ndvi_df["date"] = pd.to_datetime(ndvi_df["date"])
dates = sorted(ndvi_df["date"].dt.date.unique())

# ── sidebar controls ─────────────────────────────────────────
st.sidebar.markdown("### 🛰️ Satellite Controls")

selected_idx = st.sidebar.slider(
    "Observation date",
    0, len(dates) - 1, len(dates) - 1,
    format="",
)
selected_date = dates[selected_idx]
st.sidebar.markdown(f"**{selected_date}**")

color_by = st.sidebar.radio(
    "Color layer",
    ["NDVI (Satellite)", "Forest Health Index"],
    index=0,
)

show_sensors = st.sidebar.checkbox("Show sensor overlay", value=True)
show_anomalies = st.sidebar.checkbox("Highlight anomalies", value=True)

# ── helpers ───────────────────────────────────────────────────

def _parse_rc(plot_id: str):
    """Return (row, col) 0-indexed from plot_01_02 style ids."""
    parts = plot_id.replace("plot_", "").split("_")
    return int(parts[0]) - 1, int(parts[1]) - 1


def ndvi_to_rgb(v: float):
    """False-color: dark-red (low) → yellow → bright green (high)."""
    v = np.clip(v, 0.1, 0.9)
    t = (v - 0.1) / 0.8  # 0..1
    if t < 0.35:
        r, g, b = 140 + int(115 * (t / 0.35)), int(80 * (t / 0.35)), 20
    elif t < 0.65:
        s = (t - 0.35) / 0.3
        r, g, b = 255 - int(155 * s), 80 + int(100 * s), 20 + int(30 * s)
    else:
        s = (t - 0.65) / 0.35
        r, g, b = 100 - int(70 * s), 180 + int(75 * s), 50 + int(40 * s)
    return f"rgb({r},{g},{b})"


def fhi_to_rgb(v: float):
    """Red (0) → orange (40) → yellow (60) → green (100)."""
    v = np.clip(v, 0, 100)
    t = v / 100.0
    if t < 0.4:
        s = t / 0.4
        r, g, b = 200, int(60 * s), 20
    elif t < 0.6:
        s = (t - 0.4) / 0.2
        r, g, b = 200 + int(55 * s), 60 + int(140 * s), 20
    else:
        s = (t - 0.6) / 0.4
        r, g, b = 255 - int(225 * s), 200 + int(55 * s), 20 + int(70 * s)
    return f"rgb({r},{g},{b})"


# ── build grid data ───────────────────────────────────────────

ndvi_day = ndvi_df[ndvi_df["date"].dt.date == selected_date]
if ndvi_day.empty:
    nearest = min(dates, key=lambda d: abs((d - selected_date).days))
    ndvi_day = ndvi_df[ndvi_df["date"].dt.date == nearest]
    st.info(f"No NDVI for {selected_date}; showing nearest: {nearest}")
    selected_date = nearest

# NDVI matrix
ndvi_grid = np.full((NROWS, NCOLS), np.nan)
for _, row in ndvi_day.iterrows():
    r, c = _parse_rc(row["plot_id"])
    ndvi_grid[r][c] = row["ndvi"]

# FHI matrix (use nearest date)
fhi_grid = np.full((NROWS, NCOLS), np.nan)
if not fhi_df.empty:
    fhi_df["date"] = pd.to_datetime(fhi_df["date"])
    fhi_near = fhi_df[fhi_df["date"].dt.date == selected_date]
    if fhi_near.empty:
        fhi_near = fhi_df.loc[fhi_df.groupby("plot_id")["date"].idxmax()]
    for _, row in fhi_near.iterrows():
        r, c = _parse_rc(row["plot_id"])
        fhi_grid[r][c] = row["fhi"]

# Sensor latest per plot (aggregated daily)
sensor_info = {}
if show_sensors and not sensor_df.empty:
    sensor_df["date"] = pd.to_datetime(sensor_df["date"])
    day_sensors = sensor_df[sensor_df["date"].dt.date == selected_date]
    if day_sensors.empty:
        # Use latest available date
        latest_date = sensor_df["date"].dt.date.max()
        day_sensors = sensor_df[sensor_df["date"].dt.date == latest_date]
    agg = (
        day_sensors.groupby("plot_id")
        .agg(
            temp=("temperature", "mean"),
            hum=("humidity", "mean"),
            sm=("soil_moisture", "mean"),
            anom=("is_anomaly", "sum"),
            readings=("sensor_id", "count"),
        )
        .reset_index()
    )
    for _, row in agg.iterrows():
        sensor_info[row["plot_id"]] = row

# ── 1) False-color satellite image (heatmap) ─────────────────
st.markdown("### False-Color Satellite Image")
st.caption(f"Date: {selected_date}  |  Layer: {color_by}")

use_fhi = color_by == "Forest Health Index"
grid_data = fhi_grid if use_fhi else ndvi_grid
colorscale = "RdYlGn"
zmin, zmax = (0, 100) if use_fhi else (0.2, 0.85)
label = "FHI" if use_fhi else "NDVI"

# Build annotations with plot_id + value
annotations = []
for r in range(NROWS):
    for c in range(NCOLS):
        pid = f"plot_{r+1:02d}_{c+1:02d}"
        val = grid_data[r][c]
        val_str = f"{val:.0f}" if use_fhi else (f"{val:.2f}" if not np.isnan(val) else "—")
        text = f"<b>{pid}</b><br>{label}: {val_str}"
        if pid in sensor_info:
            si = sensor_info[pid]
            text += f"<br>🌡{si['temp']:.1f}°C 💧{si['sm']:.1f}%"
            if si["anom"] > 0 and show_anomalies:
                text += f"<br>⚠️ {int(si['anom'])} anomalies"
        annotations.append(
            dict(
                x=c, y=r,
                text=text,
                showarrow=False,
                font=dict(size=11, color="white"),
            )
        )

fig_sat = go.Figure(
    data=go.Heatmap(
        z=grid_data,
        colorscale=colorscale,
        zmin=zmin,
        zmax=zmax,
        colorbar=dict(title=label),
        hovertemplate=(
            "Row %{y}  Col %{x}<br>"
            + label
            + ": %{z:.2f}<extra></extra>"
        ),
    )
)
fig_sat.update_layout(
    annotations=annotations,
    xaxis=dict(
        tickmode="array",
        tickvals=list(range(NCOLS)),
        ticktext=[f"Col {c+1}" for c in range(NCOLS)],
        side="top",
    ),
    yaxis=dict(
        tickmode="array",
        tickvals=list(range(NROWS)),
        ticktext=[f"Row {r+1}" for r in range(NROWS)],
        autorange="reversed",
    ),
    height=600,
    margin=dict(t=60, b=20),
)
st.plotly_chart(fig_sat, use_container_width=True)

# ── 2) Sensor detail grid (5×5 cards) ────────────────────────
st.markdown("### Sensor Readings — 5×5 Grid")

for r in range(NROWS):
    cols = st.columns(NCOLS)
    for c in range(NCOLS):
        pid = f"plot_{r+1:02d}_{c+1:02d}"
        ndvi_val = ndvi_grid[r][c]
        fhi_val = fhi_grid[r][c]
        si = sensor_info.get(pid)

        # Card color based on NDVI
        if np.isnan(ndvi_val):
            bg = "#555"
        elif ndvi_val >= 0.6:
            bg = "#2d6a2d"
        elif ndvi_val >= 0.45:
            bg = "#8a7c23"
        else:
            bg = "#8a2323"

        with cols[c]:
            card_html = f"""
<div style="
    background:{bg}; color:white; border-radius:8px;
    padding:8px 6px; text-align:center; min-height:150px;
    font-size:12px; line-height:1.4;
    border: 1px solid rgba(255,255,255,0.15);
">
<div style="font-weight:bold; font-size:13px; margin-bottom:4px;">{pid}</div>
<div>NDVI: <b>{ndvi_val:.2f}</b></div>
<div>FHI: <b>{fhi_val:.0f}</b></div>
"""
            if si is not None:
                card_html += f"""
<hr style="border-color:rgba(255,255,255,0.3);margin:4px 0">
<div>🌡 {si['temp']:.1f}°C</div>
<div>💧 H: {si['hum']:.0f}%  SM: {si['sm']:.1f}%</div>
<div style="font-size:11px">{int(si['readings'])} readings</div>
"""
                if si["anom"] > 0 and show_anomalies:
                    card_html += f'<div style="color:#ff6b6b;font-weight:bold">⚠ {int(si["anom"])} anomalies</div>'
            card_html += "</div>"
            st.markdown(card_html, unsafe_allow_html=True)

# ── 3) Selected plot drill-down ───────────────────────────────
st.markdown("---")
st.markdown("### Plot Drill-Down")

all_plots = [f"plot_{r+1:02d}_{c+1:02d}" for r in range(NROWS) for c in range(NCOLS)]
sel_plot = st.selectbox("Select a plot to inspect", all_plots)

if sel_plot:
    r, c = _parse_rc(sel_plot)
    tab1, tab2, tab3 = st.tabs(["📈 NDVI History", "🌡 Sensor Trends", "📊 Feature Summary"])

    with tab1:
        plot_ndvi = ndvi_df[ndvi_df["plot_id"] == sel_plot].sort_values("date")
        if not plot_ndvi.empty:
            fig_n = go.Figure()
            fig_n.add_trace(go.Scatter(
                x=plot_ndvi["date"], y=plot_ndvi["ndvi"],
                mode="lines+markers", name="NDVI",
                line=dict(color="#4CAF50", width=2),
                marker=dict(size=4),
                fill="tozeroy",
                fillcolor="rgba(76,175,80,0.15)",
            ))
            if "ndvi_30d_avg" in plot_ndvi.columns:
                fig_n.add_trace(go.Scatter(
                    x=plot_ndvi["date"], y=plot_ndvi["ndvi_30d_avg"],
                    mode="lines", name="30-day avg",
                    line=dict(color="gray", dash="dash"),
                ))
            if show_anomalies and "is_ndvi_anomaly" in plot_ndvi.columns:
                anom = plot_ndvi[plot_ndvi["is_ndvi_anomaly"] == True]
                if not anom.empty:
                    fig_n.add_trace(go.Scatter(
                        x=anom["date"], y=anom["ndvi"],
                        mode="markers", name="Anomaly",
                        marker=dict(color="red", size=10, symbol="x"),
                    ))
            fig_n.update_layout(
                title=f"NDVI — {sel_plot}", yaxis_range=[0, 1], height=350,
                yaxis_title="NDVI",
            )
            st.plotly_chart(fig_n, use_container_width=True)
        else:
            st.info("No NDVI data for this plot.")

    with tab2:
        if not sensor_df.empty:
            ps = sensor_df[sensor_df["plot_id"] == sel_plot].copy()
            if not ps.empty:
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

                fig_s = make_subplots(rows=3, cols=1, shared_xaxes=True,
                                      subplot_titles=["Temperature (°C)", "Humidity (%)", "Soil Moisture (%)"],
                                      vertical_spacing=0.08)
                fig_s.add_trace(go.Scatter(x=daily["day"], y=daily["temperature"],
                                           line=dict(color="#FF6B6B"), name="Temp"), row=1, col=1)
                fig_s.add_trace(go.Scatter(x=daily["day"], y=daily["humidity"],
                                           line=dict(color="#4ECDC4"), name="Humidity"), row=2, col=1)
                fig_s.add_trace(go.Scatter(x=daily["day"], y=daily["soil_moisture"],
                                           line=dict(color="#45B7D1"), name="Soil M."), row=3, col=1)
                fig_s.update_layout(height=550, showlegend=False)
                st.plotly_chart(fig_s, use_container_width=True)
            else:
                st.info("No sensor data.")

    with tab3:
        if not feature_df.empty:
            feature_df["date"] = pd.to_datetime(feature_df["date"])
            pf = feature_df[feature_df["plot_id"] == sel_plot]
            if not pf.empty:
                latest = pf.sort_values("date").iloc[-1]
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Temperature (avg)", f"{latest.get('temperature_avg', 0):.1f}°C")
                c2.metric("Soil Moisture (avg)", f"{latest.get('soil_moisture_avg', 0):.1f}%")
                c3.metric("NDVI (latest)", f"{latest.get('ndvi', 0):.3f}" if pd.notna(latest.get("ndvi")) else "—")
                c4.metric("Anomaly 7d", int(latest.get("anomaly_count_7d", 0)))

                flags = []
                for f in ["has_disease_flag", "has_drought_flag", "has_pest_flag", "has_damage_flag"]:
                    if latest.get(f, 0) == 1:
                        flags.append(f.replace("has_", "").replace("_flag", "").title())
                if flags:
                    st.warning(f"Active flags: {', '.join(flags)}")
                else:
                    st.success("No active flags")

                st.dataframe(
                    pf.sort_values("date", ascending=False).head(30),
                    use_container_width=True, height=300,
                )
            else:
                st.info("No feature data.")

# ── 4) Summary metrics bar ───────────────────────────────────
st.markdown("---")
st.markdown("### Overall Summary")

if not plot_summary_df.empty:
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Avg NDVI", f"{ndvi_grid[~np.isnan(ndvi_grid)].mean():.3f}" if not np.all(np.isnan(ndvi_grid)) else "—")
    m2.metric("Avg FHI", f"{fhi_grid[~np.isnan(fhi_grid)].mean():.1f}" if not np.all(np.isnan(fhi_grid)) else "—")

    healthy = (plot_summary_df["latest_status"] == "Healthy").sum()
    stressed = (plot_summary_df["latest_status"].isin(["Stressed", "Critical"])).sum()
    m3.metric("Healthy Plots", f"{healthy}/{len(plot_summary_df)}")
    m4.metric("Stressed/Critical", stressed)
    m5.metric("Total Anomalies", f"{int(plot_summary_df['total_anomalies'].sum()):,}")

# ── footer ────────────────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Service UIs**\n\n"
    "- [MinIO Console](http://localhost:9001)\n"
    "- [Kafka UI](http://localhost:8080)\n"
    "- [Spark UI](http://localhost:4040) *(during jobs)*\n"
    "- [Dashboard](http://localhost:8501)\n"
)
