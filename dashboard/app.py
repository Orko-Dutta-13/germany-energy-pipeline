"""
Germany Energy Dashboard
────────────────────────
Reads from dbt mart tables + forecast table in Postgres.
Run with: streamlit run app.py
"""

import os
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Germany Energy Pipeline",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── DB connection ──────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://airflow:airflow@postgres:5432/airflow"
    )
    return create_engine(db_url)


def query(sql: str) -> pd.DataFrame:
    try:
        with get_engine().connect() as conn:
            return pd.read_sql(text(sql), conn)
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.title("⚡ Germany Energy")
st.sidebar.markdown("**Data source:** SMARD (Bundesnetzagentur)")
st.sidebar.markdown("**Pipeline:** Airflow → Postgres → dbt → Prophet")
st.sidebar.divider()

days_back = st.sidebar.slider("Days of history to show", 7, 90, 30)
st.sidebar.divider()

# Data freshness indicator
freshness = query("""
    SELECT MAX(run_at) AS last_ingest
    FROM raw_energy.ingestion_log
    WHERE status = 'success'
""")
if not freshness.empty and freshness["last_ingest"].iloc[0]:
    last = pd.to_datetime(freshness["last_ingest"].iloc[0])
    st.sidebar.success(f"Last ingestion:\n{last.strftime('%Y-%m-%d %H:%M')} UTC")
else:
    st.sidebar.warning("No ingestion data yet")

# ── Title ──────────────────────────────────────────────────────────────────────
st.title("⚡ Germany Energy Dashboard")
st.markdown(
    "Real-time view of Germany's electricity generation, consumption, "
    "renewable share, and 7-day demand forecast — powered by the SMARD public API."
)

# ── Top KPI row ────────────────────────────────────────────────────────────────
kpi = query(f"""
    SELECT
        date_berlin,
        total_mwh,
        peak_mwh,
        avg_hourly_mwh,
        load_factor
    FROM analytics.mart_daily_demand
    WHERE is_incomplete_day = FALSE
      AND date_berlin >= CURRENT_DATE - INTERVAL '{days_back} days'
    ORDER BY date_berlin DESC
    LIMIT 1
""")

renewable = query(f"""
    SELECT renewable_pct, renewable_demand_coverage_pct
    FROM analytics.mart_renewable_share
    WHERE date_berlin >= CURRENT_DATE - INTERVAL '{days_back} days'
    ORDER BY date_berlin DESC
    LIMIT 1
""")

if not kpi.empty:
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("📅 Latest Date", str(kpi["date_berlin"].iloc[0]))
    col2.metric("⚡ Total Generation", f"{kpi['total_mwh'].iloc[0]/1000:,.1f} GWh")
    col3.metric("📈 Peak (15-min)", f"{kpi['peak_mwh'].iloc[0]:,.0f} MWh")
    col4.metric("⏱ Avg Hourly", f"{kpi['avg_hourly_mwh'].iloc[0]:,.0f} MWh")
    if not renewable.empty:
        col5.metric("🌱 Renewable Share", f"{renewable['renewable_pct'].iloc[0]:.1f}%")
    else:
        col5.metric("🔄 Load Factor", f"{kpi['load_factor'].iloc[0]:.2f}")
else:
    st.info("⏳ No demand data yet. Run the ingestion DAG first.")

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "🏭 Generation Mix",
    "📊 Daily Demand",
    "🌱 Renewable Share",
    "🔮 7-Day Forecast",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Generation Mix
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Electricity Generation by Source")
    st.markdown(
        "Each energy source's contribution in MWh per day. "
        "Green = renewable, grey = fossil/conventional."
    )

    gen = query(f"""
        SELECT date_berlin, source_label, is_renewable, total_mwh
        FROM analytics.mart_generation_by_source
        WHERE date_berlin >= CURRENT_DATE - INTERVAL '{days_back} days'
        ORDER BY date_berlin, total_mwh DESC
    """)

    if gen.empty:
        st.info("⏳ No generation data yet.")
    else:
        # Stacked area chart
        color_map = {
            "Wind Offshore":   "#00B4D8",
            "Wind Onshore":    "#48CAE4",
            "Solar":           "#FFD166",
            "Biomass":         "#52B788",
            "Run-of-river":    "#74C69D",
            "Pumped storage":  "#B7E4C7",
            "Geothermal":      "#40916C",
            "Hard coal":       "#6D6875",
            "Lignite":         "#A67C52",
            "Natural gas":     "#E9C46A",
            "Nuclear":         "#E76F51",
            "Other renewables":"#95D5B2",
            "Other conventional": "#CDD3D5",
        }

        fig = px.area(
            gen,
            x="date_berlin",
            y="total_mwh",
            color="source_label",
            color_discrete_map=color_map,
            title=f"Generation Mix — Last {days_back} Days",
            labels={"total_mwh": "MWh", "date_berlin": "Date", "source_label": "Source"},
            template="plotly_dark",
        )
        fig.update_layout(height=450, legend_title="Energy Source")
        st.plotly_chart(fig, use_container_width=True)

        # Pie chart of average mix
        avg_mix = gen.groupby("source_label")["total_mwh"].mean().reset_index()
        fig2 = px.pie(
            avg_mix,
            names="source_label",
            values="total_mwh",
            title=f"Average Mix (last {days_back} days)",
            color="source_label",
            color_discrete_map=color_map,
            template="plotly_dark",
            hole=0.4,
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)

        # Raw table
        with st.expander("📋 Raw data table"):
            st.dataframe(
                gen.sort_values(["date_berlin", "total_mwh"], ascending=[False, False]),
                use_container_width=True,
            )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Daily Demand
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Daily Electricity Demand (Consumption)")

    demand = query(f"""
        SELECT
            date_berlin,
            total_mwh,
            peak_mwh,
            min_mwh,
            avg_hourly_mwh,
            load_factor
        FROM analytics.mart_daily_demand
        WHERE is_incomplete_day = FALSE
          AND date_berlin >= CURRENT_DATE - INTERVAL '{days_back} days'
        ORDER BY date_berlin
    """)

    if demand.empty:
        st.info("⏳ No demand data yet.")
    else:
        # Demand trend with peak overlay
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=demand["date_berlin"], y=demand["total_mwh"],
            mode="lines+markers", name="Total Demand (MWh)",
            line=dict(color="#00B4D8", width=2),
            fill="tozeroy", fillcolor="rgba(0,180,216,0.1)",
        ))
        fig.add_trace(go.Scatter(
            x=demand["date_berlin"], y=demand["peak_mwh"],
            mode="lines", name="Peak 15-min (MWh)",
            line=dict(color="#FFD166", width=1, dash="dot"),
        ))
        fig.update_layout(
            title=f"Daily Demand — Last {days_back} Days",
            xaxis_title="Date", yaxis_title="MWh",
            template="plotly_dark", height=400,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Load factor bar chart
        fig2 = px.bar(
            demand, x="date_berlin", y="load_factor",
            title="Load Factor (avg/peak ratio — higher = more stable demand)",
            labels={"load_factor": "Load Factor", "date_berlin": "Date"},
            template="plotly_dark", color="load_factor",
            color_continuous_scale="RdYlGn",
        )
        fig2.update_layout(height=300)
        st.plotly_chart(fig2, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Renewable Share
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Renewable Energy Share")
    st.markdown(
        "**Renewable share** = renewables / total generation.  \n"
        "**Coverage** = renewables / total demand (>100% means export surplus)."
    )

    renew = query(f"""
        SELECT
            date_berlin,
            renewable_mwh,
            conventional_mwh,
            renewable_pct,
            renewable_demand_coverage_pct
        FROM analytics.mart_renewable_share
        WHERE date_berlin >= CURRENT_DATE - INTERVAL '{days_back} days'
        ORDER BY date_berlin
    """)

    if renew.empty:
        st.info("⏳ No renewable share data yet.")
    else:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=renew["date_berlin"], y=renew["renewable_mwh"],
            name="Renewable", marker_color="#52B788",
        ))
        fig.add_trace(go.Bar(
            x=renew["date_berlin"], y=renew["conventional_mwh"],
            name="Conventional", marker_color="#6D6875",
        ))
        fig.add_trace(go.Scatter(
            x=renew["date_berlin"], y=renew["renewable_pct"],
            mode="lines+markers", name="Renewable %",
            yaxis="y2", line=dict(color="#FFD166", width=2),
        ))
        fig.update_layout(
            title=f"Generation Mix & Renewable % — Last {days_back} Days",
            barmode="stack",
            yaxis=dict(title="MWh"),
            yaxis2=dict(title="Renewable %", overlaying="y", side="right", range=[0, 105]),
            template="plotly_dark", height=450,
            legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Coverage metric
        fig2 = px.line(
            renew, x="date_berlin", y="renewable_demand_coverage_pct",
            title="Renewable Coverage of Demand (%)",
            labels={"renewable_demand_coverage_pct": "Coverage %", "date_berlin": "Date"},
            template="plotly_dark",
        )
        fig2.add_hline(y=100, line_dash="dash", line_color="red",
                       annotation_text="100% coverage (net exporter)")
        fig2.update_layout(height=300)
        st.plotly_chart(fig2, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Forecast
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("7-Day Demand Forecast (Prophet)")
    st.markdown(
        "Trained on historical daily consumption. "
        "Grey band = 90% confidence interval. "
        "Blue line = actual (fitted), orange = future prediction."
    )

    forecast = query("""
        SELECT
            forecast_date,
            predicted_mwh,
            lower_bound_mwh,
            upper_bound_mwh,
            is_future
        FROM analytics.demand_forecast
        ORDER BY forecast_date
    """)

    if forecast.empty:
        st.info(
            "⏳ No forecast yet. The forecasting DAG runs daily at 08:00. "
            "It needs at least 2 days of ingestion data. "
            "You can also trigger `smard_forecasting_dag` manually in Airflow."
        )
    else:
        historical = forecast[~forecast["is_future"]]
        future = forecast[forecast["is_future"]]

        fig = go.Figure()

        # Confidence band
        fig.add_trace(go.Scatter(
            x=pd.concat([forecast["forecast_date"], forecast["forecast_date"][::-1]]),
            y=pd.concat([forecast["upper_bound_mwh"], forecast["lower_bound_mwh"][::-1]]),
            fill="toself", fillcolor="rgba(100,100,200,0.15)",
            line=dict(color="rgba(255,255,255,0)"),
            name="90% Confidence",
            showlegend=True,
        ))
        # Historical fitted
        fig.add_trace(go.Scatter(
            x=historical["forecast_date"], y=historical["predicted_mwh"],
            mode="lines", name="Fitted (historical)",
            line=dict(color="#00B4D8", width=2),
        ))
        # Future forecast
        fig.add_trace(go.Scatter(
            x=future["forecast_date"], y=future["predicted_mwh"],
            mode="lines+markers", name="Forecast (next 7 days)",
            line=dict(color="#FF6B6B", width=3, dash="dash"),
            marker=dict(size=8, color="#FF6B6B"),
        ))

        # Vertical line at today
        today = pd.Timestamp.today().date()
        # add_vline with date strings is broken in some Plotly builds — use a shape instead
        fig.add_shape(type="line",
            x0=str(today), x1=str(today), y0=0, y1=1,
            xref="x", yref="paper",
            line=dict(color="white", width=1, dash="dot"),
        )
        fig.add_annotation(x=str(today), y=1, yref="paper",
            text="Today", showarrow=False,
            font=dict(color="white", size=11),
            xanchor="left", yanchor="bottom",
        )

        fig.update_layout(
            title="Germany Electricity Demand Forecast",
            xaxis_title="Date", yaxis_title="MWh",
            template="plotly_dark", height=500,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Future forecast table
        if not future.empty:
            st.markdown("**Next 7 days:**")
            display = future[["forecast_date", "predicted_mwh", "lower_bound_mwh", "upper_bound_mwh"]].copy()
            display.columns = ["Date", "Predicted (MWh)", "Lower bound", "Upper bound"]
            display["Predicted (MWh)"] = display["Predicted (MWh)"].apply(lambda x: f"{x:,.0f}")
            display["Lower bound"] = display["Lower bound"].apply(lambda x: f"{x:,.0f}")
            display["Upper bound"] = display["Upper bound"].apply(lambda x: f"{x:,.0f}")
            st.dataframe(display, use_container_width=True, hide_index=True)

        model_run = query("SELECT MAX(model_run_at) AS ran_at FROM analytics.demand_forecast")
        if not model_run.empty and model_run["ran_at"].iloc[0]:
            ran = pd.to_datetime(model_run["ran_at"].iloc[0])
            st.caption(f"Model last trained: {ran.strftime('%Y-%m-%d %H:%M UTC')}")

# ── Footer ─────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Data: SMARD API (Bundesnetzagentur) · "
    "Pipeline: Apache Airflow + dbt + Prophet · "
    "Dashboard: Streamlit + Plotly"
)
