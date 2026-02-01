"""Airlines dashboard page for airline punctuality analysis."""
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from connect_data_warehouse import get_cached_ddb_conn


@st.cache_data
def get_available_airlines():
    """Get list of available airlines from mart_airline_punctuality."""
    conn = get_cached_ddb_conn()
    df = conn.sql(
        """
        SELECT DISTINCT airline_iata, airline_name
        FROM flights_marts.mart_airline_punctuality
        ORDER BY airline_iata
        """
    ).to_df()
    return df


@st.cache_data
def get_date_range():
    """Get min and max dates for airline punctuality mart."""
    conn = get_cached_ddb_conn()
    result = conn.sql(
        """
        SELECT
            MIN(flight_date) as min_date,
            MAX(flight_date) as max_date
        FROM flights_marts.mart_airline_punctuality
        """
    ).to_df()

    min_date = pd.to_datetime(result["min_date"][0]).date()
    max_date = pd.to_datetime(result["max_date"][0]).date()
    return min_date, max_date


@st.cache_data
def get_airline_punctuality(airline_iata, start_date, end_date, flight_type, market):
    """Fetch airline punctuality data with SQL filtering."""
    conn = get_cached_ddb_conn()

    flight_type_filter = ""
    if flight_type == "Arrivals":
        flight_type_filter = "AND flight_type = 'arrival'"
    elif flight_type == "Departures":
        flight_type_filter = "AND flight_type = 'departure'"

    market_filter = ""
    if market == "Domestic":
        market_filter = "AND is_domestic = TRUE"
    elif market == "International":
        market_filter = "AND is_domestic = FALSE"

    query = f"""
        SELECT *
        FROM flights_marts.mart_airline_punctuality
        WHERE airline_iata = '{airline_iata}'
          AND flight_date BETWEEN '{start_date}' AND '{end_date}'
          {flight_type_filter}
          {market_filter}
        ORDER BY flight_date
    """

    df = conn.sql(query).to_df()
    df["flight_date"] = pd.to_datetime(df["flight_date"]).dt.date
    return df


def create_airline_kpis(punctuality: pd.DataFrame):
    """Render KPI cards for airline punctuality."""
    def weighted_avg_delay(df):
        completed = df["completed_flights"].sum()
        if completed <= 0:
            return 0
        return (df["avg_delay_minutes"] * df["completed_flights"]).sum() / completed

    total_flights = punctuality["total_flights"].sum()
    completed = punctuality["completed_flights"].sum()
    on_time = punctuality["on_time_flights"].sum()
    delayed = punctuality["delayed_flights"].sum()
    early = punctuality["early_flights"].sum()
    cancelled = punctuality["cancelled_flights"].sum()

    on_time_pct = (on_time / completed * 100) if completed > 0 else 0
    delayed_pct = (delayed / completed * 100) if completed > 0 else 0
    early_pct = (early / completed * 100) if completed > 0 else 0
    cancelled_pct = (cancelled / total_flights * 100) if total_flights > 0 else 0
    avg_delay = weighted_avg_delay(punctuality)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Flights", f"{int(total_flights):,}")
    with col2:
        st.metric("On-Time %", f"{on_time_pct:.1f}%")
    with col3:
        st.metric("Cancelled %", f"{cancelled_pct:.1f}%")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Delayed %", f"{delayed_pct:.1f}%")
    with col2:
        st.metric("Early %", f"{early_pct:.1f}%")
    with col3:
        st.metric("Avg Delay", f"{avg_delay:.1f} min")


def create_airline_trend_chart(punctuality: pd.DataFrame):
    """Line chart for on-time performance trend."""
    daily = (
        punctuality.groupby("flight_date")
        .agg(on_time_flights=("on_time_flights", "sum"), completed_flights=("completed_flights", "sum"))
        .reset_index()
        .sort_values("flight_date")
    )
    daily["on_time_pct"] = (
        daily["on_time_flights"] / daily["completed_flights"] * 100
    ).round(1)

    fig = px.line(
        daily,
        x="flight_date",
        y="on_time_pct",
        title="On-Time Performance Trend",
        labels={"flight_date": "Date", "on_time_pct": "On-Time %"},
        markers=True,
    )
    fig.update_layout(height=380, yaxis_range=[0, 105], hovermode="x unified")
    st.plotly_chart(fig, width="stretch")


def main():
    st.set_page_config(page_title="Airlines", layout="wide")
    st.title("✈️ Airlines")

    with st.sidebar:
        st.header("Filters")
        airlines = get_available_airlines()
        airline_labels = airlines.apply(
            lambda row: f"{row['airline_iata']} — {row['airline_name']}" if pd.notna(row["airline_name"]) else row["airline_iata"],
            axis=1,
        )
        airline_lookup = dict(zip(airline_labels, airlines["airline_iata"]))

        selected_airline_label = st.selectbox("Select Airline", list(airline_labels))
        selected_airline = airline_lookup[selected_airline_label]

        min_date, max_date = get_date_range()
        st.caption(f"📅 Data available: {min_date.strftime('%b %d, %Y')} to {max_date.strftime('%b %d, %Y')}")
        date_range = st.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        flight_type = st.radio(
            "Flight Type",
            options=["All", "Arrivals", "Departures"],
            horizontal=True,
        )
        market = st.radio(
            "Market",
            options=["All", "Domestic", "International"],
            horizontal=True,
        )

        if st.button("🔄 Refresh Data", help="Clear cache and reload data"):
            st.cache_data.clear()
            st.rerun()

    if not (isinstance(date_range, tuple) and len(date_range) == 2):
        st.info("📅 Please select both start and end dates to display airline metrics")
        return

    start_date, end_date = date_range
    punct = get_airline_punctuality(selected_airline, start_date, end_date, flight_type, market)

    if punct.empty:
        st.warning("No airline data found for selected filters")
        return

    st.markdown(f"### {selected_airline_label} — {start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}")
    
    st.subheader("Summary KPIs")
    create_airline_kpis(punct)
    
    st.divider()
    
    st.subheader("On-Time Performance Trend")
    st.markdown("Track airline punctuality over time")
    create_airline_trend_chart(punct)


if __name__ == "__main__":
    main()
