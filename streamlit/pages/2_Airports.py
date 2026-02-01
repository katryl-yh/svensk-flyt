"""Airport Overview dashboard page with operational metrics."""
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

from connect_data_warehouse import get_cached_ddb_conn


@st.cache_data
def get_available_airports():
    """Get list of unique airports from data."""
    conn = get_cached_ddb_conn()
    df = conn.sql(
        "SELECT DISTINCT airport_iata FROM flights_marts.mart_airport_hourly_traffic ORDER BY airport_iata"
    ).to_df()
    return sorted(df["airport_iata"].tolist())


@st.cache_data
def get_airport_names():
    """Get airport names for display."""
    conn = get_cached_ddb_conn()
    df = conn.sql("SELECT airport_iata, airport_name FROM flights_dimensions.dim_airport").to_df()
    return df.set_index("airport_iata")["airport_name"].to_dict()


@st.cache_data
def get_hourly_traffic(airport, start_date, end_date, flight_type):
    """Get hourly traffic data filtered by user selections."""
    conn = get_cached_ddb_conn()
    
    flight_type_filter = ""
    if flight_type == "Arrivals":
        flight_type_filter = "AND flight_type = 'arrival'"
    elif flight_type == "Departures":
        flight_type_filter = "AND flight_type = 'departure'"
    
    query = f"""
        SELECT * 
        FROM flights_marts.mart_airport_hourly_traffic
        WHERE airport_iata = '{airport}'
          AND flight_date BETWEEN '{start_date}' AND '{end_date}'
          {flight_type_filter}
        ORDER BY flight_date, flight_time_period
    """
    
    df = conn.sql(query).to_df()
    df["flight_date"] = pd.to_datetime(df["flight_date"]).dt.date
    return df


@st.cache_data
def get_punctuality(airport, start_date, end_date, flight_type):
    """Get punctuality data filtered by user selections."""
    conn = get_cached_ddb_conn()
    
    flight_type_filter = ""
    if flight_type == "Arrivals":
        flight_type_filter = "AND flight_type = 'arrival'"
    elif flight_type == "Departures":
        flight_type_filter = "AND flight_type = 'departure'"
    
    query = f"""
        SELECT * 
        FROM flights_marts.mart_airport_punctuality
        WHERE airport_iata = '{airport}'
          AND flight_date BETWEEN '{start_date}' AND '{end_date}'
          {flight_type_filter}
        ORDER BY flight_date
    """
    
    df = conn.sql(query).to_df()
    df["flight_date"] = pd.to_datetime(df["flight_date"]).dt.date
    return df


def filter_data(hourly_traffic, punctuality, airport, date_range, flight_type):
    """
    Filter datasets based on user selections (deprecated - using SQL instead).
    Kept for compatibility but now functions are done via SQL queries above.
    """
    return hourly_traffic, punctuality


def create_summary_cards(hourly_traffic, punctuality):
    """Create 3 rows of summary KPI cards (Total, Domestic, International)."""
    def weighted_avg_delay(df):
        completed = df["completed_flights"].sum()
        if completed <= 0:
            return 0
        return (df["avg_delay_minutes"] * df["completed_flights"]).sum() / completed

    total_movements = hourly_traffic["flight_count"].sum()
    total_completed = punctuality["completed_flights"].sum()
    total_on_time = punctuality["on_time_flights"].sum()
    total_on_time_pct = (total_on_time / total_completed * 100) if total_completed > 0 else 0
    total_avg_delay = weighted_avg_delay(punctuality)

    domestic_hourly = hourly_traffic.copy()
    domestic_punct = punctuality[punctuality["is_domestic"] == True]
    domestic_movements = domestic_hourly["domestic_flights"].sum()
    domestic_completed = domestic_punct["completed_flights"].sum()
    domestic_on_time = domestic_punct["on_time_flights"].sum()
    domestic_on_time_pct = (domestic_on_time / domestic_completed * 100) if domestic_completed > 0 else 0
    domestic_avg_delay = weighted_avg_delay(domestic_punct)

    intl_hourly = hourly_traffic.copy()
    intl_punct = punctuality[punctuality["is_domestic"] == False]
    intl_movements = intl_hourly["international_flights"].sum()
    intl_completed = intl_punct["completed_flights"].sum()
    intl_on_time = intl_punct["on_time_flights"].sum()
    intl_on_time_pct = (intl_on_time / intl_completed * 100) if intl_completed > 0 else 0
    intl_avg_delay = weighted_avg_delay(intl_punct)

    # Row 1: Total
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Total Movements", value=f"{int(total_movements):,}")
    with col2:
        st.metric(label="On-Time Performance", value=f"{total_on_time_pct:.1f}%")
    with col3:
        st.metric(label="Avg Delay", value=f"{total_avg_delay:.1f} min")

    # Row 2: Domestic
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Domestic Movements", value=f"{int(domestic_movements):,}")
    with col2:
        st.metric(label="Domestic On-Time", value=f"{domestic_on_time_pct:.1f}%")
    with col3:
        st.metric(label="Domestic Avg Delay", value=f"{domestic_avg_delay:.1f} min")

    # Row 3: International
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="International Movements", value=f"{int(intl_movements):,}")
    with col2:
        st.metric(label="International On-Time", value=f"{intl_on_time_pct:.1f}%")
    with col3:
        st.metric(label="International Avg Delay", value=f"{intl_avg_delay:.1f} min")


def create_raw_data_summary_table(flight_type):
    """Create a raw data summary table for all airports."""
    conn = get_cached_ddb_conn()
    
    # Build WHERE clause based on flight_type
    if flight_type == "Arrivals":
        ft_where = "AND flight_type = 'arrival'"
    elif flight_type == "Departures":
        ft_where = "AND flight_type = 'departure'"
    else:
        ft_where = ""
    
    query = f"""
        WITH movements AS (
            SELECT airport_iata, SUM(flight_count) as total_movements
            FROM flights_marts.mart_airport_hourly_traffic
            WHERE 1=1 {ft_where}
            GROUP BY airport_iata
        ),
        punctual AS (
            SELECT 
                airport_iata,
                SUM(on_time_flights) as on_time_flights,
                SUM(completed_flights) as completed_flights,
                AVG(avg_delay_minutes) as avg_delay_minutes
            FROM flights_marts.mart_airport_punctuality
            WHERE 1=1 {ft_where}
            GROUP BY airport_iata
        )
        SELECT 
            m.airport_iata,
            a.airport_name,
            m.total_movements,
            (p.on_time_flights / NULLIF(p.completed_flights, 0) * 100) as on_time_performance,
            p.avg_delay_minutes
        FROM movements m
        LEFT JOIN punctual p ON m.airport_iata = p.airport_iata
        LEFT JOIN flights_dimensions.dim_airport a ON m.airport_iata = a.airport_iata
        ORDER BY total_movements DESC
    """
    
    df = conn.sql(query).to_df()
    
    if df.empty:
        st.warning("No data available for selected filters")
        return
    
    # Format for display
    df["Airport"] = df.apply(
        lambda row: f"{row['airport_iata']} — {row['airport_name']}" 
        if pd.notna(row["airport_name"]) else row["airport_iata"],
        axis=1
    )
    
    summary = df[
        ["Airport", "total_movements", "on_time_performance", "avg_delay_minutes"]
    ].rename(
        columns={
            "total_movements": "Total Movements",
            "on_time_performance": "On-Time Performance (%)",
            "avg_delay_minutes": "Avg Delay (min)",
        }
    )
    
    st.dataframe(
        summary,
        use_container_width=True,
        hide_index=True,
    )


def create_hourly_distribution_chart(hourly_traffic):
    """Create horizontal bar chart for peak hours analysis."""
    if hourly_traffic.empty:
        st.warning("No data available for selected filters")
        return
    
    # Aggregate by time period
    time_period_agg = (
        hourly_traffic.groupby("flight_time_period")
        .agg({
            "flight_count": "sum",
            "domestic_flights": "sum",
            "international_flights": "sum",
            "avg_delay_minutes": "mean"
        })
        .reset_index()
        .sort_values("flight_count", ascending=True)
    )
    
    # Define time period order for proper visualization (full labels)
    time_period_order = [
        "Morning (06:00-11:59)",
        "Midday/Afternoon (12:00-16:59)",
        "Evening (17:00-21:59)",
        "Night/Red-eye (22:00-05:59)",
    ]
    time_period_agg["flight_time_period"] = pd.Categorical(
        time_period_agg["flight_time_period"],
        categories=time_period_order,
        ordered=True
    )
    time_period_agg = time_period_agg.sort_values("flight_time_period")
    
    fig = px.bar(
        time_period_agg,
        y="flight_time_period",
        x="flight_count",
        orientation="h",
        title="Peak Hours Distribution",
        labels={
            "flight_count": "Number of Flights",
            "flight_time_period": "Time Period"
        },
        color="avg_delay_minutes",
        color_continuous_scale="Blues",
        hover_data={
            "flight_count": ":,.0f",
            "avg_delay_minutes": ":.1f",
        }
    )
    
    fig.update_layout(
        height=400,
        showlegend=True,
        coloraxis_colorbar=dict(title="Avg Delay (min)")
    )
    
    st.plotly_chart(fig, use_container_width=True)


def create_ontime_trend_chart(punctuality):
    """Create line chart for on-time performance trend."""
    if punctuality.empty:
        st.warning("No data available for selected filters")
        return
    
    # Aggregate by date
    daily_agg = (
        punctuality.groupby("flight_date")
        .agg({
            "on_time_flights": "sum",
            "completed_flights": "sum"
        })
        .reset_index()
        .sort_values("flight_date")
    )
    
    daily_agg["on_time_pct"] = (
        daily_agg["on_time_flights"] / daily_agg["completed_flights"] * 100
    ).round(1)
    
    fig = px.line(
        daily_agg,
        x="flight_date",
        y="on_time_pct",
        title="On-Time Performance Trend",
        labels={
            "flight_date": "Date",
            "on_time_pct": "On-Time %"
        },
        markers=True
    )
    
    # Add reference line at 90% (industry standard)
    fig.add_hline(
        y=90,
        line_dash="dash",
        line_color="green",
        annotation_text="Target: 90%",
        annotation_position="right"
    )
    
    # Style the chart
    fig.update_layout(
        height=400,
        hovermode="x unified",
        yaxis_range=[0, 105]
    )
    
    fig.update_traces(
        line=dict(color="#1f77b4", width=2),
        marker=dict(size=6)
    )
    
    st.plotly_chart(fig, use_container_width=True)


def get_airport_full_name(airport_iata, airport_names):
    """Return full airport name for a given IATA code, if available."""
    name = airport_names.get(airport_iata)
    if name:
        return f"{airport_iata} — {name}"
    return airport_iata


def get_date_range():
    """Get min and max dates from the warehouse."""
    conn = get_cached_ddb_conn()
    result = conn.sql(
        """
        SELECT 
            MIN(flight_date) as min_date,
            MAX(flight_date) as max_date
        FROM flights_marts.mart_airport_hourly_traffic
        """
    ).to_df()
    
    min_date = pd.to_datetime(result["min_date"][0]).date()
    max_date = pd.to_datetime(result["max_date"][0]).date()
    return min_date, max_date


def main():
    st.set_page_config(page_title="Airport Overview", layout="wide")
    st.title("✈️ Airport Overview")
    
    # ===== SIDEBAR FILTERS =====
    with st.sidebar:
        st.header("Filters")
        
        # Get available data
        available_airports = get_available_airports()
        airport_names = get_airport_names()
        min_date, max_date = get_date_range()
        
        # Filter 1: Airport selector
        selected_airport = st.selectbox(
            label="Select Airport",
            options=available_airports,
            help="Choose an airport to analyze"
        )
        
        # Filter 2: Date range
        st.caption(f"📅 Data available: {min_date.strftime('%b %d, %Y')} to {max_date.strftime('%b %d, %Y')}")
        
        date_range = st.date_input(
            label="Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            help="Filter data by date range (defaults to all available data)"
        )
        
        # Ensure we have a valid date range tuple
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = end_date = date_range
        
        # Filter 3: Flight type
        flight_type = st.radio(
            label="Flight Type",
            options=["All", "Arrivals", "Departures"],
            horizontal=True,
            help="Filter by flight direction"
        )
        
        # Clear cache button (for development/refresh)
        if st.button("🔄 Refresh Data", help="Clear cache and reload data"):
            st.cache_data.clear()
            st.rerun()
    
    # ===== FETCH FILTERED DATA VIA SQL =====
    # Check if we have a valid date range (both start and end dates selected)
    if not (isinstance(date_range, tuple) and len(date_range) == 2):
        st.info("📅 Please select both start and end dates to display the dashboard")
        return
    
    hourly_filtered = get_hourly_traffic(selected_airport, start_date, end_date, flight_type)
    punct_filtered = get_punctuality(selected_airport, start_date, end_date, flight_type)
    
    if hourly_filtered.empty or punct_filtered.empty:
        st.warning(f"No data found for {selected_airport} with selected filters")
        return
    
    # ===== MAIN CONTENT =====
    airport_display = get_airport_full_name(selected_airport, airport_names)
    st.markdown(f"### {airport_display} — {start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}")
    
    # KPI Cards
    st.subheader("Summary KPIs")
    create_summary_cards(hourly_filtered, punct_filtered)
    
    # Chart 1: Peak Hours Distribution
    st.subheader("1. Peak Hours Distribution")
    st.markdown("Identify operational bottlenecks by analyzing flight distribution across different times of day")
    create_hourly_distribution_chart(hourly_filtered)
    
    st.divider()
    
    # Chart 2: On-Time Performance Trend
    st.subheader("2. On-Time Performance Trend")
    st.markdown("Track punctuality performance over time with 90% industry standard reference line")
    create_ontime_trend_chart(punct_filtered)

    st.divider()

    # Raw data summary table
    st.subheader("Raw Data Summary")
    st.markdown("Summary of Total Movements, On-Time Performance, and Avg Delay for all airports")
    create_raw_data_summary_table(flight_type)
    


if __name__ == "__main__":
    main()
