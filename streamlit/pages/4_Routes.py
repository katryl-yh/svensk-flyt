"""Route Popularity dashboard page for analyzing top destinations."""
import pandas as pd
import plotly.express as px
import streamlit as st

from connect_data_warehouse import get_cached_ddb_conn


@st.cache_data
def get_available_airports():
    """Get list of airports used in route popularity."""
    conn = get_cached_ddb_conn()
    df = conn.sql(
        """
        SELECT DISTINCT airport_iata
        FROM flights_marts.mart_route_popularity
        ORDER BY airport_iata
        """
    ).to_df()
    return df["airport_iata"].tolist()


@st.cache_data
def get_airport_name(airport_iata):
    """Get airport name for a given IATA code."""
    conn = get_cached_ddb_conn()
    result = conn.sql(
        f"""
        SELECT airport_name
        FROM flights_dimensions.dim_airport
        WHERE airport_iata = '{airport_iata}'
        LIMIT 1
        """
    ).to_df()
    
    if not result.empty and pd.notna(result["airport_name"].iloc[0]):
        return result["airport_name"].iloc[0]
    return None


def get_date_range():
    """Get min and max dates for route popularity mart."""
    conn = get_cached_ddb_conn()
    result = conn.sql(
        """
        SELECT
            MIN(flight_date) as min_date,
            MAX(flight_date) as max_date
        FROM flights_marts.mart_route_popularity
        """
    ).to_df()

    min_date = pd.to_datetime(result["min_date"][0]).date()
    max_date = pd.to_datetime(result["max_date"][0]).date()
    return min_date, max_date


@st.cache_data
def get_top_departures(airport_iata, start_date, end_date, market, limit=10):
    """Get top destinations FROM selected airport (departures) by market."""
    conn = get_cached_ddb_conn()
    
    market_filter = ""
    if market == "Domestic":
        market_filter = "AND is_domestic = 1"
    elif market == "International":
        market_filter = "AND is_domestic = 0"
    
    query = f"""
        SELECT
            r.other_airport_iata as airport_iata,
            a.airport_name,
            SUM(r.flight_count) as total_flights,
            SUM(r.cancelled_flights) as cancelled_flights,
            COUNT(DISTINCT r.flight_date) as days_with_service,
            AVG(r.unique_airlines) as avg_airlines_per_day
        FROM flights_marts.mart_route_popularity r
        LEFT JOIN flights_dimensions.dim_airport a ON r.other_airport_iata = a.airport_iata
        WHERE r.airport_iata = '{airport_iata}'
          AND r.flight_date BETWEEN '{start_date}' AND '{end_date}'
          AND r.flight_type = 'departure'
          AND r.flight_count > 0
          {market_filter}
        GROUP BY r.other_airport_iata, a.airport_name
        HAVING SUM(r.flight_count) > 0
        ORDER BY total_flights DESC
        LIMIT {limit}
    """
    
    df = conn.sql(query).to_df()
    df["destination"] = df.apply(
        lambda row: f"{row['airport_iata']} — {row['airport_name']}" if pd.notna(row["airport_name"]) else row["airport_iata"],
        axis=1
    )
    return df


@st.cache_data
def get_top_arrivals(airport_iata, start_date, end_date, market, limit=10):
    """Get top origins TO selected airport (arrivals) by market."""
    conn = get_cached_ddb_conn()
    
    market_filter = ""
    if market == "Domestic":
        market_filter = "AND is_domestic = 1"
    elif market == "International":
        market_filter = "AND is_domestic = 0"
    
    query = f"""
        SELECT
            r.other_airport_iata as airport_iata,
            a.airport_name,
            SUM(r.flight_count) as total_flights,
            SUM(r.cancelled_flights) as cancelled_flights,
            COUNT(DISTINCT r.flight_date) as days_with_service,
            AVG(r.unique_airlines) as avg_airlines_per_day
        FROM flights_marts.mart_route_popularity r
        LEFT JOIN flights_dimensions.dim_airport a ON r.other_airport_iata = a.airport_iata
        WHERE r.airport_iata = '{airport_iata}'
          AND r.flight_date BETWEEN '{start_date}' AND '{end_date}'
          AND r.flight_type = 'arrival'
          AND r.flight_count > 0
          {market_filter}
        GROUP BY r.other_airport_iata, a.airport_name
        HAVING SUM(r.flight_count) > 0
        ORDER BY total_flights DESC
        LIMIT {limit}
    """
    
    df = conn.sql(query).to_df()
    df["origin"] = df.apply(
        lambda row: f"{row['airport_iata']} — {row['airport_name']}" if pd.notna(row["airport_name"]) else row["airport_iata"],
        axis=1
    )
    return df


@st.cache_data
def get_route_trend(airport_iata, start_date, end_date):
    """Get daily route traffic trend for both arrivals and departures."""
    conn = get_cached_ddb_conn()
    
    query = f"""
        SELECT
            flight_date,
            flight_type,
            SUM(flight_count) as total_flights,
            COUNT(DISTINCT other_airport_iata) as unique_destinations
        FROM flights_marts.mart_route_popularity
        WHERE airport_iata = '{airport_iata}'
          AND flight_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY flight_date, flight_type
        ORDER BY flight_date
    """
    
    df = conn.sql(query).to_df()
    df["flight_date"] = pd.to_datetime(df["flight_date"]).dt.date
    return df


def create_route_kpis(dep_domestic: pd.DataFrame, dep_international: pd.DataFrame, arr_domestic: pd.DataFrame, arr_international: pd.DataFrame):
    """Display summary KPI cards."""
    total_flights = (
        dep_domestic["total_flights"].sum() + 
        dep_international["total_flights"].sum() + 
        arr_domestic["total_flights"].sum() + 
        arr_international["total_flights"].sum()
    )
    
    total_cancelled = (
        dep_domestic["cancelled_flights"].sum() + 
        dep_international["cancelled_flights"].sum() + 
        arr_domestic["cancelled_flights"].sum() + 
        arr_international["cancelled_flights"].sum()
    )
    cancellation_rate = (total_cancelled / total_flights * 100) if total_flights > 0 else 0
    
    unique_routes = len(dep_domestic) + len(dep_international) + len(arr_domestic) + len(arr_international)
    
    all_data = pd.concat([dep_domestic, dep_international, arr_domestic, arr_international], ignore_index=True)
    avg_airlines = all_data["avg_airlines_per_day"].mean() if not all_data.empty else 0
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Flights", f"{int(total_flights):,}")
    with col2:
        st.metric("Unique Routes", f"{int(unique_routes)}")
    with col3:
        st.metric("Cancellation Rate", f"{cancellation_rate:.1f}%")
    with col4:
        st.metric("Avg Airlines/Day", f"{avg_airlines:.1f}")


def create_departures_chart(departures: pd.DataFrame, market: str):
    """Bar chart of top departure destinations."""
    fig = px.bar(
        departures,
        x="total_flights",
        y="destination",
        orientation="h",
        title=f"Top 10 {market} Destinations FROM Airport (Departures)",
        labels={"total_flights": "Total Flights", "destination": "Destination"},
        color_discrete_sequence=["#1f77b4"],
        hover_data={
            "total_flights": ":,.0f",
            "cancelled_flights": ":,.0f",
            "days_with_service": True,
            "avg_airlines_per_day": ":.1f",
        }
    )
    fig.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, width="stretch")


def create_arrivals_chart(arrivals: pd.DataFrame, market: str):
    """Bar chart of top arrival origins."""
    fig = px.bar(
        arrivals,
        x="total_flights",
        y="origin",
        orientation="h",
        title=f"Top 10 {market} Origins TO Airport (Arrivals)",
        labels={"total_flights": "Total Flights", "origin": "Origin"},
        color_discrete_sequence=["#ff7f0e"],
        hover_data={
            "total_flights": ":,.0f",
            "cancelled_flights": ":,.0f",
            "days_with_service": True,
            "avg_airlines_per_day": ":.1f",
        }
    )
    fig.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, width="stretch")


def create_trend_chart(trend: pd.DataFrame):
    """Line chart showing route traffic over time by direction."""
    fig = px.line(
        trend,
        x="flight_date",
        y="total_flights",
        color="flight_type",
        title="Daily Route Traffic by Direction",
        labels={
            "flight_date": "Date", 
            "total_flights": "Total Flights",
            "flight_type": "Direction"
        },
        markers=True,
        color_discrete_map={"arrival": "#1f77b4", "departure": "#ff7f0e"}
    )
    fig.update_layout(height=380, hovermode="x unified")
    st.plotly_chart(fig, width="stretch")


def main():
    st.set_page_config(page_title="Routes", layout="wide")
    st.title("🛫 Route Popularity")
    
    with st.sidebar:
        st.header("Filters")
        
        airports = get_available_airports()
        
        # Create airport labels with full names
        airport_labels = []
        airport_lookup = {}
        for code in airports:
            name = get_airport_name(code)
            if name:
                label = f"{code} — {name}"
            else:
                label = code
            airport_labels.append(label)
            airport_lookup[label] = code
        
        selected_airport_label = st.selectbox("Select Airport", airport_labels)
        selected_airport = airport_lookup[selected_airport_label]
        
        min_date, max_date = get_date_range()
        st.caption(f"📅 Data available: {min_date.strftime('%b %d, %Y')} to {max_date.strftime('%b %d, %Y')}")
        
        date_range = st.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        
        if st.button("🔄 Refresh Data", help="Clear cache and reload data"):
            st.cache_data.clear()
            st.rerun()
    
    if not (isinstance(date_range, tuple) and len(date_range) == 2):
        st.info("📅 Please select both start and end dates to display route data")
        return
    
    start_date, end_date = date_range
    
    # Fetch data for both domestic and international
    dep_domestic = get_top_departures(selected_airport, start_date, end_date, "Domestic")
    dep_international = get_top_departures(selected_airport, start_date, end_date, "International")
    arr_domestic = get_top_arrivals(selected_airport, start_date, end_date, "Domestic")
    arr_international = get_top_arrivals(selected_airport, start_date, end_date, "International")
    
    if dep_domestic.empty and dep_international.empty and arr_domestic.empty and arr_international.empty:
        st.warning(f"No route data found for {selected_airport} with selected filters")
        return
    
    # Get airport name for header
    airport_name = get_airport_name(selected_airport)
    if airport_name:
        airport_display = f"{selected_airport} — {airport_name}"
    else:
        airport_display = selected_airport
    
    st.markdown(f"### {airport_display} — {start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}")
    
    st.subheader("Summary KPIs")
    create_route_kpis(dep_domestic, dep_international, arr_domestic, arr_international)
    
    st.divider()
    
    # Domestic routes section
    st.subheader("Domestic Routes")
    col1, col2 = st.columns(2)
    
    with col1:
        if not dep_domestic.empty:
            create_departures_chart(dep_domestic, "Domestic")
        else:
            st.info("No domestic departure data available")
    
    with col2:
        if not arr_domestic.empty:
            create_arrivals_chart(arr_domestic, "Domestic")
        else:
            st.info("No domestic arrival data available")
    
    st.divider()
    
    # International routes section
    st.subheader("International Routes")
    if dep_international.empty and arr_international.empty:
        st.warning(
            "⚠️ No international route data available. The current mart_route_popularity only contains domestic flights "
            "(is_domestic = True). International routes may need to be added to the data model."
        )
    else:
        col1, col2 = st.columns(2)
        
        with col1:
            if not dep_international.empty:
                create_departures_chart(dep_international, "International")
            else:
                st.info("No international departure data available")
        
        with col2:
            if not arr_international.empty:
                create_arrivals_chart(arr_international, "International")
            else:
                st.info("No international arrival data available")
    
    st.divider()
    
    st.subheader("Route Traffic Trend")
    st.markdown("Daily flight volume for departures and arrivals")
    trend = get_route_trend(selected_airport, start_date, end_date)
    if not trend.empty:
        create_trend_chart(trend)
    else:
        st.warning("No trend data available")


if __name__ == "__main__":
    main()
