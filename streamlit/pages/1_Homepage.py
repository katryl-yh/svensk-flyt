"""Homepage for Svensk Flyt Dashboard."""
import streamlit as st


st.set_page_config(
    page_title="Svensk Flyt Dashboard",
    page_icon="✈️",
    layout="wide",
)

st.title("Welcome to Svensk Flyt Dashboard")
st.markdown("""
This dashboard visualizes key operational metrics from Swedish aviation data, 
covering 10 Swedish airports with real-time flight information.
""")

st.markdown("---")

# Airports Page
st.header("✈️ Airports")
st.markdown("""
Analyze individual airport performance with comprehensive operational metrics:

**Summary KPIs** (Total, Domestic, International breakdown):
- Total Movements | On-Time Performance % | Average Delay

**Visualizations:**
- **Peak Hours Distribution**: Flight volume by time period with delay indicators
- **On-Time Performance Trend**: Daily punctuality tracking with 90% target reference line
- **Raw Data Summary Table**: Cross-airport comparison of movements and performance
""")

st.markdown("---")

# Airlines Page
st.header("🛫 Airlines")
st.markdown("""
Track airline-specific performance and punctuality metrics:

**Summary KPIs**:
- Total Flights | On-Time % | Cancelled %
- Delayed % | Early % | Average Delay

**Visualizations:**
- **On-Time Performance Trend**: Daily airline punctuality over time

**Filters**: Flight Type (Arrivals/Departures), Market (Domestic/International)
""")

st.markdown("---")

# Routes Page
st.header("🗺️ Routes")
st.markdown("""
Explore route popularity and traffic patterns for each airport:

**Summary KPIs**:
- Total Flights | Unique Routes | Cancellation Rate | Avg Airlines per Day

**Route Analysis** (Domestic & International):
- **Top 10 Destinations**: Most popular departure destinations from selected airport
- **Top 10 Origins**: Most frequent arrival origins to selected airport
- **Route Traffic Trend**: Daily flight volume by direction (arrivals vs departures)

**Metrics per Route**: Flight count, Cancelled flights, Days with service, Average airlines
""")

st.markdown("---")

st.info("👈 Use the sidebar navigation to explore detailed analytics for Airports, Airlines, and Routes.")
