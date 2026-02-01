"""Main Streamlit application - Homepage."""
import streamlit as st


st.set_page_config(
    page_title="Svensk Flyt Dashboard",
    page_icon="✈️",
    layout="wide",
)

st.title("Welcome to Svensk Flyt Dashboard")
st.markdown("""
This dashboard visualizes key metrics from Swedish aviation data operations.
""")

st.markdown("---")

# Airports Page
st.header("Airports")
st.markdown("""
Comprehensive airport operational analysis including:

- **Airport Punctuality**: On-time performance, delays, and completion rates by airport
  - Metrics: On-time %, Delay %, Cancellation %, Avg Delay Minutes

- **Hourly Traffic**: Real-time traffic patterns and peak hour analysis
  - Metrics: Flight counts, Domestic vs International, Unique airlines, Avg delays by hour

- **Baggage Performance**: Baggage handling efficiency and performance metrics
  - Metrics: Baggage handling times, Lost/Damaged bags, Processing efficiency
""")

st.markdown("---")

# Airlines Page
st.header("Airlines")
st.markdown("""
Airline performance and route analysis:

- **Airline Punctuality**: Operator-specific performance metrics
  - Metrics: On-time %, Delay %, Cancellation %, Avg Delay Minutes, Early arrivals

- **Route Popularity**: Traffic distribution and route performance
  - Metrics: Flight volume, Popular routes, Seasonal trends, Airline market share
""")

st.markdown("---")

st.info("👈 Use the sidebar to navigate to the Airports and Airlines pages to explore detailed analytics.")
