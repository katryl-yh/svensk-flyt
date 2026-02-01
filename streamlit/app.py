"""Main Streamlit application entry point."""
import streamlit as st

# Setup streamlit pages
st.set_page_config(
    page_title="Svensk Flyt Dashboard",
    page_icon="✈️",
    layout="wide",
)

pages = {
    "": [
        st.Page("pages/1_Homepage.py", title="Home", icon="🏠"),
    ],
    "Analysis": [
        st.Page("pages/2_Airports.py", title="Airports", icon="✈️"),
        st.Page("pages/3_Airlines.py", title="Airlines", icon="🛫"),
        st.Page("pages/4_Routes.py", title="Routes", icon="🗺️"),
    ],
}

# Run navigation
pg = st.navigation(pages)
pg.run()
