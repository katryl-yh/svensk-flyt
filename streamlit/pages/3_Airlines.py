"""Airlines dashboard page with multiple tabs."""
import streamlit as st


def main():
    st.title("Airlines")
    
    # Create tabs for different airline-related analyses
    tab1, tab2 = st.tabs([
        "Airline Punctuality",
        "Route Popularity"
    ])
    
    with tab1:
        st.header("Airline Punctuality")
        # TODO: Load data from mart_airline_punctuality
        # from connect_data_warehouse import get_cached_ddb_df
        # df = get_cached_ddb_df("mart_airline_punctuality")
        st.write("Airline punctuality metrics will be displayed here.")
    
    with tab2:
        st.header("Route Popularity")
        # TODO: Load data from mart_route_popularity
        # from connect_data_warehouse import get_cached_ddb_df
        # df = get_cached_ddb_df("mart_route_popularity")
        st.write("Route popularity analysis will be displayed here.")


if __name__ == "__main__":
    main()
