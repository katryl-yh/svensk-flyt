"""Module for connecting to the DuckDB data warehouse with caching."""
import os
from pathlib import Path

import duckdb
import streamlit as st
from dotenv import load_dotenv
from pandas import DataFrame

# Load environment variables from .env file
load_dotenv()

# Path to DuckDB database file
# Default to relative path if environment variable not set
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "../data_warehouse/svenska-flyt.duckdb")

# Default schema to query (flights_marts contains transformed/aggregated data)
STREAMLIT_DEFAULT_SCHEMA = "flights_marts"


@st.cache_resource
def get_cached_ddb_conn(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """
    Returns a cached Streamlit-resource DuckDBPyConnection.

    Cached as a resource (not data) because connections are stateful objects.
    Read-only mode prevents accidental writes from the dashboard.

    Args:
        read_only: Whether to open the connection in read-only mode (default: True)

    Returns:
        duckdb.DuckDBPyConnection: Cached DuckDB connection
    """
    return duckdb.connect(str(DUCKDB_PATH), read_only=read_only)


def get_ddb_df(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    schema: str,
    uppercase_columns: bool = False,
) -> DataFrame:
    """
    Fetches a table and returns the data as a pandas DataFrame.

    Args:
        conn: Active DuckDB connection
        table: Table name to query
        schema: Schema name containing the table
        uppercase_columns: Whether to convert column names to uppercase

    Returns:
        DataFrame: Table data as a pandas DataFrame
    """
    print(f"Fetching data from '{schema}.{table}'...")

    # Execute SQL query and convert to pandas DataFrame
    df = conn.sql(f"SELECT * FROM {schema}.{table}").to_df()

    # Optionally uppercase column names for consistency
    if uppercase_columns:
        df.columns = [str(col).upper() for col in df.columns]

    print(f"Successfully fetched {len(df)} rows into a Pandas DataFrame.")
    return df


@st.cache_data
def get_cached_ddb_df(
    table: str,
    schema: str | None = None,
    uppercase_columns: bool = False,
) -> DataFrame:
    """
    Fetches a table and returns the data as a cached Streamlit-dataset pandas DataFrame.

    Cached as data (not resource) because DataFrames are immutable/serializable.
    Uses default schema if none provided.

    Args:
        table: Table name to query
        schema: Schema name (defaults to STREAMLIT_DEFAULT_SCHEMA)
        uppercase_columns: Whether to convert column names to uppercase

    Returns:
        DataFrame: Cached table data as a pandas DataFrame
    """
    # Use default schema if not specified
    if schema is None:
        schema = STREAMLIT_DEFAULT_SCHEMA

    return get_ddb_df(get_cached_ddb_conn(), table, schema, uppercase_columns)
