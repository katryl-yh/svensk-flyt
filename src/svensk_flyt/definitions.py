# ==================== #
#       Imports        #
# ==================== #

import os
from pathlib import Path
from datetime import datetime, timedelta

import dlt
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_duckdb import DuckDBResource
from dotenv import load_dotenv

import dagster as dg

from .defs.dlt.pipelines.swedavia import swedavia_source
from .constants import SWEDAVIA_AIRPORTS, SWEDAVIA_API_BASE_URL, API_CALL_DELAY_SECONDS

# Load environment variables from .env file
load_dotenv()

# Path to DuckDB database file
DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(Path(__file__).parents[2] / "data_warehouse" / "svenska-flyt.duckdb"))

# Path to DBT profiles directory (contains connection configs)
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", str(Path.home() / ".dbt"))


# ==================== #
#       DLT Asset      #
# ==================== #

# DLT resource for executing data pipeline loads
dlt_resource = DagsterDltResource()


@dlt_assets(
    # Source: Swedavia Flight API configuration
    dlt_source=swedavia_source(
        api_key=os.getenv("SWEDAVIA_API_KEY"),
        base_url=os.getenv("SWEDAVIA_BASE_URL", SWEDAVIA_API_BASE_URL),
        airports=SWEDAVIA_AIRPORTS,
        date=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),  # Yesterday's date
        api_call_delay=API_CALL_DELAY_SECONDS,
    ),
    # Pipeline: Extract from API and load into DuckDB staging schema
    dlt_pipeline=dlt.pipeline(
        pipeline_name="swedavia_flights",
        # Target schema for raw/staging data
        dataset_name="staging",
        # Destination: DuckDB warehouse
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),
    ),
)
def dlt_load(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    """
    Asset: Extract flight data from Swedavia API and load into DuckDB.

    Creates assets for all airport arrivals and departures
    Data flows to: staging.flights_arrivals_raw and staging.flights_departures_raw tables
    """
    yield from dlt.run(context=context)


# ==================== #
#       DBT Asset      #
# ==================== #

# Path to DBT project directory
dbt_project_directory = Path(__file__).parents[2] / "dbt"

# DBT project instance with project and profiles paths
dbt_project = DbtProject(project_dir=dbt_project_directory, profiles_dir=DBT_PROFILES_DIR)

# DBT CLI resource for executing DBT commands
dbt_resource = DbtCliResource(project_dir=dbt_project)

# Generate manifest.json in development mode
# Manifest defines model dependencies for Dagster's lineage graph
dbt_project.prepare_if_dev()


@dbt_assets(
    # Path to manifest.json (defines all DBT models and dependencies)
    manifest=dbt_project.manifest_path,
)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """
    Asset: Transform raw flight data using DBT models.

    Creates assets: All models in staging, intermediate, and marts schemas
    Data flows from: staging schema (loaded by DLT)
    """
    # Execute 'dbt build' command and stream progress to Dagster UI
    yield from dbt.cli(["build"], context=context).stream()


# ==================== #
#         Jobs         #
# ==================== #

# Job: Extract and load flight data from Swedavia API
swedavia_extract_job = dg.define_asset_job(
    name="swedavia_extract_job",
    # Run all DLT assets
    selection=dg.AssetSelection.groups("swedavia_flights"),
)

# Job: Transform data using DBT models
dbt_transform_job = dg.define_asset_job(
    name="dbt_transform_job",
    # Run all DBT models: staging → intermediate → dimensions + facts → marts
    selection=dg.AssetSelection.key_prefixes("staging", "intermediate", "dimensions", "facts", "marts"),
)

# Job: Full pipeline - extract and transform
full_pipeline_job = dg.define_asset_job(
    name="full_pipeline_job",
    # Run all assets
    selection=dg.AssetSelection.all(),
)


# ==================== #
#       Schedule       #
# ==================== #

# Schedule: Run data extraction daily at 7 PM Swedish time (UTC+1/UTC+2)
swedavia_daily_schedule = dg.ScheduleDefinition(
    name="swedavia_daily_schedule",
    job=swedavia_extract_job,
    cron_schedule="0 19 * * *",  # 7 PM daily (local Swedish time)
    description="Extract previous day's flight data from Swedavia API daily at 7 PM Swedish time",
)


# ==================== #
#        Sensor        #
# ==================== #


# Sensor: Automatically trigger DBT job when new flight data is loaded
@dg.asset_sensor(
    # Watch for materialization of any DLT asset in swedavia_flights group
    asset_key=dg.AssetKey(["staging", "flights_arrivals_raw"]),
    # Trigger the DBT transformation job
    job=dbt_transform_job,
    description="Triggers DBT transformations after Swedavia flight data is loaded",
)
def swedavia_load_sensor(context: dg.SensorEvaluationContext, asset_event: dg.EventLogEntry):
    """
    Sensor: Triggers DBT transformations after DLT load completes.

    Data flow: DLT loads raw flight data -> Sensor detects -> DBT transforms
    """
    yield dg.RunRequest(
        run_key=f"dbt_after_swedavia_{asset_event.dagster_event.event_specific_data.materialization.asset_key}"
    )


# ==================== #
#     Definitions      #
# ==================== #

# Main Dagster definitions object
# Wires together all resources, assets, jobs, sensors, and schedules
defs = dg.Definitions(
    # Shared resources available to all assets
    resources={
        # DLT for data ingestion
        "dlt": DagsterDltResource(),
        # DBT for data transformation
        "dbt": dbt_resource,
        # DuckDB connection
        "duckdb": DuckDBResource(database=DUCKDB_PATH),
    },
    # Data assets to materialize
    assets=[
        dlt_load,  # Raw flight data extraction
        dbt_models,  # Data transformation
    ],
    # Jobs that can be executed
    jobs=[
        swedavia_extract_job,  # Extract job
        dbt_transform_job,  # Transform job
        full_pipeline_job,  # Full pipeline
    ],
    # Event-driven automation
    sensors=[
        swedavia_load_sensor,  # Auto-trigger DBT after DLT
    ],
    # Time-based automation
    schedules=[
        swedavia_daily_schedule,  # Daily extraction at 1 AM
    ],
)
