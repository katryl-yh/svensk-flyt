"""
Main entry point for the svensk-flyt DLT pipeline.

This script:
1. Loads configuration from environment variables
2. Sets up DuckDB as the destination
3. Runs the Swedavia source to fetch flight data for all airports
4. Loads data into DuckDB raw tables
5. Validates and logs results
"""

import os
import logging
from datetime import datetime
from pathlib import Path

import dlt
from dotenv import load_dotenv

from svensk_flyt.constants import (
    SWEDAVIA_AIRPORTS,
    SWEDAVIA_API_BASE_URL,
    DUCKDB_FILE_PATH,
    DUCKDB_DATASET_NAME,
    API_CALL_DELAY_SECONDS,
    API_RETRY_ATTEMPTS,
    API_RETRY_DELAY_SECONDS,
    TABLE_ARRIVALS_RAW,
    TABLE_DEPARTURES_RAW,
)
from svensk_flyt.defs.dlt.pipelines.swedavia import swedavia_source

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_configuration():
    """Load configuration from .env and environment variables."""
    # Load .env file if it exists
    load_dotenv()
    
    # Required configuration
    api_key = os.getenv("SWEDAVIA_API_KEY")
    if not api_key:
        raise ValueError(
            "SWEDAVIA_API_KEY not set. Please set it in .env or as an environment variable."
        )
    
    # Optional configuration with defaults
    duckdb_path = os.getenv("DUCKDB_FILE_PATH", DUCKDB_FILE_PATH)
    ingest_date = os.getenv("INGEST_DATE", datetime.now().strftime("%Y-%m-%d"))
    airports = os.getenv("AIRPORTS", ",".join(SWEDAVIA_AIRPORTS)).split(",")
    airports = [a.strip().upper() for a in airports]  # Normalize
    
    return {
        "api_key": api_key,
        "base_url": SWEDAVIA_API_BASE_URL,
        "airports": airports,
        "date": ingest_date,
        "duckdb_path": duckdb_path,
        "api_call_delay": API_CALL_DELAY_SECONDS,
        "api_retry_attempts": API_RETRY_ATTEMPTS,
        "api_retry_delay": API_RETRY_DELAY_SECONDS,
    }


def setup_destination(duckdb_path: str) -> dict:
    """Configure DuckDB as the dlt destination."""
    # Ensure parent directory exists
    db_dir = Path(duckdb_path).parent
    db_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Using DuckDB database: {duckdb_path}")
    
    return dlt.destinations.duckdb(duckdb_path)


def validate_results(pipeline) -> dict:
    """Validate that data was successfully loaded."""
    results = {}
    
    try:
        with pipeline.sql_client() as client:
            # Check arrivals table
            arrivals_result = client.execute_sql(
                f"SELECT COUNT(*) as count FROM {TABLE_ARRIVALS_RAW}"
            )
            results["arrivals_rows"] = arrivals_result[0][0] if arrivals_result else 0
            
            # Check departures table
            departures_result = client.execute_sql(
                f"SELECT COUNT(*) as count FROM {TABLE_DEPARTURES_RAW}"
            )
            results["departures_rows"] = departures_result[0][0] if departures_result else 0
            
            # Check unique airports in data
            try:
                arrivals_airports = client.execute_sql(
                    f"SELECT DISTINCT airport FROM {TABLE_ARRIVALS_RAW} ORDER BY airport"
                )
                results["arrivals_airports"] = [row[0] for row in arrivals_airports] if arrivals_airports else []
            except Exception as e:
                logger.warning(f"Could not fetch arrivals airports: {e}")
                results["arrivals_airports"] = []
            
    except Exception as e:
        logger.error(f"Validation error: {e}")
        results["arrivals_rows"] = 0
        results["departures_rows"] = 0
    
    return results


def main():
    """Run the full ingestion pipeline."""
    logger.info("=" * 80)
    logger.info("Starting svensk-flyt ingestion pipeline")
    logger.info("=" * 80)
    
    try:
        # Load configuration
        config = load_configuration()
        logger.info(f"Configuration loaded:")
        logger.info(f"  - Airports: {', '.join(config['airports'])}")
        logger.info(f"  - Date: {config['date']}")
        logger.info(f"  - Database: {config['duckdb_path']}")
        logger.info(f"  - Base URL: {config['base_url']}")
        
        # Setup destination
        destination = setup_destination(config["duckdb_path"])
        
        # Create pipeline
        pipeline = dlt.pipeline(
            pipeline_name="svenska_flyt_ingestion",
            destination=destination,
            dataset_name=DUCKDB_DATASET_NAME,
        )
        logger.info(f"Pipeline created: {pipeline.pipeline_name}")
        
        # Load data from source
        logger.info(f"Fetching flight data...")
        source = swedavia_source(
            api_key=config["api_key"],
            base_url=config["base_url"],
            airports=config["airports"],
            date=config["date"],
            api_call_delay=config["api_call_delay"],
        )
        
        load_info = pipeline.run(source)
        logger.info(f"Data load completed")
        logger.info(f"Load info: {load_info}")
        
        # Validate results
        logger.info("Validating results...")
        validation = validate_results(pipeline)
        
        logger.info("=" * 80)
        logger.info("Pipeline completed successfully!")
        logger.info(f"  - Arrivals rows: {validation.get('arrivals_rows', 0)}")
        logger.info(f"  - Departures rows: {validation.get('departures_rows', 0)}")
        if validation.get('arrivals_airports'):
            logger.info(f"  - Airports with data: {', '.join(validation['arrivals_airports'])}")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())
