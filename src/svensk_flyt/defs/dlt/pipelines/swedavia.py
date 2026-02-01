import time
import os
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from dlt.common.typing import TSecretStrValue
import logging
from typing import List, Optional
from datetime import datetime, timedelta

from ....constants import (
    SWEDAVIA_AIRPORTS,
    SWEDAVIA_API_BASE_URL,
    API_CALL_DELAY_SECONDS,
)

logger = logging.getLogger(__name__)


@dlt.source(name="swedavia_flights")
def swedavia_source(
    api_key: Optional[TSecretStrValue] = None,
    base_url: Optional[str] = None,
    airports: Optional[List[str]] = None,
    date: Optional[str] = None,
    api_call_delay: Optional[float] = None,
):
    """
    DLT source for Swedavia arrivals and departures for multiple airports.
    
    Loops through each airport individually using /{airport}/arrivals/{date} and
    /{airport}/departures/{date} endpoints (proven reliable in testing).
    
    Args:
        api_key: Swedavia API subscription key
        base_url: API base URL
        airports: List of airport IATA codes (e.g., ['ARN', 'GOT', 'MMX'])
        date: Date in YYYY-MM-DD format
        api_call_delay: Delay between API calls in seconds (recommend 2.0+)
    """
    
    # Resolve parameters at runtime (allows Dagster to pass values at execution time)
    if api_key is None:
        api_key = dlt.secrets.get("swedavia_api_key")
    if not api_key:
        api_key = os.getenv("SWEDAVIA_API_KEY")
    if base_url is None:
        base_url = dlt.config.get("swedavia_base_url")
    if not base_url:
        base_url = os.getenv("SWEDAVIA_BASE_URL", SWEDAVIA_API_BASE_URL)
    if airports is None:
        airports = dlt.config.get("swedavia_airports")
    if not airports:
        airports = SWEDAVIA_AIRPORTS
    if date is None:
        date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if api_call_delay is None:
        api_call_delay = dlt.config.get("api_call_delay")
    if api_call_delay is None:
        api_call_delay = API_CALL_DELAY_SECONDS

    if not api_key:
        raise ValueError("Missing Swedavia API key. Set swedavia_api_key in dlt secrets.")
    if not airports:
        raise ValueError("Missing airport list. Set swedavia_airports in dlt config or constants.")
    
    headers = {
        "Ocp-Apim-Subscription-Key": api_key,
        "Accept": "application/json",
    }
    
    logger.info(f"Fetching flights for {len(airports)} airports on {date}")
    logger.info(f"Airports: {', '.join(airports)}")
    
    # Build resource configurations for all airports
    resources_config = []
    
    for airport in airports:
        # Arrivals for this airport
        resources_config.append({
            "name": f"{airport.lower()}_arrivals",
            "endpoint": {
                "path": f"/{airport}/arrivals/{date}",
                "data_selector": "flights",
            },
            "table_name": "flights_arrivals_raw",
            "write_disposition": "append",
        })
        
        # Departures for this airport
        resources_config.append({
            "name": f"{airport.lower()}_departures",
            "endpoint": {
                "path": f"/{airport}/departures/{date}",
                "data_selector": "flights",
            },
            "table_name": "flights_departures_raw",
            "write_disposition": "append",
        })
    
    api_config: RESTAPIConfig = {
        "client": {
            "base_url": base_url,
            "headers": headers,
        },
        "resources": resources_config,
    }
    
    # Generate resources from config
    resources = rest_api_resources(api_config)
    
    # Yield resources with rate limiting
    for i, resource in enumerate(resources):
        logger.info(f"Fetching {resource.name} ({i+1}/{len(resources_config)})")
        
        # Add delay between calls to avoid 429 rate limit errors
        if i > 0:  # No delay before first call
            time.sleep(api_call_delay)
        
        yield resource
