import time
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from dlt.common.typing import TSecretStrValue
import logging
from typing import List

logger = logging.getLogger(__name__)


@dlt.source(name="swedavia_flights")
def swedavia_source(
    api_key: TSecretStrValue = dlt.secrets.value,
    base_url: str = dlt.config.value,
    airports: List[str] = dlt.config.value,
    date: str = dlt.config.value,
    api_call_delay: float = dlt.config.value,
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
