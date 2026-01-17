import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from dlt.common.typing import TSecretStrValue


@dlt.source(name="swedavia_flights")
def swedavia_source(
    api_key: TSecretStrValue = dlt.secrets.value,
    base_url: str = dlt.config.value,
    airport_code: str = dlt.config.value,
    date: str = dlt.config.value,
):
    """DLT source for Swedavia arrivals and departures."""
    
    api_config: RESTAPIConfig = {
        "client": {
            "base_url": base_url,
            "headers": {
                "Ocp-Apim-Subscription-Key": api_key,
                "Accept": "application/json",  # Required by API
            },
        },
        "resources": [
            {
                "name": "arrivals",
                "endpoint": {
                    "path": f"{airport_code}/arrivals/{date}",
                    "data_selector": "flights",
                },
                "table_name": "flights_arrivals_raw",
                "write_disposition": "append",
            },
            {
                "name": "departures",
                "endpoint": {
                    "path": f"{airport_code}/departures/{date}",
                    "data_selector": "flights",
                },
                "table_name": "flights_departures_raw",
                "write_disposition": "append",
            },
        ],
    }
    
    yield from rest_api_resources(api_config)