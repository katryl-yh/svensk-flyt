"""
Constants for the svensk-flyt project.
Includes airport codes, API configuration, and default settings.
"""

# All 10 Swedavia airports (IATA codes)
SWEDAVIA_AIRPORTS = [
    "ARN",  # Stockholm Arlanda Airport
    "BMA",  # Bromma Stockholm Airport
    "GOT",  # Göteborg Landvetter Airport
    "MMX",  # Malmö Airport
    "LLA",  # Luleå Airport
    "UME",  # Umeå Airport
    "OSD",  # Åre Östersund Airport
    "VBY",  # Visby Airport
    "RNB",  # Ronneby Airport
    "KRN",  # Kiruna Airport
]

# API configuration
SWEDAVIA_API_BASE_URL = "https://api.swedavia.se/flightinfo/v2"
SWEDAVIA_API_DATE_FORMAT = "%Y-%m-%d"  # YYYY-MM-DD (UTC)

# DLT configuration
DUCKDB_FILE_PATH = "data_warehouse/svenska-flyt.duckdb"
DUCKDB_DATASET_NAME = "flights"

# Rate limiting
API_CALL_DELAY_SECONDS = 2.0  # Delay between API calls (avoid 429 errors)
API_RETRY_ATTEMPTS = 3
API_RETRY_DELAY_SECONDS = 2.0

# Raw table names
TABLE_ARRIVALS_RAW = "flights_arrivals_raw"
TABLE_DEPARTURES_RAW = "flights_departures_raw"
