import os
import httpx
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Read API key from environment
api_key = os.getenv("SWEDAVIA_API_KEY")
if not api_key:
    raise ValueError("SWEDAVIA_API_KEY not set in environment. Please add it to .env or set it as an environment variable.")

base_url = "https://api.swedavia.se/flightinfo/v2"

headers = {
    "Ocp-Apim-Subscription-Key": api_key,
    "Accept": "application/json"
}

# Test 1: Try single airport endpoint with today's date (YYYY-MM-DD)
today = datetime.now()
date_iso = today.strftime("%Y-%m-%d")

print(f"\n=== Test 1: Single Airport Endpoint (ARN, {date_iso}) ===")
try:
    response = httpx.get(
        f"{base_url}/ARN/arrivals/{date_iso}",
        headers=headers,
        timeout=10.0
    )
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"✓ Arrivals: {data.get('numberOfFlights', 0)}")
        if data.get('flights'):
            print(f"Sample: {data['flights'][0].get('flightId', 'N/A')}")
    else:
        print(f"Error: {response.text[:200]}")
except Exception as e:
    print(f"Error: {e}")

time.sleep(2)

# Test 2: Query with REORDERED filter (flightType before scheduled)
airports = ["ARN", "BMA", "GOT", "MMX", "LLA", "UME", "OSD", "VBY", "RNB", "KRN"]
airport_filter = " or ".join([f"airport eq '{code}'" for code in airports])
date_yymmdd = today.strftime("%y%m%d")
query_filter_reordered = f"({airport_filter}) and flightType eq 'A' and scheduled eq '{date_yymmdd}'"

print(f"\n=== Test 2: Query Endpoint REORDERED (all airports, {date_iso}) ===")
print(f"Filter: {query_filter_reordered[:100]}...")
try:
    response = httpx.get(
        f"{base_url}/query",
        params={"filter": query_filter_reordered},
        headers=headers,
        timeout=10.0
    )
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"✓ Arrivals: {data.get('numberOfFlights', 0)}")
        if data.get('flights'):
            print(f"Sample: {data['flights'][0].get('flightId', 'N/A')}")
    else:
        print(f"Error: {response.text[:200]}")
except Exception as e:
    print(f"Error: {e}")

time.sleep(2)

# Test 3: Query with exact doc example format (just ARN and VBY)
query_filter_doc = f"(airport eq 'ARN' or airport eq 'VBY') and flightType eq 'A' and scheduled eq '{date_yymmdd}'"

print(f"\n=== Test 3: Query Endpoint DOC EXAMPLE (ARN + VBY, {date_iso}) ===")
print(f"Filter: {query_filter_doc}")
try:
    response = httpx.get(
        f"{base_url}/query",
        params={"filter": query_filter_doc},
        headers=headers,
        timeout=10.0
    )
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"✓ Arrivals: {data.get('numberOfFlights', 0)}")
        if data.get('flights'):
            print(f"Sample: {data['flights'][0].get('flightId', 'N/A')}")
    else:
        print(f"Error: {response.text[:200]}")
except Exception as e:
    print(f"Error: {e}")

time.sleep(2)

# Test 4: Single Airport 7 Days Ago
past_date = datetime.now() - timedelta(days=7)
date_iso_past = past_date.strftime("%Y-%m-%d")

print(f"\n=== Test 4: Single Airport 7 Days Ago (ARN, {date_iso_past}) ===")
try:
    response = httpx.get(
        f"{base_url}/ARN/arrivals/{date_iso_past}",
        headers=headers,
        timeout=10.0
    )
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"✓ Arrivals: {data.get('numberOfFlights', 0)}")
        if data.get('flights'):
            print(f"Sample: {data['flights'][0].get('flightId', 'N/A')}")
    else:
        print(f"Error: {response.text[:200]}")
except Exception as e:
    print(f"Error: {e}")