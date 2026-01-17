import httpx
from datetime import datetime

api_key = "76ed5401084841d08b340e2bcc4cb9ef"
base_url = "https://api.swedavia.se/flightinfo/v2"

# Try today's date in different formats
today = datetime.now()
date_formats = [
    today.strftime("%Y-%m-%d"),  # 2026-01-17
    today.strftime("%Y%m%d"),    # 20260117
    today.strftime("%d-%m-%Y"),  # 17-01-2026
]

headers = {
    "Ocp-Apim-Subscription-Key": api_key,
    "Accept": "application/json"  # Required!
}

for date_str in date_formats:
    print(f"\n--- Testing date format: {date_str} ---")
    try:
        response = httpx.get(
            f"{base_url}/ARN/arrivals/{date_str}",
            headers=headers,
            timeout=10.0
        )
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Success! Flights: {data.get('numberOfFlights', 0)}")
            print(f"Response keys: {list(data.keys())}")
            break
        else:
            print(f"Response: {response.text[:200]}")
    except Exception as e:
        print(f"Error: {e}")