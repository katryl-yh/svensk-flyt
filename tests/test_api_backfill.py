"""Test backfill capability: fetch data for multiple dates."""

from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from svensk_flyt.defs.dlt.pipelines.swedavia import swedavia_source
from svensk_flyt.constants import (
    SWEDAVIA_API_BASE_URL,
    API_CALL_DELAY_SECONDS,
)


def test_backfill_single_airport():
    """Test fetching data for a single airport over available date range.
    
    Note: Swedavia API has a limit on historical data availability.
    This test will fetch until the API returns 400 Bad Request.
    """
    
    load_dotenv()
    api_key = os.getenv("SWEDAVIA_API_KEY")
    if not api_key:
        raise ValueError("SWEDAVIA_API_KEY not set")
    
    # Test parameters
    test_airport = "ARN"  # Stockholm Arlanda
    max_backfill_days = 8  # Try up to 8 days
    
    print("=" * 80)
    print(f"Testing backfill: {test_airport} (attempting up to {max_backfill_days} days)")
    print("=" * 80)
    
    total_arrivals = 0
    total_departures = 0
    successful_days = 0
    
    # Loop through each date until API returns error
    for i in range(max_backfill_days):
        date = datetime.now() - timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        
        print(f"\nFetching {test_airport} for {date_str}...")
        
        try:
            # Fetch data for single airport
            source = swedavia_source(
                api_key=api_key,
                base_url=SWEDAVIA_API_BASE_URL,
                airports=[test_airport],
                date=date_str,
                api_call_delay=API_CALL_DELAY_SECONDS,
            )
            
            # Extract arrivals
            arrivals_resource = source.resources.get("arn_arrivals")
            if arrivals_resource:
                arrivals = list(arrivals_resource)
                total_arrivals += len(arrivals)
                print(f"  ✓ Arrivals: {len(arrivals)}")
            
            # Extract departures
            departures_resource = source.resources.get("arn_departures")
            if departures_resource:
                departures = list(departures_resource)
                total_departures += len(departures)
                print(f"  ✓ Departures: {len(departures)}")
            
            successful_days += 1
                
        except Exception as e:
            error_msg = str(e)
            if "400" in error_msg or "Bad Request" in error_msg:
                print(f"  ⚠ API limit reached (date {date_str} not available)")
                break  # Stop trying older dates
            else:
                print(f"  ✗ Error: {error_msg[:100]}")
                raise
    
    print("\n" + "=" * 80)
    print(f"Backfill Summary:")
    print(f"  - Successful days: {successful_days}")
    print(f"  - Total arrivals: {total_arrivals}")
    print(f"  - Total departures: {total_departures}")
    print(f"  - Total movements: {total_arrivals + total_departures}")
    print(f"  - Average arrivals/day: {total_arrivals // max(successful_days, 1)}")
    print(f"  - Average departures/day: {total_departures // max(successful_days, 1)}")
    print("=" * 80)
    
    assert successful_days > 0, "No days fetched successfully"
    assert total_arrivals > 0, "No arrivals fetched"
    assert total_departures > 0, "No departures fetched"
    print(f"\n✅ Backfill test passed! API supports {successful_days}-day backfill.")


if __name__ == "__main__":
    test_backfill_single_airport()
