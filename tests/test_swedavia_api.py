import dlt
from svensk_flyt.defs.dlt.pipelines.swedavia import swedavia_source


def test_swedavia_connection():
    """Test Swedavia API connection and data retrieval."""
    
    # Load the source
    source = swedavia_source()
    
    # Extract data (doesn't load to destination, just fetches)
    print(f"✓ Successfully connected to Swedavia API")
    
    for resource in source.resources.values():
        resource_name = resource.name
        rows = list(resource)
        print(f"  - {resource_name}: {len(rows)} flights")
        
        if rows:
            print(f"    Sample flight keys: {list(rows[0].keys())}")
            print(f"    Sample flight ID: {rows[0].get('flightId', 'N/A')}")


if __name__ == "__main__":
    test_swedavia_connection()