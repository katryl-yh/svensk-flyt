# svensk-flyt

**Operational Analytics for Swedish Airport Traffic**

This project builds an end-to-end data pipeline that collects, processes, and analyzes flight information from Swedavia's public API. 

It demonstrates how a modern, open-source data stack (dlt, Dagster, dbt, DuckDB, Streamlit) can transform raw airport data into actionable operational insights.

## Project Goal

Deliver a fully functioning analytics platform that demonstrates:
- **Automated extraction and loading** of flight data from all Swedish airports
- **Scheduled orchestration** via Dagster
- **Robust data transformations** in dbt
- **Well-structured analytical models** (data marts)
- **User-friendly dashboard** with interactive visualizations

## Core Research Questions (KPIs)

This pipeline answers five key operational questions:

1. **Peak Hours** — When do departures and arrivals peak per airport? (hourly aggregates)
2. **Punctuality & Delays** — What % of flights are on-time? Which airports/airlines have the worst delays?
3. **Airline Performance** — How do airlines compare on on-time performance and route frequency?
4. **Route Popularity** — Which routes (airport pairs) are busiest? How does traffic vary over time?
5. **Airport Capacity Utilization** — How busy is each airport relative to its peer airports?

## Data Sources

- **FlightInfo API** (Swedavia): Real-time flight schedules and status for 10 Swedish airports
  - **Airports:** ARN (Stockholm Arlanda), BMA (Bromma), GOT (Göteborg), MMX (Malmö), LLA (Luleå), UME (Umeå), OSD (Åre Östersund), VBY (Visby), RNB (Ronneby), KRN (Kiruna)
  - **Endpoints:** `/query` (OData filters for multiple airports), `/arrivals/{airport}/{date}`, `/departures/{airport}/{date}`
  - **Auth:** API subscription key (free tier: 10,001 requests/30 days)

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SWEDAVIA FLIGHTINFO API                          │
│  (10 airports × 2 calls [arrivals, departures] per day)            │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   dlt Source          │
                    │  (throttled, retry)   │
                    └───────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  DuckDB Warehouse     │
                    │  staging.flights_*    │
                    └───────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   Dagster             │
                    │  (orchestration)      │
                    └───────────────────────┘
                                │
         ┌──────────────────────┼──────────────────────┐
         ▼                      ▼                      ▼
   ┌─────────────┐      ┌──────────────┐     ┌──────────────┐
   │ dbt Models  │      │ Data Marts   │     │ Dimensions   │
   │ (staging)   │  →   │ (analytics)  │  ←  │ & Facts      │
   └─────────────┘      └──────────────┘     └──────────────┘
                                │
                                ▼
                        ┌───────────────────────┐
                        │  Streamlit Dashboard  │
                        │  (interactive UI)     │
                        └───────────────────────┘
```

## Setup Instructions

### Prerequisites
- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (recommended package manager)

### Installation

1. **Install uv** (if not already installed):
   ```bash
   # On macOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # On Windows
   powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

2. **Clone and install dependencies:**
   ```bash
   git clone <repo-url>
   cd svensk-flyt
   uv sync
   ```

3. **Set up environment:**
   ```bash
   cp .env.example .env
   # Edit .env and add your Swedavia API key
   # Windows: set SWEDAVIA_API_KEY=your-api-key-here
   # Linux/Mac: export SWEDAVIA_API_KEY="your-api-key-here"
   ```
   
   Obtain your API key at: https://www.swedavia.se/en/about-swedavia/about-us/api/

4. **Run the orchestrated pipeline:**
   ```bash
   uv run dagster dev
   ```
   
   This will:
   - Start the Dagster web UI at http://localhost:3000
   - Schedule daily data extraction at 7 PM
   - Automatically trigger dbt transformations after data loads
   - Provide monitoring and observability for all pipeline runs

5. **Run the Streamlit dashboard:**
   ```bash
   cd streamlit
   uv run streamlit run app.py
   ```
   
   Dashboard will be available at http://localhost:8501

### Data Warehouse Structure

**DuckDB file:** `data_warehouse/svenska-flyt.duckdb`

**Schemas:**
- `staging.*` - Raw data from Swedavia API
- `flights_dimensions.*` - Dimension tables (airports, airlines, dates)
- `flights_facts.*` - Fact table (individual flight events)
- `flights_marts.*` - Analytics-ready data marts

**Data Marts:**
- `mart_airport_hourly_traffic` - Peak hours and traffic patterns by airport
- `mart_airport_punctuality` - Airport-level punctuality and delay statistics
- `mart_airline_punctuality` - Airline performance comparison
- `mart_route_popularity` - Route traffic and destination analysis
- `mart_baggage_performance` - Baggage handling metrics (bonus analytics)

## Troubleshooting

- **401 Unauthorized:** Check that `SWEDAVIA_API_KEY` is set in your environment
- **Dagster won't start:** Ensure port 3000 is available or set `DAGSTER_PORT` environment variable
- **Streamlit connection error:** Verify DuckDB file exists at `data_warehouse/svenska-flyt.duckdb`
- **No data in dashboard:** Run a manual job in Dagster UI to trigger data extraction and transformation

## Project Structure

```
svensk-flyt/
├── src/svensk_flyt/           # Pipeline source code
│   ├── definitions.py          # Dagster assets, jobs, schedules
│   ├── constants.py            # Configuration constants
│   └── defs/dlt/pipelines/     # dlt extraction pipelines
├── dbt/                        # dbt transformation models
│   ├── models/                 # SQL transformation models
│   │   ├── staging/            # Raw data normalization
│   │   ├── intermediate/       # Business logic layer
│   │   ├── dim/                # Dimension tables
│   │   ├── fct/                # Fact tables
│   │   └── mart/               # Analytics-ready marts
│   └── dbt_project.yml         # dbt configuration
├── streamlit/                  # Dashboard application
│   ├── app.py                  # Main dashboard entry
│   └── pages/                  # Multi-page dashboard views
├── data_warehouse/             # DuckDB database file
├── tests/                      # API and pipeline tests
└── docs/                       # Design decisions and documentation
```

---

**Status:** ✅ MVP Complete | Last updated: 1 Feb 2026
