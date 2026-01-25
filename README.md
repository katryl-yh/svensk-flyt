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

This pipeline answers six key operational questions:

1. **Peak Hours** — When do departures and arrivals peak per airport? (hourly aggregates)
2. **Punctuality & Delays** — What % of flights are on-time? Which airports/airlines have the worst delays?
3. **Airline Performance** — How do airlines compare on on-time performance and route frequency
4. **Route Popularity** — Which routes (airport pairs) are busiest? How does traffic vary by day/season?
5. **Airport Capacity Utilization** — How busy is each airport relative to its peer airports?
6. **Seasonal Trends** — Are there weekly/monthly patterns in flight volume and punctuality?

## Data Sources

- **FlightInfo API** (Swedavia): Real-time flight schedules and status for 10 Swedish airports
  - **Airports:** ARN (Stockholm Arlanda), BMA (Bromma), GOT (Göteborg), MMX (Malmö), LLA (Luleå), UME (Umeå), OSD (Åre Östersund), VBY (Visby), RNB (Ronneby), KRN (Kiruna)
  - **Endpoints:** `/query` (OData filters for multiple airports), `/arrivals/{airport}/{date}`, `/departures/{airport}/{date}`
  - **Auth:** API subscription key (free tier: 10,001 requests/30 days)

## Week 1: Ingestion Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SWEDAVIA FLIGHTINFO API                          │
│  (10 airports × 2 calls [arrivals, departures] per day)            │
│  Or: Query endpoint with OData filter for all airports in 1 call   │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   dlt Source         │
                    │  (OData query,       │
                    │   throttled, retry)  │
                    └───────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  DuckDB (local)      │
                    │  flights_arrivals_raw │
                    │  flights_departures_raw
                    └───────────────────────┘
                                │
         ┌──────────────────────┼──────────────────────┐
         ▼                      ▼                      ▼
   ┌─────────────┐      ┌──────────────┐     ┌──────────────┐
   │ dbt: stg_   │      │ dbt: marts   │     │ Validation   │
   │ flights     │  →   │ (Week 2)     │  →  │ & Logging    │
   └─────────────┘      └──────────────┘     └──────────────┘
                                │
                                ▼
                        ┌───────────────────────┐
                        │  Streamlit Dashboard  │
                        │  (Week 3)             │
                        └───────────────────────┘
```

**Week 1 scope:** API → dlt → DuckDB (raw JSON load)
**Week 2:** Add dbt transformations and Dagster orchestration
**Week 3:** Build Streamlit dashboard
**Week 4:** Polish, testing, report

## Setup Instructions

### Prerequisites
- Python 3.10+
- Poetry or pip

### Installation

1. **Clone and install:**
   ```bash
   git clone <repo-url>
   cd svensk-flyt
   poetry install
   ```

2. **Set up environment:**
   ```bash
   cp .env.example .env
   # Edit .env and add your Swedavia API key
   export SWEDAVIA_API_KEY="your-api-key-here"
   ```
   
   Obtain your API key at: https://www.swedavia.se/en/about-swedavia/about-us/api/

3. **Run the pipeline:**
   ```bash
   poetry run python src/svensk_flyt/pipelines/run.py
   ```

   This will:
   - Fetch arrivals and departures for all 10 Swedish airports (today's date)
   - Load raw JSON into DuckDB (default: `./svenska-flyt.duckdb`)
   - Create tables: `flights_arrivals_raw`, `flights_departures_raw`

### Output

- **DuckDB file:** `svenska-flyt.duckdb` (local, git-ignored)
- **Tables:**
  - `flights_arrivals_raw`: Raw arrival records
  - `flights_departures_raw`: Raw departure records

### Data Schema (Key Fields)

| Field | Type | Description |
|-------|------|-------------|
| `flightId` | string | Unique flight identifier |
| `airport` | string | IATA airport code (ARN, GOT, etc.) |
| `scheduled` | timestamp | Scheduled arrival/departure time |
| `estimated` | timestamp | Estimated or actual arrival/departure time |
| `status` | string | Flight status (On-time, Delayed, Cancelled, etc.) |
| `airline` | string | Airline code (SAS, FR, etc.) |
| `destination` / `origin` | string | IATA airport code |
| `flightNumber` | string | Airline flight number |

## Troubleshooting

- **401 Unauthorized:** Check that `SWEDAVIA_API_KEY` is set and valid
- **No data returned:** Verify date format is YYYY-MM-DD and airport codes are valid
- **DuckDB file not created:** Ensure write permissions in project root

---

**Status:** Week 1 in progress | Last updated: 25 Jan 2026
