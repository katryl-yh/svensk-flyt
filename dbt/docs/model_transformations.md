# dbt Model Transformations & Details

This document provides detailed transformation logic for each dbt model, complementing the high-level architecture in `data_model.md`.

---

## Model Structure Overview

```
models/
├── staging/               # Source system conformance + deduplication
│   ├── stg_flights_arrivals.sql
│   ├── stg_flights_departures.sql
│   └── schema.yml
│
├── intermediate/          # Business entity integration
│   ├── int_flights.sql
│   └── schema.yml
│
├── dim/                   # Conformed dimensions
│   ├── dim_airline.sql
│   ├── dim_airport.sql
│   ├── dim_date.sql
│   └── schema.yml
│
├── fct/                   # Atomic fact table
│   ├── fct_flights.sql
│   └── schema.yml
│
└── mart/                  # Dashboard-optimized aggregates
    ├── mart_airport_hourly_traffic.sql
    ├── mart_airport_punctuality.sql
    ├── mart_airline_punctuality.sql
    ├── mart_route_popularity.sql
    ├── mart_baggage_performance.sql
    └── schema.yml
```

### Layer Definitions

- **staging/**: Flattens, deduplicates, and standardizes raw source data
- **intermediate/**: Consolidates and integrates business entities (e.g., unions arrivals + departures)
- **dim/**: Dimension tables (conformed dimensions used across multiple facts)
- **fct/**: Atomic grain fact table - one row per flight event
- **mart/**: Pre-aggregated tables optimized for Streamlit dashboards

---

## Staging Layer

### stg_flights_arrivals

**Purpose:** Flatten, deduplicate, and standardize arrivals raw data with calculated KPI fields.

**Source:** `flights.flights_arrivals_raw`

**Critical Fix:** Deduplication added to handle duplicate records in source data.

**Deduplication Logic:**
```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY flight_id, scheduled_arrival_utc 
    ORDER BY _dlt_load_id DESC
) = 1
```
- Partitions by: flight ID + scheduled time (unique flight identifier)
- Orders by: DLT load ID descending (keeps most recent version)
- Result: One row per unique flight

**Key Transformations:**

#### Column Flattening & Renaming
| Raw Column | Staged Column | Notes |
|-----------|---------------|-------|
| `flight_leg_identifier__flight_id` | `flight_number` | Flight identifier |
| `flight_leg_identifier__departure_airport_iata` | `origin_airport_iata` | Departure airport code |
| `flight_leg_identifier__arrival_airport_iata` | `destination_airport_iata` | Arrival airport code |
| `flight_leg_identifier__flight_departure_date_utc` | `departure_date_utc` | Flight date |
| `airline_operator__name` | `airline_name` | Airline operator name |
| `airline_operator__iata` | `airline_iata` | Airline IATA code |
| `arrival_time__scheduled_utc` | `scheduled_arrival_utc` | Scheduled arrival (UTC) |
| `arrival_time__actual_utc` | `actual_arrival_utc` | Actual arrival (UTC, NULL if not landed) |
| `location_and_status__flight_leg_status` | `flight_status` | Flight status code |
| `location_and_status__terminal` | `terminal` | Terminal assignment |
| `location_and_status__gate` | `gate` | Gate assignment |
| `baggage__baggage_claim_unit` | `baggage_claim_unit` | Baggage carousel number |
| `baggage__first_bag_utc` | `first_bag_utc` | First bag arrival time |
| `baggage__last_bag_utc` | `last_bag_utc` | Last bag arrival time |

#### Calculated Fields

| Field | Logic | Purpose |
|-------|-------|---------|
| `route_key` | `origin_airport_iata \|\| '-' \|\| destination_airport_iata` | Unique route identifier (e.g., 'ARN-GOT') for grouping |
| `delay_minutes` | `EXTRACT(EPOCH FROM (actual_arrival_utc - scheduled_arrival_utc)) / 60.0` | Delay in minutes (NULL if not landed) |
| `is_on_time` | `delay_minutes <= 15` | Boolean flag for punctuality (on-time = ≤15 min delay) |
| `baggage_handling_minutes` | `EXTRACT(EPOCH FROM (last_bag_utc - first_bag_utc)) / 60.0` | Duration from first to last bag |

#### Flag Fields

| Field | Logic | Values | Purpose |
|-------|-------|--------|---------|
| `is_deleted` | `flight_status = 'DEL'` | TRUE/FALSE | Track deleted flights (18% of data) |
| `is_cancelled` | `flight_status = 'CAN'` | TRUE/FALSE | Track cancelled flights |
| `is_landed` | `flight_status = 'LAN'` | TRUE/FALSE | Track completed arrivals |
| `is_domestic` | Origin in Swedish airport list | TRUE/FALSE | Distinguish domestic vs international |

#### Time Dimension Fields

| Field | Logic | Example |
|-------|-------|---------|
| `arrival_hour` | `EXTRACT(HOUR FROM scheduled_arrival_utc)` | 0-23 |
| `arrival_day_of_week` | `EXTRACT(ISODOW FROM scheduled_arrival_utc)` | 1-7 (Mon-Sun) |
| `arrival_day_name` | `STRFTIME(scheduled_arrival_utc, '%A')` | 'Monday', 'Tuesday', ... |
| `arrival_date` | `DATE_TRUNC('day', scheduled_arrival_utc)` | 2026-01-27 |
| `arrival_time_period` | CASE statement on hour | 'Morning (06:00-11:59)', 'Midday/Afternoon (12:00-16:59)', 'Evening (17:00-21:59)', 'Night/Red-eye (22:00-05:59)' |

**Data Quality Tests:**
- `not_null`: flight_id, origin_airport_iata, destination_airport_iata, scheduled_arrival_utc, flight_status
- `accepted_values`: destination_airport_iata (Swedish airports), flight_status (SCH, LAN, DEL, CAN, etc.)

---

### stg_flights_departures

**Purpose:** Flatten, deduplicate, and standardize departures raw data with calculated KPI fields.

**Source:** `flights.flights_departures_raw`

**Deduplication Logic:**
```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY flight_id, scheduled_departure_utc 
    ORDER BY _dlt_load_id DESC
) = 1
```
- Same deduplication approach as arrivals
- Ensures data quality before downstream processing

**Key Transformations:**

Mirrors `stg_flights_arrivals` structure with departure-specific columns:

#### Column Flattening & Renaming
| Raw Column | Staged Column | Notes |
|-----------|---------------|-------|
| `departure_time__scheduled_utc` | `scheduled_departure_utc` | Scheduled departure (UTC) |
| `departure_time__actual_utc` | `actual_departure_utc` | Actual departure (UTC, NULL if not departed) |
| `arrival_airport_swedish` | `destination_airport_swedish` | Destination airport name |
| `arrival_airport_english` | `destination_airport_english` | Destination airport English name |

#### Calculated Fields
- `delay_minutes`: Same logic as arrivals
- `route_key`: Same logic as arrivals
- `is_on_time`: Same logic (≤15 min)

#### Flag Fields
- `is_deleted`: Same logic
- `is_cancelled`: Same logic
- `is_scheduled`: `flight_status = 'SCH'` (departures-specific)
- `is_domestic`: Checks destination airport (inverse of arrivals)

#### Time Dimension Fields
- `departure_hour`, `departure_day_of_week`, `departure_day_name`, `departure_date`, `departure_time_period`
- **NOTE:** Time periods use same ranges as arrivals for consistency

**Data Quality Tests:** Same as arrivals

**Design Note:** Removed `check_in_counter` (not available in source data) for data quality

---

## Intermediate Layer

### int_flights

**Purpose:** Union deduplicated arrivals and departures with standardized column names and `flight_type` discriminator.

**Sources:** `stg_flights_arrivals`, `stg_flights_departures` (both already deduplicated)

**Key Design Decisions:**

1. **Standardized Column Names:** Both branches use same column names to avoid confusion
   - `scheduled_arrival_utc` / `scheduled_departure_utc` → `scheduled_time_utc`
   - `arrival_hour` / `departure_hour` → `flight_hour`
   - `arrival_time_period` / `departure_time_period` → `flight_time_period`

2. **Flight Type Discriminator:** Added `flight_type` column ('arrival' or 'departure') to distinguish in downstream models

3. **Arrival-Specific Fields:** Set to NULL for departures
   - `is_landed`: Only arrivals can be landed
   - `baggage_claim_unit`, `first_bag_utc`, `last_bag_utc`, `baggage_handling_minutes`: Baggage data only for arrivals

4. **Code Deduplication:** Enables single model for all 6 fact tables instead of separate arrivals/departures logic

**Data Quality Tests:**
- `not_null`: flight_type
- `accepted_values`: flight_type ('arrival', 'departure')

---

## Dimension Layer

### dim_airline

**Purpose:** Unique airline dimension for fact table joins.

**Source:** `int_flights`

**Transformations:**
- Extract distinct airlines (`airline_iata`, `airline_name`)
- Generate surrogate key: `{{ dbt_utils.generate_surrogate_key(['airline_iata']) }}`

**Business Key:** `airline_iata`

**Data Quality Tests:**
- `unique`: airline_key, airline_iata
- `not_null`: airline_iata, airline_key

---

### dim_airport

**Purpose:** Swedish airports dimension for traffic and capacity analysis.

**Source:** Hard-coded list of 10 Swedavia airports

**Airports Included:**
- ARN: Stockholm Arlanda (largest hub)
- GOT: Göteborg Landvetter
- MMX: Malmö
- BMA: Bromma Stockholm (small)
- LLA: Luleå
- UME: Umeå
- OSD: Åre Östersund
- VBY: Visby
- RNB: Ronneby
- KRN: Kiruna

**Transformations:**
- Generate surrogate key: `{{ dbt_utils.generate_surrogate_key(['airport_iata']) }}`

**Business Key:** `airport_iata`

**Data Quality Tests:**
- `unique`: airport_key, airport_iata
- `not_null`: airport_iata, airport_key

---

### dim_date

**Purpose:** Date dimension with time intelligence attributes.

**Source:** Date spine generated from flight data date range

**Key Attributes:**
- `date_key`: Integer surrogate key (YYYYMMDD format)
- `date_day`: Actual date
- `week_number`, `month`, `year`: Time hierarchy
- `week_start_date`: Week aggregation
- Swedish holidays and calendar attributes

**Business Key:** `date_day`

---

## Fact Layer

### fct_flights

**Purpose:** Atomic grain fact table following Kimball star schema methodology.

**Source:** `int_flights`

**Grain:** One row per flight event (flight_id + flight_type + scheduled_time_utc)

**Surrogate Key Generation:**
```sql
{{ dbt_utils.generate_surrogate_key(['flight_id', 'flight_type', 'scheduled_time_utc']) }}
```
- **Why include scheduled_time_utc**: Same flight ID can appear multiple times (recurring daily flights)
- Ensures true uniqueness at the atomic grain

**Foreign Keys:**
- `airline_key` → dim_airline
- `origin_airport_key` → dim_airport
- `dest_airport_key` → dim_airport  
- `flight_date_key` → dim_date

**Degenerate Dimensions:** 
Attributes that don't warrant separate dimensions:
- `flight_id`, `flight_number`, `flight_type`, `flight_status`
- `terminal`, `gate`, `route_key`

**Measures:**
- `delay_minutes`: Continuous measure
- `baggage_handling_minutes`: Continuous measure (arrivals only)
- Boolean flags: `is_on_time`, `is_cancelled`, `is_deleted`, `is_domestic`, `is_landed`

**Data Quality Tests:**
- `unique` + `not_null`: flight_key
- `not_null`: All foreign keys
- `accepted_values`: flight_type (arrival, departure)

---

## Mart Layer - Dashboard Aggregates

Mart tables aggregate `fct_flights` with optimized grain for dashboard queries. All marts support filtering by:
- Time period (date, week, month)
- Flight type (arrivals, departures, or both)
- Domestic vs international

**SQL Fix Applied:** All GROUP BY clauses use actual column expressions instead of aliases to avoid DuckDB binding errors.

### mart_airport_hourly_traffic

**Purpose:** Hourly traffic patterns per airport for peak hours analysis and capacity planning.

**Source:** `fct_flights` (aggregated)

**Grain:** Airport + Date + Hour + Flight Type

**Aggregation Logic:**
- Groups by: airport_iata (derived from CASE on flight_type), flight_date, flight_hour, flight_time_period, flight_type
- **SQL Fix**: Uses positional GROUP BY (1 for airport_iata CASE expression) and actual column names (d.date_day instead of flight_date alias)

**Key Metrics:**

| Metric | Calculation | Usage |
|--------|-----------|-------|
| `flight_count` | COUNT(*) WHERE NOT is_deleted | Active flights per hour |
| `domestic_flights` / `international_flights` | COUNT(*) filtered by is_domestic | Market segmentation |
| `unique_airlines` | COUNT(DISTINCT airline_key) | Airline diversity |
| `avg_delay_minutes` | AVG(delay_minutes) WHERE actual_time_utc IS NOT NULL | Delay patterns by hour |
| `on_time_flights` / `completed_flights` | Punctuality metrics | On-time performance by hour |

**Dashboard Filters:**
- Airport selection (ARN, GOT, MMX, etc.)
- Time period: date, week, month
- Flight type: arrivals, departures, or both
- Time period buckets: Morning, Midday/Afternoon, Evening, Night/Red-eye

**Use Case:** "Show me peak arrival hours at ARN for January 2026"

**Data Quality Tests:**
- `unique` + `not_null`: hourly_traffic_key

---

### mart_airport_punctuality

**Purpose:** Airport operational efficiency and punctuality metrics.

**Source:** `fct_flights` (aggregated)

**Grain:** Airport + Date + Flight Type + Domestic/International

**Aggregation Logic:**
- Groups by: airport_iata, flight_date, flight_type, is_domestic
- **SQL Fix**: Positional GROUP BY for CASE expressions, actual column names for date fields

**Punctuality Categories (Industry Standard):**
- `ahead_of_schedule_flights` - Negative delay (arrived/departed early)
- `on_time_flights` - Delay ≤ 15 minutes
- `delayed_flights` - Delay > 15 minutes  
- `cancelled_flights` - Status = CAN

**Key Metrics:**

| Metric | Calculation | Usage |
|--------|-----------|-------|
| `total_flights` | COUNT(*) WHERE NOT is_deleted | All non-deleted flights |
| `on_time_percentage` | on_time_flights / completed_flights * 100 | **Primary KPI** |
| `avg_delay_minutes` / `median_delay_minutes` | Delay statistics | Performance analysis |
| `completion_rate` | completed_flights / total_flights * 100 | Reliability metric |

**Dashboard Filters:**
- Airport selection
- Time period: date, week, month
- Flight type: arrivals or departures
- Domestic vs international

**Use Case:** "Compare punctuality between domestic and international flights at GOT in week 4"

**Data Quality Tests:**
- `unique` + `not_null`: punctuality_key

---

### mart_airline_punctuality

**Purpose:** Airline performance comparison and competitive analysis.

**Source:** `fct_flights` (aggregated)

**Grain:** Airline + Date + Flight Type + Domestic/International

**Aggregation Logic:**
- Groups by: airline_iata, airline_name, flight_date, flight_type, is_domestic
- **SQL Fix**: Uses d.date_day instead of flight_date alias in GROUP BY

**Punctuality Categories:** Same as airport punctuality (industry standard)

**Key Metrics:**

| Metric | Calculation | Usage |
|--------|-----------|-------|
| `total_flights` | COUNT(*) WHERE NOT is_deleted | All non-deleted flights |
| `on_time_percentage` | on_time_flights / completed_flights * 100 | **Airline reliability KPI** |
| `delayed_percentage` / `early_percentage` / `cancelled_percentage` | Performance breakdown | Detailed analysis |
| `avg_delay_minutes` / `median_delay_minutes` | Delay distribution | Central tendency |
| `min_delay_minutes` / `max_delay_minutes` | Best/worst performance | Range analysis |

**Dashboard Filters:**
- Airline selection (SAS, Norwegian, Finnair, etc.)
- Time period: date, week, month
- Flight type: arrivals or departures
- Domestic vs international

**Use Case:** "Compare SAS vs Norwegian on-time performance for January 2026"

**Data Quality Tests:**
- `unique` + `not_null`: airline_punctuality_key

---

### mart_route_popularity

**Purpose:** Route demand analysis and traffic distribution.

**Source:** `fct_flights` (aggregated)

**Grain:** Airport + Route (directional) + Date + Flight Type

**Aggregation Logic:**
- Groups by: airport_iata, route_key, origin_airport_iata, destination_airport_iata, other_airport_iata, flight_type, is_domestic, flight_date
- **SQL Fix**: Positional GROUP BY (1 for airport_iata, 2 for other_airport_iata) since both are CASE expressions

**Route Directionality:** Routes are directional (ARN→GOT ≠ GOT→ARN) to capture asymmetric traffic patterns

**Key Metrics:**

| Metric | Calculation | Usage |
|--------|-----------|-------|
| `flight_count` | COUNT(*) WHERE NOT is_deleted | Total flights on route |
| `unique_airlines` | COUNT(DISTINCT airline_key) | Airline competition |
| `cancelled_flights` | COUNT(*) WHERE is_cancelled | Route reliability |

**Key Fields:**
- `route_key` - e.g., 'CPH-ARN' or 'ARN-GOT'
- `airport_iata` - The airport being analyzed
- `other_airport_iata` - The connected airport (other end of route)

**Dashboard Filters:**
- Airport selection
- Flight direction: departures from OR arrivals to airport
- Time period: week, month, all-time
- Domestic vs international routes

**Use Case:** "Show top 10 departure routes from ARN in January by flight count"

**Data Quality Tests:**
- `unique` + `not_null`: route_popularity_key

---

### mart_baggage_performance

**Purpose:** Passenger experience metric - baggage handling efficiency.

**Source:** `fct_flights` (aggregated, arrivals only)

**Grain:** Airport + Date + Baggage Claim Unit + Domestic/International + Hour + Day of Week

**Aggregation Logic:**
- Groups by: airport_iata (dest_ap only), baggage_claim_unit, is_domestic, flight_date, flight_hour, flight_time_period, flight_day_of_week, flight_day_name
- **SQL Fix**: Uses d.date_day instead of flight_date alias
- Filter: Only arrivals (departures have no baggage data)

**Baggage Handling Definition:** Time from first bag appearing on carousel to last bag (passenger wait time)

**Key Metrics:**

| Metric | Calculation | Usage |
|--------|-----------|-------|
| `flights_with_baggage_data` | COUNT(*) WHERE baggage_handling_minutes IS NOT NULL | Valid data points |
| `total_arrivals` | COUNT(*) | All arrival flights |
| `avg_baggage_handling_minutes` | AVG(baggage_handling_minutes) | **Primary KPI - mean wait** |
| `median_baggage_handling_minutes` | PERCENTILE_CONT(0.5) | Typical wait (robust) |
| `p90_baggage_handling_minutes` / `p95_baggage_handling_minutes` | Upper percentiles | Worst-case planning |
| `min_baggage_handling_minutes` / `max_baggage_handling_minutes` | Range | Performance bounds |
| `avg_flight_delay_minutes` | AVG(delay_minutes) | Correlation analysis |

**Dashboard Filters:**
- Airport selection
- Baggage carousel/claim unit
- Time period: date, week, month
- Domestic vs international
- Time of day (morning, afternoon, evening, night)
- Day of week patterns

**Use Case:** "Which carousel at ARN has the longest baggage wait times on Sundays?"

**Note:** Only available for arrivals; baggage data not captured for departures.

**Data Quality Tests:**
- `unique` + `not_null`: baggage_performance_key

---

## Data Quality & Business Rules
| `active_flights` | COUNT(*) WHERE NOT is_deleted | Non-deleted flights |
| `completed_flights` | COUNT(*) WHERE actual_time_utc IS NOT NULL | Flights with actual times |
| `on_time_flights` | COUNT(*) WHERE is_on_time AND actual_time_utc IS NOT NULL | On-time arrivals/departures |
| `on_time_percentage` | on_time_flights / completed_flights * 100 | **KPI: Punctuality** |
| `avg_delay_minutes` | AVG(delay_minutes) WHERE actual_time_utc IS NOT NULL | **KPI: Airline Performance** |
| `median_delay_minutes` | PERCENTILE_CONT(0.5) on delay_minutes | More robust than average |
| `best_early_minutes` | MIN(delay_minutes) | Most punctual flight |
| `worst_late_minutes` | MAX(delay_minutes) | Most delayed flight |
| `domestic_flights` | COUNT(*) WHERE is_domestic AND NOT is_deleted | Market segmentation |
| `international_flights` | COUNT(*) WHERE NOT is_domestic AND NOT is_deleted | Market segmentation |

**Data Quality Tests:**
- `unique` + `not_null`: airline_key
- `not_null`: airline_iata

---

## Data Quality & Business Rules

### Deduplication Strategy

**Problem:** Raw source data contains duplicates (same flight loaded multiple times from API)

**Solution:** Implemented at staging layer using DuckDB's `QUALIFY` clause:

```sql
-- stg_flights_arrivals
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY flight_id, scheduled_arrival_utc 
    ORDER BY _dlt_load_id DESC
) = 1

-- stg_flights_departures  
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY flight_id, scheduled_departure_utc 
    ORDER BY _dlt_load_id DESC
) = 1
```

**Logic:**
- Partitions by: `flight_id` + scheduled time (unique flight identifier)
- Orders by: `_dlt_load_id DESC` (keeps most recent API load)
- Result: One row per unique flight

**Impact:** Resolved 1285+ duplicate records, ensuring `fct_flights.flight_key` uniqueness

### Surrogate Key Design

**fct_flights.flight_key** composite:
```sql
{{ dbt_utils.generate_surrogate_key(['flight_id', 'flight_type', 'scheduled_time_utc']) }}
```

**Why include scheduled_time_utc:**
- Same `flight_id` can appear on multiple dates (recurring daily flights like SK123)
- `flight_type` alone insufficient (same flight has arrival AND departure records)
- **Atomic grain:** One row per specific flight event at a specific time

### Deletion Handling
- **Deleted flights (`is_deleted = true`)** are kept in all tables for analysis
- 18% of arrivals, tracked separately for cancellation metrics
- Can correlate with weather data to identify weather-driven cancellations

### Delay Definition
- Only calculated for flights with actual times (`actual_time_utc IS NOT NULL`)
- For arrivals: landed flights only (status = 'LAN')
- For departures: departed flights only
- **On-time threshold:** ≤ 15 minutes delay (aviation industry standard)

### Domestic vs International
- **Domestic:** Both airports in Swedish airport list
- **International:** At least one airport outside Sweden
- Enables market segmentation in airline/airport analysis

### Time Period Classification
Used consistently across staging and marts for peak hours analysis:
- **Morning (06:00-11:59):** Early flights, business travel
- **Midday/Afternoon (12:00-16:59):** Peak period
- **Evening (17:00-21:59):** Late afternoon, evening travel
- **Night/Red-eye (22:00-05:59):** Overnight operations

---

## Next Steps for Enhancement

1. **Add data_load_date** extraction from `_dlt_load_id` for SLA tracking
2. **Weather correlation:** JOIN departures table on route + date to identify weather-driven delays
3. **Route cancellation stats:** Aggregate `is_deleted` flags by `route_key` for route reliability analysis
4. **Airline alliance groupings:** Add dimension for airline alliances (Star Alliance, OneWorld, SkyTeam)
5. **Aircraft type analysis:** Include aircraft registration for equipment-specific delay tracking
