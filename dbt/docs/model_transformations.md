# dbt Model Transformations & Details

This document provides detailed transformation logic for each dbt model, complementing the high-level architecture in `data_model.md`.

---

## Model Structure Overview

```
models/
├── staging/               # Source system conformance
│   └── flights/          
│       ├── stg_flights_arrivals.sql
│       ├── stg_flights_departures.sql
│       └── schema.yml
│
├── intermediate/          # Business entity integration
│   └── flights/
│       ├── int_flights.sql
│       └── schema.yml
│
└── marts/                 # Published dimensional models
    ├── dim/              # Conformed dimensions
    │   ├── dim_airline.sql
    │   ├── dim_airport.sql
    │   └── schema.yml
    │
    ├── fct/              # Analytical fact tables
    │   ├── fct_airline_performance.sql
    │   ├── fct_airport_daily_traffic.sql
    │   ├── fct_baggage_performance.sql
    │   ├── fct_gate_utilization.sql
    │   ├── fct_hourly_traffic.sql
    │   ├── fct_terminal_performance.sql
    │   └── schema.yml
    │
    └── reports/          # Streamlit-optimized presentation layer
        └── schema.yml   # (Future: denormalized views for dashboards)
```

### Layer Definitions

- **staging/**: Flattens and standardizes raw source data, one model per source table
- **intermediate/**: Consolidates and integrates business entities (e.g., unions arrivals + departures)
- **marts/dim/**: Dimension tables (conformed dimensions used across multiple facts)
- **marts/fct/**: Fact tables with detailed analytical metrics at various grains
- **marts/reports/**: Application-specific views optimized for Streamlit dashboards (denormalized, pre-aggregated, or filtered)

---

## Staging Layer

### stg_flights_arrivals

**Purpose:** Flatten and standardize arrivals raw data with calculated KPI fields.

**Source:** `flights.flights_arrivals_raw`

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

**Purpose:** Flatten and standardize departures raw data with calculated KPI fields.

**Source:** `flights.flights_departures_raw`

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

**Purpose:** Union arrivals and departures with standardized column names and `flight_type` discriminator.

**Sources:** `stg_flights_arrivals`, `stg_flights_departures`

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

## Marts Layer - Dimensions

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

## Marts Layer - Facts

### fct_airline_performance

**Purpose:** Airline KPI metrics for punctuality and performance analysis.

**Source:** `int_flights`

**Aggregation Level:** By airline (airline_iata, airline_name)

**Key Metrics:**

| Metric | Calculation | Usage |
|--------|-----------|-------|
| `total_flights` | COUNT(*) | All flights (incl. deleted/cancelled) |
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

### fct_hourly_traffic

**Purpose:** Hourly traffic patterns for peak hours analysis and capacity planning.

**Source:** `int_flights`

**Aggregation Level:** By flight_hour, flight_time_period, flight_type

**Key Metrics:**

| Metric | Calculation | Usage |
|--------|-----------|-------|
| `flight_count` | COUNT(*) WHERE NOT is_deleted | Active flights per hour |
| `unique_airlines` | COUNT(DISTINCT airline_iata) | Airline diversity |
| `domestic_flights` | COUNT(*) WHERE is_domestic AND NOT is_deleted | Domestic traffic per hour |
| `international_flights` | COUNT(*) WHERE NOT is_domestic AND NOT is_deleted | International traffic per hour |
| `avg_delay_minutes` | AVG(delay_minutes) WHERE actual_time_utc IS NOT NULL | Delay patterns by hour |
| `on_time_flights` | COUNT(*) WHERE is_on_time AND actual_time_utc IS NOT NULL | On-time performance by hour |
| `completed_flights` | COUNT(*) WHERE actual_time_utc IS NOT NULL | Flights with actual times |

**Time Period Buckets:**
- Morning (06:00-11:59)
- Midday/Afternoon (12:00-16:59)
- Evening (17:00-21:59)
- Night/Red-eye (22:00-05:59)

**Data Quality Tests:**
- `unique` + `not_null`: hourly_traffic_key

---

### fct_airport_daily_traffic

**Purpose:** Daily airport capacity utilization and traffic metrics.

**Source:** `int_flights`

**Aggregation Level:** By airport_iata, flight_date, flight_type

**Key Metrics:**

| Metric | Calculation | Usage |
|--------|-----------|-------|
| `flight_count` | COUNT(*) WHERE NOT is_deleted | Daily movements |
| `domestic_flights` | COUNT(*) WHERE is_domestic AND NOT is_deleted | Domestic traffic |
| `international_flights` | COUNT(*) WHERE NOT is_domestic AND NOT is_deleted | International traffic |
| `cancelled_flights` | COUNT(*) WHERE is_cancelled | Cancellation tracking |
| `deleted_flights` | COUNT(*) WHERE is_deleted | Data quality issue tracking |
| `on_time_flights` | COUNT(*) WHERE is_on_time AND actual_time_utc IS NOT NULL | **KPI: Capacity Utilization** |
| `on_time_percentage` | on_time_flights / completed_flights * 100 | Punctuality per airport |
| `unique_airlines` | COUNT(DISTINCT airline_iata) | Airline diversity |

**Data Quality Tests:**
- `unique` + `not_null`: airport_daily_key
- `not_null`: airport_iata, flight_date

---

### fct_terminal_performance

**Purpose:** Terminal efficiency metrics for operational analysis.

**Source:** `int_flights`

**Aggregation Level:** By terminal, flight_type, flight_date

**Key Metrics:**

| Metric | Calculation | Usage |
|--------|-----------|-------|
| `flight_count` | COUNT(*) WHERE NOT is_deleted | Flights per terminal |
| `gates_used` | COUNT(DISTINCT gate) | Gate utilization |
| `domestic_flights` | COUNT(*) WHERE is_domestic AND NOT is_deleted | Domestic traffic |
| `international_flights` | COUNT(*) WHERE NOT is_domestic AND NOT is_deleted | International traffic |
| `on_time_percentage` | on_time_flights / completed_flights * 100 | **KPI: Terminal Efficiency** |
| `avg_delay_minutes` | AVG(delay_minutes) WHERE actual_time_utc IS NOT NULL | Delay by terminal |
| `unique_airlines` | COUNT(DISTINCT airline_iata) | Airline diversity |

---

### fct_gate_utilization

**Purpose:** Gate-level assignment and utilization metrics.

**Source:** `int_flights`

**Aggregation Level:** By terminal, gate, flight_type, flight_date

**Key Metrics:**

| Metric | Calculation | Usage |
|--------|-----------|-------|
| `flight_count` | COUNT(*) WHERE NOT is_deleted | Flights per gate |
| `unique_airlines` | COUNT(DISTINCT airline_iata) | Airline count per gate |
| `domestic_flights` | COUNT(*) WHERE is_domestic AND NOT is_deleted | Domestic usage |
| `international_flights` | COUNT(*) WHERE NOT is_domestic AND NOT is_deleted | International usage |
| `on_time_percentage` | on_time_flights / completed_flights * 100 | **KPI: Gate Management** |

---

### fct_baggage_performance

**Purpose:** Baggage handling efficiency metrics.

**Source:** `int_flights` (arrivals only)

**Aggregation Level:** By baggage_claim_unit, airline_iata, flight_date

**Key Metrics:**

| Metric | Calculation | Usage |
|--------|-----------|-------|
| `flights_with_baggage_data` | COUNT(*) WHERE baggage_handling_minutes IS NOT NULL | Valid data points |
| `avg_baggage_handling_minutes` | AVG(baggage_handling_minutes) | **KPI: Baggage Handling** |
| `median_baggage_handling_minutes` | PERCENTILE_CONT(0.5) on baggage_handling_minutes | Typical handling time |
| `min_baggage_handling_minutes` | MIN(baggage_handling_minutes) | Fastest carousel |
| `max_baggage_handling_minutes` | MAX(baggage_handling_minutes) | Slowest carousel |
| `domestic_flights` | COUNT(*) WHERE is_domestic AND baggage_handling_minutes IS NOT NULL | Domestic flights |
| `international_flights` | COUNT(*) WHERE NOT is_domestic AND baggage_handling_minutes IS NOT NULL | International flights |
| `avg_flight_delay_minutes` | AVG(delay_minutes) | Correlation with flight delays |

**Note:** Only arrivals have baggage data. Departures' baggage columns are NULL in int_flights.

---

## Reports Layer - Streamlit-Optimized Models

The reports layer provides pre-aggregated, denormalized models optimized for interactive Streamlit dashboards. These models combine multiple dimensions and facts to enable efficient querying with flexible filtering.

### rpt_airport_hourly_traffic

**Purpose:** Peak hours analysis by airport for traffic pattern visualization.

**Source:** `int_flights`

**Grain:** Airport + Date + Hour + Flight Type

**Key Filters:**
- Airport selection (ARN, GOT, MMX, etc.)
- Time period: specific date, week number, month, or all-time
- Flight type: arrivals, departures, or both

**Key Metrics:**
- `flight_count` - Total active flights per hour (excluding deleted)
- `domestic_flights` / `international_flights` - Market segmentation
- `unique_airlines` - Airline diversity per hour
- `avg_delay_minutes` - Delay patterns by hour
- `on_time_flights` / `completed_flights` - Punctuality by hour

**Use Case:** "Show me peak arrival hours at ARN for January 2026"

---

### rpt_airport_punctuality

**Purpose:** Airport operational efficiency and punctuality metrics.

**Source:** `int_flights`

**Grain:** Airport + Date + Flight Type + Domestic/International

**Key Filters:**
- Airport selection
- Time period: date, week, month, or all-time
- Flight type: arrivals or departures
- Domestic vs international filtering

**Punctuality Categories (Industry Standard):**
- `ahead_of_schedule_flights` - Negative delay (arrived/departed early)
- `on_time_flights` - Delay ≤ 15 minutes
- `delayed_flights` - Delay > 15 minutes
- `cancelled_flights` - Status = CAN

**Key Metrics:**
- `on_time_percentage` - % of completed flights on-time
- `avg_delay_minutes` / `median_delay_minutes` - Delay statistics
- `completion_rate` - % of flights that completed (not cancelled)

**Use Case:** "Compare punctuality between domestic and international flights at GOT in week 4"

---

### rpt_airline_punctuality

**Purpose:** Airline performance comparison and competitive analysis.

**Source:** `int_flights`

**Grain:** Airline + Date + Flight Type + Domestic/International

**Key Filters:**
- Airline selection (SAS, Norwegian, Finnair, etc.)
- Time period: date, week, month, or all-time
- Flight type: arrivals or departures
- Domestic vs international filtering

**Punctuality Categories:** Same as airport punctuality (industry standard)

**Key Metrics:**
- `on_time_percentage` - Airline reliability KPI
- `delayed_percentage` / `early_percentage` / `cancelled_percentage` - Performance breakdown
- `avg_delay_minutes` / `median_delay_minutes` - Delay distribution
- `min_delay_minutes` / `max_delay_minutes` - Best/worst performance

**Use Case:** "Compare SAS vs Norwegian on-time performance for January 2026"

---

### rpt_route_popularity

**Purpose:** Route demand analysis and traffic distribution.

**Source:** `int_flights`

**Grain:** Airport + Route (directional) + Date + Flight Type

**Key Filters:**
- Airport selection (the airport being analyzed)
- Flight direction: departures from airport OR arrivals to airport
- Time period: week, month, or all-time
- Domestic vs international routes

**Route Directionality:** Routes are directional (ARN→GOT ≠ GOT→ARN) to capture asymmetric traffic patterns

**Key Metrics:**
- `flight_count` - Total flights on this route
- `unique_airlines` - Number of airlines serving the route
- `cancelled_flights` - Route reliability indicator

**Key Fields:**
- `route_key` - e.g., 'CPH-ARN' or 'ARN-GOT'
- `other_airport_iata` - The connected airport (other end of route)

**Use Case:** "Show top 10 departure routes from ARN in January by flight count"

---

### rpt_baggage_performance

**Purpose:** Passenger experience metric - baggage handling efficiency.

**Source:** `int_flights` (arrivals only)

**Grain:** Airport + Date + Baggage Claim Unit + Domestic/International + Hour + Day of Week

**Key Filters:**
- Airport selection
- Baggage carousel/claim unit
- Time period: date, week, month, or all-time
- Domestic vs international flights
- Time of day filtering (morning, afternoon, evening, night)
- Day of week patterns (weekday vs weekend)

**Baggage Handling Definition:** Time from first bag appearing on carousel to last bag (passenger wait time)

**Key Metrics:**
- `avg_baggage_handling_minutes` - Mean wait time (primary KPI)
- `median_baggage_handling_minutes` - Typical wait (more robust)
- `p90_baggage_handling_minutes` / `p95_baggage_handling_minutes` - Worst-case planning
- `min_baggage_handling_minutes` / `max_baggage_handling_minutes` - Range
- `avg_flight_delay_minutes` - Correlation with flight punctuality

**Use Case:** "Which carousel at ARN has the longest baggage wait times on Sundays?"

**Note:** Only available for arrivals; baggage data not captured for departures.

---

## Data Quality & Business Rules

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
