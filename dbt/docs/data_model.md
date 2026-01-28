# Swedish Flight Data - Dimensional Model

## Data Model Overview

This dimensional model supports 6 key KPIs:
1. **Peak Hours** - Traffic patterns by time of day
2. **Punctuality** - On-time performance metrics
3. **Airline Performance** - Delay analysis by carrier
4. **Terminal Efficiency** - Terminal and gate utilization
5. **Gate Management** - Gate assignment optimization
6. **Baggage Handling** - Baggage delivery performance

---

## Entity Relationship Diagram

```mermaid
erDiagram
    %% Sources
    flights_arrivals_raw ||--o{ stg_flights_arrivals : "1:N"
    flights_departures_raw ||--o{ stg_flights_departures : "1:N"
    
    %% Staging to Intermediate
    stg_flights_arrivals ||--o{ int_flights : "union"
    stg_flights_departures ||--o{ int_flights : "union"
    
    %% Dimensions
    dim_airline {
        varchar airline_key PK
        varchar airline_iata UK
        varchar airline_name
    }
    
    dim_airport {
        varchar airport_key PK
        varchar airport_iata UK
        varchar airport_name
    }
    
    %% Facts
    fct_airline_performance {
        varchar airline_key FK
        varchar airline_iata
        varchar airline_name
        int total_flights
        int active_flights
        int completed_flights
        int on_time_flights
        decimal on_time_percentage
        decimal avg_delay_minutes
        decimal median_delay_minutes
        int domestic_flights
        int international_flights
    }
    
    fct_hourly_traffic {
        varchar hourly_traffic_key PK
        int flight_hour
        varchar flight_time_period
        varchar flight_type
        int flight_count
        int domestic_flights
        int international_flights
        decimal avg_delay_minutes
        int on_time_flights
        int unique_airlines
    }
    
    fct_airport_daily_traffic {
        varchar airport_daily_key PK
        varchar airport_iata FK
        date flight_date
        varchar flight_type
        int flight_count
        int domestic_flights
        int international_flights
        decimal on_time_percentage
        int unique_airlines
    }
    
    fct_terminal_performance {
        varchar terminal_performance_key PK
        varchar terminal
        varchar flight_type
        date flight_date
        int flight_count
        int gates_used
        decimal on_time_percentage
        decimal avg_delay_minutes
    }
    
    fct_gate_utilization {
        varchar gate_utilization_key PK
        varchar terminal
        varchar gate
        varchar flight_type
        date flight_date
        int flight_count
        int unique_airlines
        decimal on_time_percentage
    }
    
    fct_baggage_performance {
        varchar baggage_performance_key PK
        varchar baggage_claim_unit
        varchar airline_iata FK
        date flight_date
        int flights_with_baggage_data
        decimal avg_baggage_handling_minutes
        decimal median_baggage_handling_minutes
    }
    
    %% Relationships
    int_flights ||--o{ fct_airline_performance : "aggregates"
    int_flights ||--o{ fct_hourly_traffic : "aggregates"
    int_flights ||--o{ fct_airport_daily_traffic : "aggregates"
    int_flights ||--o{ fct_terminal_performance : "aggregates"
    int_flights ||--o{ fct_gate_utilization : "aggregates"
    int_flights ||--o{ fct_baggage_performance : "aggregates"
    
    dim_airline ||--o{ fct_airline_performance : "airline_key"
    dim_airline ||--o{ fct_baggage_performance : "airline_iata"
    dim_airport ||--o{ fct_airport_daily_traffic : "airport_iata"
```

---

## Data Lineage

```mermaid
graph LR
    %% Sources
    A1[flights_arrivals_raw] --> B1[stg_flights_arrivals]
    A2[flights_departures_raw] --> B2[stg_flights_departures]
    
    %% Intermediate
    B1 --> C[int_flights]
    B2 --> C
    
    %% Dimensions
    C --> D1[dim_airline]
    C --> D2[dim_airport]
    
    %% Facts
    C --> F1[fct_airline_performance]
    C --> F2[fct_hourly_traffic]
    C --> F3[fct_airport_daily_traffic]
    C --> F4[fct_terminal_performance]
    C --> F5[fct_gate_utilization]
    C --> F6[fct_baggage_performance]
    
    %% Styling
    classDef source fill:#f9f,stroke:#333,stroke-width:2px
    classDef staging fill:#bbf,stroke:#333,stroke-width:2px
    classDef intermediate fill:#bfb,stroke:#333,stroke-width:2px
    classDef dimension fill:#fdb,stroke:#333,stroke-width:2px
    classDef fact fill:#fbb,stroke:#333,stroke-width:2px
    
    class A1,A2 source
    class B1,B2 staging
    class C intermediate
    class D1,D2 dimension
    class F1,F2,F3,F4,F5,F6 fact
```

---

## Model Layers

### 📥 **Source Layer** (Raw Data)
- `flights_arrivals_raw` - Raw arrivals from Swedavia API
- `flights_departures_raw` - Raw departures from Swedavia API

### 🔄 **Staging Layer** (Cleaned & Standardized)
- `stg_flights_arrivals` - Flattened arrivals with calculated fields
- `stg_flights_departures` - Flattened departures with calculated fields

**Key Transformations:**
- Flatten nested JSON columns
- Rename columns for clarity
- Add calculated fields: `delay_minutes`, `is_on_time`, `is_domestic`
- Add flags: `is_deleted`, `is_cancelled`, `is_landed`
- Time dimensions: `arrival_hour`, `arrival_day_name`, `arrival_time_period`

### 🔀 **Intermediate Layer** (Unified Data)
- `int_flights` - Union of arrivals + departures with `flight_type` discriminator

### 📊 **Marts Layer** (Business Logic)

#### Dimensions
- `dim_airline` - Unique airlines (surrogate key)
- `dim_airport` - Swedish airports (ARN, GOT, MMX, etc.)

#### Facts
- `fct_airline_performance` - **KPI: Punctuality & Airline Performance**
  - Metrics: on-time %, avg delay, flight counts
  
- `fct_hourly_traffic` - **KPI: Peak Hours**
  - Metrics: flight counts by hour & time period
  
- `fct_airport_daily_traffic` - **KPI: Capacity Utilization**
  - Metrics: daily movements by airport
  
- `fct_terminal_performance` - **KPI: Terminal Efficiency**
  - Metrics: flights per terminal, gates used
  
- `fct_gate_utilization` - **KPI: Gate Management**
  - Metrics: flights per gate, airline diversity
  
- `fct_baggage_performance` - **KPI: Baggage Handling**
  - Metrics: avg/median baggage handling time

---

## KPI Mapping

| KPI | Fact Table | Key Metrics |
|-----|-----------|-------------|
| **Peak Hours** | `fct_hourly_traffic` | `flight_count`, `flight_time_period` |
| **Punctuality** | `fct_airline_performance` | `on_time_percentage`, `avg_delay_minutes` |
| **Airline Performance** | `fct_airline_performance` | `median_delay_minutes`, `on_time_flights` |
| **Terminal Efficiency** | `fct_terminal_performance` | `gates_used`, `on_time_percentage` |
| **Gate Management** | `fct_gate_utilization` | `flight_count`, `unique_airlines` |
| **Baggage Handling** | `fct_baggage_performance` | `avg_baggage_handling_minutes` |

---

## Data Quality

### Tests Implemented

Tests are defined in schema.yml files (generic tests):

**Staging Layer** (`models/staging/flights/schema.yml`):
- `not_null` on `flight_id`, `scheduled_arrival_utc`, `flight_status`
- `accepted_values` on `destination_airport_iata` (Swedish airports)
- `accepted_values` on `flight_status` (SCH, LAN, DEL, CAN, etc.)

**Intermediate Layer** (`models/intermediate/flights/schema.yml`):
- `not_null` on `flight_type`
- `accepted_values` on `flight_type` (arrival, departure)

**Marts Layer** (`models/marts/*/schema.yml`):
- `unique` + `not_null` on all surrogate keys
- `not_null` on foreign keys (airline_iata, airport_iata)
- `unique` on dimension business keys

Run tests with: `dbt test`

### Business Rules
- Deleted flights (`is_deleted = true`) kept for cancellation analysis
- Delay calculations only for completed flights (`actual_time_utc IS NOT NULL`)
- On-time threshold: ≤ 15 minutes delay
- Domestic routes: Both airports in Swedish airport list
