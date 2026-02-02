# Design Decisions

This document captures key architectural and implementation decisions made during the development of the svensk-flyt data pipeline.

---

## API Ingestion Strategy

### Decision: Use Individual Airport Endpoints

The Swedavia FlightInfo API offers two approaches for fetching flight data:

1. **Individual Airport Endpoints:** `/ARN/arrivals/2026-01-25`, `/BMA/arrivals/2026-01-25`, etc.
   - Requires 1 API call per airport per flight type (arrivals/departures)
   - Proven to work reliably in testing
   - Simple, straightforward implementation

2. **Query Endpoint with OData Filters:** `/query?filter=(airport eq 'ARN' or airport eq 'BMA' ...) and ...`
   - Would allow fetching all airports in 1-2 API calls
   - More efficient in theory
   - Documented in official API documentation with examples

### Testing Results

Comprehensive API testing was conducted (see `tests/API_TESTING.md` for full results):

| Test | Endpoint | Airports | Result | Flights Returned |
|------|----------|----------|--------|------------------|
| 1 | `/ARN/arrivals/{date}` | ARN | ✅ Success | 273 |
| 2 | `/query` (all airports, reordered filter) | All 10 | ❌ Failed | 0 |
| 3 | `/query` (doc-exact syntax) | ARN, VBY | ❌ Failed | 0 |

**Key Finding:** The query endpoint returns HTTP 200 but with 0 flights, even when hundreds of flights exist for the same date/airports in the individual endpoint.



**Chosen Approach:** Individual Airport Endpoints

**Reasons:**

1. **Reliability** — Individual endpoints proven to work as expected (273 flights returned)
2. **Simplicity** — Straightforward implementation, easier to debug and maintain
3. **API Limits Non-Issue** — 600 calls/month vs 10,001 limit = only 6% utilization
4. **Time Efficiency** — Debugging broken query endpoint is low-value work
5. **Production Ready** — Can ship a working pipeline immediately

**Trade-offs Accepted:**

1. **More API Calls** — 20/day instead of 2/day (but still trivial vs quota)
2. **Slightly Longer Runtime** — ~40 seconds/day due to rate limit delays vs ~4 seconds
3. **Code Complexity** — Need to loop airports and combine results (but manageable with DLT)

### Implementation Details

**DLT Source Configuration:**
- Loop through all 10 Swedavia airports: `[ARN, BMA, GOT, MMX, LLA, UME, OSD, VBY, RNB, KRN]`
- For each airport, call:
  - `/{airport}/arrivals/{date}` → load to `flights_arrivals_raw`
  - `/{airport}/departures/{date}` → load to `flights_departures_raw`
- Add 2-second delay between calls to avoid 429 rate limit errors
- Use `write_disposition: append` to combine all airports into unified tables

**API Call Pattern (per day):**
```
ARN arrivals  → wait 2s → ARN departures  → wait 2s →
BMA arrivals  → wait 2s → BMA departures  → wait 2s →
GOT arrivals  → wait 2s → GOT departures  → wait 2s →
... (repeat for all 10 airports)
```

**Total Runtime:** ~40 seconds/day (acceptable for batch ingestion)

### Future Considerations, currently out of Scope

1. **Query Endpoint Monitoring**
   - Re-test the `/query` endpoint (e.g., monthly)
   - If Swedavia fixes it, consider migrating to reduce API calls
   - Document any changes in behavior

2. **Performance Optimization**
   - If API limits become an issue (unlikely), investigate:
     - Caching strategies
     - Incremental updates using `ContinuationToken`
     - Selective airport updates (only high-traffic airports daily)

3. **Retry Logic**
   - Implement exponential backoff for 429 rate limit errors
   - Add circuit breaker if an airport consistently fails
   - Log failures and continue with remaining airports (graceful degradation)

---

## Security Incident - API Key Exposure

### Incident: API Key Committed to Git

During initial API testing, the Swedavia API subscription key was hardcoded in `tests/test_api_raw.py` for rapid development iteration. This file was committed and pushed to the public GitHub repository, exposing the API key in the git history.

**Root Cause:**
- Hardcoded API key in test file for quick testing
- Insufficient pre-commit review

**Severity:** Medium (free-tier API key, public repository)

### Response Actions

Upon discovery, the following steps were taken immediately:

1. ✅ **Key Rotation** — Obtained new API key from Swedavia portal, invalidating the exposed key
2. ✅ **Code Remediation** — Removed hardcoded key

### Lessons Learned

1. **Never hardcode secrets** — Even for "quick tests" or "just local development"
2. **Pre-commit reviews** — Check for secrets before every commit, not just before push

**Learning Impact:** High
- Real-world security incident handling experience
- Understanding of git history and secret exposure
- Best practices for secret management in data engineering

---

## Historical Data Backfill Limitation

### Decision: 3-Day Backfill Strategy (API Constraint)

Initial design intended 7-day historical backfill to bootstrap analysis pipelines. Testing discovered Swedavia API has strict historical data availability.

### API Testing Results

Backfill test using `test_api_backfill.py` (single airport ARN, looping through dates):

| Date | Days Back | Request | Result | Flights |
|------|-----------|---------|--------|----------|
| 2026-01-25 | 0 (Today) | `GET /ARN/arrivals/2026-01-25` | ✅ 200 OK | 273 arrivals, 282 departures |
| 2026-01-24 | -1 Day | `GET /ARN/arrivals/2026-01-24` | ✅ 200 OK | 189 arrivals, 186 departures |
| 2026-01-23 | -2 Days | `GET /ARN/arrivals/2026-01-23` | ✅ 200 OK | 288 arrivals, 284 departures |
| 2026-01-22 | -3 Days | `GET /ARN/arrivals/2026-01-22` | ❌ 400 Bad Request | N/A |

**Key Finding:** Swedavia API returns HTTP 400 Bad Request for dates older than 2 days in the past (today −2 days is the oldest available data).

**Chosen Approach:** 3-Day Backfill Strategy

**What This Means:**
- Initial backfill loads: Today + 1 day back + 2 days back (3 days total)
- Default `BACKFILL_DAYS=3` in pipeline configuration
- For production with Dagster: Set `BACKFILL_DAYS=1` for daily incremental ingestion

**Rationale:**
1. **API Constraint** — Cannot exceed API's historical availability
2. **Sufficient Bootstrap** — 3 days × ~555 movements/day ≈ 1,665 initial rows for analysis
3. **Prevents Errors** — Hard-coded limit prevents failed pipeline runs on older dates
4. **Clear Documentation** — Explicit constant communicates limitation to team

### Impact on Project Plan

**Dagster Scheduling:**
- Daily job will use `BACKFILL_DAYS=1` 
- Ensures fresh data without API errors
- First-run backfill handles historical bootstrap once

### Evidence & References

- Test file: `tests/test_api_backfill.py`
- Validation output: 3 successful API calls, 1 failed with 400 error
- Pipeline configuration: `src/svensk_flyt/pipelines/run.py` — `BACKFILL_DAYS` default set to 3

---

## Airport Dimension and Data Quality Improvements

### Decision: Implement Seed-Based Airport Master Data with Auto-Discovery

Initial MVP implementation hardcoded Swedish airport IATA codes in the `dim_airport` dimension. This approach had limitations:

1. **Lack of Enrichment** — Only had IATA codes, no airport names or metadata
2. **Limited Scope** — Only 10 Swedish airports, couldn't handle flights to/from international destinations
3. **Manual Maintenance** — Any new airport route required code changes
4. **Data Quality** — No mechanism to identify unknown airports in source data

### Decision Rationale

**Chosen Approach:** Seed-Based Airport Master Data with Dynamic Auto-Discovery

**Implementation:**

1. **Airport Seed File** (`dbt/seed/airport_seed.csv`)
   - Comprehensive reference of 150+ IATA codes globally
   - Includes airport name and country information
   - Loaded via dbt seed mechanism
   - Data quality tests on IATA code format and uniqueness

2. **Refactored `dim_airport` Model**
   - Changed from hardcoded list to dynamic auto-discovery from flight data
   - LEFT JOIN to seed data for enrichment (airport name, country)
   - Auto-discovery: any airport in flight data automatically creates dimension record
   - Unknown airports (not in seed) get alert indicator for data quality monitoring
   - Eliminates ETL failures when new routes to international airports are discovered

3. **Staging Layer Improvements**
   - Replaced hardcoded Swedish airport lists with seed-based country checks
   - `stg_flights_arrivals.sql` and `stg_flights_departures.sql` now use `LEFT JOINs` to seed data
   - Domestic/international classification now based on country lookup, not hardcoded lists
   - More robust handling of null values

**Rationale:**
1. **Scalability** — Handles international routes without code changes
2. **Data Quality** — Seed provides authoritative airport reference; unknown airports flagged
3. **Maintainability** — Centralized seed data vs scattered hardcoded lists
4. **Flexibility** — Easy to add new airport metadata (timezones, regions, etc.) in future
5. **Transparency** — Clear lineage: source data → auto-discovery → enriched via seed

---

### Additional Data Quality Improvements

#### Corrected Punctuality Definitions

**Decision:** Implement Industry-Standard Punctuality Classification

**Changes in `mart_airline_punctuality.sql`:**
- **On-Time Classification:** Updated to include early arrivals up to 15 minutes before schedule
  - Previous: `delay < 15` (unclear boundary)
  - New: `delay <= 15` (explicit, includes early and exactly-on-time)
- **Delayed Classification:** Corrected to strict inequality
  - Previous: `delay >= 15` (overlapped with on-time)
  - New: `delay > 15` (mutually exclusive with on-time)

**Rationale:**
- Aligns with IATA/industry standards (15-minute threshold)
- Fixes edge case: flights exactly 15 minutes early/late are unambiguous

#### Fixed Route Popularity Join Logic

**Decision:** Prevent Data Loss in Cross-Airport Route Aggregation

**Changes in `mart_route_popularity.sql`:**
- Changed airport dimension joins from `INNER JOIN` to `LEFT JOIN`
- Prevents filtering out routes to/from unknown airports (not in seed)
- Improved filter documentation to clarify inclusion of both domestic and international routes

**Rationale:**
- Initial INNER JOIN silently dropped flights to non-Swedish airports
- LEFT JOIN preserves data; unknown airports display with NULL enrichment
- Complements auto-discovery dimension design (see Group 2 above)



### Change Summary

| Group | Files Modified | Purpose | Benefit |
|-------|----------------|---------|---------|
| 1 | `airport_seed.csv`, `dbt_project.yml`, `schema.yml` | Airport reference data | Single source of truth for 150+ airports globally |
| 2 | `dim_airport.sql` | Dynamic dimension with seed enrichment | Auto-discovers new airports, enriches with metadata |
| 3 | `mart_airline_punctuality.sql` | Correct punctuality classification | Industry-standard definitions, clearer logic |
| 4 | `mart_route_popularity.sql` | Fixed join logic and documentation | Includes international routes, prevents data loss |
| 5 | `stg_flights_arrivals.sql`, `stg_flights_departures.sql` | Seed-based domestic flag logic | Eliminates hardcoded lists, more maintainable |

### Implementation Details

**Data Flow (After Improvements):**
```
API → staging (seed-enriched classification) → facts → dimensions (auto-discovery + seed enrichment) → marts (all routes, correct metrics)
```

**Example: New International Route ARN→CDG (Stockholm→Paris)**
1. API returns flight to CDG (Paris Charles de Gaulle)
2. Staging layer receives unknown airport CDG
3. `dim_airport` auto-discovers CDG from fact data
4. LEFT JOIN to seed: CDG found, enriched with name + country (France)
5. Route dimension recognizes ARN→CDG as international route
6. `mart_route_popularity` includes ARN→CDG in aggregations
7. Dashboard shows new route with full airport details

**No Code Changes Required** — pure data-driven discovery!
