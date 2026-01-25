# Design Decisions

This document captures key architectural and implementation decisions made during the development of the svensk-flyt data pipeline.

---

## Week 1: API Ingestion Strategy

### Decision: Use Individual Airport Endpoints

**Date:** January 25, 2026  
**Status:** ✅ Implemented  
**Impact:** Critical — affects entire ingestion pipeline architecture

---

### Context

The Swedavia FlightInfo API offers two approaches for fetching flight data:

1. **Individual Airport Endpoints:** `/ARN/arrivals/2026-01-25`, `/BMA/arrivals/2026-01-25`, etc.
   - Requires 1 API call per airport per flight type (arrivals/departures)
   - Proven to work reliably in testing
   - Simple, straightforward implementation

2. **Query Endpoint with OData Filters:** `/query?filter=(airport eq 'ARN' or airport eq 'BMA' ...) and ...`
   - Would allow fetching all airports in 1-2 API calls
   - More efficient in theory
   - Documented in official API documentation with examples

---

### Testing Results

Comprehensive API testing was conducted (see `tests/API_TESTING.md` for full results):

| Test | Endpoint | Airports | Result | Flights Returned |
|------|----------|----------|--------|------------------|
| 1 | `/ARN/arrivals/{date}` | ARN | ✅ Success | 273 |
| 2 | `/query` (all airports, reordered filter) | All 10 | ❌ Failed | 0 |
| 3 | `/query` (doc-exact syntax) | ARN, VBY | ❌ Failed | 0 |

**Key Finding:** The query endpoint returns HTTP 200 but with 0 flights, even when hundreds of flights exist for the same date/airports in the individual endpoint.

---

### Decision Rationale

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

---

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

---

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

## Week 1: Date Format Standardization

### Decision: Accept YYYY-MM-DD, Convert Internally

**Date:** January 25, 2026  
**Status:** ✅ Implemented  

**Context:**
- Individual endpoints accept ISO 8601 format (YYYY-MM-DD)
- Query endpoint requires YYMMDD in `scheduled` filter field
- Users expect standard date formats

**Decision:**
- Pipeline config accepts YYYY-MM-DD (user-friendly, standard)
- Internal conversion to YYMMDD where needed (hidden from users)
- Centralized date handling in source functions

**Rationale:**
- User experience: Industry-standard date format
- Maintainability: One place to change if API requirements shift
- Clarity: Unambiguous dates (vs YYMMDD which could be misread)

---

## Week 1: DuckDB as Data Warehouse

### Decision: Use DuckDB for Local Analytics Warehouse

**Date:** January 25, 2026  
**Status:** ✅ Implemented  

**Context:**
- Need a database for raw data storage and dbt transformations
- Options: PostgreSQL, SQLite, DuckDB, cloud warehouses (BigQuery, Snowflake)

**Decision:**
- Use DuckDB with local file storage (`data_warehouse/svenska-flyt.duckdb`)

**Rationale:**
1. **Cost** — Completely free, no cloud bills
2. **Performance** — Columnar storage, fast analytical queries, handles millions of rows easily
3. **Simplicity** — Single-file database, no server to manage
4. **dbt Support** — Native dbt-duckdb adapter available
5. **Development Speed** — No infrastructure setup, works immediately
6. **Sufficient Scale** — 600 flights/day × 365 days = ~220k rows/year (trivial for DuckDB)

**Trade-offs:**
- Not suitable for multi-user concurrent access (not needed for this project)
- File-based (but simplifies backup/versioning)

**Migration Path:**
- If production deployment needed, swap to Snowflake
- dbt SQL will mostly work as-is

---

## Week 1: Secrets Management

### Decision: Use `.env` for Local Secrets, Exclude from Git

**Date:** January 25, 2026  
**Status:** ✅ Implemented  

**Context:**
- API key exposed in early commits (since rotated)
- Need secure, simple secret management

**Decision:**
- Store API key in `.env` file (git-ignored)
- Use `python-dotenv` to load at runtime
- Provide `.env.example` template
- Document setup in README

**Rationale:**
- Industry-standard approach (12-factor app)
- Simple for local development
- No external dependencies (unlike Vault)
- Works with CI/CD (secrets as environment variables)

**Security Measures:**
1. `.env` in `.gitignore`
2. API key rotated after exposure
3. `.env.example` has placeholder values only
4. README warns about secret management

**Future:**
- For production: Use CI secrets, Azure Key Vault, or similar
- Current approach sufficient for local development

---

## Week 1: Security Incident - API Key Exposure

### Incident: API Key Committed to Git

**Date:** January 25, 2026  
**Status:** ✅ Resolved  
**Severity:** Medium (free-tier API key, public repository)

---

### What Happened

During initial API testing, the Swedavia API subscription key was hardcoded in `tests/test_api_raw.py` for rapid development iteration. This file was committed and pushed to the public GitHub repository, exposing the API key in the git history.

**Root Cause:**
- Hardcoded API key in test file for quick testing
- Insufficient pre-commit review
- Missing secret detection tooling

---

### Response Actions

Upon discovery, the following steps were taken immediately:

1. ✅ **Key Rotation** — Obtained new API key from Swedavia portal, invalidating the exposed key
2. ✅ **Code Remediation** — Removed hardcoded key, implemented environment variable loading via `python-dotenv`
3. ✅ **Prevention Measures** — Added `.env` to `.gitignore`, created `.env.example` template
4. ✅ **Documentation** — Documented incident, response, and lessons learned (this section)

---

### Lessons Learned

1. **Never hardcode secrets** — Even for "quick tests" or "just local development"
2. **Environment variables from day one** — Set up `.env` infrastructure before writing code that needs secrets
3. **Pre-commit reviews** — Check for secrets before every commit, not just before push
---

### Impact Assessment

**Actual Impact:** Low
- Free-tier API key with 10,001 request/month limit
- No sensitive data accessed (only public flight schedules)
- Key rotated before any unauthorized usage detected
- No financial or data breach occurred

**Learning Impact:** High
- Real-world security incident handling experience
- Understanding of git history and secret exposure
- Best practices for secret management in data engineering
- Documentation and incident response skills

---

## Week 1: Historical Data Backfill Limitation

### Decision: 3-Day Backfill Strategy (API Constraint)

**Date:** January 25, 2026  
**Status:** ✅ Validated  
**Impact:** Medium — affects initial data load strategy

---

### Context

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

### Decision Rationale

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

**Step 2 (Feature Branch for Backfill):**
- ✅ Backfill capability validated
- ✅ Can load initial 3-day dataset
- ✅ Ready for dbt transformation layer (Step 3)

**Step 4 (Dagster Scheduling):**
- Daily job will use `BACKFILL_DAYS=1` (today only)
- Ensures fresh data without API errors
- First-run backfill handles historical bootstrap once

### Future Considerations

1. **API Documentation Check** — Verify historical limit in official Swedavia docs (was not clearly documented)
2. **Contact Swedavia Support** — Ask if historical limit can be increased for higher-tier accounts
3. **Cache Strategy** — Consider caching 3-day rolling window locally to detect data changes
4. **Data Quality** — With only 3 days, ensure robust duplicate detection for daily runs

### Evidence & References

- Test file: `tests/test_api_backfill.py`
- Validation output: 3 successful API calls, 1 failed with 400 error
- Pipeline configuration: `src/svensk_flyt/pipelines/run.py` — `BACKFILL_DAYS` default set to 3



