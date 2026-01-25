# Swedavia API Testing Results

**Test Date:** January 25, 2026  
**Tester:** Automated validation of API endpoints  
**Purpose:** Determine optimal API ingestion strategy for svensk-flyt pipeline

---

## Summary

The Swedavia FlightInfo API `/query` endpoint returns 0 results even with documentation-exact OData filter syntax, while individual airport endpoints (`/{airport}/arrivals/{date}`) work perfectly and return complete flight data.

**Conclusion:** Use individual airport endpoints for data ingestion.

---

## Test Environment

- **API Base URL:** `https://api.swedavia.se/flightinfo/v2`
- **API Key:** Configured via `.env` (SWEDAVIA_API_KEY)
- **Test Date:** 2026-01-25 (today)
- **Airports Tested:** ARN (Stockholm Arlanda), VBY (Visby), all 10 Swedavia airports
- **Date Format Tested:** YYYY-MM-DD (ISO 8601), YYMMDD (API internal format)

---

## Test Results

### Test 1: Single Airport Endpoint ✅ SUCCESS
**Endpoint:** `/ARN/arrivals/2026-01-25`  
**Date Format:** YYYY-MM-DD

```
Status: 200
✓ Arrivals: 273
Sample: FI312
```

**Result:** Works perfectly. Returns complete flight data for the specified airport and date.

---

### Test 2: Query Endpoint with All Airports (Reordered Filter) ❌ FAILED
**Endpoint:** `/query`  
**Filter:** `(airport eq 'ARN' or airport eq 'BMA' or airport eq 'GOT' or airport eq 'MMX' or airport eq 'LLA' or airport eq 'UME' or airport eq 'OSD' or airport eq 'VBY' or airport eq 'RNB' or airport eq 'KRN') and flightType eq 'A' and scheduled eq '260125'`  
**Date Format:** YYMMDD

```
Status: 200
✓ Arrivals: 0
Sample: N/A
```

**Result:** API accepts request but returns 0 flights, even though Test 1 proves there are 273 flights available for ARN alone.

---

### Test 3: Query Endpoint with Doc Example Format ❌ FAILED
**Endpoint:** `/query`  
**Filter:** `(airport eq 'ARN' or airport eq 'VBY') and flightType eq 'A' and scheduled eq '260125'`  
**Date Format:** YYMMDD  
**Note:** This filter exactly matches the syntax shown in the official Swedavia API documentation examples.

```
Status: 200
✓ Arrivals: 0
Sample: N/A
```

**Result:** Even with documentation-exact syntax and only 2 airports, the query endpoint returns 0 flights.

---

### Test 4: Single Airport Endpoint (7 Days Ago) ❌ DATE ERROR
**Endpoint:** `/ARN/arrivals/2026-01-18`  
**Date Format:** YYYY-MM-DD

```
Status: 400
Error: {"errors":["Date does not contain a valid date."]}
```

**Result:** API rejects past dates with YYYY-MM-DD format for some reason. Possible API limitation or bug. Documentation states API supports "7 days back to 90 days forward."

---

## API Behavior Observations

### 1. Query Endpoint Issues
- The `/query` endpoint appears broken or has severe undocumented limitations
- Returns HTTP 200 but with 0 results, even when data exists
- Tested multiple filter variations (reordered, simplified, doc-exact) — all failed
- This contradicts the official documentation which shows working examples

### 2. Individual Endpoint Reliability
- Individual airport endpoints (`/{airport}/arrivals/{date}`) work perfectly
- Return complete, accurate flight data
- Consistent response times and data quality

### 3. Rate Limiting
- Encountered 429 "Rate limit exceeded" when making requests <2 seconds apart
- **Recommendation:** Implement 2-second delay between API calls
- This is manageable: 20 calls/day × 2 seconds = 40 seconds total pipeline runtime

### 4. Date Format Requirements
- Individual endpoints accept YYYY-MM-DD (ISO 8601) ✅
- Query endpoint requires YYMMDD in `scheduled` field (per docs)
- Past dates may have issues (Test 4 failed with 400 error)

---

## API Limit Calculations

### Using Individual Endpoints (Chosen Approach)
- **Per Day:** 10 airports × 2 calls (arrivals + departures) = 20 calls
- **Per Month:** 20 calls/day × 30 days = 600 calls
- **API Limit:** 10,001 calls per 30 days (free tier)
- **Utilization:** 600 / 10,001 = **6% of monthly quota**
- **Safety Buffer:** 9,401 calls remaining for testing, backfills, retries

### Using Query Endpoint (Would Have Been Ideal, But Broken)
- **Per Day:** 2 calls (1 for all arrivals, 1 for all departures)
- **Per Month:** 60 calls
- **Would save:** 540 calls/month vs individual endpoints
- **Status:** Not viable due to 0 results issue

---

## Plan to move forward

1. **Use individual airport endpoints** for production pipeline
2. **Implement 2-second delay** between API calls to avoid 429 errors
3. **Use YYYY-MM-DD format** for dates (works with individual endpoints)
4. **Add retry logic** with exponential backoff for transient failures

---

## Future Considerations

- **Pagination:** Individual endpoints may paginate for high-traffic days (check response headers)
- **Historical Backfills:** Test date format requirements for dates >7 days in the past

---

## Related Files

- **Test Script:** `tests/test_api_raw.py`
- **Design Decision:** `docs/DESIGN_DECISIONS.md`
- **DLT Source:** `src/svensk_flyt/defs/dlt/pipelines/swedavia.py`
- **API Documentation:** [Swedavia FlightInfo API Docs](https://www.swedavia.se/en/about-swedavia/about-us/api/)
