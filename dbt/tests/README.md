# dbt Custom Data Tests

This directory contains **singular tests** that validate business logic and data quality beyond simple schema constraints (unique, not_null).

## How to Run Tests

```bash
# Run all tests (schema + custom)
dbt test

# Run only custom tests in this directory
dbt test --select test_type:singular

# Run a specific test
dbt test --select test_punctuality_percentages_add_up

# See which tests would run without executing
dbt test --select test_type:singular --dry-run
```

## Test Catalog

### 1. **test_punctuality_percentages_add_up.sql**
**What it checks:** Percentages in `mart_airline_punctuality` sum to 100%
- `on_time_percentage + delayed_percentage + early_percentage ≈ 100%`
- Allows 0.1% tolerance for rounding errors

**Why it matters:** Catches calculation errors in percentage logic

**Expected result:** ✅ 0 rows (all percentages sum correctly)

---

### 2. **test_delay_minutes_reasonable_range.sql**
**What it checks:** Delays are within realistic bounds
- No flights more than 3 hours early (`delay < -180 min`)
- No flights more than 12 hours late (`delay > 720 min`)

**Why it matters:** Extreme delays likely indicate data errors or API issues

**Expected result:** ✅ 0 rows (all delays are reasonable)

**Possible failures:** 
- API returns incorrect timestamps
- Timezone conversion errors
- Edge case: Real delays exceeding 12 hours (rare but possible)

---

### 3. **test_baggage_times_logical.sql**
**What it checks:** First bag arrives before last bag
- `first_bag_utc <= last_bag_utc`

**Why it matters:** Illogical baggage times indicate data quality issues

**Expected result:** ✅ 0 rows (all baggage times are chronological)

---

### 4. **test_no_future_actual_times.sql**
**What it checks:** Actual flight times are not in the future
- `actual_time_utc <= current_timestamp + 1 hour` (1hr buffer for sync)

**Why it matters:** Actual times should be historical; future values are data errors

**Expected result:** ✅ 0 rows (no future actual times)

**Note:** Scheduled times CAN be in future (that's expected)

---

### 5. **test_flight_date_matches_scheduled.sql**
**What it checks:** `flight_date` aligns with `scheduled_time_utc` date portion
- `flight_date = date(scheduled_time_utc)`

**Why it matters:** Mismatches indicate extraction or transformation bugs

**Expected result:** ✅ 0 rows (all dates match)

**Possible failures:**
- Timezone handling errors
- Incorrect date parsing in staging layer

---

### 6. **test_landed_flights_have_actual_times.sql**
**What it checks:** Flights with status 'LAN' have actual times populated
- `is_landed = true → actual_time_utc IS NOT NULL`

**Why it matters:** Landed flights should have completion timestamps

**Expected result:** ✅ 0 rows (all landed flights have actual times)

**Possible failures:**
- API returns incomplete data
- Status updates but timestamps don't sync

---

### 7. **test_cancelled_flights_not_marked_on_time.sql**
**What it checks:** Cancelled flights aren't marked as on-time
- `is_cancelled = true → is_on_time = false`

**Why it matters:** Catches illogical business rule combinations

**Expected result:** ✅ 0 rows (no cancelled flights marked on-time)

---

## Test Results Interpretation

dbt tests return **the rows that FAIL the test**:
- ✅ **0 rows returned** = Test passed (no violations found)
- ❌ **N rows returned** = Test failed (N violations found)

When a test fails, examine the returned rows to understand:
1. Which flights violated the rule
2. Whether it's a data quality issue or a real edge case
3. If the test threshold needs adjustment

## Adding New Tests

Custom tests are just SQL queries that return failing rows:

```sql
-- Template for a new test
select
    flight_key,
    flight_id,
    -- ... relevant columns ...
    'Description of what failed' as issue_type
from {{ ref('model_name') }}
where <condition_that_should_never_be_true>
```

Save as `test_description_of_check.sql` in this directory.

## Maintenance

Review test thresholds periodically:
- Are `-180 to +720` minutes realistic for delay bounds?
- Should baggage time checks have tolerance (e.g., allow equal times)?
- Do future-time checks need wider buffers during DST transitions?

Adjust based on real-world data patterns observed in your pipeline.
