# Analytics Guide

The four products describe requests **created locally in NYC during September
2026**, using each request's saved observation time. Source agency, classification,
status, and dates can change after creation. A closure in October remains part
of this cohort. Results are not a snapshot of status at September 30.

This guide provides local queries over an already verified publication. The
examples use the README's synthetic `sample-docs-001` run, not real NYC findings.
Replace the base, mode, and run ID explicitly for your own successful run.
Fixture expectations assume that no additional sample creation days exist in
the chosen base; the coordinator includes existing sample days in Gold.

## Verify Before Querying

Only a complete Gold manifest authorizes the four products together. The
pipeline checks that manifest before reporting `succeeded`; independently verify
an existing publication through the read API:

```bash
uv run python - <<'PY'
from pathlib import Path
from nyc311.gold_store import load_gold

manifest, products = load_gold(Path("data/local"), "sample-docs-001", mode="sample")
print("Coverage:", manifest["coverage"], manifest["covered_days"])
print("Observed:", manifest["observation_min_utc"], manifest["observation_max_utc"])
print("Quality:", manifest["quality_exclusions"])
print("Rows:", {name: table.num_rows for name, table in products.items()})
PY
```

`load_gold` verifies all four files, hashes, schemas, row counts, modes, coverage,
quality accounting, and frozen input provenance. It does not require current
Silver to equal historical Silver. Do not interpret partial `prepared` files as
published, use a failed run, or read a wildcard spanning multiple Gold runs.

After verification, open the Nix-provided DuckDB CLI from the repository root:

```bash
duckdb
```

Create temporary views for exactly one run. These are consumption queries; the
production transformations are the four versioned files in `sql/`.

```sql
CREATE TEMP VIEW demand_daily AS
SELECT * FROM read_parquet('data/local/sample/gold/run=sample-docs-001/demand_daily/part.parquet');

CREATE TEMP VIEW closure_duration AS
SELECT * FROM read_parquet('data/local/sample/gold/run=sample-docs-001/closure_duration/part.parquet');

CREATE TEMP VIEW outstanding_requests AS
SELECT * FROM read_parquet('data/local/sample/gold/run=sample-docs-001/outstanding_requests/part.parquet');

CREATE TEMP VIEW channel_usage AS
SELECT * FROM read_parquet('data/local/sample/gold/run=sample-docs-001/channel_usage/part.parquet');
```

For real data, use `data/local/published/gold/run=<successful-id>/...` only after
the 30-day completeness and post-extraction checks in the runbook. Changing a
path does not turn sample results into an official publication.

## 1. Demand

**Question:** Which problems concentrate demand, and how does it vary by borough
and creation day within September?

Grain: `created_date`, `borough`, `complaint_type`. `request_count` is the number
of accepted unique Silver requests in that group. `category_share` is:

```text
group request_count / all accepted requests for that creation day and borough
```

Unknown borough/problem buckets remain in counts and denominators. Records with
optional defects or invalid closure durations still contribute if accepted in
Silver. Quarantined records and discarded identical duplicates do not contribute.

```sql
SELECT
    created_date,
    borough,
    complaint_type,
    request_count,
    round(category_share * 100, 2) AS category_share_pct
FROM demand_daily
ORDER BY created_date, borough, request_count DESC, complaint_type;
```

The fixture contains 12 accepted requests across September 1-2. On September 1
in Manhattan, Street Condition has 3 of 4 requests (75 percent); Noise -
Residential has 1 of 4 (25 percent). Across all groups, counts sum to 12.

Counts measure reported service-request demand, not actual incident prevalence.
No population dataset is joined, so these are not per-resident rates. A single
month supports daily/weekly analysis inside September, not between-month
comparisons, annual seasonality, or assertions that a borough's service is worse.

## 2. Administrative Closure Duration

**Question:** How long do requests in the September-created cohort take to close
administratively, grouped by agency and problem?

Grain: `agency`, `complaint_type`. Metrics are `valid_closed_count`,
`median_closure_hours`, and `p90_closure_hours`. The SQL uses Silver's
`lifecycle_class = closed` and `duration_eligible`, without recoding lifecycle or
quality. Eligibility excludes missing/unresolvable dates, negative durations,
closure after observation, and requests that are not explicitly closed.

Elapsed hours use the difference between UTC-normalized instants at microsecond
precision. The median and p90 are continuous interpolated quantiles, not a
nearest-rank percentile or rounded integer-second duration. Zero-hour closures
are valid. NYC daylight-saving changes affect elapsed UTC time correctly.

```sql
SELECT
    agency,
    complaint_type,
    valid_closed_count,
    round(median_closure_hours, 2) AS median_closure_hours,
    round(p90_closure_hours, 2) AS p90_closure_hours
FROM closure_duration
ORDER BY valid_closed_count DESC, agency, complaint_type;
```

With fixture observations fixed to October 13, 2026 UTC in tests, DOT's eligible
hours `[1, 3, 9, 696]` give count 4, median 6, and p90 489.9. The 696-hour case
closes in October but was created in September. NYPD has one eligible zero-hour
closure. Invalid, ambiguous, future, open, and unmapped-status cases are excluded
from this population, not from accepted demand counts.

Fresh CLI samples record the execution's actual observation times. Consequently,
an intentionally future closure can become eligible later. Read the manifest's
observation range rather than assuming the fixed test timestamp or a permanently
fixed closure count. Offline replay preserves the saved observation and decisions.

Administrative closure is not customer satisfaction or proof that an issue was
resolved satisfactorily. The distribution excludes still-open requests, creating
closed-case selection/right-censoring bias. Agency comparisons also depend on
problem mix. Do not average or sum group medians/p90s to claim a global percentile;
a new distribution calculation would require the underlying eligible durations.

## 3. Outstanding And Unknown Requests

**Question:** Which September-created requests are confirmed outstanding, and
what is their age at their recorded observation?

Grain: `agency`, `borough`, `lifecycle_class`, `age_band`. Gold reuses Silver's
classification. `outstanding_count` is nonzero only for `outstanding` rows;
`unknown_status_count` is nonzero only for `unknown` rows. Explicitly closed
requests do not enter this product. Unknown status is never inferred as pending.

Age is exact elapsed UTC microseconds from creation to saved observation, grouped
into `[0,7)`, `[7,14)`, `[14,30)`, and `30+` days. Negative or uncomputable ages use
`UNKNOWN`. The calculation does not use `now()` or Gold's generation time.

```sql
SELECT
    agency,
    borough,
    lifecycle_class,
    age_band,
    outstanding_count,
    unknown_status_count
FROM outstanding_requests
ORDER BY agency, borough, lifecycle_class, age_band;
```

The fixture has two confirmed outstanding requests and one unmapped-status
request. With October 13 observations, all are `30+`; the unknown-state count
remains separate. A fresh sample's bands depend on its actual observation times,
but replay does not make requests older merely because it runs later.

These counts cover only the loaded September-created cohort, not total NYC
backlog. They are not a historical backlog series. Selected-day refreshes can
mix observations from different dates: inspect `observations_by_day` and overall
`observation_min_utc`/`observation_max_utc`. Generation time does not establish
that every request was refreshed at that instant.

## 4. Intake Channels

**Question:** How are intake channels used within each creation day and borough?

Grain: `created_date`, `borough`, `channel`. `request_count` counts accepted unique
requests, and `channel_share` uses all accepted requests for that day/borough as
its denominator. Missing values remain `UNKNOWN`; new channel labels preserved
by Silver remain visible rather than being collapsed by Gold.

```sql
SELECT
    created_date,
    borough,
    channel,
    request_count,
    round(channel_share * 100, 2) AS channel_share_pct
FROM channel_usage
ORDER BY created_date, borough, channel;
```

The current default fixture has ten output groups:

| Day | Borough | Channel | Count | Share |
|---|---|---|---:|---:|
| 2026-09-01 | MANHATTAN | ONLINE | 2 | 50% |
| 2026-09-01 | MANHATTAN | PHONE | 2 | 50% |
| 2026-09-01 | QUEENS | MOBILE | 1 | 50% |
| 2026-09-01 | QUEENS | UNKNOWN | 1 | 50% |
| 2026-09-01 | UNKNOWN | UNKNOWN | 1 | 100% |
| 2026-09-02 | MANHATTAN | MOBILE | 1 | 50% |
| 2026-09-02 | MANHATTAN | UNKNOWN | 1 | 50% |
| 2026-09-02 | QUEENS | PHONE | 1 | 100% |
| 2026-09-02 | UNKNOWN | MOBILE | 1 | 50% |
| 2026-09-02 | UNKNOWN | ONLINE | 1 | 50% |

Globally, the fixture has three requests for each of ONLINE, PHONE, MOBILE, and
UNKNOWN. That overall balance does not make every daily share 25 percent.
Do not exclude UNKNOWN from the denominator or average daily shares without
weighting by their daily request populations. For a selected-period channel mix
within each borough, recompute shares from summed counts:

```sql
SELECT
    borough,
    channel,
    sum(request_count) AS request_count,
    round(100.0 * sum(request_count) / sum(sum(request_count)) OVER (
        PARTITION BY borough
    ), 2) AS period_channel_share_pct
FROM channel_usage
GROUP BY borough, channel
ORDER BY borough, channel;
```

For the fixture's Manhattan population of six requests, ONLINE and PHONE each
have two (33.33 percent), MOBILE and UNKNOWN each have one (16.67 percent).

## Population Reconciliation

Demand and channel counts must independently equal the accepted Silver
population of the selected publication. Pending/unknown counts and eligible
closed counts are subsets, not an alternative total of all lifecycle states.
The following check is over exactly the chosen Gold run:

```sql
SELECT
    (SELECT coalesce(sum(request_count), 0) FROM demand_daily) AS demand_count,
    (SELECT coalesce(sum(request_count), 0) FROM channel_usage) AS channel_count,
    (SELECT coalesce(sum(valid_closed_count), 0) FROM closure_duration) AS eligible_closed_count,
    (SELECT coalesce(sum(outstanding_count), 0) FROM outstanding_requests) AS confirmed_outstanding_count,
    (SELECT coalesce(sum(unknown_status_count), 0) FROM outstanding_requests) AS unknown_status_count;
```

At the fixed test observation this returns `(12, 12, 5, 2, 1)`. It is a fixture
expectation, not an expected real September record total. Quality exclusions
and warnings in the manifest explain why accepted counts, eligible counts, and
raw source counts differ. Reason-frequency counters are not disjoint populations.
