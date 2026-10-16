# Analytical Findings

Results of the four Gold products for the full September 2026 cohort, produced
by the authorized local run `september-2026-001`. These are data findings, not a
deployed-AWS result: they come from the same SQL and schemas that the AWS path
executes.

## Scope And Coverage

- **Cohort:** 323,627 requests `created_date` in `[2026-09-01, 2026-10-01)` NYC
  local time (30 days).
- **Observation range:** `2026-10-14T14:15:13Z` to `2026-10-14T14:18:57Z`, saved
  per cross-partition provenance. Ages and closures use these observations, not
  the query run time.
- **Accounting:** `received = accepted = 323,627`; `quarantined = 0`;
  `identical_duplicates_discarded = 0`.
- **Limits:** descriptions are the September-created cohort at the recorded
  observation, not NYC-wide backlog or the historical status on September 30.
  Metric definitions are in [analytics.md](analytics.md).

## 1. Demand Concentrates In A Few Problems And Boroughs

`demand_daily`, 14,598 day/borough/category rows.

- Top problem categories: **Illegal Parking 53,276 (16.5%)**, Noise - Residential
  31,602 (9.8%), Noise - Street/Sidewalk 19,039 (5.9%), Blocked Driveway 16,331
  (5.0%), UNSANITARY CONDITION 14,193 (4.4%).
- Demand by borough: **Brooklyn 105,704 (32.7%)**, Queens 80,443 (24.9%),
  Manhattan 64,994 (20.1%), Bronx 59,212 (18.3%), Staten Island 12,876 (4.0%),
  `UNKNOWN` 398 (0.1%).
- Busiest creation days: **2026-09-29 (11,976)**, 2026-09-18 (11,964),
  2026-09-30 (11,854).

Parking and noise dominate a service mix that is otherwise long-tailed; shares
use all accepted requests in the day/borough, including unknown dimensions.

## 2. Closure Times Are Bimodal Across Agencies

`closure_duration`, 160 agency/category rows, 285,644 valid closed requests
(37,780 not closed + 203 closed without a valid duration are excluded).

- Fast administrative closures: NYPD Illegal Parking 53,274 closed, median
  **1.7 h**, p90 8.2 h; NYPD Noise - Residential 1.2 h / 6.1 h.
- Slow, work-ordered closures: HPD UNSANITARY CONDITION 11,868 closed, median
  **179.5 h (~7.5 days)**, p90 458.9 h (~19 days); DOT Street Condition 39.3 h /
  154.3 h.
- Slowest well-populated groups: DOT Broken Parking Meter (176 closed) median
  **522.2 h (~21.8 days)**, p90 589.5 h; DOT Curb Condition (539) 386.9 h.

Median/p90 are computed per agency and category from eligible closed requests
with `created <= closed <= observed`. Administrative closure is not resident
satisfaction.

## 3. Confirmed Outstanding Requests Are Mostly Older Than Two Weeks

`outstanding_requests`, 212 rows, **37,756 confirmed outstanding** plus **24**
separately labeled unknown-status requests at the observation.

- Age bands: `[7,14)` 1,846; `[14,30)` 23,601; `30+` 12,309; `[0,7)` none.
- Agencies with the most confirmed outstanding: **DPR 10,201**, HPD 9,081,
  DOB 3,894, DOT 3,770.

Most confirmed open requests have already been open 14 days or more at the
observation, led by Parks (DPR) and Housing (HPD). Unknown-status rows are never
added to confirmed backlog totals.

## 4. Intake Is Digital-First, With A Visible Unknown Channel

`channel_usage`, 698 day/borough/channel rows.

- **ONLINE 175,205 (54.1%)**, PHONE 62,777 (19.4%), MOBILE 60,815 (18.8%),
  `UNKNOWN` 24,830 (7.7%).

Online leads, but a non-trivial 7.7% lacks a mapped channel; it is reported as
`UNKNOWN` rather than dropped so shares keep their denominator.

## Reproduction

Each finding is recomputed from
`data/local/published/gold/run=september-2026-001/<product>/part.parquet`. The
DuckDB and Athena examples in
[analytics.md](analytics.md) reproduce the grains and denominators used here;
present counts with the cohort and observation range above so they are never
read as NYC-wide or point-in-time claims.
