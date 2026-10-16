# Data Dictionary

Field-level reference for the Silver table and the four Gold products, plus the
control manifests that authorize them. Types are the explicit PyArrow types in
`src/nyc311/silver_contract.py` and `src/nyc311/gold.py`, mapped to Glue types in
`infra/locals.tf`.

## Conventions

- **Cohort.** Requests `created_date` in `[2026-09-01, 2026-10-01)` NYC local
  time. Closures and observations may fall after September.
- **Local dates/timestamps.** `*_local` fields are floating NYC-local
  timestamps (`America/New_York`); `created_date` is the NYC-local calendar day.
- **UTC instants.** `*_utc` fields are timezone-free microsecond timestamps that
  are already normalized to UTC. Elapsed-time math uses only `_utc` fields.
- **Observation.** `observed_at_utc` is the source retrieval time saved in
  Bronze. Request ages use the saved observation, never the clock at query time.
- Missing dimensions are normalized to the literal `UNKNOWN`; they are kept, not
  dropped, so shares have a stable denominator.

## Silver Table (`silver`)

One Parquet file per NYC-local creation day. The physical file carries a
`created_date` column; the Glue table exposes it only as the Hive partition key,
so Athena reads a single `created_date`.

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| `unique_key` | string | no | Source request identifier. |
| `agency` | string | yes | Responsible agency code. |
| `agency_name` | string | yes | Agency display name. |
| `complaint_type` | string | yes | Problem category (not fuzzy-merged). |
| `descriptor` | string | yes | Source descriptor. |
| `borough` | string | yes | Normalized borough or `UNKNOWN`. |
| `incident_zip` | string | yes | Postal code kept as text. |
| `status` | string | yes | Normalized source status. |
| `open_data_channel_type` | string | yes | Intake channel or `UNKNOWN`. |
| `created_date_local` | timestamp | no | NYC-local creation instant. |
| `closed_date_local` | timestamp | yes | NYC-local closure instant. |
| `resolution_action_updated_date_local` | timestamp | yes | NYC-local last-action instant. |
| `created_date_utc` | timestamp | no | UTC-normalized creation instant. |
| `closed_date_utc` | timestamp | yes | UTC-normalized closure instant. |
| `resolution_action_updated_date_utc` | timestamp | yes | UTC-normalized last-action instant. |
| `source_updated_at_utc` | timestamp | yes | Source `:updated_at`, informational only (not CDC). |
| `observed_at_utc` | timestamp | no | Retrieval instant saved in Bronze. |
| `created_date` | date | no | Partition key: NYC-local creation day. |
| `lifecycle_class` | string | no | `closed`, `outstanding` or `unknown`. |
| `duration_eligible` | boolean | no | Closed with a usable, non-negative duration. |
| `quality_flags` | array&lt;string&gt; | no | Optional-field warning labels (see below). |
| `source_run_id` | string | no | Bronze extraction run that produced the row. |
| `source_page_key` | string | no | Bronze page object reference. |
| `source_row_number` | bigint | no | 1-based row index within its page. |
| `transformation_version` | string | no | Silver transformation version. |

### `lifecycle_class`

| Value | Rule |
|---|---|
| `closed` | Exact normalized `CLOSED`. |
| `outstanding` | `OPEN`, `IN PROGRESS`, `PENDING`, `ASSIGNED`, `STARTED`. |
| `unknown` | Any other status; never counted as confirmed backlog. |

### `quality_flags` (non-disjoint warnings)

`closure_status_mismatch`, `negative_closure_duration`, `unknown_status`,
`unknown_borough`, `unknown_channel`, `missing_agency`, `missing_complaint_type`,
`created_date_outside_window`. Flags do not mean the row was rejected.

## Control Manifests

Small JSON files that authorize consumption; a file alone is not evidence of a
complete publication.

| Key | Purpose |
|---|---|
| `control/bronze/<run>/<day>.json` | Extraction window, cursor order, page references, counts, status. |
| `control/silver/<day>.json` | Input Bronze, versions, `received = accepted + quarantined + identical_duplicates_discarded`, partition/quality/quarantine references. |
| `control/gold/<run>.json` | Cohort, covered days, per-partition provenance, per-day observation range, quality exclusions, the four product references, status. |
| `control/runs/<run>.json` | Local run record (CLI status). |

## Gold Products

Each product is one Parquet file under
`gold/run=<publication-run>/<product>/part.parquet` with an explicit schema.

### `demand_daily`

Grain: creation day × borough × problem category.

| Column | Type | Meaning |
|---|---|---|
| `created_date` | date | NYC-local creation day. |
| `borough` | string | Borough or `UNKNOWN`. |
| `complaint_type` | string | Problem category. |
| `request_count` | bigint | Accepted requests in the grain. |
| `category_share` | double | Percent within the day/borough (all accepted requests, unknown included). |

### `closure_duration`

Grain: agency × problem category.

| Column | Type | Meaning |
|---|---|---|
| `agency` | string | Agency code. |
| `complaint_type` | string | Problem category. |
| `valid_closed_count` | bigint | Closed requests with `created <= closed <= observed`. |
| `median_closure_hours` | double | Continuous median closure hours. |
| `p90_closure_hours` | double | Continuous 90th-percentile closure hours. |

### `outstanding_requests`

Grain: agency × borough × lifecycle class × age band.

| Column | Type | Meaning |
|---|---|---|
| `agency` | string | Agency code. |
| `borough` | string | Borough or `UNKNOWN`. |
| `lifecycle_class` | string | `outstanding` or `unknown`. |
| `age_band` | string | `[0,7)`, `[7,14)`, `[14,30)`, `30+` elapsed days, or `UNKNOWN`. |
| `outstanding_count` | bigint | Confirmed outstanding requests in the grain. |
| `unknown_status_count` | bigint | Unknown-status requests, reported separately. |

Age is `observed_at_utc - created_date_utc`, not time since query execution.

### `channel_usage`

Grain: creation day × borough × channel.

| Column | Type | Meaning |
|---|---|---|
| `created_date` | date | NYC-local creation day. |
| `borough` | string | Borough or `UNKNOWN`. |
| `channel` | string | Intake channel or `UNKNOWN`. |
| `request_count` | bigint | Accepted requests in the grain. |
| `channel_share` | double | Percent within the day/borough (all accepted requests, unknown included). |

## Analysis Limits

These products describe the **September-created cohort at the recorded
observation times**. They are not NYC-wide backlog, satisfaction scores, or the
historical status on September 30. Open cases are absent from closure
distributions, and there are no between-month comparisons. See
[analytics.md](analytics.md) for metric definitions and query examples.
