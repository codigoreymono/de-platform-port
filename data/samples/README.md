# Offline Samples

All records are hand-written synthetic examples, not downloaded NYC requests.
They contain no addresses, customer information, credentials, or real request
identifiers. They are fixture inputs, not published September analytical results.

| File | Purpose |
|---|---|
| `pagination-001.json` | Two ordered September 1 rows; omitted and null optional fields. |
| `pagination-002.json` | One-row final page; closure after September. |
| `updated-request.json` | Later closed observation of `sample-0001`, with unchanged creation time. |
| `quality-cases.json` | Null fields, unmapped status/category, missing identifier, invalid date, negative duration, wrong daily window, DST ambiguity, and future closure. |
| `duplicates.json` | Two identical rows and one conflicting observation with the same identifier. |
| `gold-metrics.json` | Twelve September 1-2 requests with known demand counts and closure hours. DOT eligible hours are 1, 3, 9, 696 (median 6, continuous p90 489.9); NYPD has one valid zero-hour closure. Open, negative, malformed, ambiguous, future, and unmapped-status cases are excluded from closure metrics. |

Pagination fixtures assume a page size of two and a September 1 daily window.
The future-closure case assumes observation before December 1, 2026; the
ambiguous closure is the NYC daylight-saving transition on November 1.
The postal code `00123` is deliberately synthetic and must remain a string.

The CLI `sample` command defaults to `gold-metrics.json`. Its channels are three
`ONLINE`, three `PHONE`, three `MOBILE`, and three omitted fields (records 03,
07, 11), which Silver represents as `UNKNOWN`. Channel shares are calculated
within each creation-day/borough group, not across the entire fixture. For
September 1 in Manhattan, `ONLINE` and `PHONE` each account for 2 of 4 requests
(50 percent each). Missing-channel requests remain in every denominator.

Run fixture and contract checks with `uv run pytest`. The default test fixtures
block real network connections and isolate AWS configuration. The implemented
source, transformations, and local publications use these fixtures for offline
verification. Real full-month extraction requires separate operator execution.
