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

Pagination fixtures assume a page size of two and a September 1 daily window.
The future-closure case assumes observation before December 1, 2026; the
ambiguous closure is the NYC daylight-saving transition on November 1.
The postal code `00123` is deliberately synthetic and must remain a string.

Run fixture and contract checks with `uv run pytest`. The default test fixtures
block real network connections and isolate AWS configuration. Validation,
pagination, and transformation implementations belong to subsequent tasks.
