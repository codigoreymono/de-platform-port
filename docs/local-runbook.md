# Local Runbook

This guide covers the implemented local pipeline. Full real September extraction
is **operator-only and has not been performed as part of this documentation
change**. Do not run the network commands below as an offline verification.

## Execution Contract

Work from the repository root in the existing Nix shell, with Python commands
through `uv run`. Use new safe run IDs containing letters, digits, `_`, and `-`.
An ID already used in run, Bronze, or Gold namespaces is not reusable. Replay
uses a new processing ID while referencing the original Bronze source IDs.

Only one execution may write a base/mode namespace at a time. This is an
operating precondition, not a distributed lock. Do not run queries over mutable
Silver during a refresh. There is no scheduler, automatic reconciliation,
automatic resume, or global rollback transaction.

The local flow is sequential: Bronze then Silver for each creation day, followed
by one joint Gold rebuild. The run is successful only after Gold publication and
verification. `sample` and `published` never share input or output prefixes.

## Storage Layout

The base defaults to `data/local`. All keys below are relative to
`<base>/<mode>/`, such as `data/local/sample/` or `data/local/published/`.

```text
bronze/run=<source-id>/created_date=2026-09-DD/page-NNNN.json.gz
control/bronze/<source-id>/2026-09-DD.json
silver/created_date=2026-09-DD/part.parquet
control/silver/2026-09-DD.json
quarantine/run=<processing-id>/2026-09-DD.json.gz
quality/run=<processing-id>/2026-09-DD.json
gold/run=<gold-id>/<product>/part.parquet
control/gold/<gold-id>.json
control/runs/<processing-id>.json
```

Bronze preserves original selected-field response bytes, compressed with gzip,
and records page counts, textual cursors, UTC observations, SHA-256 digests,
source counts before/after extraction, and source selection. Incomplete/failed
attempts remain available for diagnosis, but are not replay inputs.

Silver uses an explicit PyArrow schema and Snappy Parquet. Identifiers and ZIPs
are strings. Business timestamps retain `_local` values and `_utc` instants;
technical timestamps are UTC. All timestamps use microseconds. UTC columns are
timezone-free physical timestamps with a documented UTC convention. The
`created_date` date32 column and directory represent **NYC local creation date**,
which can differ from its UTC date. Provenance includes source run, page key,
one-based row number, and transformation version.

Silver is a current view: a successful daily refresh replaces one complete
partition, including removal of absent or newly quarantined rows. It is not a
history of status changes. Quarantine preserves original record values,
source/row references, and critical rejection reasons. Reports summarize quality
counts, accepted category/status distributions, warnings, and duration exclusions.

Gold uses new immutable run prefixes. A complete manifest authorizes all four
products together, with file/query digests, row counts, frozen Silver input
markers, coverage, observation ranges, quality exclusions, and generation time.
Individual Parquet footers describe `prepared` artifacts; they are not a joint
publication marker. Historical Gold can remain valid after mutable Silver changes.

## Source And Completeness

The read-only source is
`https://data.cityofnewyork.us/resource/erm2-nwe9.json`, currently the NYC 311
2020-to-present dataset. It represents service requests directed to agencies,
not all calls, inquiries, or interactions with 311.

Selected fields are `unique_key`, `created_date`, `closed_date`, `status`,
`complaint_type`, `descriptor`, `agency`, `agency_name`, `borough`, `incident_zip`,
`open_data_channel_type`, `resolution_action_updated_date`, and `:updated_at`.
Bronze is raw with respect to this selection, not a full copy of every source
column. `:updated_at` is retained as technical provenance, not treated as CDC.

Daily creation filters are `[start, end)`, with explicit `unique_key ASC`
ordering and a strictly greater **textual** cursor. There is no numeric cursor
conversion or offset pagination. Default page size is 5,000, configurable with
`--page-size`; default maximum attempts per request is 5, configurable with
`--max-attempts`.

The source module has finite connect/read timeouts, bounded exponential jittered
backoff, `Retry-After` handling, and an overall daily deadline. It checks the
deadline between received fragments; blocking transport operations are limited
by their finite timeouts rather than preempted instantly. Connection/transient
failures can retry; permanent errors and malformed responses do not retry forever.

Before declaring extraction complete, checked source counts before and after
pagination must agree with received records. Previously populated source days
cannot silently become empty during fresh refreshes. Bronze completion also
requires present, decompressible, checksum-matching pages with consistent counts,
ordering, terminal-page evidence, and UTC observation metadata.

Count stability does **not** provide source snapshot isolation: values can change
without changing row counts. Fresh extraction can legitimately differ from saved
Bronze. Replay is deterministic for the same saved inputs and transformation
version, and never refreshes observations to the replay wall clock.

## Quality Policy

The table below describes **Silver transformation policy**, including defensive
rules exercised directly with fixtures. Source-envelope validation takes
precedence in fresh extraction and persisted Bronze replay: missing/non-text/
blank cursor identifiers and repeated or non-advancing source keys fail Bronze
before those records can reach Silver. Such failures do not complete a day with
quarantine or duplicate exclusions. Silver still validates required fields and
duplicates defensively; invalid creation values can pass the identifier envelope
and be quarantined there.

| Condition | Outcome |
|---|---|
| Missing/invalid request identifier | Quarantine |
| Unparseable or unresolvable creation instant | Quarantine |
| Creation date outside the requested local day | Quarantine |
| Entire nonempty page lacks required columns, or incompatible record structure | Fail the day |
| Missing optional attributes or unknown dimensions | Keep the request with explicit buckets/flags |
| Optional text has a wrong type or ZIP is not five ASCII digits | Null/bucket the unusable field and flag; preserve original Bronze |
| Optional business date is invalid | Null parsed values and flag |
| Local date is ambiguous/nonexistent or cannot convert to UTC | Preserve interpretable local value, null UTC, flag; no duration contribution |
| Closed date precedes creation or follows observation | Keep request, flag, exclude closure duration |
| Unknown status | Keep its normalized value, classify `unknown`; do not infer pending/closed |
| Identical original records within an extraction | Keep first provenance deterministically and count discarded copies |
| Different originals for the same normalized identifier | Fail the day, never choose an arbitrary winner |

`CLOSED` is classified closed. `OPEN`, `IN PROGRESS`, `PENDING`, `ASSIGNED`, and
`STARTED` are explicitly outstanding. Other statuses remain unknown without a
separately justified mapping; the code does not use `status != CLOSED` as backlog.
Problem categories are not an exhaustive whitelist. New channels/categories are
visible, and missing channels remain `UNKNOWN` rather than being dropped.

Every report reconciles:

```text
received = accepted + quarantined + identical_duplicates_discarded
```

Warnings count accepted unique requests by reason and are not disjoint.
Rejection-reason counts can exceed quarantined row counts when a record has
multiple defects. Do not add those reason counts to the population equation.

## Authorized Real September Run

**Network operation: execute personally only after authorization.** The explicit
`--live` flag enables HTTP. Setting `UV_OFFLINE=1` does not disable that HTTP.
No AWS account, credentials, Terraform, or cloud operation is needed.

Before starting, run offline tests, check native imports as described in the
README, choose a new ID, and stop all other writers/readers for this namespace.
An optional Socrata App Token is read from `NYC311_APP_TOKEN` and sent via the
`X-App-Token` header, not query parameters. Never commit tokens or pass them in
CLI arguments. In Bash, optional interactive configuration avoids a literal
secret in shell history:

```bash
read -r -s -p "Socrata App Token: " NYC311_APP_TOKEN
export NYC311_APP_TOKEN
```

Run the full creation cohort, with October 1 excluded:

```bash
uv run python -m nyc311 extract \
  --base data/local \
  --mode published \
  --run-id september-2026-001 \
  --start 2026-09-01 \
  --end 2026-10-01 \
  --page-size 5000 \
  --max-attempts 5 \
  --live
```

This extracts, curates, and builds all four Gold products; there is no separate
ingest-only command. Runtime/source errors stop the run. No hard-coded total
from exploration is an acceptance criterion. The API is updated daily and
historical requests can acquire closures or corrections after creation.

The extraction has not been run in task 7.1/7.3. Keep 7.2 pending until the
operator completes it and its post-extraction checks.

## Post-Extraction Checks

First inspect CLI exit status and JSON. A successful official run must have
`status: succeeded`, `stage: finished`, `error: null`, 30 `gold_days`, and a
nonnull `gold_manifest_key` for **that run ID**. `completed_days` must match
the invocation's own `[start_date, end_date)` window: 30 for the initial full
month command, but fewer for remaining-window recovery or selected-day refresh.

The following local read-only check is intended immediately after the real run.
It verifies all source/day artifacts through the existing read APIs and compares
current Silver with the frozen inputs of the selected Gold run:

```bash
uv run python - <<'PY'
from datetime import date
from pathlib import Path
from nyc311.bronze import load_bronze
from nyc311.config import COHORT_START, COHORT_END, daily_windows
from nyc311.gold_store import load_gold
from nyc311.silver_store import load_silver
from nyc311.storage import output_root, read_json

base, mode, run_id = Path("data/local"), "published", "september-2026-001"
root = output_root(base, mode)
run = read_json(root, f"control/runs/{run_id}.json")
assert run["status"] == "succeeded" and run["error"] is None
gold, products = load_gold(base, run_id, mode=mode)
expected = [day.isoformat() for day, _ in daily_windows(COHORT_START, COHORT_END)]
processed = [day.isoformat() for day, _ in daily_windows(date.fromisoformat(run["start_date"]), date.fromisoformat(run["end_date"]))]
assert run["completed_days"] == processed
assert run["gold_days"] == gold["covered_days"] == expected
assert run["gold_manifest_key"] == f"control/gold/{run_id}.json"
totals = {name: 0 for name in ("received", "accepted", "quarantined", "identical_duplicates_discarded")}
identifiers = set()
frozen = {item["start_date"]: item for item in gold["input_partitions"]}
for day, _ in daily_windows(COHORT_START, COHORT_END):
    silver, table = load_silver(base, day, mode=mode)
    bronze, _ = load_bronze(base, silver["source_manifest_key"], mode=mode)
    report = read_json(root, silver["quality_report_key"])
    assert bronze["source_count_before"] == bronze["source_count_after"] == bronze["record_count"] == report["received"]
    assert report["received"] == report["accepted"] + report["quarantined"] + report["identical_duplicates_discarded"]
    assert report["accepted"] == table.num_rows
    assert silver == frozen[day.isoformat()], "Silver changed since this Gold publication"
    current = set(table["unique_key"].to_pylist())
    assert identifiers.isdisjoint(current)
    identifiers.update(current)
    for name in totals:
        totals[name] += report[name]
assert totals["received"] == totals["accepted"] + totals["quarantined"] + totals["identical_duplicates_discarded"]
assert sum(products["demand_daily"]["request_count"].to_pylist()) == totals["accepted"]
assert sum(products["channel_usage"]["request_count"].to_pylist()) == totals["accepted"]
assert all(gold["quality_exclusions"][name] == value for name, value in totals.items())
print(totals)
print("Observation range:", gold["observation_min_utc"], gold["observation_max_utc"])
print("Verified official Gold:", run["gold_manifest_key"])
PY
```

Later refreshes intentionally change current Silver, so the frozen/current
comparison can fail for an otherwise valid historical Gold publication.
`load_gold` independently verifies historical Gold without rereading current
Silver. Select the latest successful run for post-refresh comparisons.

Do not rely only on SQL `count(*)`: the read APIs verify completion, SHA-256,
schema, partition date, uniqueness, row counts, and referenced quality/quarantine
files. A Parquet path alone is not evidence of successful publication.

## Refresh And Recovery

Inspect `<base>/<mode>/control/runs/<failed-id>.json`. The record or CLI failure
result reports `stage`, `active_day`, `completed_days`, `bronze_keys`, and output
references. `completed_days` means successfully curated days, not full-run success.
Only error types are stored, not arbitrary potentially sensitive error text.

| Failure point | What remains | Recovery |
|---|---|---|
| Bronze extraction incomplete | Original pages/expected references and failed or incomplete marker | Fresh extraction of failed/remaining days with a new ID; incomplete Bronze cannot replay |
| Transformation/temporary preparation fails | Previous Silver partition/publication remains unchanged | Fix inputs or retry using complete saved Bronze |
| Silver invalidated, replacement or final marker fails | Old or new Parquet may exist, but marker is failed/incomplete | Replay the complete Bronze day; never restore an old completion marker manually |
| Gold fails | Complete Silver days, partial failed/incomplete new Gold run, older historical runs | Replay at least a selected complete Bronze window with a new ID to trigger one full Gold rebuild |
| Pipeline record persistence fails | Last valid progress checkpoint; inspect layer markers separately | Treat the invocation as failed; verify/replay explicitly rather than inferring success |

For a failed full-month run that completed September 1-4, personally retry the
remaining range after fixing the error:

```bash
uv run python -m nyc311 extract \
  --base data/local --mode published --run-id september-2026-retry-001 \
  --start 2026-09-05 --end 2026-10-01 --live
```

This is another **real HTTP** operation. Gold includes all 30 current published
days, not just the retried window. Initial partial official runs fail at Gold
until every required day has a complete, verified partition.

For a Silver/Gold failure with complete saved September 5 Bronze, replay offline:

```bash
uv run python -m nyc311 replay \
  --base data/local --mode published --run-id september-2026-replay-001 \
  --start 2026-09-05 --end 2026-09-06 \
  --bronze control/bronze/september-2026-001/2026-09-05.json
```

Substitute actual complete keys from the failure record. Multiple-day replay
needs exactly one complete reference per day in the requested interval; the
entire plan is validated before replacing any Silver day. Do not use a failed
attempt's ID as the new processing ID.

To incorporate source updates after a successful month, personally re-extract a
selected day using a new ID:

```bash
uv run python -m nyc311 extract \
  --base data/local --mode published --run-id september-2026-refresh-001 \
  --start 2026-09-05 --end 2026-09-06 --live
```

The selected day is replaced, other Silver days remain unchanged, and all four
Gold products are rebuilt from the current full cohort. Fresh queries can change
data; offline replay cannot acquire new source updates. Mixed observation times
are retained and exposed in Gold metadata rather than disguised as one snapshot.

Never force `complete`, edit hashes, delete diagnostic history to reuse an ID,
or fall back to an old Gold run as evidence that a failed refresh succeeded.
