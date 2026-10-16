# Simulated Lambda Stages

The S3 adapter and three entry points reuse the local Bronze, Silver, and Gold
logic. Their default tests use a simulated SDK and HTTP transport, not AWS.
These entry points do not constitute a deployed pipeline or authorize invocation
against real S3 or the live source. ZIP assembly is described separately in
[Lambda Packaging](lambda-packaging.md); the runtime/resource gates are pending.

## Inputs And Composition

All object references are relative to `<prefix>/<mode>/` in the named bucket.
The prefix can be omitted. `mode` defaults to `published`; `sample` is a disjoint
namespace, not an automatic HTTP fixture switch. Only the tests inject a fixture
transport. An actual ingestion invocation with `operation: extract` calls the
source and an actual SDK client accesses S3.

Daily input metadata has this shape:

```json
{
  "bucket": "project-owned-bucket",
  "prefix": "nyc311",
  "mode": "sample",
  "run_id": "example-001",
  "operation": "extract",
  "start_date": "2026-09-01",
  "end_date": "2026-09-02",
  "days": ["2026-09-01"],
  "config": {"page_size": 5000, "max_attempts": 5}
}
```

`nyc311.lambda_ingest.handler` accepts exactly one September creation day.
It returns the complete `bronze_manifest_key` and `record_count` alongside
execution metadata. `operation: replay` requires an explicit complete
`bronze_manifest_key`; it downloads and verifies the original saved pages
without creating an HTTP client. Dates must match that extraction. Config uses
the existing finite execution settings and cannot override storage mode.

`nyc311.lambda_curate.handler` accepts that result directly, downloads verified
Bronze, prepares Silver locally, and publishes the daily partition plus quality,
quarantine, and completion marker. It returns `silver_manifest_key` and
`row_count`. Execution `days`, `config`, and `operation` survive both stages.

`nyc311.lambda_gold.handler` accepts the curation result directly when it carries
the intended Gold `days`. It returns `gold_manifest_key`, coverage dates, and
four product keys/counts. For partial sample execution, provide an explicit
subset; omitted `days` means all 30 September days. Official `published` Gold
always requires all 30 verified daily inputs. It executes the same four SQL files, bundled at
root `sql/` beside the package rather than resolved from the working directory.

For multiple days, run Ingest then Curate for each day sequentially, then invoke
Gold once with the complete selected coverage and a publication `run_id`.
The three stages may share a run ID because their artifact kinds have separate
paths. There is no state-machine implementation yet. Inputs/results are bounded
JSON metadata below 256 KiB; they never carry source pages, tables, or DataFrames.
Unsupported input fields and invalid daily/official coverage fail before SDK
construction. Typical fixture events/results are below 2 KiB.

## Publication And Recovery

Every invocation has a private temporary workspace, cleaned on success or
exception. Correctness does not depend on a warm instance or retained `/tmp`.
Objects are uploaded with `PutObject`, not a POSIX rename emulation. Individual
objects are replaced; there is no transaction across a prefix.

- Bronze uploads validated original pages and diagnostic progress incrementally.
  Its remote completion marker follows verification of the uploaded page hashes.
- Silver is prepared and verified before invalidating the remote daily marker.
  Only then are quality, quarantine, and the partition replaced. Artifact hashes
  are checked before completion. Failed invalidation leaves old artifacts alone;
  later failures leave the day failed/incomplete and ineligible for Gold.
- Gold retains run-scoped outputs and incremental diagnostics. All four uploaded
  product hashes must match before marking the artifact run complete. This is
  **not** Glue catalog publication or a global pipeline-success marker; those
  orchestration/catalog tasks remain pending.

A retry of an already complete Bronze or Gold input run verifies its saved
artifacts and returns the existing references. Bronze makes no new source call;
Gold remains a historical snapshot even if current Silver has changed. A failed
or incomplete Bronze/Gold attempt cannot be reused or overwritten: recover with
a new run ID and complete input references. An existing unmarked artifact also
blocks reuse. Curation can retry complete saved Bronze, replacing that day's
partition without appending rows. A fresh selected-day observation removes
absent rows and replaces old values, while unrelated days are unchanged.

An exception always signals task failure. Diagnostics are best effort when S3
itself fails; a failed attempt never becomes a successful result. Repair affected
days and rebuild all four Gold products before treating a refresh as complete.
Keep one writer and no readers during refresh, just as for local execution.

## Offline Verification

```bash
UV_OFFLINE=1 uv run pytest tests/test_s3_store.py tests/test_lambda_stages.py
```

Tests cover SDK stubs, isolated cleanup, direct metadata composition, replay
without HTTP, local/simulated-S3 logical Silver/Gold and quality parity, fresh
daily replacement, missing/incomplete Silver coverage, remote read/write
failures, corrupted input hashes, marker ordering, completion-write failures,
and manual retry. The default suite also blocks accidental network/credential
discovery. None of this verifies native Lambda runtime compatibility or resource
budgets, and no AWS authorization is implied.
