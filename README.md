# NYC 311 Data Platform

A bounded data-engineering portfolio project: a manually executed batch pipeline
for NYC 311 requests **created in September 2026**, with reproducible local
processing, auditable quality decisions, and four SQL analytical products.

```text
Manual CLI
    |
    +-- Daily HTTPX extraction or offline replay
    |           |
    |           v
    |     Bronze: original JSON pages + gzip + extraction manifests
    |           |
    |           v
    |     Silver: Pandas normalization + explicit PyArrow schema
    |           +--> Quarantine + quality reports
    |           |
    |           v
    |     Daily Parquet partitions + completion manifests
    |           |
    +-----------v
          Gold: DuckDB SQL -> four Parquet products -> complete run manifest
```

Local processing, offline tests, and a full-month local run are implemented and
verified. AWS infrastructure is defined under `infra/` but is **not deployed or
verified**: a deployed run requires explicit operator authorization. There are no
automatic schedules, Spark, Airflow, Kafka, or continuous operations.

## Local Setup

Run commands from the repository root. Python is pinned to the 3.12 series.
Nix supplies development tools and native libraries; uv manages Python packages
using `uv.lock`. Do not install project packages globally.

From a new shell, enter the environment once:

```bash
nix develop
uv sync --locked --group dev
```

If already inside `nix develop`, use that shell without nesting another one.
The existing `devenv.nix` exposes the C++ runtime and zlib through
`LD_LIBRARY_PATH`, needed by the binary Python packages.

```bash
uv run python -c "import numpy, pandas, pyarrow, duckdb, httpx; print('Imports OK')"
uv run pytest
ruff check .
uv run python -m nyc311 --help
```

The default test suite uses synthetic fixtures, temporary storage, and simulated
HTTP. It requires neither internet nor AWS credentials. `UV_OFFLINE=1 uv run
pytest` also prevents uv from fetching packages; **`UV_OFFLINE` does not block
HTTP made by application code**. Use `sample` or `replay` for offline execution.

## Offline Demonstration

Use a new `run_id` for every invocation, including recovery attempts. Run only
one pipeline at a time and do not consume mutable Silver while refreshing it.

```bash
uv run python -m nyc311 sample \
  --base data/local \
  --run-id sample-docs-001 \
  --page-size 3
```

This exercises the complete pipeline with `httpx.MockTransport`, not the live
API. Its default fixture is `data/samples/gold-metrics.json`: 12 synthetic
September 1-2 requests with three `ONLINE`, three `PHONE`, three `MOBILE`, and
three absent channels that become `UNKNOWN`. The interval ends exclusively on
September 3. All outputs live under `data/local/sample/`.

Fixture totals assume no additional sample days in that namespace. If other
sample days already exist, Gold includes them too. Use a different `--base` for
an isolated demonstration and adjust the query paths accordingly; do not delete
existing diagnostic history merely to reproduce the example.

Expected completion is exit code `0` and JSON `status: succeeded`. The result
lists processed days, Bronze references, and `gold_manifest_key`. A failed
invocation returns exit code `1`, or `130` for interruption, with saved progress
when available. Do not treat an older complete Gold run as that failure's result.

Replay the saved input without creating an HTTP client:

```bash
uv run python -m nyc311 replay \
  --base data/local \
  --mode sample \
  --run-id sample-docs-replay-001 \
  --start 2026-09-01 \
  --end 2026-09-03 \
  --bronze \
  control/bronze/sample-docs-001/2026-09-01.json \
  control/bronze/sample-docs-001/2026-09-02.json
```

Bronze arguments are storage keys relative to the selected mode root, not
absolute paths. Exactly one complete extraction is required for every requested
day. Replay preserves original observation times; changing a fixture does not
change historical Bronze. Logical data idempotence does not imply identical
processing run IDs or byte-identical Parquet files.

## Commands And Coverage

| Command | Input | Network | Coverage behavior |
|---|---|---|---|
| `sample` | Local fixture, override with `--fixture` | Simulated HTTP only | `sample` namespace; September 1-2 by default |
| `extract --live` | NYC 311 API | Real HTTP, explicit operator opt-in | Selected creation days; official Gold requires all September days |
| `replay` | Explicit complete Bronze manifests | No HTTP client | Selected days are replaced, then all four Gold products are rebuilt |

`--start` is inclusive and `--end` exclusive, interpreted as NYC local calendar
dates. Supported creation dates are `[2026-09-01, 2026-10-01)`. `--mode sample`
and `--mode published` are separate local namespaces, not AWS settings.

For sample refreshes, Gold includes the requested days plus existing sample
Silver markers, including failed markers that must be repaired. For published
runs, Gold always requires 30 verified Silver days. An initial partial published
extraction can retain completed days but ends in failure until full coverage
exists. Later selected-day refreshes can succeed against an already complete
month.

## Analytical Products

| Product | Question | Grain and measures |
|---|---|---|
| `demand_daily` | Which problems concentrate demand, and how does it vary by borough? | Creation day, borough, problem; counts and within-day/borough shares |
| `closure_duration` | How long do requests take to close administratively? | Agency, problem; eligible count, median hours, continuous p90 hours |
| `outstanding_requests` | Which cohort requests remain pending, and how old are they? | Agency, borough, lifecycle, age band; confirmed pending and separate unknown-status counts |
| `channel_usage` | How are intake channels used? | Creation day, borough, channel; counts and within-day/borough shares |

These describe the **September-created cohort at recorded observation times**,
not every 311 interaction, NYC-wide backlog, satisfaction, or historical status
at September 30. Closure can occur after September. Open cases are excluded from
closure duration distributions. There are no between-month comparisons,
population-normalized rates, or annual seasonality claims.

## Operating And Reviewing

- [Architecture](docs/architecture.md): local and AWS execution paths, storage
  layout, catalog boundary, and operating limits.
- [Data Dictionary](docs/data-dictionary.md): Silver and Gold fields, types,
  control manifests, and lifecycle/quality semantics.
- [Findings](docs/findings.md): the four analytical results for the full
  September cohort with cohort and observation coverage.
- [Test Evidence](docs/test-evidence.md): offline suite, packaging, compatibility,
  benchmark and no-spill evidence; deployment evidence pending.
- [Local Runbook](docs/local-runbook.md): storage layout, source contract,
  quality rules, authorized full-month command, verification, refresh, recovery.
- [Analytics Guide](docs/analytics.md): metric definitions, DuckDB examples for
  all four products, expected fixture results, and interpretation limits.
- [Fixture Inventory](data/samples/README.md): synthetic examples and channels.
- [Lambda Handlers](docs/lambda-handlers.md): simulated S3 stages, metadata
  composition, publication boundaries, and manual retry.
- [Lambda Packaging](docs/lambda-packaging.md): offline locked-wheel ZIP builds,
  per-stage inventories, cache prerequisites, and pending compatibility gates.
- [Deployment Readiness](docs/deployment-readiness.md): compatibility gate,
  measured resource budgets, and non-spill evidence.
- [AWS Deployment Runbook](docs/deployment.md): manual Terraform deployment,
  Step Functions execution, Athena queries, costs, rollback, and cleanup.

The verified read APIs are `load_bronze`, `load_silver`, and `load_gold`. An
existing JSON or Parquet file alone is not evidence of a complete publication.
Validate a selected Gold run before reading its four product paths with DuckDB;
never glob across different Gold run IDs because that double-counts snapshots.

## Repository Layout

```text
src/nyc311/       CLI, coordinator, source, layers, storage, manifests, AWS adapters
sql/             Four versioned DuckDB transformation queries
infra/           Terraform: S3, three Lambdas, Step Functions, Glue, Athena
data/samples/    Small synthetic fixtures tracked in Git
data/local/      Ignored local outputs: sample/ and published/
tests/           Offline unit, integrity, and end-to-end tests
docs/            Operating instructions, analytical and deployment documentation
```

Existing AWS tooling and Boto3 do not authorize resource access. The S3, Lambda,
Step Functions, Glue Catalog, Athena, and Terraform definitions in this
repository are not a deployment: every AWS plan, apply, read, or invocation
requires explicit operator authorization, and no run is a verified deployment
until it is authorized and observed.
