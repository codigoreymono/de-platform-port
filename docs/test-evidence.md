# Test And Verification Evidence

Summary of the local, offline evidence that gates deployment. Detailed
measurements and method are in
[deployment-readiness.md](deployment-readiness.md); packaging inventories are in
[lambda-packaging.md](lambda-packaging.md).

## Offline Suite And Tooling

Run from the repository root, with no network and no AWS credentials:

```bash
export PATH="$PWD/.devenv/profile/bin:$PATH"
uv run --offline --frozen pytest
ruff check .
ruff format --check .
openspec validate "build-nyc311-september-2026-pipeline" --strict
```

| Check | Result |
|---|---|
| `uv run --offline --frozen pytest` | **1,210 passed, 6 skipped, 34 subtests** |
| `ruff check .` | Pass |
| `ruff format --check .` | Pass (75 files) |
| `openspec validate --strict` | Change valid |

The suite covers source pagination/retry, Bronze/Silver quality and replacement,
replay and refresh, the four Gold products, S3 adapter parity, Lambda stage
composition, packaging inventories, runtime/resource policy, no-spill policy,
and Glue catalog publication (including simulated catalog failure that leaves the
run unsuccessful and prior locations intact).

## Packaging Gate

Each stage ZIP is rebuilt reproducibly (repeat builds byte-identical), no layers
are used, and every third-party/native member is byte-identical to the approved
optimized ZIP — only application `nyc311/*` modules differ.

| Stage | ZIP bytes | Uncompressed bytes | Margin below 250 MiB |
|---|---:|---:|---:|
| Ingest | 17,188,574 | 24,708,118 | 237,435,882 |
| Curate | 90,949,288 | 256,025,246 | 6,118,754 |
| Gold | 89,699,311 | 242,002,122 | 20,141,878 |

## Runtime Compatibility

Verified in the pinned official runtime image
`public.ecr.aws/lambda/python@sha256:471e480c...` (Amazon Linux 2023, Python
3.12.15, x86_64, glibc 2.34): native imports, timezone resolution, fixture
transformations, and Parquet reads. Native linkage rechecks passed for 105 Curate
and 39 Gold ELF objects with 0 failures. Curate sets
`PYTHONTZPATH=/var/task/timezone`.

## Benchmark Gate

15 representative trials (two high-volume days and the full 323,627-row month,
three fresh processes each) all exit zero within the budgets:

| Case | Handler seconds | Max RSS MiB | Temp peak MiB |
|---|---:|---:|---:|
| Ingest, Sep 29 (11,976 rows) | 2.800–2.910 | 59.25 | 0.324 |
| Curate, Sep 29 | 2.982–3.292 | 254.21 | 0.926 |
| Gold, 30 days | 8.676–9.622 | 493.42 | 17.176 |

Minimum memory headroom **88.38% / 75.17% / 75.91%** and observed temporary
headroom **99.94% / 99.81% / 96.65%** for Ingest/Curate/Gold, above the 20%
target and within the 900-second timeout.

## No-Spill Policy

The Gold engine runs fail-closed with `memory_limit=512MiB`, `threads=1`,
`temp_directory=''`. Five production-policy `strace` traces pass: a positive
control detects real spill, while the Gold full-month, negative-control and
fail-first/fail-late cases show no spill. Across all Gold connections the
requested, native and effective configs match exactly and are never rewritten.

## Static Infrastructure Checks

```bash
terraform -chdir=infra fmt -check -recursive
terraform -chdir=infra init -backend=false -input=false
terraform -chdir=infra validate
```

All pass against AWS provider v5.100.0. These read no resource and do **not**
authorize a plan, apply or invocation.

## Reviewer Walkthrough (offline reproduction)

Executed from the documented environment (Nix shell, Python via `uv run`, offline)
against a fresh isolated `--base`, with no network and no AWS credentials. The
commands are exactly those in the [README](../README.md) and the
[Analytics Guide](analytics.md).

| Step | Observed result |
|---|---|
| `python -m nyc311 sample --base <b> --run-id sample-docs-001 --page-size 3` | exit `0`, `status: succeeded`, `stage: finished`, `error: null`, `gold_manifest_key: control/gold/sample-docs-001.json` |
| `python -m nyc311 replay --base <b> --mode sample --run-id sample-docs-replay-001 --bronze .../2026-09-01.json .../2026-09-02.json` | exit `0`, `status: succeeded`, `gold_manifest_key: control/gold/sample-docs-replay-001.json` |

Read-API verification (`load_gold`) on both runs reproduced the documented fixture
analytics exactly (see [Analytics Guide](analytics.md)):

| Gold manifest | Coverage | demand | channel | eligible_closed | outstanding | unknown_status |
|---|---|---:|---:|---:|---:|---:|
| `control/gold/sample-docs-001.json` | sample | 12 | 12 | 5 | 2 | 1 |
| `control/gold/sample-docs-replay-001.json` | sample | 12 | 12 | 5 | 2 | 1 |

Exact successful publications, identified by their Gold manifest key:

- Offline sample (reproducible by any reviewer): `data/local/sample/`,
  `control/gold/sample-docs-001.json`.
- Local real September month: `data/local/published/`,
  `control/gold/september-2026-001.json` (323,627 accepted rows).
- Authorized AWS month (deployed, verified, then destroyed):
  `control/gold/september-2026-002.json`, Step Functions run
  `september-2026-002`.

The AWS recovery and cleanup instructions in the [deployment runbook](deployment.md)
were reviewed and exercised: the bucket was emptied and `terraform destroy`
completed (the Athena workgroup required `--recursive-delete`). No project resource
remained afterwards.

## Deployment Evidence (authorized operator run)

Recorded from an authorized manual deployment on 2026-10-16.

- **Account / region / bucket**: `667869257907` / `us-east-1` / `nyc311-portfolio-crm`
  (deploy identity: IAM user `crm` via `AWS_PROFILE`, temporary scope, no stored keys).
- **Terraform**: `init -backend=false`, `plan`, `apply` produced **59 added, 1 changed,
  0 destroyed** against AWS provider v5.100.0; the pre-existing bucket was imported into
  state. No scheduler, VPC or extra service was created.
- **IAM correction found during the run**: the Lambda execution-role `data` policy
  needed `s3:ListBucket`; without it `HeadObject` on a missing key returns 403 instead of
  404 and the idempotency checks fail. Added the statement and re-applied (3 policies
  updated). See `infra/iam.tf`.

### Full-month Step Functions run (10.4)

Execution `september-2026-002` — **SUCCEEDED** (2026-10-16T17:30:18Z → 17:41:41Z, ~11m23s):

- 30 daily `ingest` + 30 daily `curate` + 1 `gold`, strictly sequential (`MaxConcurrency=1`).
- 30 Bronze manifests, 30 Silver partitions, 30 quality reports, 30 quarantine objects, 1 Gold manifest.
- Silver accepted rows: **323,627** across 30 complete partitions (`coverage: full_month`).
- 4 Gold products written under `gold/run=september-2026-002/` and the four Glue tables
  repointed to that run.

### Athena verification over all four products (10.4)

Queried via workgroup `nyc311-queries` against `nyc311.<product>`:

| Query | Result |
|---|---|
| Reconciliation | demand = channel = **323,627**; eligible_closed = **287,808**; confirmed_outstanding = **35,592**; unknown_status = **24** |
| `demand_daily` | Illegal Parking in Brooklyn leads daily demand (~846/day; 19–22% of the borough-day) |
| `closure_duration` | NYPD Illegal Parking median 1.67 h / p90 8.24 h; HPD UNSANITARY CONDITION median 187.49 h / p90 473.53 h |
| `outstanding_requests` | DPR/HPD dominate outstanding; age bands `[14,30)` and `30+`; unknown status kept separate |
| `channel_usage` | ONLINE leads every borough (~52–58%); UNKNOWN channel preserved |

Demand and channel counts independently reconcile to the accepted Silver population.

### Selected-day saved-Bronze replay and full Gold rebuild (10.5)

Run `september-2026-003`: replayed one day from saved Bronze
(`control/bronze/september-2026-002/2026-09-18.json`), recurated, then rebuilt Gold:

- Silver `2026-09-18` provenance updated (`run_id` 002 → 003); `row_count` unchanged at
  **11,964** — no duplicate accumulation.
- Rebuilt Gold `september-2026-003`: `coverage: full_month`, 30/30 complete, accepted rows
  **323,627** — identical to the original run (deterministic replay).
- Catalog was repointed to 003 to prove republication, then restored to the canonical
  run 002.

### Coverage gate (10.3)

Invoking Gold for the published mode with an incomplete selection
(`days = ["2026-09-01"]`) failed closed with **"Official Gold requires all September
days"** and wrote nothing, before any store was created.

### Observed AWS measurements (within configured limits)

| Stage | Memory size | Max memory used | Representative duration |
|---|---:|---:|---:|
| Ingest | 512 MB | 123 MB | 3.3–23.4 s (live API pagination) |
| Curate | 1024 MB | 333 MB | ~1.7–2.1 s (cold 6.4 s) |
| Gold | 2048 MB | 580 MB | ~15–16 s cold full-month; ~1 s idempotent re-run |

All well inside the 900-second timeout and the 512 MB DuckDB no-spill policy.

### Cleanup

Project-owned AWS resources and data are demonstrated and ready for authorized
cleanup (`terraform -chdir=infra destroy` plus bucket contents). Cleanup remains
pending explicit operator authorization.
