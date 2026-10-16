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
| `ruff format --check .` | Pass (71 files) |
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

## Deployment Evidence — PENDING (operator-owned)

No AWS resource has been read, changed, deployed or invoked, and no deployment
has been verified. The smoke run, full-month Step Functions run, Athena queries,
selected-day refresh, and rollback/cleanup demonstrations in the
[deployment runbook](deployment.md) remain to be executed and recorded by the
operator under explicit authorization. This section stays pending until then.
