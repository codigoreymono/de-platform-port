# Deployment Readiness

## Decision

Task 8.6's measured resource, timeout, retry/deadline and DuckDB policy is now
**integrated into the code** and reverified. The authorized [no-spill
experimental spike](duckdb-no-spill-spike.md) established that DuckDB 1.5.6's
`max_temp_directory_size` is not a reliable hard aggregate bound, so the chosen
mechanism is **fail-closed no-spill**: `temp_directory=''`. Every Lambda Gold
connection now receives exactly three effective settings:

```text
memory_limit = 512MiB
threads = 1
temp_directory = ''
```

There is no spill directory and no `max_temp_directory_size`. A query that
cannot complete inside the 512-MiB engine buffer raises a controlled DuckDB
`OutOfMemoryException` during the business `execute(query)`, and the affected
run fails closed without publishing a claim of success.

**Task 8.6 passes its local compatibility and budget gate; progress is 38/46.**
All 15 representative workload trials, the native linkage checks and the
production-policy no-spill traces pass. No dependency or architecture was
changed and no AWS resource was read or changed. Stage 9 now adds Terraform and
catalog-publication definitions under `infra/` and `src/nyc311/catalog.py`; see
the [deployment runbook](deployment.md).

The remaining gap is **AWS-side verification only**: this report certifies local
compatibility and budget checks against a pinned official runtime image, not
deployed Lambda behavior. Terraform is defined and statically validated, but no
AWS plan, apply or invocation has run.

| Check | Result |
| --- | --- |
| Existing Linux/Python/architecture baseline | Local pass, pinned official runtime |
| Locked distributions, native bytes, ZIP integrity/reproducibility | Pass |
| All three aggregate uncompressed package sizes, no layers | Pass, below 250 MiB |
| Representative daily and full-month correctness | 15/15 local trials pass |
| Memory/time and observed temporary headroom | Pass; 20% target met with sampling caveats |
| Effective limits on all Gold connections | Three keys read back correctly, no rewrite |
| Reliable fail-closed no-spill enforcement | Pass, production-policy traces |
| AWS account, deployment and actual service verification | Not accessed/not authorized by this task |
| Overall 8.6 local readiness gate | **Passed** |

## Selected Profiles

`src/nyc311/lambda_runtime.py` is the machine-readable policy. Memory, timeout
and temporary storage are applied as deployment parameters by the Terraform in
`infra/lambda.tf`; the application code only declares the policy. See the
[deployment runbook](deployment.md).

| Stage | Memory MiB | Function Timeout Seconds | Temporary MiB | Conservative Test CPU Quota |
| --- | ---: | ---: | ---: | ---: |
| Ingest | 512 | 900 | 512 | 0.25 |
| Curate | 1,024 | 900 | 512 | 0.5 |
| Gold | 2,048 | 900 | 512 | 1.0 |

These allocations follow the 8.5 observations of about 59, 254 and 493 MiB peak
process memory while avoiding excessively low CPU quotas. The local quotas are
below the approximate memory/1,769-MiB CPU relationship, but Docker scheduling,
host CPU and Lambda allocation are not equivalent. All runs fixed
OpenBLAS/OpenMP to one thread. Curate requires `PYTHONTZPATH=/var/task/timezone`
in the future runtime environment to select its packaged TZif. These environment
settings must be carried into any later deployment definition and reverified.

## Execution And Request Budgets

Only Lambda operational defaults changed; local CLI defaults, schemas, quality
policy, SQL text and analytical definitions remain unchanged.

| Budget | Policy |
| --- | --- |
| Application allowance | At most 720 seconds from handler entry |
| Normal work | Allowance minus 60 seconds: normally 660 seconds |
| Failure reporting | Remaining application allowance, normally 60 seconds |
| Lambda reserve | Shorten allowance to `min(720, remaining_lambda_seconds - 180)` |
| HTTP extraction | At most 600 seconds, additionally bounded by the original invocation work deadline |
| Daily work unit | One creation day; unchanged |
| Maximum requested page size | 5,000 rows; smaller event overrides allowed |
| HTTP attempts | Three total attempts per request |
| HTTP connect/pool and read | 5 and 30 seconds, clamped to remaining extraction time |
| Exponential full-jitter backoff | At most 10 seconds; distinct from source-advised Retry-After |
| S3 SDK policy | Standard mode, three **total** attempts, 5-second connect, 20-second read |
| DuckDB per Gold connection | `memory_limit=512MiB`, `threads=1`, `temp_directory=''` |

Event configuration may shorten these HTTP limits, not enlarge them. A valid
`Retry-After` may exceed the exponential-backoff cap, but must fit the remaining
extraction deadline. Absolute deadline plumbing through Bronze/source prevents
earlier provenance/S3 work from creating a fresh independent allowance.

The adapter checks budgets before/after S3 operations and between 1-MiB body
reads, always closing bodies. Handlers check after CPU preparation and before
publication/completion. Failed-marker callbacks may use only the reporting
allowance, skip new artifact uploads, and preserve the original failure when
diagnostics also fail. Existing incomplete/failed manifest semantics remain.

**These are cooperative checks, not hard cancellation.** They cannot interrupt
an in-flight SDK call, Python/native calculation, JSON/gzip operation or DuckDB
query. Socket timeouts are not whole-request deadlines; body streaming and SDK
backoff do not create a guaranteed aggregate RPC bound. Lambda's 900-second hard
timeout is the outer termination boundary and may prevent cleanup/diagnostics.
Cold imports precede handler-entry timing, so benchmark comparisons include them
separately. Real latency/failure behavior remains an AWS verification risk.

The row limit is not a byte or daily-row bound. Larger field values, source
growth and decompression expansion can increase memory; no new data truncation,
rejection policy or automatic subdivision was introduced to hide that risk.

### Future Orchestration Contract

This is operating guidance only, not a state-machine or Terraform definition.
Retain sequential daily execution and no overlapping writers/readers during
refresh. Future orchestration timeout accounting must allow the function's
termination/reporting window. Do not blanket-retry application, resource or
validation failures with the same ID: incomplete Bronze/Gold attempts are
immutable and need a new attempt ID or an explicitly selected complete replay.
Already complete retries remain verified saved-artifact reads. Future Glue
publication adds operations and must join the same verified budget.

## DuckDB Policy

`src/nyc311/gold.py` (`DUCKDB_LIMITS`) is the single source of truth for the
Gold engine policy and is applied through the existing shared compute path.
`src/nyc311/lambda_gold.py` passes `dict(DUCKDB_LIMITS)` directly; it does not
construct an alternate spill configuration and does not install extensions.

- The default connection config is the three concrete keys above.
- `None`, `{}` and partial overrides inherit the protected defaults; a caller
  cannot silently drop a key.
- Overrides are rejected before connecting when they request a non-empty or
  path-like `temp_directory`, any `max_temp_directory_size`, `threads` greater
  than 1, or a non-positive `memory_limit` above 512 MiB.
- The four SQL files, hashes, schemas and validators are unchanged.

The managed memory limit does not bound the full process: Arrow inputs, Python
validation structures and other allocations are outside DuckDB's buffer limit.
`temp_directory=''` removes spill-to-disk as an untrusted escape hatch; memory
exhaustion becomes a controlled, observed failure instead of unbounded disks.

### Why No Spill

The separate [spike](duckdb-no-spill-spike.md) recorded a fresh-process,
private-directory sort that wrote **35,749,888 bytes (34.09375 MiB)** of physical
storage while `current_setting('max_temp_directory_size')` returned **1.0 MiB**.
That disproves treating the setting alone as a hard aggregate directory bound for
the exercised 1.5.6 ORDER BY case. Rather than raise limits to make a test pass
or change dependencies, the reviewed alternative was adopted: a supported
no-spill/fail-closed mode, with the exact disabling mechanism validated against
the pinned native binary rather than assumed.

### Production No-Spill Evidence

Five production-policy traces were captured under `strace` at
`/tmp/opencode/nyc311-nospill-production-final86/`. Each is audited by
`scripts/audit_no_spill_trace.py --production`; the JSON alone never proves
no spill and the host trace verdict is required separately.

| Case | Result | Spill attempts / bytes | Key boundary |
| --- | --- | ---: | --- |
| Positive control | pass | 5 / 53,968,896 | tracer detects real spill |
| Negative control | pass | 0 / 0 | no spill on a non-spilling query |
| Gold full month | pass | 0 / 0 | four products, 323,627 rows, reference-equivalent |
| Fail-first | pass | 0 / 0 | native OOM at `business_execute_query`, zero products, failed target rejected |
| Fail-late | pass | 0 / 0 | native OOM after one product, failed target rejected |

Across **all 12 Gold business connections** (three trials) and the production
traces, requested config equals native config equals effective
`512.0 MiB / 1 thread / ''`; `injected=false`, `config_rewritten=false` and
`native_kwargs_unchanged=true`. Protected inputs, seeds and any pre-existing Gold
were unchanged, and no unexpected mutation or unresolved trace event occurred.
The exact native binary SHA-256 was
`cd857a08b9c56a9a253b475a3424383adf7a6d51e613e0db122c5760498e0b21` in both the
Gold ZIP and the trace.

## Profiled Workload Results

The same original local data and protocol from [Lambda Benchmarks](lambda-benchmarks.md)
were used: two high-volume daily cases and the complete 323,627-row month, three
fresh processes per case, against the rebuilt `-ready` artifacts. Original
response bytes/counts/cursors and observations are replayed by an offline HTTP
service, not a saved-Bronze shortcut. All original Silver and four Gold reference
comparisons pass.

| Case | Handler Range Seconds | Maximum RSS MiB | Maximum Cgroup MiB | Observed Temporary Peak MiB |
| --- | --- | ---: | ---: | ---: |
| Ingest, September 29 (11,976 rows) | 2.800-2.910 | 59.25 | 50.48 | 0.324 |
| Ingest, September 18 (11,964 rows) | 2.515-3.000 | 59.50 | 50.44 | 0.332 |
| Curate, September 29 | 2.982-3.292 | 254.21 | 155.86 | 0.926 |
| Curate, September 18 | 2.899-3.113 | 235.01 | 137.32 | 0.949 |
| Gold, 30 days (323,627 rows) | 8.676-9.622 | 493.42 | 396.57 | 17.176 |

Maximum cold-import and setup intervals were **6.590 / 0.875 seconds** across the
15 trials. Using `max(RSS, cgroup peak)` against each selected allocation gives
minimum memory headroom **88.38% / 75.17% / 75.91%** for Ingest/Curate/Gold.
Observed temporary headroom is **99.94% / 99.81% / 96.65%**. All local
comparisons fit 720 seconds and the 20% target; temporary peaks are sampled lower
bounds.

All 15 trials exited zero, without OOMs, outer timeouts or memory-limit events.
CPU-quota throttling is expected at 0.25/0.5/1 CPU and occurred; it is not an OOM
or correctness failure. Containers were removed, workspaces returned to their
empty baselines, and all **240 original publication files** retain their hashes.

**Historical reporting caveat:** the earlier retained embedded profile reports
compared only cgroup memory peak. The conservative figures above were recomputed
from both raw RSS and cgroup snapshots. Both methods pass for these trials, but
the old helper's cgroup-only comparison was too optimistic. The helper has been
corrected and tested to use the larger RSS/cgroup value, with unavailable metrics
remaining unknown; the no-spill spike uses that correction. Historical raw
evidence was not rewritten to hide the defect.

These observations are not AWS capacity or cold-start certification. CPU quota
is not exact Lambda scheduling; tmpfs is not Lambda ephemeral disk; shared
library/cache ownership changes cgroup charges; local HTTP/S3 emulation omits
network/signing/throttling/retry latency; three trials do not establish tails.

## Rebuilt Artifacts And Compatibility

The offline builder includes `nyc311/lambda_runtime.py` and the revised handlers.
Every **third-party and native** member (including the DuckDB shared object) is
byte-identical to the approved optimized ZIP; only application `nyc311/*` modules
changed. Each `-ready` ZIP, inventory and requirements file is byte-identical to
its repeat build.

| Stage | ZIP Bytes | Uncompressed Bytes | Margin Below 250 MiB |
| --- | ---: | ---: | ---: |
| Ingest | 17,188,574 | 24,708,118 | 237,435,882 bytes |
| Curate | 90,949,288 | 256,025,246 | 6,118,754 bytes |
| Gold | 89,699,311 | 242,002,122 | 20,141,878 bytes |

No layers are used. RAM/spill budgets do not add ZIP/layer space. Curate's package
margin remains about 5.84 MiB and must be rechecked for any later change.

```text
Image: public.ecr.aws/lambda/python@sha256:471e480c997b3150ab1a4fb3b862554fa45d54a4f4a5cd21e3115b320197b717
Amazon Linux 2023, Python 3.12.15, x86_64, glibc 2.34

ingest  7da5a45bff772bc14ae54023ddc04830253a74ce5892b2435bf6d10e544db6ac
curate  60b82e1b2b6e6331fdcdc4f52c9e8de76b7e2addd0df609aebd0335e1028a872
gold    8f9f6012557ff2d830fd5fb033c9618e7ac1b327a3b536478f28589875abfb6f

src/nyc311/gold.py          010fb53296e4d53d1148671e1d2577f9fc25224690f64d006019bbb9f0fd4e12
src/nyc311/lambda_gold.py   db7637fab6e29f3962e644cccb75675c66c462e698139bdd4f8f94bd00da048d
src/nyc311/lambda_runtime.py 33b5aeb820e80c28248293550c40a92f797c7292a6af6295848848a8dbf13fa5
```

Native linkage rechecks in the official image: `ldd` passed for **105** Curate and
**39** Gold ELF objects; 0 failures.

## Budget And AWS Risks

The profile resource ceilings are not provisioned AWS resources. The theoretical
maximum billed compute at a full 900-second invocation is **450 / 900 / 1,800
GB-seconds** for Ingest/Curate/Gold, before rounding and any service pricing.
One manual pass with 30 Ingest, 30 Curate and one Gold invocation would have an
arithmetic ceiling of **42,300 GB-seconds** at those timeouts, excluding outer
retries. This is not a forecast: local durations are not AWS-billed durations.
No account, regional pricing, free-tier assumption or USD cost was queried.
S3 requests/storage, logs, orchestration, Glue and Athena budgets still need
later explicitly authorized deployment planning and operating controls.

Actual Lambda memory/tmp accounting, source connectivity/mutation, SDK streaming
and retries, IAM/S3 behavior, cold starts, managed-runtime updates, CPU features,
interrupted-invocation residues, Glue publication and Athena consumption remain
unverified in AWS. The fail-closed no-spill policy means a genuine memory shortfall
fails the run rather than degrading; the 512-MiB engine buffer plus the observed
75%+ process-memory headroom make this acceptable locally, but the real AWS memory
accounting has not been observed. Stage 9 definitions exist, but no AWS plan,
apply or invocation has been performed; all AWS operations remain out of scope.

## Reproduction And Evidence

Current `-ready` artifacts are in `/tmp/opencode/nyc311-lambda-<stage>-ready/`,
with repeat builds at the same names plus `-repeat`. From the repository root:

```bash
export PATH="$PWD/.devenv/profile/bin:$PATH"
ls /tmp/opencode
uv run --offline --frozen --no-python-downloads python scripts/build_lambda_zip.py gold \
  --output /tmp/opencode/nyc311-lambda-gold-ready-recheck
uv run --offline --frozen --no-python-downloads python scripts/run_lambda_benchmarks.py \
  --profiled --artifact-suffix ready --work /tmp/opencode/nyc311-benchmark-ready-recheck
uv run --offline --frozen --no-python-downloads python scripts/run_no_spill_spike.py \
  --production --work /tmp/opencode/nyc311-nospill-production-recheck --case gold --trial 1
uv run --offline --frozen --no-python-downloads python scripts/audit_no_spill_trace.py \
  --production /tmp/opencode/nyc311-nospill-production-recheck/trials/nospill-gold-production-trial1
uv run --offline --frozen --no-python-downloads pytest
uv run --offline --frozen --no-python-downloads ruff check .
uv run --offline --frozen --no-python-downloads ruff format --check .
openspec validate "build-nyc311-september-2026-pipeline" --strict
```

Build destinations must be new. The runner uses the existing `-ready` artifacts,
not the single example recheck build, and records SHA provenance. No pull, package
installation from the network or original-data write is allowed.

The profiled workload evidence is `/tmp/opencode/nyc311-benchmark-ready/`, with
exact Docker commands, original/cloned hashes, resource profiles, 15 reports and
container exit states. Each Gold report records all four live connection values.
The production-policy no-spill evidence is
`/tmp/opencode/nyc311-nospill-production-final86/` with per-case `report.json`,
`trace-audit.json`, `observer.json` and the raw `trace.log`. Earlier preserved
diagnostics (`spill-diagnostic.json`, `-v2`, `-cold-physical`) are retained
unchanged as the historical reason the no-spill mechanism was chosen.

The full offline suite passed **1,210 tests with 6 skipped**, including explicit
settings/deadline, SDK-config, body-close, failure-marker, Gold-parity,
no-spill-policy and catalog-publication checks. At least one test induces a real out-of-memory error in
the native `execute(query)`, then confirms the connection closes and the run
stays failed and never complete. Passing the suite and Ruff/OpenSpec does not
substitute for the AWS-side verification listed above.
