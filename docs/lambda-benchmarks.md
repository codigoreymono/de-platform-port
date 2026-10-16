# Representative Local Lambda Benchmarks

## Task 8.5 Status

The subsequent 8.6 candidate profiles and their native spill-safety blocker are
recorded in [Deployment Readiness](deployment-readiness.md). The 8.5 baseline
below is preserved unchanged; 8.6 remains pending review, not deployment-ready.

**Completed: 15 isolated, successful local trials.** There are five workload
cases with three fresh processes/containers each. The change is at 32/46 tasks
complete. These measurements are a baseline for review, not a production sizing
decision, AWS performance certification, or task 8.6 implementation.

No dependency, business rule, page size, timeout, DuckDB connection setting,
permanent resource configuration, Nix file, lockfile, architecture, or original
publication was modified. No network/download, AWS/credential access, Terraform,
commit or push was performed.

## Workloads And Provenance

The source is the existing successful local run `september-2026-001` under
`data/local/published`. It covers September 1 through October 1 exclusive, with
**323,627 accepted records, 30 days, 84 gzip pages, zero quarantine and zero
discarded duplicates**. Daily page size is 5,000, unchanged from the saved run.
Source observations range from `2026-10-14T14:15:13.243351+00:00` through
`2026-10-14T14:18:57.885833+00:00`. These are October observations of the
September-created cohort, not September-30 historical status.

| Load | Rows | Pages | Saved Gzip Bytes | Original JSON Bytes | Silver Parquet Bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| September 29, maximum rows | 11,976 | 3: 5,000 / 5,000 / 1,976 | 330,055 | 5,127,880 | 612,255 |
| September 18, maximum byte volume | 11,964 | 3: 5,000 / 5,000 / 1,964 | 339,570 | 5,154,288 | 629,815 |
| Entire September | 323,627 | 84 | 9,221,609 | 139,660,005 | 17,160,396 |

Maximum rows and maximum bytes were selected by aggregating all 30 manifests,
not by choosing the tiny compatibility fixture. Both daily cases were measured
for Ingest and Curate; Gold used the full 30-day published cohort.

The runner hashes the **entire 240-file original publication tree** before and
after. All paths and hashes remain identical. It creates independent byte copies,
never moves, symlinks or hard-links data. Containers receive only read-only
clones, not the originals. The reusable clone has 234 input objects and
26,590,347 bytes, with the five original Gold reference files stored separately.

## Runtime And Isolation

The same existing official image and optimized ZIPs approved in 8.4 were used:

```text
public.ecr.aws/lambda/python@sha256:471e480c997b3150ab1a4fb3b862554fa45d54a4f4a5cd21e3115b320197b717
Amazon Linux 2023, Linux x86_64, Python 3.12.15, glibc 2.34

ingest  9072576c33b46728c4ce51b23e1d19d9509992ba3f20fbf617971a01397b9157
curate  73902edcbbbfb76ed20487b75c4664c956d4ad2ff92f6ae4ad1e8196a1efee6e
gold    109fb2a373b96831b254e04d1fa3916d7abb5d62f2744d53a635c730a29b377c
```

Every extracted member was checked against its ZIP inventory. Each container
mounts only one extracted stage ZIP read-only at `/var/task`, read-only probe
files at `/checks`, read-only cloned inputs at `/inputs`, and its own writable
trial directory at `/evidence`. Python runs with `-I -S -B`; module-origin audits
exclude image SDK/site-packages, host source and Nix dependencies.

Docker uses the local Unix socket, `--pull=never`, `--network=none`, a read-only
root, no capabilities/new privileges, and the caller's unprivileged UID/GID.
Environment inheritance is cleared; HOME is temporary, AWS credential/config
paths are `/dev/null`, metadata discovery is disabled, and SDK session/client
construction plus socket/DNS calls are blocked. No source or S3 call reaches a
network service.

### Comparison Envelope

The fixed **test-only** envelope matches the previous compatibility probe:

| Resource | Test Ceiling / Setting |
| --- | --- |
| Memory | 2 GiB, observed cgroup `memory.max=2147483648` |
| Swap | No additional swap permitted |
| CPU | Two quota equivalents, observed `cpu.max="200000 100000"` |
| Temporary storage | Private 512 MiB `/tmp` tmpfs |
| PIDs | 256 |
| BLAS/OpenMP | `OPENBLAS_NUM_THREADS=1`, `OMP_NUM_THREADS=1` |
| Business configuration | Existing defaults: page size 5,000; task deadline 720 s; no DuckDB overrides |

This envelope is not a selected Lambda configuration. In particular, Docker CPU
quota is independent of memory, whereas Lambda CPU allocation is memory-linked.
Two CPU equivalents at 2 GiB must not be treated as an actual Lambda allocation.

## Measurement Protocol

`scripts/run_lambda_benchmarks.py` prepares inputs and verified extracted ZIPs,
then launches trials sequentially, interleaving the five cases for each of three
repetitions. The image must already be present. The runner never pulls/builds an
image, installs packages, changes a payload, or overwrites prior evidence.

`scripts/benchmark_lambda.py` invokes the actual packaged handler once per
trial. Preparation and correctness validation are outside the handler timer.
Each run has a unique ID and an exact fresh object seed; prior output manifests
are absent, preventing a complete-run retry from replacing the measured work.

- **Ingest:** A packaged HTTPX `MockTransport` serves one original decompressed
  page at a time, with original response bytes, counts, ordering, textual cursors,
  selected fields and observation timestamps. It executes fresh count/page/gzip/
  publication logic, not saved-Bronze replay. Reading/decompressing/hash-checking
  the simulated service response is inside handler time; HTTP latency is absent.
- **Curate:** Only the selected original complete Bronze manifest and three
  pages are seeded. The real handler normalizes and publishes a new partition,
  quality report and quarantine object under a new processing run ID.
- **Gold:** Exactly 150 dependencies are seeded: 30 Silver manifests, 30 Parquet
  files, 30 quality reports, 30 quarantine files and 30 source manifests. There
  are no prior target Gold files. The real handler reads all 30 days and executes
  its four packaged SQL files with its unchanged DuckDB defaults.

Disk-backed S3 emulation does not retain the monthly population in Python memory.
Each successful get is copied into a `BytesIO`, as in the existing test helper;
those costs and local filesystem effects are included, not mistaken for AWS S3.

### Correctness And Retained Failures

All 15 trials exit zero and pass cleanup and origin audits. Ingest page hashes,
counts and observations match the originals. Curate tables and metadata match
the original Silver; quality matches except the new processing run ID, and
quarantine content is identical.

Every Gold trial has full-month coverage and matches the original four products,
schemas and frozen provenance. Nonfloat values match exactly; float comparison
allows only `rel_tol=1e-10`, `abs_tol=1e-9` roundoff. Gold product rows are:
14,598 demand, 160 closure, 212 outstanding, and 698 channel rows. Demand and
channel totals are each **323,627**; eligible closed requests total **285,644**.

No failed trial was excluded. Runner metadata preserves stdout/stderr, return
codes, outer timeouts, exact commands and container state before removal. No
trial had stderr output, an OOM kill, a 900-second outer timeout, a memory-limit
event or CPU-quota throttling. Every private `/tmp` returned to its empty baseline.

## Results

Seconds are measured with `perf_counter`. Each range is the minimum and maximum
of **three** trials, not a tail percentile. Memory and storage columns are the
maximum observed across those trials, captured at handler return before
post-validation. MiB = 1,048,576 bytes.

| Case | Handler Median Seconds | Handler Range Seconds | Import Median Seconds | Peak Process RSS MiB | Peak Cgroup Charge MiB | Observed `/tmp` Peak MiB |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Ingest, September 29 | 0.666 | 0.586-0.667 | 1.083 | 58.73 | 59.05 | 0.324 |
| Ingest, September 18 | 0.544 | 0.532-0.591 | 0.865 | 59.24 | 52.55 | 0.332 |
| Curate, September 29 | 1.315 | 1.261-1.385 | 3.083 | 244.06 | 148.73 | 0.926 |
| Curate, September 18 | 1.282 | 1.258-1.409 | 2.789 | 235.50 | 137.38 | 0.949 |
| Gold, 30 days | 8.416 | 8.244-8.732 | 0.951 | 457.10 | 361.24 | 17.211 |

Exact largest handler-phase `/tmp` allocated observations were 348,160 bytes for
Ingest, 995,328 for Curate, and 18,046,976 for Gold. Logical-file peaks are also
recorded separately. These are **sampled observations**, not exact maxima.

### Local Target Comparison

OpenSpec's initial targets are at most **720 seconds** per task, with **at least
20% memory and temporary-storage headroom**, and a Lambda timeout no greater
than 900 seconds. Compare only with the stated local test envelope, not an
unselected AWS function allocation.

For memory comparison, use the larger of process high-water RSS and cgroup peak,
not their sum. This avoids relying solely on cgroup charge when shared page
ownership lowers it relative to RSS.

| Stage | Largest Import + Setup + Handler Seconds | Minimum Local Memory Headroom | Minimum Observed `/tmp` Headroom |
| --- | ---: | ---: | ---: |
| Ingest | 1.951 | 97.11% | 99.94% |
| Curate | 4.686 | 88.08% | 99.81% |
| Gold | 10.093 | 77.68% | 96.64% |

All local time/memory comparisons pass the initial targets. Observed storage
comparisons also pass, but sampling cannot certify the exact storage maximum.
The worst observed end-of-handler process interval leaves **709.907 seconds**
to the 720-second target, and **889.907 seconds** to 900 seconds. These are
arithmetic local margins, **not a measured allowance for real HTTP/S3, retries,
managed-runtime initialization or AWS cold starts**.

### Bottlenecks Observed

Gold is the largest measured compute/memory load. Its handler used approximately
8.75-9.00 CPU seconds across the measured 8.24-8.73 wall seconds; no quota
throttling occurred. This points to computation/input validation in the local
pipeline, not an observed quota or memory ceiling. The timings do not separately
attribute native SQL execution versus the handler's independent validation loops.

Cold dependency imports dominate the daily Curate process: median imports of
2.79-3.08 seconds exceed its 1.28-1.32-second handler. Lambda execution-environment
reuse could amortize imports, but no warm-process trials or AWS cold starts were
measured here.

Potential network sensitivity is visible in the actual emulated object counts:

| Stage | Get / Head / Put Calls | Handler Object Bytes Read | Handler Object Bytes Written |
| --- | --- | ---: | ---: |
| Ingest, daily | 3 / 3 / 10 | 330,055-339,570 | 338,404-347,919 |
| Curate, daily | 7 / 0 / 5 | 948,603-975,574 | 618,673-636,129 |
| Gold, monthly | 154 / 5 / 10 | 17,482,287 | 309,989 |

Ingest separately serves five HTTP requests (two counts and three pages), with
5,127,880 or 5,154,288 decoded page-response bytes. These are not encrypted wire
byte counts. Gold's many sequential remote operations can add meaningful latency
in AWS that the local fake does not reproduce.

## Recommendations For 8.6, Not Applied

- Select candidate memory/CPU/tmp allocations together using Lambda's actual
  CPU-to-memory relationship, then rerun representative workloads. Do not copy
  the local two-CPU quota into a presumed 2 GiB Lambda configuration.
- Preserve both RSS and cgroup evidence. In particular, Gold's observed 457 MiB
  RSS alone would leave less than 20% headroom at 512 MiB; do not choose that
  budget by extrapolating from tiny fixtures or cgroup charge alone.
- Explicitly decide DuckDB memory, spill directory and threads, and test their
  behavior under the selected limits. This baseline did not change connections,
  force memory pressure, characterize spills, or prove a safe default spill path.
- Budget HTTP/S3 request latency, signing, retries, backoff, failure reporting
  and total deadlines separately. A sub-second offline Ingest handler does not
  justify a matching production timeout.
- Page size 5,000 and daily granularity fit the observed local baseline. That is
  evidence for comparison, not a decision to lock granularity or remove the
  future safety gate. Remeasure any changes rather than silently tuning now.
- Keep ZIP-size checks separate: Curate still has only 5.84 MiB of uncompressed
  package margin. RAM headroom does not provide ZIP/layer headroom.

## Measurement Limits

RSS is Linux process-lifetime `ru_maxrss * 1024`, including imports and setup;
it is not handler-only allocation or AWS Max Memory Used. `memory.peak` is a
kernel cgroup counter including file cache/tmpfs and fake-I/O effects. Shared
pages can be charged elsewhere, explaining cgroup values lower than RSS. Neither
metric is identical to Lambda's managed-runtime memory reporting.

The private `/tmp` sampler requests 5 ms intervals; thread scheduling, the GIL
and non-atomic filesystem walks can miss short-lived peaks. A tmpfs consumes
cgroup memory and is not Lambda's ephemeral disk. Simulated remote object data
under `/evidence` is excluded from `/tmp`, though its cache effects may still
affect cgroup memory. No exact peak or disk-performance equivalence is claimed.

These are fresh Python processes, not cold host caches: extracting ZIPs, checking
input hashes, earlier trials and shared libraries warm the host page cache.
Setup, imports, handler, validation and overall Docker wall time are recorded
separately. Final report serialization and interpreter/container startup are not
part of the handler interval. Three repetitions describe a small observed range,
not stable p95/p99, cost, scaling, source mutation, failure-tail or AWS capacity.

## Reproduction And Evidence

Run from the repository root with existing local data, artifacts and cached
image. The work directory must be new and directly beneath `/tmp/opencode`:

```bash
ls /tmp/opencode
uv run --offline --frozen --no-python-downloads python scripts/run_lambda_benchmarks.py \
  --work /tmp/opencode/nyc311-benchmark85-recheck
uv run --offline --frozen --no-python-downloads pytest tests/test_lambda_benchmark.py
```

The runner checks image identity without pulling, hashes/copies inputs, verifies
every ZIP member, seeds fresh trial object roots, runs all 15 probes with recorded
commands, captures timeout/OOM evidence, removes only its own containers and
checks the original publication hashes again. It stops on a failed trial with
evidence intact. It never reuses successful target outputs or changes settings
to make a trial pass.

Recorded evidence is under **`/tmp/opencode/nyc311-benchmark85/`**:

- `inputs/manifest.json`: image/payload fingerprints, all 240 original hashes,
  clone identity, daily counts/bytes and selected loads.
- `trials/<run-id>/report.json`: probe/helper hashes, inputs, exact defaults,
  import origins, timers, pre/post-handler resource snapshots, tmp observations,
  object I/O and post-timer correctness results.
- `trials/<run-id>/runner.json`, `stdout.txt`, `stderr.txt`: exact command,
  container state, return code, Docker wall time and failure evidence.
- `trials.json`: all 15 trials, with no excluded failure or retry substitution.
- `summary.json`: grouped min/median/max, raw per-trial metrics and local target
  arithmetic, computed from retained reports after execution.
- `originals-preserved.json`: 240 original files unchanged; read-only input
  clones, reference files, extracted payloads and trial outputs remain available.

The full offline regression suite passes **559 tests**, including 13 new
benchmark-helper tests. Those host tests supplement, rather than replace, the
actual pinned-image measurements. Ruff, format and strict OpenSpec checks pass.

Task 8.6 and Terraform remain pending. AWS connectivity, actual Lambda startup,
resource accounting and volume behavior, real SDK failure/retry paths, catalog
publication and Athena verification still require their later tasks and explicit
AWS authorization. Stop here to review these measurements before sizing.
