# DuckDB No-Spill Offline Spike

## Decision And Scope

**Viable for the recorded local workload and native payload, not implemented
in production.** Empty `temp_directory` produced controlled resource rejection
for the spill-forcing control and completed the four real Gold products without
unexpected filesystem mutations in the recorded syscall scope. The strengthened
Gold audits also require zero data mutations during each native compute window.

This was an explicitly authorized experimental spike within task 8.6. It does
**not** approve the production policy, establish AWS behavior, complete 8.6, or
authorize Terraform. Progress remains **32/46**, with 8.6 unchecked. The earlier
`max_temp_directory_size` enforcement discrepancy remains valid evidence; this
spike tests a different mechanism, not a larger limit or an updated DuckDB.

Only experimental scripts, instrumentation, helper tests and documentation
changed. `gold.py`, `lambda_gold.py`, `lambda_runtime.py`, SQL, dependencies,
lockfiles, Nix, original publications and approved ZIPs remain unchanged.

## Runtime And Exact Binary

All execution used the already-cached official image, with no pull or download:

```text
public.ecr.aws/lambda/python@sha256:471e480c997b3150ab1a4fb3b862554fa45d54a4f4a5cd21e3115b320197b717
Amazon Linux 2023, Python 3.12.15, glibc 2.34, linux/amd64

DuckDB version: 1.5.6
_duckdb.cpython-312-x86_64-linux-gnu.so SHA-256:
cd857a08b9c56a9a253b475a3424383adf7a6d51e613e0db122c5760498e0b21
```

That native SHA matches both the approved optimized Gold ZIP and the bounded
candidate ZIP. The latter supplies the existing 8.6 resource-plumbing code for
the experimental handler. It was extracted to a separate read-only payload;
every member was verified against its inventory. No ZIP was rebuilt or patched.

Containers had read-only roots, `--network=none`, `--pull=never`, no capabilities
or new privileges, caller UID/GID, 2,048 MiB memory with no extra swap, one CPU
quota equivalent, and private 512-MiB `/tmp` tmpfs. Only individual scripts and
cloned inputs were mounted read-only; each trial had its own writable evidence
and fake-S3 object root. No Docker socket, credentials, host interpreter, Nix
libraries or host application tree was mounted into the workload.

Python ran with `-I -S -B`. SDK/session and network guards remained active;
module-origin audits found only ZIP dependencies and permitted image stdlib.
The host's existing `strace` observed the process externally; it was not installed
inside the image or used as a runtime dependency.

## Experimental Boundary

The unmodified application currently rejects an empty temporary path and the
Lambda handler supplies its private spill path. The spike therefore copies the
configuration **only at `duckdb.connect`**, after application validation:

- Normal Gold connections retain 512-MiB engine memory and one thread, replacing
  only the native `temp_directory` with an empty string.
- The native connection object is returned unchanged. No production SQL, schema,
  transformation, publication callback or resource-policy file is replaced.
- `max_temp_directory_size=256MiB` remains identical in positive and negative
  cases. It is **not** the no-spill enforcement mechanism or a trusted hard cap.
- Failure injection lowers only the selected native connection's memory to 1 B,
  producing a real DuckDB allocation exception rather than raising a fake error.

This proves an experimental native-connection configuration, **not** that the
current production validator/handler already accepts or requests no-spill mode.

## Observation Method

The probe grants ptrace permission for its own temporary process using image
glibc's `PR_SET_PTRACER_ANY`. No host sysctl, PID namespace, container capability
or production process was changed. A READY/GO barrier occurs before native
imports. The runner records the actual host PID and confirms `TracerPid` for
every pre-GO thread before allowing execution.

Existing host `strace` 7.2 follows threads and records file opens/mutations, FD
writes, shared writable mappings, process lifecycle and additional kernel I/O
interfaces. PID-namespace decoding reconciles container clone-return IDs with
the observed host TIDs. Write buffers are raw pointers, not record-content dumps.
All observed threads must have clean exits; unsupported captured operations,
unknown FDs/paths or incomplete evidence cannot silently pass.

The auditor checks the entire trace, not merely a spill-directory prefix or an
end-of-run snapshot. It covers alternate paths such as relative cwd, HOME,
`/tmp`, `/dev/shm` and unnamed-file attempts within the recorded syscall scope.

### Strict Compute Windows

A broad application-path allowlist alone cannot prove which component wrote a
file. The strengthened probe brackets the original `gold_store._compute_query`
delegate, including connection creation, business SQL, Arrow conversion and
independent result validation, with `native-product-N` markers. Inside these
windows, **all data mutations are forbidden**, even to otherwise valid Silver,
Gold, staging or evidence paths. Only empty probe delimiters and stdout/stderr
pipe I/O are exempted; those raw output buffers are not inspected. The selected
strict windows contain no such pipe writes, only empty delimiters. Parquet
preparation and publication occur after the compute delegate returns and remain
separately classified.

This closes the earlier allowlist blind spot for the tested compute intervals.
It does not attribute arbitrary later asynchronous writes to a particular
library; no successful async-I/O mechanism appeared in the selected traces.

## Primary Evidence

All primary trials use expanded process/kernel-I/O capture with a pre-GO thread
census. Gold/failure trials additionally require `strict_compute_coverage=true`.
A functional `passed` JSON alone is not a no-spill verdict.

| Trial | Functional Result | Trace Result |
| --- | --- | --- |
| `nospill-positive-trial5` | 350,000 generated rows, real physical spill | Pass, positive writes corroborated |
| `nospill-negative-trial3` | Controlled native OOM with no temp directory | Pass, zero unexpected mutations/unresolved events |
| `nospill-gold-trial4` | Four reference-equivalent products, full month | Pass, strict compute coverage |
| `nospill-gold-trial5` | Four reference-equivalent products, full month | Pass, strict compute coverage |
| `nospill-gold-trial6` | Four reference-equivalent products, full month | Pass, strict compute coverage |
| `nospill-fail-first-trial2` | Failed target, zero uploaded product objects | Pass, strict compute coverage |
| `nospill-fail-late-trial2` | Failed target after one uploaded product | Pass, strict compute coverage |

### Positive And Negative Controls

The two controls execute identical SQL and data with a 32-MiB engine limit and
one thread. Only the temporary-directory setting changes:

```text
Query SHA-256:
02469e01e351f0a5edee3e535018ace5a2e6654db60204ec7869e3341beefeb4

SELECT i, repeat(md5(i::VARCHAR), 4) AS value
FROM range(350000) t(i) ORDER BY value
```

With `/tmp/positive-spill` enabled, all 350,000 rows completed. The sampled
physical occupancy reached **29,655,040 bytes**; the trace corroborates five
spill writable opens and **59,473,920 cumulative bytes written**. Occupancy and
cumulative I/O are different measures. The occupancy peak is a sampled lower bound.

With `temp_directory=''`, DuckDB raised `OutOfMemoryException` before producing
rows, explicitly reporting that no temporary directory was specified and unused
blocks could not be offloaded. There were **zero unexpected mutations, zero
unresolved events, no spill writable opens and zero observed temporary bytes**.
Writable sentinels succeeded in `/evidence`, `/tmp`, cwd and `/dev/shm`, so this
was not a rejection caused by read-only filesystem permissions.

### Full Real Gold

Three fresh containers consumed the existing **323,627-record, 30-day** cohort
through the actual packaged handler and its unchanged four SQL files. All 12
normal connections reported 512.0 MiB memory, empty `temp_directory` and one
thread. No unexpected mutation or writable data operation occurred within the
strict compute windows; outside those windows only known application staging,
manifest and publication paths were mutated.

All three results match the existing Gold reference: schemas and metadata,
exact nonfloat columns, floating values within `rel_tol=1e-10`/`abs_tol=1e-9`,
quality accounting, observations, input provenance and SQL hashes. Product rows
are demand **14,598**, closure **160**, outstanding **212**, channels **698**.
Demand/channel populations each reconcile to 323,627; eligible closures total
285,644. The real source was not fetched again.

| Trial | Traced Handler Seconds | Import Seconds | RSS MiB | Cgroup Peak MiB | Conservative Peak MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Gold 4 | 13.385 | 2.470 | 490.76 | 395.00 | 490.76 |
| Gold 5 | 12.212 | 2.192 | 491.28 | 393.50 | 491.28 |
| Gold 6 | 11.681 | 2.139 | 489.55 | 391.70 | 489.55 |

The largest conservative observation is **491.28 MiB**, leaving about **76.01%**
against 2,048 MiB. Observed private `/tmp` allocation peaked at **18,010,112 bytes
(17.176 MiB)** for necessary application input/output staging, not detected
DuckDB spill. These sampled storage peaks are lower bounds. This tracing workload
must not be compared directly with untraced 8.5/8.6 timing as an optimization.

### Publication Failures

The early/late trials use a real 1-byte native memory setting on connection one
or two. The allocation failure occurs in the observer's `SELECT current_setting`
inside product computation setup, **before that product's business SQL**. Live
settings cannot be read for the failing connection and are explicitly marked
unavailable; no values are fabricated. This proves the native-allocation-error
publication path, not execution of the four queries under a 1-byte limit.

The early case uploads zero product objects. The late case uploads the first
product, then fails preparing the second connection. Both record:

- No attempted `complete` progress state or complete target-manifest write.
- A persisted new target manifest in `failed`, with zero or one product descriptor.
- Rejection by the normal `load_gold` reader.
- All five old successful Gold objects unchanged and still readable.
- Removal of invocation workspaces and preservation of every seed/input hash.

Partial staged artifacts under a new failed run are permitted by the existing
publication design; they are not an authorized complete publication. No Glue or
catalog publication was performed.

## Conservative Memory Helper

The experimental helper now uses **`max(RSS, cgroup memory.peak)`**, not cgroup
alone. It records both validated metrics, availability flags and completeness.
When either is missing/invalid, the full peak/headroom verdict stays unknown;
the maximum available observation is retained only as a lower bound. Missing
values are never replaced by zero. Tests cover both maximum directions,
thresholds, malformed/missing counters and unknown handling.

The strict summary uses this corrected helper with the actual retained raw
metrics. Historical 8.6 reports were not rewritten to conceal their earlier
cgroup-only calculation.

## Observability And Safety Limits

- This is empirical evidence for the exact payload, controls, operators and
  frozen business workload, not a mathematical guarantee for every DuckDB query,
  future native version, plugin or malicious kernel-I/O mechanism.
- The capture does not include every possible syscall. `mprotect`/`munmap`,
  `close_range`/`unshare` and other uncaptured FD/metadata operations remain
  outside the required filter. Successful captured async I/O or non-shared
  processes make the audit inconclusive; none occurred in primary evidence.
- The shared FD-table model requires observed shared-thread clones. Host census,
  PID mapping, initial stdout/stderr pipes and namespace path annotations are
  explicit trusted assumptions, not independently attested kernel facts.
- `strace` affects scheduling, file-I/O latency, imports and handler duration;
  its host process is outside the workload cgroup. RSS/cgroup observations are
  local process/container measures, not CloudWatch Max Memory Used.
- `/tmp` is private tmpfs, not Lambda ephemeral disk; sampling can miss brief
  occupancy peaks. The zero-spill conclusion uses syscall evidence and controls,
  not just that sampler or an empty final directory.
- A controlled DuckDB exception is different from an OS OOM kill, hard Lambda
  timeout or host failure. Those can prevent diagnostic markers and cleanup.
  No universal success/error atomicity beyond the existing publication contract
  is claimed.
- Actual managed Lambda initialization, CPU/kernel behavior, IAM/S3, signing,
  network retries and service failures remain untested here.

## Reproduce And Review

All files are under `/tmp/opencode/nyc311-nospill-spike/`, separate from prior
benchmarks and original artifacts. `baseline.json` records native/archive hashes,
production/policy/lock/Nix fingerprints and all 240 original data hashes. Each
trial retains `observer.json`, functional `report.json`, raw `trace.log`,
`trace-audit.json`, stdout and stderr. `summary-strict.json` selects only the
seven primary trials listed above; `summary.json` is the earlier exploratory
summary and is not the final strict evidence set.

Earlier attempts remain visible: positive 1 failed the probe's origin check
before native SQL because the legitimate multiprocessing alias was not yet
recognized; positive 2/negative 1 have legacy trace scope; positive 3 is
inconclusive due to unannotated PID-namespace mapping; positive 4/negative 2
support the controls but precede the final probe version. Gold 1-3 and first
failure trials lack strict compute windows. None are substituted for the primary
evidence or deleted to hide harness failures.

With the same local cached image, ZIPs and cloned-input sources, use a **new**
work directory for a fresh spike. No images/tools/packages may be installed:

```bash
ls /tmp/opencode
WORK=/tmp/opencode/nyc311-nospill-spike-recheck
uv run --offline --frozen --no-python-downloads python scripts/run_no_spill_spike.py --work "$WORK" --case positive --trial 1
uv run --offline --frozen --no-python-downloads python scripts/audit_no_spill_trace.py "$WORK/trials/nospill-positive-trial1"
uv run --offline --frozen --no-python-downloads python scripts/run_no_spill_spike.py --work "$WORK" --case negative --trial 1
uv run --offline --frozen --no-python-downloads python scripts/audit_no_spill_trace.py "$WORK/trials/nospill-negative-trial1"
```

Stop if either control or observation is not a pass. Then run three `gold`
trials and one each of `fail-first`/`fail-late`, auditing each new trial before
acceptance. Require expanded scope for every primary trial, and strict compute
coverage for Gold/failure cases; a legacy-scope pass is not equivalent.

Production sources, policies, native dependencies, both original Gold archives,
all cloned protected input files and all 240 original publication files were
checked unchanged. No AWS, credentials, deployment, commit, push or Terraform
was used. OpenSpec 8.6 remains unchecked.

The full offline regression suite passes **1,081 tests**. The spike-focused
probe/trace/conservative-memory suites pass **413 tests**; their mocked checks
supplement the seven primary native trials rather than replace them. The tests
also exercise strict delegate arguments/restoration and marker closure on
resource errors. Ruff, formatting, whitespace checks and strict OpenSpec
validation pass. Production/Nix/lock hashes match their pre-spike fingerprints,
and no spike container remains running.

## Minimal Future Production Changes, Not Applied

After review and separate approval:

1. Allow the exact empty-string no-spill sentinel in Gold's resource validation,
   while retaining rejection of NUL/unsafe relative paths and unrelated settings.
2. Make the runtime policy and Lambda handler request the empty path directly;
   do not let the handler reinstate an absolute spill directory. Retain the
   measured memory/thread policy. Any retained size setting is not the safety
   mechanism.
3. Update direct production-path tests and the benchmark observer to require
   the empty effective setting, without the spike's connection monkeypatch.
   Add actual business-SQL exhaustion tests as well as the verified setup-failure
   publication tests, keeping early/late failed-run and old-publication checks.
4. Rebuild separate candidate artifacts, recheck size/ABI/parity/headroom and
   review residual AWS risks before deciding whether 8.6 can be completed.

The current spike establishes local viability for review; it does not apply
these changes, replace the production policy, or permit proceeding to stage 9.
