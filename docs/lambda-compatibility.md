# Lambda Compatibility Gate

## Task 8.4 Status

**Passed locally for the three existing optimized ZIPs.** Task 8.4 is complete;
this completed the first 31/46 tasks. Native imports, handler execution,
timezone/DST, Parquet, library resolution, size and fixture-resource observations
passed in the authorized official image. No incompatibility or missing shared
library was detected for these checks. The subsequent task 8.5 local volume
baseline is documented in [Lambda Benchmarks](lambda-benchmarks.md). The 8.6
native spill-safety finding is documented in
[Deployment Readiness](deployment-readiness.md); it was resolved by the fail-closed
DuckDB limits, after which the stack was deployed and verified
(see [Test Evidence](test-evidence.md)).

The target is AWS Lambda Python 3.12 on Linux x86_64, using Amazon Linux 2023
with glibc 2.34. The NixOS development interpreter and development libraries are
not substitutes for this runtime. Successful imports on a newer Linux glibc
would not establish compatibility with the older target baseline.

## Runtime Baseline

The host is NixOS 26.11, x86_64. Docker is available through the local Unix
socket `unix:///var/run/docker.sock`. No remote Docker context was used.

Initially only Debian-based Python images were available, so runtime testing
paused rather than substituting them or NixOS for Lambda. The operator then
authorized downloading only the official Python 3.12 image for local testing.

| Property | Observed Value |
| --- | --- |
| Authorized image | `public.ecr.aws/lambda/python:3.12` |
| Repo digest / observed local image ID | `sha256:471e480c997b3150ab1a4fb3b862554fa45d54a4f4a5cd21e3115b320197b717` |
| Platform | `linux/amd64`, machine `x86_64` |
| Base OS | Amazon Linux 2023.12.20260914 |
| Python | 3.12.15, GCC 11.5.0, `/var/lang/bin/python3.12` |
| glibc | 2.34 |
| Host kernel shared by containers | Linux 6.18.48 x86_64 |

The pull and identity commands were:

```bash
docker --host unix:///var/run/docker.sock pull --platform linux/amd64 public.ecr.aws/lambda/python:3.12
docker --host unix:///var/run/docker.sock image inspect public.ecr.aws/lambda/python:3.12 \
  --format '{{json .RepoDigests}} {{.Id}} {{.Os}}/{{.Architecture}}'
```

This is a recorded, explicitly authorized public-image retrieval, not general
permission for additional pulls or AWS operations. Every subsequent container
used the fixed digest, `--pull=never`, and `--network=none`.

The first fingerprint command printed the matching runtime but its Docker client
exceeded a 30-second tool timeout during completion. No container was left running.
All functional and linkage runs completed successfully with a 120-second outer
command budget. This was not an import failure or an AWS task-timeout measurement.

## Isolation And Provenance

Each stage ran in a fresh container with only its verified, extracted ZIP at
read-only `/var/task`. Individual probe/fixture files were mounted read-only at
`/checks`; only test evidence and simulated S3 objects were writable at `/evidence`.
No repository tree, `.venv`, Nix libraries, Docker socket, or credentials were
mounted. ZIPs, dependencies and business logic were not rebuilt or modified.

`/usr/bin/env -i` removed inherited environment settings. Python used `-I -S -B`;
`sys.path` contained `/var/task` and only the image's three stdlib/dynamic-module
paths. Image `site-packages` and the runtime's bundled SDK were excluded. Initial
and final module-origin audits found zero violations: SDK, stage dependencies,
native Python extensions and handlers all came from the ZIP under `/var/task`.

`LD_LIBRARY_PATH` was explicitly set to the official image's own default paths,
not the Nix host's value. The containers ran unprivileged with a read-only root,
no capabilities and no new privileges. Test-only ceilings were 2 GiB memory,
no additional swap, two CPU quota equivalents, 256 PIDs, and private 512 MiB
`/tmp` tmpfs. `OPENBLAS_NUM_THREADS=1` and `OMP_NUM_THREADS=1` bounded fixture
threading; these are probe settings, not production Lambda resource configuration.

Real SDK client construction, socket connection calls and DNS resolution were
blocked. HTTPX used `MockTransport`; a disk-backed SDK-shaped S3 fake exchanged
known objects between containers. HOME was `/tmp`, metadata discovery was
disabled, and AWS config/credential paths were `/dev/null`. No AWS account,
live source, external service or credential was used by the tests.

## Size And Integrity

The actual optimized ZIPs under `/tmp/opencode/` were rechecked using stdlib-only
host Python for archive arithmetic, exact member lists, per-file SHA-256 and
CRC validation before extraction. Host Python was only an archive tool; all
payload execution used the pinned image interpreter and ZIP dependencies.

No layers are supplied by the current packaging configuration. For that zero-layer
configuration, aggregate uncompressed bytes equal the ZIP member-size sum.
Future layers require reevaluation; the sidecar inventory is not part of the
deployment ZIP.

| Stage | ZIP Bytes | Aggregate Uncompressed Bytes | Margin Below 250 MiB | Size Portion |
| --- | ---: | ---: | ---: | --- |
| Ingest | 17,186,734 | 24,702,836 | 237,441,164 bytes / 226.44 MiB | Pass |
| Curate | 90,947,569 | 256,020,499 | 6,123,501 bytes / 5.84 MiB | Pass |
| Gold | 89,697,040 | 241,995,174 | 20,148,826 bytes / 19.22 MiB | Pass |

The limit used is 262,144,000 bytes (250 MiB). The ZIP hashes match their
inventories, all CRCs pass, and ordered member paths match the inventory entries:

```text
ingest  9072576c33b46728c4ce51b23e1d19d9509992ba3f20fbf617971a01397b9157
curate  73902edcbbbfb76ed20487b75c4664c956d4ad2ff92f6ae4ad1e8196a1efee6e
gold    109fb2a373b96831b254e04d1fa3916d7abb5d62f2744d53a635c730a29b377c
```

Complete assembly inventories, per-file hashes, optimization rationale and
preserved native-file evidence are in [Lambda Packaging](lambda-packaging.md).
Curate's size margin is modest. This size portion and the runtime checks below
pass 8.4 locally, but do not authorize deployment or provide future layer headroom.

## Executed Runtime Checks

`scripts/check_lambda_runtime.py` is a test-only probe, not a deployment module.
It executed the unmodified packaged handlers directly with the sanitized
`data/samples/gold-metrics.json` fixture: 12 requests across September 1-2, with
fixed source observation time `2026-10-13T12:00:00+00:00`.

| Stage | Checks | Observed Result |
| --- | ---: | --- |
| Ingest | 16 | Packaged HTTPX/SDK and handler imports; page counts `[3,3,1]` and `[3,2]`; complete Bronze counts 7 and 5; original observations; complete retries and replay without HTTP clients |
| Curate | 23 | NumPy/Pandas native operations; packaged timezone summer/winter offsets; ambiguous and nonexistent optional DST dates; real Snappy Parquet round trip; 7/5 verified Silver rows and schema/value-preserving replay |
| Gold | 17 | Packaged DuckDB/PyArrow and handler imports; Snappy Parquet round trip; four SQL products and verified schemas; count/quantile reconciliation; historical retry without current Silver reads |

All **56 assertions passed**. Gold product row counts were demand 9, closure 2,
outstanding 3 and channels 10. Demand/channel population totals were 12 each,
with 5 eligible closures, 2 confirmed outstanding and 1 unknown-status request.
DOT closure durations `[1,3,9,696]` produced median 6.0 hours and continuous p90
489.9 hours (within floating-point tolerance). Saved observation/provenance
hashes and all four packaged SQL digests reconcile.

Curate's `PYTHONTZPATH` points only to `/var/task/timezone`. The exact packaged
New York TZif produced summer UTC-4 and winter UTC-5, and the existing business
logic retained local DST-defective optional timestamps while making UTC values
unresolved. The fixture's ambiguous closure remained flagged and duration-ineligible.

### Shared Libraries

`scripts/check_lambda_linkage.py` ran the image's `/usr/bin/ldd` against every
ELF member: **105 Curate + 39 Gold**, with none in the pure-Python Ingest ZIP.
Every return code was zero; no missing-library, missing-version or Nix-path
diagnostic occurred. ELF hashes match the original optimized inventories.
Curate's two static `.a` archives also remain unchanged; they have no dynamic
dependency list and were not represented as dynamically loaded libraries.

This checks declared shared dependencies plus the symbols exercised by actual
imports/fixture execution. It is not exhaustive testing of every optional native
API, plugin, codec or network filesystem.

## Fixture Measurements

One fresh process/container per stage, including validation and retry exercises:

| Stage | Import Seconds | Handler Seconds, Sum | Total Probe Seconds | Process Peak RSS MiB | Observed Peak `/tmp` Allocation |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ingest | 0.972 | 0.084 | 1.204 | 41.59 | 16 KiB |
| Curate | 3.272 | 0.979 | 4.683 | 174.08 | 40 KiB |
| Gold | 0.943 | 0.330 | 1.417 | 191.27 | 80 KiB |

Observed peak logical file sizes in `/tmp` were 2,145, 12,000 and 36,778 bytes,
respectively. Every invocation-specific workspace was removed, and every final
private `/tmp` returned to zero allocated bytes with no new entries.

- Durations use `perf_counter`. Import time includes dependency and handler
  imports, not AWS cold start. Handler sums include actual invocations and local
  simulated S3 I/O. Total includes setup/validation but excludes interpreter and
  container startup, linkage auditing and final JSON encoding/writes.
- Peak RSS is Linux process-lifetime `ru_maxrss` in KiB multiplied by 1024,
  sampled before final report encoding. It includes probe overhead and is not
  handler-only memory, a cgroup peak or AWS's reported maximum memory.
- `/tmp` peaks use repeated `statvfs` allocation measurements and logical-size
  walks, with a requested 5 ms interval. Scheduling/GIL/filesystem races can miss
  short-lived peaks, so these are observed lower bounds, not exact maxima.
- `/evidence` emulates remote S3 and is deliberately excluded from `/tmp`.
  tmpfs uses host memory, unlike provisioned Lambda ephemeral-disk behavior.
- The host CPU/kernel, local disk emulation, thread settings, warm host page
  cache and Docker quotas are not Lambda's vCPU allocation or microVM startup.
  These 12-row fixtures do not size representative daily/full-month workloads,
  establish 900-second safety margins, or replace tasks 8.5/8.6.

## Reproduce Locally

Use the existing optimized artifacts, not a new build or development environment
copy. Keep the original evidence. Select a new work directory for a fresh run:

```bash
ls /tmp/opencode
WORK=/tmp/opencode/nyc311-runtime84-recheck
mkdir "$WORK" "$WORK/evidence"
sha256sum /tmp/opencode/nyc311-lambda-{ingest,curate,gold}-optimized/nyc311-*.zip
for stage in ingest curate gold; do
  uv run --offline --frozen --no-python-downloads python -I -S -B -m zipfile -t "/tmp/opencode/nyc311-lambda-$stage-optimized/nyc311-$stage.zip" || break
  uv run --offline --frozen --no-python-downloads python -I -S -B -m zipfile -e "/tmp/opencode/nyc311-lambda-$stage-optimized/nyc311-$stage.zip" "$WORK/$stage" || break
done
```

Check the hashes against the table above before running. From the repository
root, this helper uses the same isolation settings as the recorded runs:

```bash
IMAGE=public.ecr.aws/lambda/python@sha256:471e480c997b3150ab1a4fb3b862554fa45d54a4f4a5cd21e3115b320197b717
run_check() {
  stage=$1
  script=$2
  shift 2
  docker --host unix:///var/run/docker.sock run --pull=never --rm \
    --network=none --read-only --cap-drop=ALL --security-opt=no-new-privileges \
    --user "$(id -u):$(id -g)" --memory=2g --memory-swap=2g --cpus=2 --pids-limit=256 \
    --tmpfs /tmp:rw,nosuid,nodev,size=536870912,mode=1777 \
    --mount "type=bind,src=$WORK/$stage,dst=/var/task,readonly" \
    --mount "type=bind,src=$PWD/scripts/$script,dst=/checks/$script,readonly" \
    --mount "type=bind,src=$PWD/data/samples/gold-metrics.json,dst=/checks/gold-metrics.json,readonly" \
    --mount "type=bind,src=$WORK/evidence,dst=/evidence" \
    --entrypoint /usr/bin/env "$IMAGE" -i \
    PATH=/var/lang/bin:/usr/bin:/bin HOME=/tmp \
    LD_LIBRARY_PATH=/var/lang/lib:/lib64:/usr/lib64:/var/runtime:/var/runtime/lib:/var/task:/var/task/lib:/opt/lib \
    AWS_EC2_METADATA_DISABLED=true AWS_CONFIG_FILE=/dev/null AWS_SHARED_CREDENTIALS_FILE=/dev/null \
    OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
    /var/lang/bin/python3.12 -I -S -B "/checks/$script" --stage "$stage" "$@"
}
run_check ingest check_lambda_runtime.py --objects /evidence/objects --fixture /checks/gold-metrics.json && \
run_check curate check_lambda_runtime.py --objects /evidence/objects --fixture /checks/gold-metrics.json && \
run_check gold check_lambda_runtime.py --objects /evidence/objects --fixture /checks/gold-metrics.json
for stage in ingest curate gold; do
  run_check "$stage" check_lambda_linkage.py || break
done
```

No image/package/tool installation happens during these runs. Never remove
`--network=none`, inject credentials, or mount the development interpreter to
make a failed check pass. Stop on actual incompatibilities before changing payloads.

## Evidence And Limits

The recorded work directory is `/tmp/opencode/nyc311-runtime84/`. Its extracted
stage directories were verified entry-by-entry against the optimized ZIPs.
`evidence/artifacts.json` records their original hashes/sizes; each
`evidence/<stage>-runtime.json` contains assertions, timings, RSS, tmp samples,
module origins and fixture provenance. Each `<stage>-linkage.json` contains all
ELF hashes and full `ldd` output. `<stage>-results.json` and `evidence/objects/`
preserve the staged fixture outputs. No source fixture or production data was
overwritten, and the original ZIP hashes are unchanged.

The offline project regression suite passes **546 tests**, including five probe
helper tests and 65 packaging tests. Ruff, formatting, `git diff --check`, and
strict OpenSpec validation pass. Those host tests are additional regression
checks; the native evidence is the separate pinned-image execution above.

No AWS account was accessed, no deployment occurred, and no dependency, lockfile,
Nix file, business rule, container deployment architecture, commit or push changed.
The only authorized download was the public official test image.

The representative local task 8.5 baseline is now available in
[Lambda Benchmarks](lambda-benchmarks.md), without definitive sizing changes.
Later verified under explicit authorization: production resource configuration and
headroom (8.6), managed Lambda cold starts, CPU/memory/tmp behavior, public-source
connectivity, IAM/S3 permissions and persistence, retries under AWS failures, Step
Functions orchestration, catalog publication and Athena verification (see
[Test Evidence](test-evidence.md)). Local success never grants that authorization;
it is required for every run. The review boundary is the completed baseline plus
the authorized deployment record.
