# Lambda ZIP Assembly

This builder covers ZIP assembly for OpenSpec task **8.3 only** for
`build-nyc311-september-2026-pipeline`. ZIP assembly targets Python **3.12**,
Linux **x86_64**, and Amazon Linux 2023's glibc baseline via uv's
`x86_64-manylinux_2_34` wheel selection. Matching wheel tags is not proof of native
ABI compatibility. No AWS calls, source HTTP requests, deployment, layers,
containers, or environment copying are part of this builder.

## Offline Commands

Run from the repository root in the existing development environment. The output
parent must exist and each output directory must be new. For this workspace,
`/tmp/opencode` is the verified isolated parent. Outputs must be kept outside the
source tree and outside `.venv` or the Nix environment.

```bash
ls /tmp/opencode
uv run --offline --frozen --no-python-downloads python scripts/build_lambda_zip.py ingest --output /tmp/opencode/nyc311-lambda-ingest-optimized
uv run --offline --frozen --no-python-downloads python scripts/build_lambda_zip.py curate --output /tmp/opencode/nyc311-lambda-curate-optimized
uv run --offline --frozen --no-python-downloads python scripts/build_lambda_zip.py gold --output /tmp/opencode/nyc311-lambda-gold-optimized
uv run --offline --frozen --no-python-downloads pytest tests/test_lambda_packaging.py
```

Each stage directory contains `nyc311-<stage>.zip`, `requirements.txt`, and
`inventory.json`. Dependencies are installed into a fresh temporary target below
that directory, with copy link mode, then the temporary target is removed even if
installation fails. An existing output is never replaced. On an offline cache
miss the build fails; it does not fetch a wheel, build an sdist, modify dependencies
or the lock, or fall back to `.venv`. Preserve failed-build evidence and use a new
output path for a later attempt.

The script exports the frozen lock with no default, dev, or notebooks groups,
no project distribution, and stage-specific pruning. It then uses:

```text
uv pip install --offline --no-python-downloads --python-version 3.12
  --python-platform x86_64-manylinux_2_34 --only-binary :all: --require-hashes
  --link-mode copy --no-compile-bytecode --target <target>
  --requirements <output>/requirements.txt
```

`uv pip install` has no frozen-lock option: its input is the hash-bearing export
produced by `uv export --offline --frozen`. No implicit runtime SDK is used.

## Stage Isolation

The explicit module allowlists are in `scripts/build_lambda_zip.py::STAGES`.
Every ZIP has `nyc311/` at its root. Shared modules are `__init__`, `config`,
`manifests`, `storage`, `s3_store`, `lambda_common`, `source_contract`, and `bronze`.
The shared Bronze loader does not import HTTPX at runtime. The Gold Silver reader
does not import the Pandas transformation at module load; its lazy publication
path is not used by Gold. Local CLI/pipeline modules and the builder are excluded.

| Stage | Handler | Additional Modules | Runtime Roots | Pruned Roots |
| --- | --- | --- | --- | --- |
| ingest | `nyc311.lambda_ingest.handler` | `source`, `lambda_ingest` | Boto3, HTTPX | DuckDB, Pandas, PyArrow |
| curate | `nyc311.lambda_curate.handler` | `silver_contract`, `silver`, `silver_store`, `lambda_curate` | Boto3, Pandas, PyArrow | HTTPX, DuckDB |
| gold | `nyc311.lambda_gold.handler` | `silver_contract`, `silver_store`, `gold`, `gold_store`, `lambda_gold` | Boto3, DuckDB, PyArrow | HTTPX, Pandas |

Gold contains exactly the four product SQL inputs at root `sql/`, beside root
`nyc311/`, matching the handler's explicit SQL directory regardless of its working
directory. Wheel libraries, SDK service models, certificates, package metadata,
and licenses are retained. Wheel `tests`/`test` directories, notebooks, bytecode,
caches, and console-script `bin` files are omitted. Host-dependent installed
`RECORD`, `INSTALLER`, `REQUESTED`, and `direct_url.json` files are omitted; the
sidecar entry manifest describes the final payload instead of claiming the wheel
RECORD still describes it. No dev or notebook distribution is allowed by the
inventory check. Symlinks and payload references to `/nix/store/` are rejected.
The limited optimization below also excludes narrowly selected nonruntime
compilation inputs, root Pytest configuration, and two static prose examples.
The installed distributions are not modified; these are ZIP selection rules.

## Exact Inventory

All stages include this full SDK closure, consistently pinned rather than relying
on the SDK provided by Lambda:

| Distribution | Locked Version |
| --- | --- |
| boto3 | 1.43.113 |
| botocore | 1.43.113 |
| jmespath | 1.1.0 |
| python-dateutil | 2.9.0.post0 |
| s3transfer | 0.19.2 |
| six | 1.17.0 |
| urllib3 | 2.8.0 |

Stage-specific transitive additions:

| Stage | Additional Locked Distributions |
| --- | --- |
| ingest | anyio 4.15.1, certifi 2026.7.22, h11 0.16.0, httpcore 1.0.9, httpx 0.28.1, idna 3.20, typing-extensions 4.16.0 |
| curate | numpy 2.5.4, pandas 3.0.6, pyarrow 26.0.0 |
| gold | duckdb 1.5.6, pyarrow 26.0.0 |

`inventory.json` is authoritative for each actual build: it checks the installed
distribution names and exact versions against `uv.lock`, rejects an unexpected or
missing transitive distribution, and identifies each selected locked wheel from
its installed WHEEL tags. Every wheel entry records the lock's URL, SHA-256,
size, upload time, tags, and dependency edges including platform markers.
The wheel SHA-256 is lock provenance enforced by uv's hash-required installation,
not a separate rehash of a cached original wheel archive. Payload bytes are
independently hashed in the ZIP entry manifest.

## Timezone Resource

On Linux, the locked Pandas dependency graph does **not** include `tzdata` (its
marker selects Windows/Emscripten). Curation currently interprets floating dates
using `America/New_York` through Pandas/ZoneInfo. Depending on the development
machine's system timezone files would make the package incomplete.

The already-required `python-dateutil==2.9.0.post0` wheel contains
`dateutil/zoneinfo/dateutil-zoneinfo.tar.gz`. The builder reads only its named
`America/New_York` TZif member and `METADATA`, without filesystem tar extraction,
and places that member at `timezone/America/New_York`. The inventory records the
archive SHA-256, original IANA metadata (`tzversion=2024a`), and TZif entry hash.
This is a reproducible existing-wheel resource, not a new dependency or a host
timezone copy. It is an older pinned timezone database, not a claim to contain
the latest global timezone rules.

**Required curation runtime configuration:** `PYTHONTZPATH=/var/task/timezone`.
This selects the packaged resource instead of system paths. In a future local
compatible-runtime check, set it to the absolute extracted ZIP timezone directory
before starting Python. Ingestion and Gold do not interpret NYC-local instants
and need no added TZif member. No builder import or test claims timezone runtime
behavior has passed; DST and timestamp transformation checks remain task 8.4.

## Reproducible Inputs

Preserve the three outputs together. `inventory.json` records SHA-256 inputs for
`pyproject.toml`, `uv.lock`, and the builder; exact source and SQL hashes are in
its ordered `entries`. Each entry records path, origin, SHA-256, fixed mode
`0644`, and timestamp `1980-01-01 00:00:00`. ZIPs have sorted POSIX paths, no
directory entries or wall-clock timestamps, Unix regular-file modes, and Deflate
compression. Source checkout mtimes, output paths, and installer script shebangs
do not affect payload bytes.

The inventory also captures the hash of the exported requirements, normalized
export/install commands, uv version, Python build string, zlib runtime version,
and ZIP SHA-256. Reproducing compressed bytes requires the same source bytes,
locked wheel selection, builder, uv, Python, and zlib. Cached wheel availability is
an offline prerequisite. Changing any source/lock/tool input requires new evidence;
an uncommitted checkout is supported because source hashes, not Git status, define
the packaged input. The JSON excludes absolute build paths and build time.

Tests mock export/installation, inspect repeatable ZIP bytes and manifests, reject
unlocked versions, and check the real lock's full stage dependency closures and
top-level application import graphs. They do not load native libraries from a ZIP.
Optimized inventories also contain `omitted_nonruntime`, a sorted audit of each
new exclusion's path, category, original byte size, and SHA-256. This distinguishes
intentional pruning from a missing installation file and makes savings auditable.

## Baseline Evidence

Task 8.3 assembly initially completed with Curate over the size limit. The
following original artifacts are preserved as the optimization baseline, not
the current recommended payloads. They used the pre-optimization builder. Curate and
Gold were built offline after the operator explicitly authorized retrieval from
PyPI of only missing, locked runtime wheels. No dependencies, lockfiles, Nix
configuration, architecture, or stage payload rules were changed. No source
build, global installation, AWS access, deployment, commit, or push occurred.

| Output Directory Under `/tmp/opencode` | Actual Result |
| --- | --- |
| `nyc311-lambda-ingest-final` | Built `nyc311-ingest.zip`, `inventory.json`, and `requirements.txt` from the final handler code; 14 locked distributions and 2,358 ZIP entries verified |
| `nyc311-lambda-ingest-final-repeat` | A second actual offline ingestion build; ZIP SHA-256, full inventory JSON, and ordered entry manifest identical to the first |
| `nyc311-lambda-curate-verified` | Built all three sidecars/artifacts; 10 locked distributions and 3,694 ZIP entries verified |
| `nyc311-lambda-curate-verified-repeat` | ZIP, full inventory JSON, and requirements byte-identical to the first Curate build |
| `nyc311-lambda-gold-verified` | Built all three sidecars/artifacts; 9 locked distributions and 2,921 ZIP entries verified |
| `nyc311-lambda-gold-verified-repeat` | ZIP, full inventory JSON, and requirements byte-identical to the first Gold build |

Each original build was repeated with `-repeat` appended to its output directory.
`cmp` verified the ZIP, inventory, and requirements for each baseline pair. The
baseline ZIP SHA-256 values are:

```text
ingest  bc78052cfa0f6a1e3602a6ab5dcd66043db7c14d9a8d444982c05e3cca78313a
curate  43278743ebb66680747f2f6621cf4c7fa44b0c7d6bc2ab72aadfcc01d50b9f83
gold    9a3a013ef0dc8b8ae4f7561e9753541572ff3a99a9ca02064e23f53066468675
```

Every actual entry in all three ZIPs was checked against its manifest SHA-256,
CRC, decompressed length, ordered canonical path, fixed timestamp, regular-file
mode, exclusion policy, and absence of Nix store references. Actual packaged
`METADATA` dependency closures, version constraints, and target-specific markers
also reconcile to the lock and inventory with no missing active dependency.
Packaged source/SQL and builder/project/lock input hashes matched the checkout
at the baseline build time. All four SQL members and Curate's exact dateutil-derived TZif
member were verified as bytes, without executing them or importing the payload.

Only native wheel tags `cp312-cp312` and x86_64 manylinux baselines at most 2.34
were selected: NumPy 2.27/2.28, Pandas 2.24/2.28, PyArrow 2.28, and DuckDB
2.26/2.28. This is metadata/file-tag verification, not native ABI proof.

### Authorized Cache Preparation

The first offline attempts and their requirements exports were preserved, not
overwritten. NumPy and DuckDB were additionally downloaded from their exact
locked `files.pythonhosted.org` URLs into `/tmp/opencode/nyc311-locked-wheels/`;
independent `sha256sum` results matched `uv.lock`:

```text
numpy  fbde6962867ee75b48b0ee29b2b9372ec5d617799dbaf38e82dc0596f2f7738a
duckdb bc9619ed7d4ffa117b5155d84b44794366bb6635178d78ed5e13a6024845c757
```

Four exact native requirements (NumPy 2.5.4, DuckDB 1.5.6, Pandas 3.0.6, and
PyArrow 26.0.0) with their selected locked hashes were prepared in
`/tmp/opencode/nyc311-missing-runtime-wheels.txt`. uv cache preparation used only
the selected runtime requirements and hash-bearing frozen stage exports;
already-cached distributions were reused. Downloads/resolution were restricted
to PyPI, with keyring disabled, compatible wheel selection, and no source builds.
All installation destinations were isolated temporary targets, not global or
project environments. For example, the complete Curate cache preparation used:

```bash
uv pip install --no-config --default-index https://pypi.org/simple \
  --keyring-provider disabled --no-python-downloads \
  --python-version 3.12 --python-platform x86_64-manylinux_2_34 \
  --only-binary :all: --require-hashes --link-mode copy --no-compile-bytecode \
  --target /tmp/opencode/nyc311-curate-locked-cache-warm \
  --requirements /tmp/opencode/nyc311-lambda-curate-authorized/requirements.txt
```

Gold used the identical flags with target
`/tmp/opencode/nyc311-gold-locked-cache-warm` and requirements
`/tmp/opencode/nyc311-lambda-gold-authorized/requirements.txt`. This authorized
cache preparation was separate from the unchanged, offline builder. The uv
cache and cache-preparation targets are not deployment payloads; the builder
performs a fresh isolated wheel installation for each ZIP. Original failed
attempt directories remain diagnostic history. Future
cache misses still stop the offline builder; this permission is not a general
authorization to update dependencies or access external services.

### Requested Sizes

These measurements were explicitly requested with task 8.3. Compressed bytes
are the ZIP file size; uncompressed payload bytes are
`SUM(ZipInfo.file_size)`, not filesystem allocation or the size of sidecars.
MiB means 1,048,576 bytes. No layers are included.

| Stage | Compressed Bytes | Compressed MiB | Uncompressed Bytes | Uncompressed MiB |
| --- | ---: | ---: | ---: | ---: |
| Ingest | 17,189,265 | 16.39 | 24,709,754 | 23.57 |
| Curate | 92,800,709 | 88.50 | 263,439,875 | 251.24 |
| Gold | 91,534,495 | 87.29 | 249,352,327 | 237.80 |

**Baseline size blocker:** The original Curate ZIP exceeds 250 MiB
(262,144,000 bytes) by **1,295,875 bytes** and is not deployable as-is. Work
stopped for review. The operator subsequently authorized only the limited
builder optimization documented below, not task 8.4 or an architecture change.

The baseline existing-environment offline suite passed **491 tests**. This includes
15 packaging tests with mocked installations; the independent actual-ZIP audit
above checks all three real artifacts. Neither constitutes a Lambda runtime
execution test.

## Limited Optimization

The operator authorized ZIP-only exclusions before task 8.4. Business logic,
installed dependency versions, `pyproject.toml`, `uv.lock`, native binaries,
architecture, and SDK capabilities remain unchanged. All new builds are offline;
no additional downloads, AWS access, deployments, commits, or pushes occurred.

### Size Attribution

The dominant distributions below include their package trees, metadata,
licenses, and associated native libraries. Sizes are uncompressed bytes.
DuckDB includes `_duckdb`, `_duckdb-stubs`, and `adbc_driver_duckdb`, not just
`duckdb/`. Complete totals for every distribution and directory are in the audit
report referenced below; application source, SQL, and TZif are attributed separately.

| Distribution | Stage | Before Bytes | After Bytes |
| --- | --- | ---: | ---: |
| PyArrow 26.0.0 | Curate, Gold | 165,556,094 | 158,205,859 |
| DuckDB 1.5.6 | Gold | 61,038,828 | 61,038,828 |
| NumPy 2.5.4 | Curate | 49,431,531 | 49,422,758 |
| Pandas 3.0.6 | Curate | 25,708,725 | 25,655,275 |
| Botocore 1.43.113 | All | 20,361,637 | 20,361,637 |
| Boto3 1.43.113 | All | 985,563 | 978,645 |

Important recursive directory totals follow. Parent/child rows overlap; do not
sum them as independent distribution totals.

| Directory | Stage | Before Bytes | After Bytes |
| --- | --- | ---: | ---: |
| `pyarrow/` | Curate, Gold | 165,439,647 | 158,089,412 |
| `pyarrow/include/` | Curate, Gold | 4,685,526 | 0 |
| `pyarrow/src/` | Curate, Gold | 619,516 | 2,582 |
| `numpy/` | Curate | 21,868,561 | 21,859,788 |
| `numpy.libs/` | Curate | 27,463,459 | 27,463,459 |
| `pandas/` | Curate | 25,576,867 | 25,523,417 |
| `botocore/data/` | All | 18,642,139 | 18,642,139 |

Native libraries dominate the remaining Arrow, NumPy, Pandas, and DuckDB sizes.
They were not stripped, deduplicated, or removed to increase the margin.

### Safe Selection Rules

`scripts/build_lambda_zip.py::nonruntime_category` applies only these rules:

| Category | Exact Scope | Safety Evidence |
| --- | --- | --- |
| Arrow Cython inputs | Root `pyarrow/*.pyx`, `*.pxd`, `*.pxi`; direct `pyarrow/includes/*.pxd` | Arrow imports its prebuilt `pyarrow.lib` extension; no stage compiles or JIT-loads these inputs |
| Arrow C/C++ inputs | Files ending `.c`, `.cc`, `.cpp`, `.h`, `.hpp` beneath `pyarrow/src/` | Compile-time sources for existing native extensions; CMake/template files remain |
| Arrow headers | `.h` and `.hpp` beneath `pyarrow/include/` | `pyarrow.get_include()` supports building downstream extensions, which the stages do not do |
| Root Pytest configuration | Only `numpy/conftest.py`, `pandas/conftest.py`, `pyarrow/conftest.py` | Their contents define dependency pytest/doctest configuration; retained production Python has no conftest imports |
| Prose examples | Only `boto3/examples/cloudfront.rst`, `boto3/examples/s3.rst` | Static text, not imported SDK code or model-loader resources |

Legal basenames starting with `LICENSE`, `NOTICE`, or `COPYING`, and files under
`license`/`licenses` directories, are protected from the new rules. Both Arrow
standalone legal files remain unchanged, including bundled third-party notices.
Removing compile inputs sacrifices downstream extension-building and some source
debugging context, not current pipeline behavior. Revisit these rules if future
stages compile extensions or add JIT support.

Do **not** broadly remove `testing`, `_testing`, SDK `docs`, generated Python
modules, or SDK `examples-1.json`: retained runtime code imports/loads them.
NumPy testing helpers and `PytestTester`, Pandas testing helpers, AnyIO testing
support, all SDK models (including Glue/credential-related services), certificates,
dateutil zoneinfo, Curate's TZif, metadata, licenses, and Gold SQL remain.
The baseline already excluded test directories and bytecode/caches; additional
savings from those categories are zero.

### Savings By Category

These are exact additional uncompressed bytes saved, independently checked
against the original ZIP members and `omitted_nonruntime` manifests.

| Category | Ingest Bytes | Curate Bytes | Gold Bytes |
| --- | ---: | ---: | ---: |
| Arrow Cython inputs | 0 | 2,037,896 | 2,037,896 |
| Arrow C/C++ inputs | 0 | 616,934 | 616,934 |
| Arrow headers | 0 | 4,685,526 | 4,685,526 |
| Root Pytest configuration | 0 | 72,102 | 9,879 |
| Prose examples | 6,918 | 6,918 | 6,918 |
| **Total** | **6,918** | **7,419,376** | **7,357,153** |

### Final Artifacts

The current commands at the top build these optimized artifacts. Each directory
contains its ZIP, inventory, and requirements. Each build was repeated into a
directory with `-repeat` appended; all three files in every pair are byte-identical.

| Stage | Directory Under `/tmp/opencode` | Distributions | ZIP Entries |
| --- | --- | ---: | ---: |
| Ingest | `nyc311-lambda-ingest-optimized` | 14 | 2,356 |
| Curate | `nyc311-lambda-curate-optimized` | 10 | 3,105 |
| Gold | `nyc311-lambda-gold-optimized` | 9 | 2,334 |

| Stage | Compressed Bytes | Compressed MiB | Uncompressed Bytes | Uncompressed MiB | Margin Below 250 MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ingest | 17,186,734 | 16.39 | 24,702,836 | 23.56 | 226.44 MiB |
| Curate | 90,947,569 | 86.73 | 256,020,499 | 244.16 | 5.84 MiB |
| Gold | 89,697,040 | 85.54 | 241,995,174 | 230.78 | 19.22 MiB |

Curate now has **6,123,501 bytes** of headroom (2.34% of 250 MiB), and Gold has
**20,148,826 bytes**. This is ZIP-only arithmetic with no layers. Curate's margin
is modest, not broad growth capacity: any changed dependencies, SDK models,
added code, or future layers require a fresh aggregate check. No deployment
readiness claim or native-runtime compatibility verdict follows from these sizes.

```text
ingest  9072576c33b46728c4ce51b23e1d19d9509992ba3f20fbf617971a01397b9157
curate  73902edcbbbfb76ed20487b75c4664c956d4ad2ff92f6ae4ad1e8196a1efee6e
gold    109fb2a373b96831b254e04d1fa3916d7abb5d62f2744d53a635c730a29b377c
```

### Verification

All **7,795 retained entries** pass SHA-256, CRC, length, path, mode, timestamp,
exclusion, and provenance checks. Baseline-minus-optimized paths exactly equal
the omission manifests: 2 Ingest, 589 Curate, and 587 Gold files. There are no
added payload members, and every retained file is byte-identical to baseline.
Requirements and installed distribution inventories are unchanged; actual wheel
metadata dependency closures and tags still match the locked target.

In particular, Curate retains all **107 native files** (105 ELF shared objects and
two `.a` archives), and Gold retains all **39 native files**. Their complete path
and SHA-256 sets are unchanged, as are the legal files, `METADATA`/`WHEEL`,
certificates, SDK resources, source/SQL, and timezone data. No native payload was
imported or executed for this audit.

The full offline suite passes **541 tests**, including **65 packaging tests**.
The tests cover narrow exclusions, license/native/runtime preservation,
omission hashes/categories, and repeatable ZIPs/inventories. The independent
real-artifact audit is recorded at
`/tmp/opencode/nyc311-optimized-artifact-audit-verified.json`, including complete
distribution/directory accounting and all retained/omitted/protected path hashes.
Baseline analysis and safety excerpts are preserved in
`/tmp/opencode/nyc311_zip83_readonly_analysis.json` and
`/tmp/opencode/nyc311_zip83_static_evidence.txt`.

## Remaining Gates

The bounded 8.6 candidate rebuilds and their unresolved native spill-safety gate
are recorded in [Deployment Readiness](deployment-readiness.md). Earlier ZIP
assembly/compatibility evidence below does not override that blocker.

The task 8.4 local compatibility and aggregate-size gate passed in the explicitly
authorized official Python 3.12 x86_64 test image. Imports, handler fixtures,
timezone, Parquet and shared-library evidence are recorded in
[Lambda Compatibility](lambda-compatibility.md). The existing optimized ZIPs
were tested unchanged, with no image SDK or Nix dependency substitution.

These are locally compatibility-checked artifacts, **not deployment-ready
artifacts**. Task 8.5's representative local workload baseline is recorded in
[Lambda Benchmarks](lambda-benchmarks.md). Task 8.6 must still select and verify
production resource headroom. Deployment remains blocked until every later gate passes
and AWS operations receive explicit
authorization. No task 8.6 or later work, container deployment fallback, or
additional-service change was performed; stop here for review.
