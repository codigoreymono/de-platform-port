"""Offline task 8.5 replay/opt-in production-profile 8.6 probe, not AWS certification.

Caller: pinned official CP312 AL2023 x86_64 image, --network none, 2 GiB/2 CPU,
read-only stage ZIP at /var/task, this script and check_lambda_runtime.py at
/checks, cloned originals at /inputs, fresh writable /evidence, private 512 MiB
/tmp tmpfs. Run /var/lang/bin/python3.12 -I -S -B /checks/benchmark_lambda.py.
--profiled uses host/ZIP SHA-verified runtime profiles instead of 2 GiB/2 CPU.
It observes four native Gold business connections with one settings SELECT each:
exact DUCKDB_LIMITS config, 512 MiB, one thread, empty temp_directory (no spill).
It never replaces policy/config, SETs settings, or proxies connections. Its
simulated 900-second Lambda clock includes imports/setup from main() entry but
excludes interpreter startup. Memory/tmp comparisons retain 20% headroom and
stage/handler comparisons use <=720 seconds; missing memory metrics stay unknown.
No extraction, downloads, SQL overrides or tuning here. Unprofiled 8.5 replay
retains its original defaults and does not require current runtime metadata.
"""

import argparse
import gzip
import importlib
import json
import math
import os
import platform
import re
import resource
import runpy
import socket
import sys
import sysconfig
import traceback
from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from hashlib import sha256
from pathlib import Path, PurePosixPath
from time import perf_counter

TASK = Path("/var/task")
INPUTS = Path("/inputs")
ORIGINAL_RUN = "september-2026-001"
IMAGE_DIGEST = "sha256:471e480c997b3150ab1a4fb3b862554fa45d54a4f4a5cd21e3115b320197b717"
DAYS = [f"2026-09-{number:02d}" for number in range(1, 31)]


def require(condition, label):
    if not condition:
        error = AssertionError(label)
        error.benchmark_check = label
        raise error


def safe_path(root, key):
    relative = PurePosixPath(key)
    if (
        not key
        or relative.is_absolute()
        or ".." in relative.parts
        or "\\" in key
        or relative.as_posix() != key
    ):
        raise ValueError("Noncanonical object reference")
    path = root / key
    if path.resolve() != path or not path.is_relative_to(root):
        raise ValueError("Object reference escapes root")
    return path


def read_json(root, key):
    return json.loads(safe_path(root, key).read_bytes())


def file_digest(path):
    digest = sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def block_external(*args, **kwargs):
    raise RuntimeError("Network, SDK clients and credential sessions are disabled")


def guard_network():
    socket.getaddrinfo = socket.gethostbyname = socket.gethostbyname_ex = block_external
    socket.gethostbyaddr = socket.getnameinfo = socket.create_connection = (
        block_external
    )
    socket.socket.connect = socket.socket.connect_ex = block_external
    socket.socket.sendto = socket.socket.sendmsg = block_external


def guard_sdk():
    import boto3
    import botocore.session

    # Guard captured class references as well as top-level construction helpers.
    boto3.session.Session.client = boto3.session.Session.resource = block_external
    boto3.session.Session.__init__ = boto3.session.Session.get_credentials = (
        block_external
    )
    boto3.client = boto3.resource = boto3.Session = boto3.session.Session = (
        block_external
    )
    botocore.session.Session.create_client = (
        botocore.session.Session.get_credentials
    ) = block_external
    botocore.session.Session.__init__ = botocore.session.get_session = block_external


def module_origins():
    origins, violations = {}, {}
    stdlib = Path(sysconfig.get_path("stdlib")).resolve()
    for name, module in list(sys.modules.items()):
        if module is None:
            continue
        spec = getattr(module, "__spec__", None)
        origin = getattr(spec, "origin", None) or getattr(module, "__file__", None)
        locations = list(getattr(module, "__path__", ()))
        paths = [origin] if origin not in (None, "built-in", "frozen") else []
        for location in paths + locations:
            path = Path(location).resolve()
            if path.is_relative_to(TASK):
                origins[name] = {"origin": origin, "search_locations": locations}
            elif (
                name in {"__main__", "__mp_main__"}
                and module is sys.modules.get("__main__")
                and path == Path("/checks/benchmark_lambda.py")
            ):
                continue
            elif (
                (
                    name.split(".")[0] in sys.stdlib_module_names
                    or name.startswith("_sysconfigdata_")
                )
                and path.is_relative_to(stdlib)
                and not {"site-packages", "dist-packages"}.intersection(path.parts)
            ):
                continue
            else:
                violations[name] = str(path)
    return {"payload_modules": origins, "violations": violations}


def cgroup_snapshot():
    result = {}
    for name in (
        "memory.current",
        "memory.peak",
        "memory.max",
        "memory.events",
        "cpu.stat",
        "cpu.max",
    ):
        try:
            result[name] = (Path("/sys/fs/cgroup") / name).read_text().strip()
        except OSError as error:
            result[name] = {"unavailable": type(error).__name__}
    return result


def resources():
    return {
        "process_highwater_rss_bytes": resource.getrusage(
            resource.RUSAGE_SELF
        ).ru_maxrss
        * 1024,
        "cgroup": cgroup_snapshot(),
    }


def profiled_settings(stage, report):
    """Check immutable payload bytes against the caller's host-verified fingerprint."""
    runtime = read_json(INPUTS, "manifest.json")["runtime_profile"]
    path = TASK / "nyc311/lambda_runtime.py"
    require(file_digest(path) == runtime["module_sha256"], "ZIP/host runtime SHA-256")
    namespace = runpy.run_path(str(path))
    constants = {key: value for key, value in namespace.items() if key.isupper()}
    require(constants == runtime["constants"], "ZIP/host runtime constants")
    profile = constants["PROFILES"][stage]
    report["runtime_profile"] = runtime
    report["stage_profile"] = profile
    memory, tmp = (
        profile["memory_mib"] * 1024**2,
        profile["temporary_storage_mib"] * 1024**2,
    )
    report["local_budgets"].update(
        profiled=True,
        memory_bytes=memory,
        cpu=profile["benchmark_cpus"],
        tmp_bytes=tmp,
        memory_80pct_bytes=0.8 * memory,
        tmp_80pct_bytes=0.8 * tmp,
        handler_seconds=720,
        application_seconds=720,
        work_seconds=660,
        failure_publication_seconds=60,
        lambda_reserve_seconds=180,
        lambda_timeout_seconds=profile["timeout_seconds"],
    )
    return profile


def verify_profile_cgroup(profile, counters):
    require(
        counters["memory.max"] == str(profile["memory_mib"] * 1024**2),
        "Profile --memory matches actual cgroup memory.max",
    )
    quota, period = map(int, counters["cpu.max"].split())
    require(
        quota / period == profile["benchmark_cpus"], "Profile actual cgroup CPU quota"
    )


class LambdaContext:
    def __init__(self, started, timeout_seconds):
        self.deadline = started + timeout_seconds

    def get_remaining_time_in_millis(self):
        return max(0, int((self.deadline - perf_counter()) * 1000))


def setting_bytes(value):
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*(B|KiB|MiB|GiB)", str(value))
    require(match is not None, "DuckDB live byte setting has known units")
    return (
        float(match[1])
        * {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}[match[2]]
    )


@contextmanager
def observe_duckdb(report):
    """Return native connections unchanged; query settings before business SQL."""
    from nyc311 import gold

    connect = gold.duckdb.connect
    records = []
    report["duckdb_settings"] = {
        "connections": records,
        "measurement": "One SELECT current_setting query per business connection inside handler timing; no SET, extra connection, SQL override or connection proxy.",
    }
    limits = report["runtime_profile"]["constants"]["DUCKDB_LIMITS"]

    def observed_connect(*args, **kwargs):
        connection = connect(*args, **kwargs)
        try:
            config = dict(args[2] if len(args) > 2 else kwargs.get("config", {}))
            row = connection.execute(
                "SELECT current_setting('memory_limit'), "
                "current_setting('threads'), current_setting('temp_directory')"
            ).fetchone()
            effective = dict(
                zip(
                    (
                        "memory_limit",
                        "threads",
                        "temp_directory",
                    ),
                    row,
                )
            )
            records.append({"requested_config": config, "effective": effective})
            require(
                config == limits,
                "Gold requests exactly the three production DuckDB resource settings",
            )
            require(
                setting_bytes(effective["memory_limit"]) == 512 * 1024**2
                and effective["threads"] == 1
                and effective["temp_directory"] == "",
                "Gold live DuckDB no-spill resource limits",
            )
        except Exception:
            connection.close()
            raise
        return connection

    gold.duckdb.connect = observed_connect
    try:
        yield
        require(len(records) == 4, "Exactly four observed Gold business connections")
    finally:
        gold.duckdb.connect = connect


def compare_profile_budgets(report):
    """Require both memory metrics for a full comparison, even to report failure.

    RSS must be a nonnegative native int (not bool); cgroup peak must be a
    nonnegative decimal string. Missing/invalid metrics are unknown, never zero.
    With either unknown, memory_peak_bytes and memory_20pct_headroom are None;
    observed_memory_peak_bytes is only a lower bound, even if over budget.
    """
    budgets = report["local_budgets"]
    snapshot = report.get("resources_after_handler", {})
    snapshot = snapshot if isinstance(snapshot, dict) else {}
    rss = snapshot.get("process_highwater_rss_bytes")
    rss = rss if type(rss) is int and rss >= 0 else None
    cgroup = snapshot.get("cgroup", {})
    peak = cgroup.get("memory.peak") if isinstance(cgroup, dict) else None
    try:
        cgroup_peak = int(peak) if isinstance(peak, str) and peak.isdecimal() else None
    except ValueError:
        cgroup_peak = None
    memory_complete = rss is not None and cgroup_peak is not None
    observed_peak = max(
        (value for value in (rss, cgroup_peak) if value is not None), default=None
    )
    memory_peak = observed_peak if memory_complete else None
    tmp_peak = report["tmp_handler_peaks"]["allocated_bytes"]
    budgets["comparisons"] = {
        "stage_elapsed_seconds": report["elapsed"][
            "stage_seconds_including_imports_and_setup"
        ],
        "stage_within_720_seconds": report["elapsed"][
            "stage_seconds_including_imports_and_setup"
        ]
        <= 720,
        "handler_within_720_seconds": report["elapsed"]["handler_seconds"] <= 720,
        "process_highwater_rss_bytes": rss,
        "process_highwater_rss_available": rss is not None,
        "cgroup_memory_peak_bytes": cgroup_peak,
        "cgroup_memory_peak_available": cgroup_peak is not None,
        "memory_metrics_complete": memory_complete,
        "observed_memory_peak_bytes": observed_peak,
        "memory_peak_bytes": memory_peak,
        "memory_20pct_headroom": memory_peak <= budgets["memory_80pct_bytes"]
        if memory_peak is not None
        else None,
        "tmp_observed_peak_bytes": tmp_peak,
        "tmp_observed_20pct_headroom": tmp_peak <= budgets["tmp_80pct_bytes"],
        "tmp_is_sampled_lower_bound": True,
    }


def metered_s3(helper, root, client_error):
    class MeteredS3(helper["DiskS3"]):
        def __init__(self):
            super().__init__(root, client_error)
            self.io = {"read_bytes": 0, "write_bytes": 0}

        def get_object(self, **kwargs):
            result = super().get_object(**kwargs)
            self.io["read_bytes"] += result["ContentLength"]
            return result

        def put_object(self, **kwargs):
            result = super().put_object(**kwargs)
            self.io["write_bytes"] += len(kwargs["Body"])
            return result

        def snapshot(self):
            return {
                **self.io,
                "calls": dict(Counter(op for op, _ in self.calls)),
                "call_log": list(self.calls),
            }

    return MeteredS3()


class OriginalPages:
    """One original gzip file per request, no record projection or reserialization.

    The service read, decompression and digest overhead is inside handler timing.
    Original metadata supplies cursors/counts; business code validates actual JSON.
    """

    def __init__(self, root, manifest):
        self.root, self.manifest = root, manifest
        self.index = self.counts = self.clock_calls = 0
        self.observed = None
        self.stats = {
            "count_requests": 0,
            "page_requests": 0,
            "response_bytes": 0,
            "page_response_bytes": 0,
            "gzip_file_read_bytes": 0,
            "source_counts": [],
        }

    def transport(self):
        import httpx

        from nyc311.source_contract import ENDPOINT, SELECTED_FIELDS

        manifest = self.manifest
        where = (
            f"created_date >= '{manifest['start_date']}T00:00:00' "
            f"AND created_date < '{manifest['end_date']}T00:00:00'"
        )

        def respond(request):
            params = dict(request.url.params)
            require(
                request.method == "GET" and str(request.url).split("?")[0] == ENDPOINT,
                "Unexpected source endpoint or method",
            )
            require(
                len(request.url.params.multi_items()) == len(params),
                "Duplicate query parameters",
            )
            if params.get("$select") == "count(*) as n":
                require(
                    params == {"$select": "count(*) as n", "$where": where},
                    "Count query",
                )
                require(
                    (self.counts == 0 and self.index == 0)
                    or (self.counts == 1 and self.index == len(manifest["pages"])),
                    "Count sequence",
                )
                position = (
                    "source_count_before" if self.counts == 0 else "source_count_after"
                )
                content = json.dumps([{"n": str(manifest[position])}]).encode()
                self.counts += 1
                self.stats["count_requests"] += 1
                self.stats["source_counts"].append(
                    {"position": position, "count": manifest[position]}
                )
            else:
                require(
                    self.counts == 1 and self.index < len(manifest["pages"]),
                    "Fresh page sequence",
                )
                require(
                    self.clock_calls == self.index, "Previous page observation consumed"
                )
                page_where = where
                if self.index:
                    cursor = manifest["pages"][self.index - 1]["last_cursor"].replace(
                        "'", "''"
                    )
                    page_where += f" AND unique_key > '{cursor}'"
                require(
                    params
                    == {
                        "$select": ",".join(SELECTED_FIELDS),
                        "$where": page_where,
                        "$order": "unique_key ASC",
                        "$limit": str(manifest["page_size"]),
                    },
                    "Original page selection, day, text cursor, order and limit",
                )
                page = manifest["pages"][self.index]
                expected = (
                    f"bronze/run={ORIGINAL_RUN}/created_date={manifest['start_date']}/"
                    f"page-{self.index + 1:04d}.json.gz"
                )
                require(page["key"] == expected, "Original page key")
                compressed = safe_path(self.root, page["key"]).read_bytes()
                content = gzip.decompress(compressed)
                require(
                    sha256(content).hexdigest() == page["sha256"],
                    "Original page SHA-256",
                )
                self.observed = page["observed_at_utc"]
                self.index += 1
                self.stats["page_requests"] += 1
                self.stats["gzip_file_read_bytes"] += len(compressed)
                self.stats["page_response_bytes"] += len(content)
            self.stats["response_bytes"] += len(content)
            return httpx.Response(200, content=content)

        return httpx.MockTransport(respond)

    def clock(self):
        replay = self

        class ObservationClock(datetime):
            @classmethod
            def now(cls, tz=None):
                require(
                    replay.observed is not None and replay.clock_calls < replay.index,
                    "Observation clock is only for successful original pages",
                )
                replay.clock_calls += 1
                value = datetime.fromisoformat(replay.observed)
                return value.astimezone(tz) if tz else value.replace(tzinfo=None)

        return ObservationClock

    def complete(self):
        require(
            self.counts == 2
            and self.index == self.clock_calls == len(self.manifest["pages"]),
            "Exactly two count queries and one observation per original page",
        )


def prepare(args, report):
    """Read only metadata/stat originals; verify caller seeds outside handler timing."""
    original = INPUTS / "published"
    output = args.output / "objects/nyc311/published"
    bronze = [
        read_json(original, f"control/bronze/{ORIGINAL_RUN}/{day}.json") for day in DAYS
    ]
    require(
        all(
            m["status"] == "complete"
            and m["mode"] == "published"
            and m["run_id"] == ORIGINAL_RUN
            and m["start_date"] == day
            and m["page_size"] == 5000
            for day, m in zip(DAYS, bronze)
        ),
        "Original complete month",
    )
    require(
        sum(m["record_count"] for m in bronze) == 323627, "Original month count 323627"
    )
    inventory = [p for p in original.rglob("*") if p.is_file()]
    require(len(inventory) == 234, "Complete original relative-key clone: 234 files")
    for path in inventory:
        safe_path(original, path.relative_to(original).as_posix())
    selected = next((m for m in bronze if m["start_date"] == args.day), None)
    expected = set()
    if args.stage == "curate":
        expected = {f"control/bronze/{ORIGINAL_RUN}/{args.day}.json"}
        expected.update(page["key"] for page in selected["pages"])
    elif args.stage == "gold":
        for day in DAYS:
            key = f"control/silver/{day}.json"
            partition = read_json(original, key)
            expected.update([key, partition["source_manifest_key"]])
            expected.update(
                partition[field]
                for field in ("partition_key", "quality_report_key", "quarantine_key")
            )
        require(len(expected) == 150, "Minimal Gold seed: 150 dependencies")
    actual = {
        p.relative_to(output).as_posix() for p in output.rglob("*") if p.is_file()
    }
    require(actual == expected, "Fresh exact caller seed; no existing target outputs")
    for key in expected:
        require(
            file_digest(safe_path(output, key))
            == file_digest(safe_path(original, key)),
            "Caller seed digest matches original",
        )
    report["inputs"] = {
        "original_run_id": ORIGINAL_RUN,
        "root": str(original),
        "month_rows": 323627,
        "original_files": len(inventory),
        "original_file_bytes": sum(p.stat().st_size for p in inventory),
        "original_pages": sum(len(m["pages"]) for m in bronze),
        "seed_files": len(expected),
        "seed_file_bytes": sum(safe_path(output, k).stat().st_size for k in expected),
        "selected_day": selected,
        "selected_day_gzip_file_bytes": sum(
            safe_path(original, p["key"]).stat().st_size for p in selected["pages"]
        )
        if selected
        else None,
        "measurement": "Metadata/stat and seed SHA checks before handler timer; may warm filesystem cache. "
        "Original page JSON is not parsed here. Original bytes means on-disk file lengths; "
        "HTTP byte counters are decoded JSON response lengths, not network wire bytes.",
    }
    if (INPUTS / "manifest.json").is_file():
        report["inputs"]["caller_manifest_sha256"] = file_digest(
            INPUTS / "manifest.json"
        )
    return selected


def compare_gold(left, right):
    """Exact Arrow schema/metadata and nonfloat values; floats allow roundoff only."""
    import pyarrow as pa

    require(
        left.schema.equals(right.schema, check_metadata=True)
        and left.num_rows == right.num_rows,
        "Gold reference schema, metadata and row count",
    )
    for field in left.schema:
        a, b = left[field.name], right[field.name]
        if pa.types.is_floating(field.type):
            for x, y in zip(a.to_pylist(), b.to_pylist()):
                require(
                    (x is None and y is None)
                    or (
                        x is not None
                        and y is not None
                        and (
                            (math.isnan(x) and math.isnan(y))
                            or math.isclose(x, y, rel_tol=1e-10, abs_tol=1e-9)
                        )
                    ),
                    "Gold reference float column mismatch",
                )
        else:
            require(a.equals(b), "Gold reference exact column mismatch")


def validate(args, result, report, original):
    from nyc311.storage import read_json as stored_json

    base, root = (
        args.output / "objects/nyc311",
        args.output / "objects/nyc311/published",
    )
    require(
        result["status"] == "complete" and result["run_id"] == args.run_id,
        "Complete fresh result",
    )
    if args.stage == "ingest":
        from nyc311.bronze import load_bronze

        manifest, extraction = load_bronze(
            base, result["bronze_manifest_key"], mode="published"
        )
        require(
            extraction["record_count"] == original["record_count"],
            "Fresh ingest row count",
        )
        for field in (
            "record_count",
            "source_count_before",
            "source_count_after",
            "page_size",
            "selected_fields",
            "order",
            "start_date",
            "end_date",
        ):
            require(
                manifest[field] == original[field], "Original ingest manifest metadata"
            )
        require(
            len(manifest["pages"]) == len(original["pages"]),
            "Original ingest page count",
        )
        for actual, expected in zip(manifest["pages"], original["pages"]):
            require(
                {k: v for k, v in actual.items() if k != "key"}
                == {k: v for k, v in expected.items() if k != "key"},
                "Original page digest and observation",
            )
        report["validation"] = {
            "rows": manifest["record_count"],
            "pages": len(manifest["pages"]),
            "original_page_hashes_and_observations": True,
        }
    elif args.stage == "curate":
        from nyc311.silver_store import load_silver

        manifest, table = load_silver(
            base, date.fromisoformat(args.day), mode="published"
        )
        reference, expected = load_silver(
            INPUTS, date.fromisoformat(args.day), mode="published"
        )
        require(
            table.equals(expected, check_metadata=True),
            "Original Silver logical table and metadata",
        )
        quality = stored_json(root, manifest["quality_report_key"])
        original_quality = stored_json(
            INPUTS / "published", reference["quality_report_key"]
        )
        require(
            {k: v for k, v in quality.items() if k != "run_id"}
            == {k: v for k, v in original_quality.items() if k != "run_id"},
            "Original quality minus run_id",
        )
        require(
            manifest["run_id"] == quality["run_id"] == args.run_id
            and manifest["source_manifest_key"] == reference["source_manifest_key"],
            "Silver provenance",
        )
        require(
            gzip.decompress(safe_path(root, manifest["quarantine_key"]).read_bytes())
            == gzip.decompress(
                safe_path(
                    INPUTS / "published", reference["quarantine_key"]
                ).read_bytes()
            ),
            "Original quarantine",
        )
        report["validation"] = {
            "rows": table.num_rows,
            "original_logical_table_metadata": True,
            "quality_equal_except_run_id": True,
            "accounting": quality,
        }
    else:
        from nyc311.gold import PRODUCT_SCHEMAS
        from nyc311.gold_store import _verify_run, load_gold

        manifest, tables = load_gold(base, args.run_id, mode="published")
        require(
            manifest["covered_days"] == DAYS
            and manifest["coverage"] == "full_month"
            and set(tables) == set(PRODUCT_SCHEMAS),
            "Full month four Gold products",
        )
        quality = manifest["quality_exclusions"]
        closed = (
            quality["accepted"]
            - quality["duration_exclusions"]["not_closed"]
            - quality["duration_exclusions"]["closed_without_valid_duration"]
        )
        totals = {
            "demand": sum(tables["demand_daily"]["request_count"].to_pylist()),
            "channel": sum(tables["channel_usage"]["request_count"].to_pylist()),
            "valid_closed": sum(
                tables["closure_duration"]["valid_closed_count"].to_pylist()
            ),
        }
        require(
            totals
            == {
                "demand": quality["accepted"],
                "channel": quality["accepted"],
                "valid_closed": closed,
            }
            and quality["received"] == 323627,
            "Gold demand, channel and eligible closure totals",
        )
        reference_key = f"control/gold/{ORIGINAL_RUN}.json"
        has_reference = safe_path(INPUTS / "reference", reference_key).is_file()
        if has_reference:
            # Reference is already an object root, not a base/published directory.
            reference = read_json(INPUTS / "reference", reference_key)
            require(
                reference["status"] == "complete"
                and reference["mode"] == "published"
                and reference["run_id"] == ORIGINAL_RUN,
                "Original Gold complete reference",
            )
            expected = _verify_run(INPUTS / "reference", reference)
            for name in tables:
                compare_gold(tables[name], expected[name])
            for field in (
                "input_partitions",
                "quality_exclusions",
                "observations_by_day",
                "observation_min_utc",
                "observation_max_utc",
            ):
                require(
                    manifest[field] == reference[field],
                    "Gold original frozen provenance",
                )
        report["validation"] = {
            "totals": totals,
            "product_rows": {k: t.num_rows for k, t in tables.items()},
            "original_reference_compared": has_reference,
            "float_policy": "rel_tol=1e-10 abs_tol=1e-9; NaN matches NaN only; null matches null only",
        }


def failure(error):
    # Exception messages (and chained exceptions) may contain source records.
    frames = traceback.extract_tb(error.__traceback__)
    return {
        "type": type(error).__name__,
        "check": getattr(error, "benchmark_check", None),
        "traceback": [
            {"file": f.filename, "line": f.lineno, "function": f.name} for f in frames
        ],
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("ingest", "curate", "gold"), required=True)
    parser.add_argument("--day")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profiled", "--task86", action="store_true")
    args = parser.parse_args(argv)
    if (
        not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", args.run_id)
        or args.run_id == ORIGINAL_RUN
    ):
        parser.error("--run-id must be safe and different from the original run")
    if args.stage != "gold" and args.day not in DAYS:
        parser.error("--day YYYY-MM-DD in September 2026 is required for ingest/curate")
    if args.stage == "gold" and args.day is not None:
        parser.error("Gold is full-month; omit --day")
    if args.output != Path("/evidence") or args.output.resolve() != args.output:
        parser.error("--output must be the canonical /evidence bind")
    return args


def main(argv=None):
    args = parse_args(argv)
    started = perf_counter()
    report = {
        "stage": args.stage,
        "day": args.day,
        "run_id": args.run_id,
        "status": "failed",
        "expected_image_digest": IMAGE_DIGEST,
        "image_verified_by": "caller",
        "elapsed": {},
        "cleanup": {"workspaces": []},
        "interpretation": {
            "timing": "Cold process imports, not AWS startup. One fresh handler, including disk S3 and original gzip service overhead. Setup and validation separate; interpreter startup and final report encoding excluded.",
            "rss": "Linux ru_maxrss KiB * 1024: process lifetime highwater including imports/setup, not handler-only allocation.",
            "cgroup": "Raw cgroup-v2 snapshots; peak includes file cache, tmpfs and disk-fake environment, not just Python RSS. Unavailable counters remain unavailable.",
            "tmp": "5ms requested sampling, allocated/logical peaks are lower bounds; no AWS headroom pass inferred.",
            "s3": "DiskS3 copies each get into BytesIO, no whole-object retention across calls. Bytes count successful get/put payloads, calls include failed head/get. Validation reads disk directly, excluded from handler IO.",
            "budgets": "Raw local comparison only; caller aggregates trials. Status passed means validation/cleanup, never a budget or AWS headroom pass. Not Lambda configuration advice or AWS capacity certification.",
        },
        "local_budgets": {
            "memory_bytes": 2 * 1024**3,
            "cpu": 2,
            "tmp_bytes": 512 * 1024**2,
            "comparison_fraction": 0.8,
            "memory_80pct_bytes": 0.8 * 2 * 1024**3,
            "tmp_80pct_bytes": 0.8 * 512 * 1024**2,
            "handler_seconds": 720,
        },
    }
    sampler = client = replay = evidence = profile = None
    baseline = set()
    try:
        evidence = (args.output / "report.json").open("x")
        guard_network()
        require(
            sys.version_info[:2] == (3, 12)
            and sys.flags.isolated
            and sys.flags.no_site
            and sys.dont_write_bytecode
            and platform.machine() == "x86_64"
            and Path(sys.executable).resolve().is_relative_to(Path("/var/lang")),
            "Isolated CP312 x86_64 Lambda interpreter",
        )
        release = platform.freedesktop_os_release()
        require(
            release.get("ID") == "amzn" and release.get("VERSION_ID") == "2023",
            "AL2023",
        )
        require(
            not any("site-packages" in p or "dist-packages" in p for p in sys.path),
            "No image site packages",
        )
        for path in (
            TASK,
            INPUTS,
            Path("/checks/benchmark_lambda.py"),
            Path("/checks/check_lambda_runtime.py"),
        ):
            require(
                os.statvfs(path).f_flag & os.ST_RDONLY, "Read-only task/checks/inputs"
            )
        tmp_stat = os.statvfs("/tmp")
        require(
            tmp_stat.f_blocks * tmp_stat.f_frsize == 512 * 1024**2,
            "512 MiB private /tmp",
        )
        mounts = Path("/proc/self/mountinfo").read_text().splitlines()
        require(
            any(line.split()[4] == "/tmp" and " - tmpfs " in line for line in mounts),
            "Private tmpfs mount",
        )
        baseline = {str(p) for p in Path("/tmp").iterdir()}
        helper = runpy.run_path("/checks/check_lambda_runtime.py")
        sampler = helper["TmpSampler"]()
        sampler.thread.start()
        os.environ["PYTHONTZPATH"], os.environ["TMPDIR"] = "/var/task/timezone", "/tmp"
        sys.path.insert(0, str(TASK))
        if args.profiled:
            profile = profiled_settings(args.stage, report)
            report["interpretation"]["profiled"] = (
                "Host/ZIP SHA-verified profiles. Lambda remaining-time clock starts at main entry, includes imports/setup, excludes interpreter startup. Gold settings queries add tiny handler overhead; sampled tmp headroom is not a certified peak. Budget comparisons do not change validation pass status."
            )
        report["environment"] = {
            "python": sys.version,
            "executable": sys.executable,
            "os_release": release,
            "sys_path": list(sys.path),
            "probe_sha256": file_digest(Path("/checks/benchmark_lambda.py")),
            "helper_sha256": file_digest(Path("/checks/check_lambda_runtime.py")),
            "tmp_baseline_entries": sorted(baseline),
            "tmp_capacity_bytes": tmp_stat.f_blocks * tmp_stat.f_frsize,
            "thread_environment": {
                k: os.environ.get(k)
                for k in (
                    "OMP_NUM_THREADS",
                    "OPENBLAS_NUM_THREADS",
                    "MKL_NUM_THREADS",
                    "NUMEXPR_NUM_THREADS",
                )
            },
            "mounts": [
                line
                for line in mounts
                if line.split()[4]
                in (
                    "/tmp",
                    "/var/task",
                    "/inputs",
                    "/evidence",
                    "/checks",
                    "/checks/benchmark_lambda.py",
                    "/checks/check_lambda_runtime.py",
                )
            ],
        }
        report["resources_before_imports"] = resources()
        if profile:
            verify_profile_cgroup(profile, report["resources_before_imports"]["cgroup"])
        imported = perf_counter()
        try:
            for name in helper["SDK"]:
                importlib.import_module(name)
            from botocore.exceptions import ClientError

            guard_sdk()
            for name in helper["REQUIRED"][args.stage]:
                importlib.import_module(name)
            module = importlib.import_module(f"nyc311.lambda_{args.stage}")
        finally:
            report["elapsed"]["cold_import_seconds"] = perf_counter() - imported
        report["origins"] = module_origins()
        require(
            not report["origins"]["violations"], "Payload and stdlib module origins"
        )
        require(
            all(
                name in report["origins"]["payload_modules"]
                for name in helper["SDK"]
                + helper["REQUIRED"][args.stage]
                + (f"nyc311.lambda_{args.stage}",)
            ),
            "Stage-only ZIP dependency origins",
        )
        original = prepare(args, report)
        client = metered_s3(helper, args.output / "objects", ClientError)
        from nyc311 import lambda_common as common

        common.create_s3_client = lambda: client
        real_workspace = module.temporary_workspace

        @contextmanager
        def workspace():
            with real_workspace() as base:
                require(
                    base.parent == Path("/tmp"), "Invocation workspace is private /tmp"
                )
                report["cleanup"]["workspaces"].append(str(base))
                yield base

        module.temporary_workspace = workspace
        event = {
            "bucket": "fixture-bucket",
            "prefix": "nyc311",
            "mode": "published",
            "run_id": args.run_id,
        }
        if args.stage != "gold":
            event.update(
                start_date=args.day,
                end_date=(date.fromisoformat(args.day) + timedelta(days=1)).isoformat(),
            )
        if args.stage == "curate":
            event["bronze_manifest_key"] = (
                f"control/bronze/{ORIGINAL_RUN}/{args.day}.json"
            )
        if args.stage == "ingest":
            from nyc311 import source

            replay = OriginalPages(INPUTS / "published", original)
            source.datetime = replay.clock()
            transport, create = replay.transport(), source.create_client
            module.create_client = lambda config: create(
                config, transport=transport, app_token=""
            )
        if args.stage == "gold":
            require(module.SQL_DIRECTORY == TASK / "sql", "Packaged SQL only")
        report["event"] = event
        report["business_settings"] = common.settings(event)
        report["duckdb_settings"] = (
            "Unmodified business connections; no extra connection opened for measurement"
        )
        report["elapsed"]["setup_seconds_excluding_cold_import"] = (
            perf_counter() - started - report["elapsed"]["cold_import_seconds"]
        )
        report["resources_before_handler"] = resources()
        context = (
            LambdaContext(started, profile["timeout_seconds"]) if profile else None
        )
        if profile:
            report["lambda_context"] = {
                "timeout_seconds": profile["timeout_seconds"],
                "remaining_ms_before_handler": context.get_remaining_time_in_millis(),
                "clock_scope": "main entry through handler; includes imports/setup, not interpreter startup",
            }
        invoked = perf_counter()
        try:
            if profile and args.stage == "gold":
                with observe_duckdb(report):
                    result = module.handler(event, context)
            else:
                result = module.handler(event, context)
        finally:
            report["elapsed"]["handler_seconds"] = perf_counter() - invoked
            report["resources_after_handler"] = resources()
            report["s3_emulation"] = client.snapshot()
            report["tmp_after_handler"] = sampler.sample()
            report["tmp_handler_peaks"] = {
                "allocated_bytes": sampler.allocated_peak,
                "logical_bytes": sampler.logical_peak,
                "scope": "Process setup/imports through handler return; sampled lower bounds, before postvalidation",
            }
            if profile:
                report["elapsed"]["stage_seconds_including_imports_and_setup"] = (
                    perf_counter() - started
                )
                report["lambda_context"]["remaining_ms_after_handler"] = (
                    context.get_remaining_time_in_millis()
                )
                compare_profile_budgets(report)
        report["result"] = result
        checking = perf_counter()
        try:
            if replay:
                replay.complete()
            validate(args, result, report, original)
        finally:
            report["elapsed"]["validation_seconds"] = perf_counter() - checking
        report["status"] = "passed"
    except Exception as error:
        report["failure"] = failure(error)
    finally:
        cleaning = perf_counter()
        try:
            if replay:
                report["source_http"] = replay.stats
            if client and "s3_emulation" not in report:
                report["s3_emulation"] = client.snapshot()
            if sampler:
                report["tmp"] = sampler.finish()
                current = {str(p) for p in Path("/tmp").iterdir()}
                remaining = [
                    p for p in report["cleanup"]["workspaces"] if Path(p).exists()
                ]
                report["cleanup"].update(
                    remaining_workspaces=remaining,
                    new_tmp_entries=sorted(current - baseline),
                    removed_baseline_entries=sorted(baseline - current),
                )
                require(
                    not remaining
                    and current == baseline
                    and sampler.error is None
                    and report["tmp"]["final"]["allocated_bytes"]
                    <= sampler.baseline["allocated_bytes"],
                    "Workspace and tmp allocation cleanup",
                )
            report["origins"] = module_origins()
            require(not report["origins"]["violations"], "Final module origin audit")
            report["cleanup"]["passed"] = True
        except Exception as error:
            report["status"] = "failed"
            report["cleanup"]["passed"] = False
            report["cleanup_failure"] = failure(error)
        report["resources_final"] = resources()
        report["elapsed"]["cleanup_seconds"] = perf_counter() - cleaning
        report["elapsed"]["total_probe_seconds"] = perf_counter() - started
        try:
            if evidence:
                with evidence:
                    json.dump(report, evidence, indent=2, allow_nan=False)
                    evidence.write("\n")
        except Exception as error:
            report["status"] = "failed"
            report["evidence_failure"] = failure(error)
        print(
            json.dumps(
                {k: report[k] for k in ("stage", "day", "run_id", "status", "elapsed")}
                | {
                    "report": str(args.output / "report.json"),
                    "failure_type": report.get(
                        "failure",
                        report.get(
                            "cleanup_failure", report.get("evidence_failure", {})
                        ),
                    ).get("type"),
                },
                separators=(",", ":"),
                allow_nan=False,
            )
        )
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
