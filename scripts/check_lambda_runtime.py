"""TEST ONLY: offline fixture probe inside one extracted Lambda ZIP container.

Run with /var/lang/bin/python3.12 -I -S -B; /var/task and /checks are read-only,
/evidence is persistent writable evidence, and /tmp is a private tmpfs. The caller
owns image-digest verification, extraction, native preflight, and network isolation.
This script neither builds nor repairs payloads and is not a volume benchmark.
"""

import argparse
import importlib
import io
import json
import os
import platform
import re
import resource
import socket
import sys
import sysconfig
import threading
import traceback
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path, PurePosixPath
from time import perf_counter

TASK = Path("/var/task")
IMAGE_DIGEST = "sha256:471e480c997b3150ab1a4fb3b862554fa45d54a4f4a5cd21e3115b320197b717"
DAYS = ("2026-09-01", "2026-09-02")
OBSERVED = "2026-10-13T12:00:00+00:00"
REQUIRED = {
    "ingest": ("httpx",),
    "curate": ("numpy", "pandas", "pyarrow", "pyarrow.parquet"),
    "gold": ("duckdb", "_duckdb", "pyarrow", "pyarrow.parquet"),
}
SDK = ("boto3", "botocore", "jmespath", "s3transfer", "dateutil", "six", "urllib3")


def check(report: dict, name: str, condition: bool, **details) -> None:
    report["checks"].append({"name": name, "passed": bool(condition), **details})
    if not condition:
        raise AssertionError(name)


class DiskS3:
    """Single test bucket; SDK-shaped calls, with no SDK session or credentials."""

    def __init__(self, root: Path, client_error: type) -> None:
        if not root.is_absolute() or root.resolve() != root:
            raise ValueError("Objects directory must be absolute and canonical")
        self.root = root
        self.client_error = client_error
        self.calls = []

    def path(self, bucket: str, key: str) -> Path:
        relative = PurePosixPath(key)
        if (
            bucket != "fixture-bucket"
            or not key
            or relative.is_absolute()
            or ".." in relative.parts
            or "\\" in key
            or relative.as_posix() != key
        ):
            raise ValueError(
                "Only canonical relative keys in the test bucket are allowed"
            )
        path = self.root / key
        if path.resolve() != path or not path.is_relative_to(self.root):
            raise ValueError("Object key escapes the canonical evidence directory")
        return path

    def head_object(self, *, Bucket: str, Key: str) -> dict:
        self.calls.append(("head", Key))
        path = self.path(Bucket, Key)
        if not path.is_file():
            raise self.client_error({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": path.stat().st_size}

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        self.calls.append(("get", Key))
        try:
            content = self.path(Bucket, Key).read_bytes()
        except FileNotFoundError:
            raise self.client_error(
                {"Error": {"Code": "NoSuchKey"}}, "GetObject"
            ) from None
        return {"Body": io.BytesIO(content), "ContentLength": len(content)}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> dict:
        self.calls.append(("put", Key))
        path = self.path(Bucket, Key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(Body)
        return {}


class TmpSampler:
    """Observed peaks only: short-lived allocations between samples can be missed."""

    def __init__(self) -> None:
        self.stop = threading.Event()
        self.allocated_peak = 0
        self.logical_peak = 0
        self.samples = 0
        self.error = None
        self.baseline = self.sample()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def sample(self) -> dict:
        stats = os.statvfs("/tmp")
        allocated = (stats.f_blocks - stats.f_bfree) * stats.f_frsize
        logical = 0
        for directory, _, files in os.walk("/tmp"):
            for name in files:
                try:
                    path = Path(directory) / name
                    if not path.is_symlink():
                        logical += path.stat().st_size
                except FileNotFoundError:
                    pass  # Handler cleanup may race the walk.
        self.samples += 1
        self.allocated_peak = max(self.allocated_peak, allocated)
        self.logical_peak = max(self.logical_peak, logical)
        return {"allocated_bytes": allocated, "logical_file_bytes": logical}

    def run(self) -> None:
        try:
            while not self.stop.wait(0.005):
                self.sample()
        except Exception as error:
            self.error = type(error).__name__

    def finish(self) -> dict:
        self.stop.set()
        self.thread.join()
        final = self.sample()
        return {
            "requested_interval_seconds": 0.005,
            "samples": self.samples,
            "baseline": self.baseline,
            "final": final,
            "peak_allocated_bytes": self.allocated_peak,
            "peak_allocated_above_baseline_bytes": max(
                0, self.allocated_peak - self.baseline["allocated_bytes"]
            ),
            "peak_logical_file_bytes": self.logical_peak,
            "sampler_error": self.error,
            "definition": "statvfs: (f_blocks - f_bfree) * f_frsize on private /tmp tmpfs",
            "limitations": (
                "Sampled lower bounds, not exact peaks. Thread scheduling, the GIL, "
                "and filesystem-walk time can exceed 5 ms; deleted files may race the "
                "logical-size walk. Allocated tmpfs includes unlinked open files; "
                "logical size excludes symlinks and directory allocation. Includes "
                "probe validation workspaces, excludes /evidence S3 emulation bytes."
            ),
        }


def module_origins() -> dict:
    """Audit every loaded module, including native extensions and namespaces."""
    origins = {}
    violations = {}
    stdlib = Path(sysconfig.get_path("stdlib")).resolve()
    for name, module in list(sys.modules.items()):
        if module is None:
            continue
        spec = getattr(module, "__spec__", None)
        origin = getattr(spec, "origin", None) or getattr(module, "__file__", None)
        locations = list(getattr(module, "__path__", ()))
        paths = [origin] if origin not in (None, "built-in", "frozen") else []
        paths += locations
        for location in paths:
            path = Path(location).resolve()
            if path.is_relative_to(TASK):
                origins[name] = {
                    "origin": origin,
                    "search_locations": locations,
                    "version": str(getattr(module, "__version__", "")),
                }
            elif (
                name in {"__main__", "__mp_main__"}
                and module is sys.modules.get("__main__")
                and path == Path(__file__).resolve()
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


def fixture_transport(path: Path):
    # Deliberately no nyc311.cli import: the optimized ingest ZIP omits that module.
    import httpx

    from nyc311.source_contract import SELECTED_FIELDS

    records = json.loads(path.read_bytes())
    dated = [(datetime.fromisoformat(row["created_date"]), row) for row in records]

    def respond(request):
        params = request.url.params
        bounds = re.findall(r"created_date (?:>=|<) '([^']+)'", params["$where"])
        if len(bounds) != 2:
            raise AssertionError("Unexpected fixture date query")
        start, end = [datetime.fromisoformat(value) for value in bounds]
        selected = [row for created, row in dated if start <= created < end]
        if params["$select"] == "count(*) as n":
            return httpx.Response(200, json=[{"n": str(len(selected))}])
        if params["$select"] != ",".join(SELECTED_FIELDS):
            raise AssertionError(
                "Fixture page must request the packaged selected fields"
            )
        if params["$order"] != "unique_key ASC":
            raise AssertionError("Unexpected fixture ordering")
        cursor = re.search(r"unique_key > '((?:''|[^'])*)'", params["$where"])
        if cursor is not None:
            last = cursor.group(1).replace("''", "'")
            selected = [row for row in selected if row["unique_key"] > last]
        selected.sort(key=lambda row: row["unique_key"])
        fields = set(SELECTED_FIELDS)
        return httpx.Response(
            200,
            json=[
                {key: value for key, value in row.items() if key in fields}
                for row in selected[: int(params["$limit"])]
            ],
        )

    return httpx.MockTransport(respond)


def work(args, report: dict, module, client: DiskS3) -> list[dict]:
    from nyc311 import lambda_common as common
    from nyc311.s3_store import S3Store, temporary_workspace
    from nyc311.storage import output_root

    common.create_s3_client = lambda: client
    store = S3Store(client, "fixture-bucket", "nyc311/sample")
    spaces = report["cleanup"]["workspaces"]

    @contextmanager
    def workspace():
        with temporary_workspace() as base:
            spaces.append(str(base))
            yield base

    module.temporary_workspace = workspace

    def invoke(event, label):
        started = perf_counter()
        try:
            return module.handler(event, None)
        finally:
            report["elapsed"]["handlers"].append(
                {"invocation": label, "seconds": perf_counter() - started}
            )

    def no_external_client(*args, **kwargs):
        raise AssertionError("Saved observations must not construct an external client")

    if args.stage == "ingest":
        from nyc311 import source

        class FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                value = cls(2026, 10, 13, 12, tzinfo=UTC)
                return value.astimezone(tz) if tz else value.replace(tzinfo=None)

        source.datetime = FixedDatetime
        transport = fixture_transport(args.fixture)
        real_create = source.create_client
        module.create_client = lambda config: real_create(
            config, transport=transport, app_token=""
        )
        results = []
        for day, count in zip(DAYS, (7, 5)):
            event = {
                "bucket": "fixture-bucket",
                "prefix": "nyc311",
                "mode": "sample",
                "run_id": "runtime-source",
                "start_date": day,
                "end_date": (date.fromisoformat(day) + timedelta(days=1)).isoformat(),
                "config": {"page_size": 3, "max_attempts": 1},
            }
            results.append(invoke(event, f"extract:{day}"))
            check(report, f"bronze_count:{day}", results[-1]["record_count"] == count)
            manifest = store.read_json(results[-1]["bronze_manifest_key"])
            check(
                report,
                f"bronze_observations:{day}",
                all(page["observed_at_utc"] == OBSERVED for page in manifest["pages"]),
            )
        module.create_client = source.create_client = no_external_client
        for first in results:
            check(
                report,
                f"complete_retry:{first['start_date']}",
                invoke(first, f"retry:{first['start_date']}") == first,
            )
            replay = invoke(
                {**first, "run_id": "runtime-ingest-replay", "operation": "replay"},
                f"replay:{first['start_date']}",
            )
            check(
                report,
                f"saved_bronze_replay:{first['start_date']}",
                replay["record_count"] == first["record_count"]
                and replay["bronze_manifest_key"] == first["bronze_manifest_key"]
                and replay["run_id"] == "runtime-ingest-replay",
            )
        return results

    import pyarrow as pa
    import pyarrow.parquet as pq

    with workspace() as base:
        check(report, "snappy_codec", pa.Codec.is_available("snappy"))
        path = base / "native-snappy.parquet"
        table = pa.table({"value": [1, 2, 3]})
        pq.write_table(table, path, compression="snappy")
        parquet = pq.ParquetFile(path)
        try:
            check(
                report,
                "native_parquet_snappy_roundtrip",
                parquet.read().equals(table, check_metadata=True)
                and parquet.metadata.row_group(0).column(0).compression == "SNAPPY",
            )
        finally:
            parquet.close()

    previous = "ingest" if args.stage == "curate" else "curate"
    inputs = json.loads((args.evidence / f"{previous}-results.json").read_bytes())
    check(
        report,
        "referenced_fixture_days",
        [value["start_date"] for value in inputs] == list(DAYS)
        and all(value["status"] == "complete" for value in inputs),
    )
    report["provenance"]["input_stage_results"] = inputs
    if args.stage == "curate":
        from zoneinfo import TZPATH, ZoneInfo

        import numpy as np
        import pandas as pd

        from nyc311.silver import normalize_page
        from nyc311.silver_store import load_silver

        check(
            report,
            "numpy_pandas_native_operations",
            int(np.dot(np.array([1, 2, 3]), np.array([1, 2, 3]))) == 14
            and int(pd.Series([1, 2, 3]).sum()) == 6,
        )
        zone = ZoneInfo("America/New_York")
        zonefile = TASK / "timezone/America/New_York"
        check(
            report,
            "packaged_timezone_offsets",
            TZPATH == ("/var/task/timezone",)
            and zonefile.is_file()
            and zonefile.resolve().is_relative_to(TASK)
            and datetime(2026, 7, 1, tzinfo=zone).utcoffset() == timedelta(hours=-4)
            and datetime(2026, 1, 1, tzinfo=zone).utcoffset() == timedelta(hours=-5),
            tzpath=list(TZPATH),
        )
        report["provenance"]["timezone_sha256"] = sha256(
            zonefile.read_bytes()
        ).hexdigest()
        rows = [
            {
                "unique_key": "dst-ambiguous",
                "created_date": "2026-09-01T00:00:00",
                "closed_date": "2026-11-01T01:30:00",
            },
            {
                "unique_key": "dst-nonexistent",
                "created_date": "2026-09-01T00:00:00",
                "closed_date": "2026-03-08T02:30:00",
            },
        ]
        normalized = normalize_page(
            rows,
            source_run_id="runtime-source",
            source_page_key="bronze/dst.json.gz",
            observed_at_utc=OBSERVED,
        )
        check(
            report,
            "business_logic_optional_dst_dates",
            normalized["closed_date_local"].notna().all()
            and normalized["closed_date_utc"].isna().all(),
        )

        def read_partition(result, expected_run):
            key = result["silver_manifest_key"]
            manifest = store.read_json(key)
            with workspace() as base:
                root = output_root(base, "sample")
                store.download(key, root)
                for field in ("partition_key", "quality_report_key", "quarantine_key"):
                    store.download(manifest[field], root)
                loaded, table = load_silver(
                    base, date.fromisoformat(result["start_date"])
                )
            quality = store.read_json(manifest["quality_report_key"])
            source = store.read_json(manifest["source_manifest_key"])
            check(
                report,
                f"silver_provenance:{expected_run}:{result['start_date']}",
                loaded == manifest
                and manifest["run_id"] == expected_run
                and quality["run_id"] == expected_run
                and quality["source_manifest_key"] == manifest["source_manifest_key"]
                and source["run_id"] == "runtime-source"
                and source["kind"] == "bronze"
                and source["status"] == "complete"
                and source["mode"] == "sample"
                and source["start_date"] == result["start_date"]
                and source["end_date"] == result["end_date"]
                and manifest["source_manifest_key"] == result["bronze_manifest_key"]
                and set(table["source_run_id"].to_pylist()) == {"runtime-source"},
            )
            return manifest, table, quality

        results = []
        for event, count in zip(inputs, (7, 5)):
            first = invoke(event, f"publish:{event['start_date']}")
            before, table, quality = read_partition(
                {**first, "bronze_manifest_key": event["bronze_manifest_key"]},
                "runtime-source",
            )
            check(report, f"silver_rows:{event['start_date']}", table.num_rows == count)
            if event["start_date"] == DAYS[1]:
                ambiguous = next(
                    row
                    for row in table.to_pylist()
                    if row["unique_key"] == "sample-gold-10"
                )
                check(
                    report,
                    "saved_fixture_ambiguous_closure",
                    ambiguous["closed_date_utc"] is None
                    and "unresolvable_closed_date" in ambiguous["quality_flags"]
                    and not ambiguous["duration_eligible"],
                )
            replay = invoke(
                {**event, "run_id": "runtime-curate-replay", "operation": "replay"},
                f"replay:{event['start_date']}",
            )
            after, restored, replay_quality = read_partition(
                {**replay, "bronze_manifest_key": event["bronze_manifest_key"]},
                "runtime-curate-replay",
            )
            check(
                report,
                f"silver_logical_replay:{event['start_date']}",
                restored.num_rows == count
                and table.equals(restored, check_metadata=True)
                and before["source_manifest_key"] == after["source_manifest_key"]
                and {key: value for key, value in quality.items() if key != "run_id"}
                == {
                    key: value
                    for key, value in replay_quality.items()
                    if key != "run_id"
                },
            )
            results.append(
                {**replay, "bronze_manifest_key": event["bronze_manifest_key"]}
            )
        return results

    from nyc311.gold import PRODUCT_SCHEMAS
    from nyc311.gold_store import load_gold

    event = {
        "bucket": "fixture-bucket",
        "prefix": "nyc311",
        "mode": "sample",
        "run_id": "runtime-gold",
        "days": [value["start_date"] for value in inputs],
    }
    result = invoke(event, "publish:two-days")
    manifest = store.read_json(result["gold_manifest_key"])
    report["provenance"]["gold_manifest"] = manifest
    with workspace() as base:
        root = output_root(base, "sample")
        store.download(result["gold_manifest_key"], root)
        for descriptor in manifest["products"].values():
            store.download(descriptor["key"], root)
        loaded, tables = load_gold(base, event["run_id"])
    check(
        report,
        "gold_four_products_schemas_counts",
        loaded == manifest
        and set(tables) == set(PRODUCT_SCHEMAS)
        and len(tables) == 4
        and all(
            table.schema.equals(PRODUCT_SCHEMAS[name])
            and table.num_rows == result["products"][name]["row_count"]
            for name, table in tables.items()
        ),
        product_row_counts={name: table.num_rows for name, table in tables.items()},
    )
    check(
        report,
        "gold_population_totals",
        sum(tables["demand_daily"]["request_count"].to_pylist()) == 12
        and sum(tables["channel_usage"]["request_count"].to_pylist()) == 12
        and sum(tables["closure_duration"]["valid_closed_count"].to_pylist()) == 5
        and sum(tables["outstanding_requests"]["outstanding_count"].to_pylist()) == 2
        and sum(tables["outstanding_requests"]["unknown_status_count"].to_pylist())
        == 1,
    )
    dot = next(
        row for row in tables["closure_duration"].to_pylist() if row["agency"] == "DOT"
    )
    check(
        report,
        "gold_fixture_quantiles",
        dot["valid_closed_count"] == 4
        and abs(dot["median_closure_hours"] - 6.0) < 1e-9
        and abs(dot["p90_closure_hours"] - 489.9) < 1e-9,
        dot=dot,
    )
    check(
        report,
        "gold_fixed_source_observations",
        manifest["observation_min_utc"] == manifest["observation_max_utc"] == OBSERVED
        and manifest["covered_days"] == list(DAYS)
        and [value["source_manifest_key"] for value in manifest["input_partitions"]]
        == [value["bronze_manifest_key"] for value in inputs],
    )
    check(
        report,
        "gold_referenced_silver_provenance",
        all(
            partition["run_id"] == result["run_id"]
            and partition["row_count"] == result["row_count"]
            and result["silver_manifest_key"]
            == f"control/silver/{partition['start_date']}.json"
            for partition, result in zip(manifest["input_partitions"], inputs)
        ),
    )
    # Make current Silver unavailable, then restore it even if historical retry fails.
    silver_keys = [f"control/silver/{day}.json" for day in DAYS]
    saved = {key: store.read(key) for key in silver_keys}
    try:
        for key in silver_keys:
            store.write_json(key, {"status": "failed"})
        start = len(client.calls)
        retry = invoke(event, "frozen-retry:two-days")
        calls = client.calls[start:]
        check(
            report,
            "gold_frozen_s3_only_retry",
            retry == result
            and all(operation != "put" for operation, _ in calls)
            and all(
                "/silver/" not in key and "/bronze/" not in key for _, key in calls
            ),
        )
    finally:
        for key, content in saved.items():
            store.write(key, content)
    return [result]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=REQUIRED, required=True)
    parser.add_argument("--objects", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, default=Path("/evidence"))
    args = parser.parse_args()
    report = {
        "stage": args.stage,
        "status": "failed",
        "test_only": True,
        "expected_image_digest": IMAGE_DIGEST,
        "image_digest_verified_by": "caller, not attestable by this script",
        "checks": [],
        "elapsed": {
            "handlers": [],
            "definition": (
                "perf_counter seconds: cold_import covers required dependencies plus "
                "handler import; handlers covers each actual invocation including "
                "disk S3 emulation. Total includes setup and validation, ends before "
                "evidence JSON encoding/writes; excludes interpreter startup."
            ),
        },
        "cleanup": {"workspaces": []},
        "provenance": {"task": str(TASK), "objects": str(args.objects)},
        "measurement_scope": "Single tiny fixture, imports, handlers, retries and validation; not task 8.5",
    }
    started = perf_counter()
    sampler = None
    client = None
    baseline_entries = set()
    try:
        if (
            not args.evidence.is_absolute()
            or args.evidence.resolve() != args.evidence
            or not args.evidence.is_dir()
            or args.evidence.is_relative_to(Path("/tmp"))
            or not args.objects.is_relative_to(args.evidence)
        ):
            raise ValueError(
                "Evidence must exist outside /tmp; objects must be beneath it"
            )
        baseline_entries = {str(path) for path in Path("/tmp").iterdir()}
        sampler = TmpSampler()
        sampler.thread.start()

        def blocked(*args, **kwargs):
            raise RuntimeError(
                "Network and real SDK clients are disabled in this probe"
            )

        socket.getaddrinfo = socket.create_connection = blocked
        socket.socket.connect = socket.socket.connect_ex = blocked
        check(
            report,
            "isolated_lambda_interpreter",
            sys.version_info[:2] == (3, 12)
            and sys.flags.isolated == 1
            and sys.flags.no_site == 1
            and sys.dont_write_bytecode
            and Path(sys.executable).resolve().is_relative_to(Path("/var/lang"))
            and platform.machine() == "x86_64",
        )
        check(
            report,
            "no_site_packages",
            not any(
                "site-packages" in path or "dist-packages" in path for path in sys.path
            ),
        )
        release = platform.freedesktop_os_release()
        check(
            report,
            "amazon_linux_2023",
            release.get("ID") == "amzn" and release.get("VERSION_ID") == "2023",
        )
        sys.path.insert(0, str(TASK))
        os.environ["PYTHONTZPATH"] = "/var/task/timezone"
        os.environ["TMPDIR"] = "/tmp"
        report["provenance"].update(
            python=sys.version,
            executable=sys.executable,
            platform=platform.platform(),
            os_release=release,
            sys_path=list(sys.path),
            fixture=str(args.fixture),
            fixture_sha256=sha256(args.fixture.read_bytes()).hexdigest(),
            python_tzpath=os.environ["PYTHONTZPATH"],
        )
        imported = perf_counter()
        try:
            for name in SDK + REQUIRED[args.stage]:
                importlib.import_module(name)
            module = importlib.import_module(f"nyc311.lambda_{args.stage}")
        finally:
            report["elapsed"]["cold_import_seconds"] = perf_counter() - imported
        report["origins"] = module_origins()
        check(report, "payload_import_origins", not report["origins"]["violations"])
        check(
            report,
            "required_modules_from_zip",
            all(
                name in report["origins"]["payload_modules"]
                for name in SDK
                + REQUIRED[args.stage]
                + (f"nyc311.lambda_{args.stage}",)
            ),
        )

        import boto3
        from botocore.exceptions import ClientError

        boto3.client = boto3.session.Session.client = blocked
        client = DiskS3(args.objects, ClientError)
        results = work(args, report, module, client)
        report["results"] = results
        report["status"] = "passed"
    except Exception as error:
        report["failure"] = {
            "type": type(error).__name__,
            "traceback": traceback.format_exc(),
        }
    finally:
        try:
            report["origins"] = module_origins()
            if sampler is not None:
                report["tmp"] = sampler.finish()
                remaining = [
                    path
                    for path in report["cleanup"]["workspaces"]
                    if Path(path).exists()
                ]
                current = {str(path) for path in Path("/tmp").iterdir()}
                report["cleanup"].update(
                    remaining_workspaces=remaining,
                    new_tmp_entries=sorted(current - baseline_entries),
                    removed_baseline_entries=sorted(baseline_entries - current),
                )
                clean = (
                    not remaining
                    and current == baseline_entries
                    and report["tmp"]["final"]["allocated_bytes"]
                    <= report["tmp"]["baseline"]["allocated_bytes"]
                )
                report["cleanup"]["passed"] = clean
                check(report, "tmp_cleanup", clean)
                check(report, "tmp_sampler", sampler.error is None)
            check(report, "final_payload_origins", not report["origins"]["violations"])
        except Exception as error:
            report["status"] = "failed"
            report.setdefault(
                "failure",
                {
                    "type": type(error).__name__,
                    "traceback": traceback.format_exc(),
                },
            )
        report["elapsed"]["total_probe_seconds"] = perf_counter() - started
        report["elapsed"]["handler_total_seconds"] = sum(
            item["seconds"] for item in report["elapsed"]["handlers"]
        )
        report["peak_memory"] = {
            "bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            "definition": (
                "Linux process high-water ru_maxrss KiB * 1024, measured before final "
                "JSON encoding; includes probe overhead, not cgroup memory or tmpfs size"
            ),
        }
        if client is not None:
            report["s3_emulation"] = {
                "root": str(client.root),
                "calls": client.calls,
                "excluded_from_tmp_metrics": True,
            }
        try:
            if report["status"] == "passed":
                (args.evidence / f"{args.stage}-results.json").write_text(
                    json.dumps(report["results"], indent=2, allow_nan=False) + "\n"
                )
            encoded = json.dumps(report, indent=2, allow_nan=False) + "\n"
            (args.evidence / f"{args.stage}-runtime.json").write_text(encoded)
        except Exception as error:
            report["status"] = "failed"
            report["evidence_write_failure"] = {
                "type": type(error).__name__,
                "traceback": traceback.format_exc(),
            }
            report.setdefault("failure", report["evidence_write_failure"])
            encoded = json.dumps(report, indent=2, allow_nan=False) + "\n"
        print(encoded, end="")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
