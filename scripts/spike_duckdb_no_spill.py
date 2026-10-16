"""Offline 8.6 verification. --production observes policy without replacing it.

Default mode retains the historical experimental policy-replacement spike.

Host owns approved ZIP/image hashes, clones, seeding and kernel strace verdict.
Mount approved Gold DuckDB 1.5.6 payload RO /var/task, 234 published + five
reference files RO /inputs, both existing helpers and this file RO /checks.
Use official AL2023 CP312 x86_64, -I -S -B, network none, 1 CPU, 2 GiB/no swap,
private 512 MiB /tmp, fresh RW /evidence with objects/nyc311/published seeded
with 150 dependencies (plus five old reference objects for failure trials).
Docker --workdir /tmp/work may create that directory; this probe also ensures it.

CLI: --case positive|negative|gold|fail-first|fail-late --run-id nospill-CASE-TRIAL
--observed is always true. Before payload imports, opt ONLY this process into
ptrace, write ready.json, wait <=120s for host /evidence/go. Host must attach
strace to the mapped host PID and verify /proc/<hostpid>/status TracerPid before
GO. No capabilities, host PID namespace, subprocess tracer or network needed.

Empty phases/NAME.begin and .end are created/deleted as syscall delimiters.
setup includes writability sentinels; imports precede native SQL; connection
marks native connect/settings; query covers the exact generated sort; handler
includes ordinary downloads, local Parquet/control writes and DiskS3 uploads.
business-N spans native return through product preparation, ending at progress
or handler failure (not a claim of SQL-only isolation). Validation/cleanup/report
writes are outside measured work. Only positive may spill under positive-spill.
Host must classify ALL write opens/FD paths, rename/unlink/mmap/etc, not a prefix
sample. Metadata snapshots and 5ms peaks cannot exclude transient/alternate
spill. A passed JSON is functional evidence, NEVER the host no-spill verdict.
Trace buffer arguments should be raw pointers to avoid exposing record content.
"""

import argparse
import ast
import ctypes
import errno
import importlib
import json
import os
import platform
import re
import runpy
import stat
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

EVIDENCE = Path("/evidence")
TASK = Path("/var/task")
INPUTS = Path("/inputs")
SOURCE = Path(__file__).resolve()
QUERY = """
        SELECT i, repeat(md5(i::VARCHAR), 4) AS value
        FROM range(350000) t(i) ORDER BY value
    """
SETTINGS = ("memory_limit", "max_temp_directory_size", "threads", "temp_directory")
LIMITS = {"memory_limit": "512MiB", "max_temp_directory_size": "256MiB", "threads": 1}
PRODUCTION_LIMITS = {"memory_limit": "512MiB", "threads": 1, "temp_directory": ""}
NATIVE_SHA256 = "cd857a08b9c56a9a253b475a3424383adf7a6d51e613e0db122c5760498e0b21"


def require(condition, label):
    if not condition:
        error = AssertionError(label)
        error.benchmark_check = label
        raise error


@contextmanager
def phase(name):
    marker = EVIDENCE / "phases" / f"{name}.begin"
    with marker.open("xb"):
        pass
    marker.unlink()
    try:
        yield
    finally:
        marker = EVIDENCE / "phases" / f"{name}.end"
        with marker.open("xb"):
            pass
        marker.unlink()


def trace_permission():
    # Image glibc only: PR_SET_PTRACER_ANY changes permission for this process.
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    libc.prctl.argtypes = [ctypes.c_int] + [ctypes.c_ulong] * 4
    libc.prctl.restype = ctypes.c_int
    ctypes.set_errno(0)
    result = libc.prctl(0x59616D61, ctypes.c_ulong(-1), 0, 0, 0)
    error = ctypes.get_errno()
    return {"result": result, "errno": error, "scope": "own test process only"}


def wait_for_go():
    deadline = time.monotonic() + 120
    while not (EVIDENCE / "go").is_file():
        require(time.monotonic() < deadline, "Host GO timeout after 120 seconds")
        time.sleep(0.02)


def origins(benchmark):
    audit = benchmark["module_origins"]()
    main = sys.modules.get("__main__")
    for name in ("__main__", "__mp_main__"):
        if (
            sys.modules.get(name) is main
            and getattr(main, "__file__", None)
            and Path(main.__file__).resolve() == SOURCE
            and audit["violations"].get(name) == str(SOURCE)
        ):
            del audit["violations"][name]
    require(not audit["violations"], "Payload and image stdlib origins only")
    return audit


def inventory(root):
    """Metadata of every path, without following directory symlinks."""
    result = {}
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            path = Path(directory) / name
            try:
                info = path.lstat()
                result[str(path)] = [info.st_mode, info.st_size, info.st_blocks * 512]
            except FileNotFoundError:
                pass  # Native cleanup may race observation; strace remains required.
    return result


def snapshots():
    return {
        str(root): inventory(root)
        for root in (Path("/tmp"), Path("/dev/shm"), EVIDENCE)
    }


def hashes(root, digest):
    return {
        p.relative_to(root).as_posix(): digest(p)
        for p in root.rglob("*")
        if p.is_file()
    }


def live_settings(connection, settings=SETTINGS):
    row = connection.execute(
        "SELECT " + ", ".join(f"current_setting('{key}')" for key in settings)
    ).fetchone()
    return dict(zip(settings, row))


def native_oom(error, duckdb):
    require(
        isinstance(error, duckdb.OutOfMemoryException),
        "Native OutOfMemoryException required",
    )
    message = str(error).lower()
    require(
        not any(
            token in message
            for token in (
                "permission",
                "read-only",
                "read only",
                "readonly",
                "errno 13",
                "errno 30",
                "errno13",
                "errno30",
            )
        ),
        "Resource rejection must not be a filesystem permission artifact",
    )


@contextmanager
def no_spill_connections(gold, report, benchmark, fail_at=0, production=False):
    """Native connections returned unchanged; never replace SQL or handler logic."""
    native = gold.duckdb.connect
    records = report["connections"] = []

    def connect(*args, **kwargs):
        require(len(args) <= 3, "Known DuckDB connect interface")
        requested = dict(args[2] if len(args) == 3 else kwargs.get("config", {}))
        if production:
            injected = len(records) + 1 == fail_at
            require(
                requested
                == PRODUCTION_LIMITS | ({"memory_limit": "1B"} if injected else {})
                and type(requested.get("threads")) is int,
                "Exact production three-key policy at native connect",
            )
            record = {
                "requested_config": requested,
                "native_config": dict(requested),
                "injected": injected,
                "config_rewritten": False,
                "native_kwargs_unchanged": True,
            }
            records.append(record)
            connection = None
            try:
                with phase(f"connection-{len(records)}"):
                    connection = native(*args, **kwargs)
                    if injected:
                        # A readback query can itself OOM at 1B, before business SQL.
                        record["effective_unavailable"] = (
                            "fault connection: readback deliberately skipped; declared config only"
                        )
                    else:
                        effective = record["effective"] = live_settings(
                            connection, PRODUCTION_LIMITS
                        )
                        require(
                            effective["temp_directory"] == ""
                            and type(effective["threads"]) is int
                            and effective["threads"] == 1
                            and benchmark["setting_bytes"](effective["memory_limit"])
                            == 512 * 1024**2,
                            "Native live production no-spill settings",
                        )
            except Exception as error:
                record["error"] = benchmark["failure"](error)
                if connection is not None:
                    connection.close()
                raise
            record["business_phase"] = phase(f"business-{len(records)}")
            record["business_phase"].__enter__()
            return connection
        temporary = Path(requested.get("temp_directory", ""))
        require(
            requested == LIMITS | {"temp_directory": str(temporary)}
            and temporary.is_absolute()
            and temporary.resolve() == temporary
            and temporary.name == "duckdb-spill"
            and temporary.parent.parent == Path("/tmp")
            and temporary.parent.name.startswith("nyc311-")
            and str(temporary.parent) in report["workspaces"],
            "Application validated original absolute private spill path and unchanged limits",
        )
        config = requested | {"temp_directory": ""}
        injected = len(records) + 1 == fail_at
        if injected:
            config["memory_limit"] = "1B"
        record = {
            "requested_config": requested,
            "native_config": config,
            "injected": injected,
        }
        records.append(record)
        positional, named = list(args), dict(kwargs)
        if len(positional) == 3:
            positional[2] = config
        else:
            named["config"] = config
        connection = None
        try:
            with phase(f"connection-{len(records)}"):
                connection = native(*positional, **named)
                effective = record["effective"] = live_settings(connection)
                # DuckDB renders sub-KiB limits as bytes, unlike the MiB settings.
                memory = effective["memory_limit"]
                memory_bytes = (
                    int(memory.split()[0])
                    if re.fullmatch(r"[01] bytes?", memory)
                    else benchmark["setting_bytes"](memory)
                )
                require(
                    effective["temp_directory"] == ""
                    and effective["threads"] == 1
                    and (
                        memory_bytes <= 1 if injected else memory_bytes == 512 * 1024**2
                    )
                    and benchmark["setting_bytes"](effective["max_temp_directory_size"])
                    == 256 * 1024**2,
                    "Native live no-spill settings",
                )
        except Exception as error:
            record["error"] = benchmark["failure"](error)
            if connection is not None:
                connection.close()
            raise
        record["business_phase"] = phase(f"business-{len(records)}")
        record["business_phase"].__enter__()
        return connection

    gold.duckdb.connect = connect
    try:
        yield
    finally:
        gold.duckdb.connect = native
        end_business(report)


def end_business(report):
    for record in report.get("connections", []):
        active = record.pop("business_phase", None)
        if active is not None:
            active.__exit__(None, None, None)


def synthetic(args, report, duckdb, helper, benchmark):
    spill = Path("/tmp/positive-spill")
    positive = args.case == "positive"
    require(not spill.exists(), "Fresh positive spill workspace")
    if positive:
        spill.mkdir()

    class PhysicalSampler(helper["TmpSampler"]):
        physical_peak = 0

        def sample(self):
            result = super().sample()
            self.physical_peak = max(
                self.physical_peak,
                sum(v[2] for v in inventory(spill).values() if stat.S_ISREG(v[0])),
            )
            return result

    sampler = PhysicalSampler()
    sampler.thread.start()
    config = LIMITS | {
        "memory_limit": "32MiB",
        "temp_directory": str(spill) if positive else "",
    }
    production = getattr(args, "production", False)
    if production:
        from nyc311.lambda_runtime import DUCKDB_LIMITS

        require(DUCKDB_LIMITS == PRODUCTION_LIMITS, "Unchanged production policy")
        config = DUCKDB_LIMITS | {"memory_limit": "32MiB"}
        if positive:
            # Direct-native positive control, deliberately outside the Gold guard.
            config |= {
                "temp_directory": str(spill),
                "max_temp_directory_size": "256MiB",
            }
        report["policy_provenance"] = {
            "declared_config": dict(DUCKDB_LIMITS),
            "config_rewritten_at_native_connect": False,
            "control_overrides": {
                key: value
                for key, value in config.items()
                if DUCKDB_LIMITS.get(key) != value
            },
        }
    report.update(
        config=config,
        rows=0,
        query_sha256=benchmark["sha256"](QUERY.encode()).hexdigest(),
    )
    connection = None
    try:
        with phase("connection"):
            connection = duckdb.connect(":memory:", config=config)
            report["effective"] = effective = live_settings(
                connection, config if production else SETTINGS
            )
            require(
                effective["temp_directory"] == config["temp_directory"]
                and effective["threads"] == 1
                and benchmark["setting_bytes"](effective["memory_limit"])
                == 32 * 1024**2
                and (
                    production
                    and not positive
                    or benchmark["setting_bytes"](effective["max_temp_directory_size"])
                    == 256 * 1024**2
                ),
                "Synthetic live settings",
            )
        started = time.perf_counter()
        try:
            with phase("query"):
                try:
                    with connection.execute(QUERY).to_arrow_reader(2048) as reader:
                        for batch in reader:
                            report["rows"] += batch.num_rows
                            sampler.sample()
                except Exception as error:
                    if positive:
                        raise
                    native_oom(error, duckdb)
                    report["expected_error"] = benchmark["failure"](error) | {
                        "message": str(error)
                    }
                else:
                    require(positive, "Negative unexpectedly succeeded; host must STOP")
        finally:
            report["elapsed"]["query_seconds"] = time.perf_counter() - started
            report["resources_after_work"] = benchmark["resources"]()
            report["tmp_prevalidation"] = sampler.sample()
        if positive:
            require(
                report["rows"] == 350000 and sampler.physical_peak > 0,
                "Positive 350000 rows and real physical spill required",
            )
            report["outcome"] = "completed_with_physical_spill"
        else:
            report["outcome"] = "expected_native_resource_rejection"
    finally:
        if connection is not None:
            with phase("connection-close"):
                connection.close()
        report["tmp"] = sampler.finish()
        report["physical_spill_peak_bytes_lower_bound"] = sampler.physical_peak
        report["physical_peak_scope"] = (
            "/tmp/positive-spill regular files only; alternate paths require host strace"
        )
        require(not inventory(spill), "Native spill files removed on connection close")
        if positive:
            spill.rmdir()  # Only this probe-created, now empty directory.
        require(sampler.error is None, "Tmp sampler succeeded")


def seed_check(args, report, benchmark):
    digest = benchmark["file_digest"]
    original = INPUTS / "published"
    reference = INPUTS / "reference"
    root = EVIDENCE / "objects/nyc311/published"
    require(len(hashes(original, digest)) == 234, "234 original published files")
    expected = set()
    for day in benchmark["DAYS"]:
        key = f"control/silver/{day}.json"
        manifest = benchmark["read_json"](original, key)
        expected.update((key, manifest["source_manifest_key"]))
        expected.update(
            manifest[k]
            for k in ("partition_key", "quality_report_key", "quarantine_key")
        )
    require(len(expected) == 150, "Minimal 150 Gold dependency objects")
    old_key = f"control/gold/{benchmark['ORIGINAL_RUN']}.json"
    old = benchmark["read_json"](reference, old_key)
    old_keys = {old_key} | {value["key"] for value in old["products"].values()}
    require(
        len(old_keys) == 5
        and set(hashes(reference, digest)) == old_keys
        and old["status"] == "complete",
        "Five complete old Gold reference objects",
    )
    actual = hashes(root, digest)
    require(
        set(actual) in (expected, expected | old_keys),
        "Exact 150 seeds plus optional five old Gold objects; no target reuse",
    )
    require(
        not args.case.startswith("fail") or old_keys <= actual.keys(),
        "Failure cases require old successful Gold seed",
    )
    for key in actual:
        source = original if key in expected else reference
        require(
            actual[key] == digest(benchmark["safe_path"](source, key)),
            "Seed matches immutable source",
        )
    require(
        not (root / f"control/gold/{args.run_id}.json").exists()
        and not (root / f"gold/run={args.run_id}").exists(),
        "Fresh target run ID",
    )
    report["seed_sha256"] = actual
    return root, actual


def gold_work(args, report, helper, benchmark):
    from botocore.exceptions import ClientError

    from nyc311 import gold, gold_store, lambda_common, lambda_gold
    from nyc311.gold_store import load_gold

    root = EVIDENCE / "objects/nyc311/published"
    target = f"nyc311/published/control/gold/{args.run_id}.json"

    class AuditedS3(helper["DiskS3"]):
        def put_object(self, **kwargs):
            key = kwargs["Key"]
            entry = {"key": key, "bytes": len(kwargs["Body"])}
            if key == target:
                payload = json.loads(kwargs["Body"])
                entry.update(
                    status=payload["status"], products=len(payload["products"])
                )
            report["put_attempts"].append(entry)  # Observe before any disk write.
            result = super().put_object(**kwargs)
            entry["persisted"] = True
            return result

    client = AuditedS3(EVIDENCE / "objects", ClientError)
    real_factory, real_workspace, real_publish = (
        lambda_common.create_s3_client,
        lambda_gold.temporary_workspace,
        lambda_gold.publish_gold,
    )
    real_compute = gold_store._compute_query
    production = getattr(args, "production", False)
    if production:
        from nyc311.lambda_runtime import DUCKDB_LIMITS

        require(DUCKDB_LIMITS == PRODUCTION_LIMITS, "Unchanged production policy")
        source_path = Path(gold.__file__)
        source = source_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        execute_lines = [
            node.lineno
            for function in tree.body
            if isinstance(function, ast.FunctionDef)
            and function.name == "_compute_query"
            for node in ast.walk(function)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "connection"
            and node.func.attr == "execute"
            and len(node.args) == 1
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id == "query"
        ]
        require(
            len(execute_lines) == 1, "One original business execute(query) boundary"
        )
        report["production_source"] = {
            "file": str(source_path),
            "sha256": benchmark["file_digest"](source_path),
            "business_execute_lines": execute_lines,
        }
        report["policy_provenance"] = {
            "declared_config": dict(DUCKDB_LIMITS),
            "config_rewritten_at_native_connect": False,
        }
    report.update(
        workspaces=[], put_attempts=[], progress=[], strict_compute_windows=[]
    )

    def compute(*positional, **named):
        index = len(report["strict_compute_windows"]) + 1
        label = f"native-product-{index}"
        product = positional[1] if len(positional) > 1 else named["product"]
        report["strict_compute_windows"].append({"phase": label, "product": product})
        if production:
            require(
                named.get("duckdb_config") == PRODUCTION_LIMITS,
                "Handler passes exact production policy before validation",
            )
            query = positional[2] if len(positional) > 2 else named["query"]
            require(
                benchmark["sha256"](query.encode()).hexdigest()
                == benchmark["file_digest"](TASK / "sql" / f"{product}.sql"),
                "Original business SQL bytes",
            )
            report.setdefault("business_query_sha256", {})[product] = benchmark[
                "sha256"
            ](query.encode()).hexdigest()
            if index == fail_at:
                config = named["duckdb_config"] | {"memory_limit": "1B"}
                report["fault_injection"] = {
                    "product_index": index,
                    "product": product,
                    "boundary": "before original gold_store._compute_query validation",
                    "original_config": dict(named["duckdb_config"]),
                    "declared_config": config,
                }
                named = named | {"duckdb_config": config}
        # Native computation and validators do not prepare/publish filesystem outputs.
        with phase(label):
            return real_compute(*positional, **named)

    @contextmanager
    def workspace():
        with real_workspace() as base:
            require(base.parent == Path("/tmp"), "Private invocation workspace")
            report["workspaces"].append(str(base))
            yield base

    def publish(*positional, **named):
        callback = named["on_progress"]

        def progress(manifest):
            end_business(report)
            report["progress"].append(
                {"status": manifest["status"], "products": len(manifest["products"])}
            )
            return callback(manifest)

        return real_publish(*positional, **(named | {"on_progress": progress}))

    require(lambda_gold.SQL_DIRECTORY == TASK / "sql", "Unchanged packaged SQL")
    fail_at = {"fail-first": 1, "fail-late": 2}.get(args.case, 0)
    sampler = helper["TmpSampler"]()
    sampler.thread.start()
    error = result = None
    invoked = time.perf_counter()
    try:
        lambda_common.create_s3_client = lambda: client
        lambda_gold.temporary_workspace, lambda_gold.publish_gold = workspace, publish
        gold_store._compute_query = compute
        connection_options = {"production": True} if production else {}
        with (
            phase("handler"),
            no_spill_connections(
                gold, report, benchmark, fail_at, **connection_options
            ),
        ):
            try:
                result = lambda_gold.handler(
                    {
                        "bucket": "fixture-bucket",
                        "prefix": "nyc311",
                        "mode": "published",
                        "run_id": args.run_id,
                    },
                    benchmark["LambdaContext"](invoked, 900),
                )
            except Exception as caught:
                error = caught
    finally:
        report["elapsed"]["handler_seconds"] = time.perf_counter() - invoked
        report["resources_after_work"] = benchmark["resources"]()
        report["tmp_prevalidation"] = sampler.finish()
        report["s3_calls"] = client.calls
        lambda_common.create_s3_client = real_factory
        gold_store._compute_query = real_compute
        lambda_gold.temporary_workspace, lambda_gold.publish_gold = (
            real_workspace,
            real_publish,
        )
    checking = time.perf_counter()
    with phase("validation"):
        if not fail_at:
            if error is not None:
                raise error
            require(
                len(report["connections"]) == 4, "Exactly four native Gold connections"
            )
            validation_args = SimpleNamespace(
                stage="gold", output=EVIDENCE, day=None, run_id=args.run_id
            )
            benchmark["validate"](validation_args, result, report, None)
            require(
                report["validation"]["original_reference_compared"],
                "Full frozen Gold reference comparison required",
            )
            manifest = benchmark["read_json"](
                root, target.removeprefix("nyc311/published/")
            )
            require(
                all(
                    value["sql_sha256"]
                    == benchmark["file_digest"](TASK / "sql" / f"{name}.sql")
                    for name, value in manifest["products"].items()
                ),
                "Unchanged packaged SQL hashes",
            )
            report["outcome"] = "complete_four_products_323627_rows"
        else:
            native_oom(error, gold.duckdb)
            report["expected_error"] = benchmark["failure"](error)
            if production:
                frames = report["expected_error"]["traceback"]
                source = report["production_source"]
                business_oom = bool(frames) and (
                    frames[-1]["file"] == source["file"]
                    and frames[-1]["function"] == "_compute_query"
                    and frames[-1]["line"] in source["business_execute_lines"]
                )
                report["native_oom_boundary"] = (
                    "business_execute_query"
                    if business_oom
                    else "not_business_execute_query"
                )
                if not business_oom and frames:
                    last = frames[-1]
                    if last["function"] in {"connect", "live_settings"}:
                        report["native_oom_boundary"] = last["function"]
                    elif last["file"] == source["file"] and any(
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and node.func.attr == "register"
                        and node.lineno == last["line"]
                        for node in ast.walk(tree)
                    ):
                        report["native_oom_boundary"] = "register"
                require(
                    business_oom,
                    "Native OOM must originate at original business execute(query)",
                )
            require(
                len(report["connections"]) == fail_at
                and report["connections"][-1]["injected"],
                "Failure at requested native resource boundary",
            )
            manifests = [p for p in report["put_attempts"] if p["key"] == target]
            products = [
                p
                for p in report["put_attempts"]
                if p["key"].startswith(f"nyc311/published/gold/run={args.run_id}/")
                and p.get("persisted")
            ]
            require(
                len(products) == fail_at - 1
                and all(
                    p["status"] != "complete" for p in report["progress"] + manifests
                ),
                "No complete authorization; expected prior uploaded products",
            )
            require(
                manifests
                and manifests[-1].get("persisted")
                and manifests[-1]["status"] == "failed"
                and manifests[-1]["products"] == fail_at - 1
                and report["progress"][-1]
                == {"status": "failed", "products": fail_at - 1},
                "Failed target manifest persisted",
            )
            controls = list((root / "control").rglob(f"{args.run_id}.json"))
            require(
                controls
                and all(
                    json.loads(p.read_bytes())["status"] == "failed" for p in controls
                ),
                "All persisted target controls failed",
            )
            try:
                load_gold(EVIDENCE / "objects/nyc311", args.run_id, mode="published")
            except gold.GoldError:
                report["failed_target_load_rejected"] = True
            else:
                raise AssertionError("Failed Gold target was consumable")
            load_gold(
                EVIDENCE / "objects/nyc311", benchmark["ORIGINAL_RUN"], mode="published"
            )
            report["outcome"] = "expected_native_failure_closed"
        require(
            not any(Path(path).exists() for path in report["workspaces"]),
            "Invocation workspaces removed",
        )
        require(sampler.error is None, "Tmp sampler succeeded")
    report["elapsed"]["validation_seconds"] = time.perf_counter() - checking


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--case",
        required=True,
        choices=("positive", "negative", "gold", "fail-first", "fail-late"),
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--production", action="store_true")
    parser.add_argument(
        "--observed",
        action="store_true",
        default=True,
        help="Mandatory host strace handshake (always enabled)",
    )
    args = parser.parse_args(argv)
    if not re.fullmatch(rf"nospill-{args.case}-[A-Za-z0-9][A-Za-z0-9_-]*", args.run_id):
        parser.error(
            "--run-id must be nospill-CASE-TRIAL with a fresh safe trial label"
        )
    return args


def main(argv=None):
    args = parse_args(argv)
    started = time.perf_counter()
    benchmark = runpy.run_path("/checks/benchmark_lambda.py")
    report = {
        "case": args.case,
        "run_id": args.run_id,
        "status": "failed",
        "experimental_offline_86_only": True,
        "production_policy_verification": args.production,
        "host_trace_verdict": "required separately; JSON never proves no spill",
        "expectation": {
            "positive": "350000 rows and physical native spill",
            "negative": "native OOM, never permission denial or success",
            "gold": "four complete reference-equivalent Gold products",
            "fail-first": "native OOM; failed target; zero uploaded products",
            "fail-late": "native OOM; failed target; one uploaded product",
        }[args.case],
        "elapsed": {},
    }
    protected = baseline = None
    try:
        benchmark["guard_network"]()
        os.environ.update(
            HOME="/tmp/home", TMPDIR="/tmp", PYTHONTZPATH="/var/task/timezone"
        )
        Path("/tmp/home").mkdir(exist_ok=True)
        Path("/tmp/work").mkdir(exist_ok=True)
        os.chdir("/tmp/work")
        sys.path.insert(0, str(TASK))
        (EVIDENCE / "phases").mkdir()
        require(
            not (EVIDENCE / "go").exists()
            and not (EVIDENCE / "ready.json").exists()
            and not (EVIDENCE / "report.json").exists(),
            "Fresh handshake and evidence paths",
        )
        report["ptrace_permission"] = trace_permission()
        require(
            report["ptrace_permission"]["result"] == 0,
            "PR_SET_PTRACER_ANY permission failed",
        )
        with (EVIDENCE / "ready.json").open("x") as stream:
            json.dump(
                {
                    "pid": os.getpid(),
                    "case": args.case,
                    "run_id": args.run_id,
                    "cwd": os.getcwd(),
                    "native_imports_started": False,
                    "production_policy_verification": args.production,
                    "ptrace_permission": report["ptrace_permission"],
                },
                stream,
            )
        wait_for_go()
        with phase("setup"):
            release = platform.freedesktop_os_release()
            require(
                sys.version_info[:2] == (3, 12)
                and sys.flags.isolated
                and sys.flags.no_site
                and sys.dont_write_bytecode
                and platform.machine() == "x86_64"
                and Path(sys.executable).resolve().is_relative_to(Path("/var/lang")),
                "Isolated image CP312 x86_64 -I -S -B",
            )
            require(
                release.get("ID") == "amzn" and release.get("VERSION_ID") == "2023",
                "Official AL2023 environment",
            )
            for path in (
                TASK,
                INPUTS,
                SOURCE,
                Path("/checks/benchmark_lambda.py"),
                Path("/checks/check_lambda_runtime.py"),
            ):
                require(
                    os.statvfs(path).f_flag & os.ST_RDONLY,
                    "RO payload, inputs and checks",
                )
            stat = os.statvfs("/tmp")
            mounts = Path("/proc/self/mountinfo").read_text().splitlines()
            require(
                stat.f_blocks * stat.f_frsize == 512 * 1024**2
                and any(
                    line.split()[4] == "/tmp" and " - tmpfs " in line for line in mounts
                ),
                "Private 512 MiB tmpfs",
            )
            report["resources_before_imports"] = resources = benchmark["resources"]()
            benchmark["verify_profile_cgroup"](
                {"memory_mib": 2048, "benchmark_cpus": 1}, resources["cgroup"]
            )
            report["writable_setup"] = {}
            for path in (
                EVIDENCE / "writable",
                Path("/tmp/writable"),
                Path(".writable"),
                Path("/dev/shm/writable"),
            ):
                try:
                    with path.open("xb") as stream:
                        stream.write(b"x")
                    path.unlink()
                    report["writable_setup"][str(path)] = True
                except OSError as error:
                    if str(path) != "/dev/shm/writable" or error.errno not in (
                        errno.EPERM,
                        errno.EACCES,
                        errno.EROFS,
                    ):
                        raise
                    report["writable_setup"][str(path)] = {
                        "not_permitted_errno": error.errno
                    }
            digest = benchmark["file_digest"]
            protected = {str(root): hashes(root, digest) for root in (INPUTS, TASK)}
            seed_check(args, report, benchmark)
            runtime = benchmark["read_json"](INPUTS, "manifest.json")["runtime_profile"]
            require(
                digest(TASK / "nyc311/lambda_runtime.py") == runtime["module_sha256"],
                "Approved packaged runtime source SHA-256",
            )
            report["environment"] = {
                "python": sys.version,
                "glibc": platform.libc_ver(),
                "machine": platform.machine(),
                "os_release": release,
                "fixture_manifest_sha256": digest(INPUTS / "manifest.json"),
                "probe_sha256": digest(SOURCE),
                "helper_sha256": {
                    name: digest(Path("/checks") / name)
                    for name in ("benchmark_lambda.py", "check_lambda_runtime.py")
                },
                "expected_image_digest_host_verified": benchmark["IMAGE_DIGEST"],
            }
            baseline = snapshots()
            report["snapshots_before_imports"] = baseline
            helper = runpy.run_path("/checks/check_lambda_runtime.py")
        imported = time.perf_counter()
        with phase("imports"):
            for name in helper["SDK"]:
                importlib.import_module(name)
            benchmark["guard_sdk"]()
            for name in helper["REQUIRED"]["gold"] + ("nyc311.lambda_gold",):
                importlib.import_module(name)
            import _duckdb
            import duckdb

            report["origins"] = origins(benchmark)
            require(
                all(
                    name in report["origins"]["payload_modules"]
                    for name in helper["SDK"]
                    + helper["REQUIRED"]["gold"]
                    + ("nyc311.lambda_gold",)
                ),
                "All required runtime modules from payload",
            )
            binary = TASK / "_duckdb.cpython-312-x86_64-linux-gnu.so"
            require(
                duckdb.__version__ == _duckdb.__version__ == "1.5.6"
                and Path(_duckdb.__file__).resolve() == binary,
                "Exact native DuckDB 1.5.6 payload",
            )
            report["native"] = {
                "version": duckdb.__version__,
                "binary": str(binary),
                "actual_binary_sha256": digest(binary),
            }
            if args.production:
                require(
                    digest(binary) == NATIVE_SHA256, "Approved unchanged native SHA-256"
                )
        report["elapsed"]["cold_import_seconds"] = time.perf_counter() - imported
        report["resources_before_work"] = benchmark["resources"]()
        if args.case in ("positive", "negative"):
            synthetic(args, report, duckdb, helper, benchmark)
        else:
            gold_work(args, report, helper, benchmark)
        report["status"] = "passed"
    except Exception as error:
        report["failure"] = benchmark["failure"](error)
    finally:
        try:
            with phase("cleanup"):
                report["origins_final"] = origins(benchmark)
                report["snapshots_after"] = snapshots()
                if baseline is not None:
                    for root in ("/tmp", "/dev/shm"):
                        require(
                            report["snapshots_after"][root] == baseline[root],
                            "No leftover or removed tmp/shm paths",
                        )
                if protected is not None:
                    require(
                        all(
                            hashes(Path(root), benchmark["file_digest"]) == before
                            for root, before in protected.items()
                        ),
                        "All original clones and packaged code/dependencies unchanged",
                    )
                    report["protected_inputs_and_payload_unchanged"] = True
                if "seed_sha256" in report:
                    root = EVIDENCE / "objects/nyc311/published"
                    require(
                        all(
                            benchmark["file_digest"](root / key) == digest
                            for key, digest in report["seed_sha256"].items()
                        ),
                        "Every seed and old successful Gold object preserved",
                    )
                    report["seeds_and_old_gold_unchanged"] = True
        except Exception as error:
            report["status"] = "failed"
            report["cleanup_failure"] = benchmark["failure"](error)
        report["elapsed"]["total_seconds_including_handshake"] = (
            time.perf_counter() - started
        )
        report["resources_final"] = benchmark["resources"]()
        try:
            with (EVIDENCE / "report.json").open("x") as stream:
                json.dump(report, stream, indent=2, allow_nan=False)
                stream.write("\n")
        except Exception as error:
            report["status"] = "failed"
            report["evidence_failure"] = benchmark["failure"](error)
        print(
            json.dumps(
                {
                    key: report.get(key)
                    for key in ("case", "run_id", "status", "outcome")
                }
                | {"report": "/evidence/report.json"},
                separators=(",", ":"),
            )
        )
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
