"""Stdlib mock contracts only. No Docker, native imports or production mutations."""

import ctypes
import hashlib
import io
import json
import os
import runpy
import stat
import sys
import unittest
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from itertools import product
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, Mock, call, patch

ROOT = Path(__file__).resolve().parents[1]
PROBE = runpy.run_path(str(ROOT / "scripts/spike_duckdb_no_spill.py"))
BENCHMARK = runpy.run_path(str(ROOT / "scripts/benchmark_lambda.py"))
HELPER = runpy.run_path(str(ROOT / "scripts/check_lambda_runtime.py"))
RUNNER = runpy.run_path(str(ROOT / "scripts/run_no_spill_spike.py"))


class NativeOOM(Exception):
    pass


class FakeSampler:
    def __init__(self):
        self.error = None
        self.thread = Mock()
        self.sample()

    def sample(self):
        return {"allocated_bytes": 0}

    def finish(self):
        return {"sampler_error": self.error}


class NoSpillProbeTests(unittest.TestCase):
    def test_cli_requires_fresh_case_label_and_cannot_disable_observation(self):
        parse = PROBE["parse_args"]
        for case in ("positive", "negative", "gold", "fail-first", "fail-late"):
            argv = ["--case", case, "--run-id", f"nospill-{case}-trial1"]
            self.assertTrue(parse(argv).observed)
            self.assertTrue(parse(argv + ["--observed"]).observed)
            self.assertFalse(parse(argv).production)
            self.assertTrue(parse(argv + ["--production"]).production)
        for argv in (
            ["--case", "gold", "--run-id", "september-2026-001"],
            ["--case", "gold", "--run-id", "nospill-gold-../escape"],
            ["--case", "gold", "--run-id", "nospill-negative-one"],
            ["--case", "gold", "--run-id", "nospill-gold-one", "--no-observed"],
        ):
            with patch("sys.stderr", new_callable=io.StringIO):
                with self.assertRaises(SystemExit):
                    parse(argv)

    def test_prctl_uses_image_glibc_unsigned_any_and_captures_errno(self):
        permission = PROBE["trace_permission"]
        for result, errno in ((0, 0), (-1, 1)):
            libc = SimpleNamespace(prctl=Mock(return_value=result))
            with (
                patch("ctypes.CDLL", return_value=libc) as load,
                patch("ctypes.set_errno") as clear,
                patch("ctypes.get_errno", return_value=errno),
            ):
                observed = permission()
            load.assert_called_once_with("libc.so.6", use_errno=True)
            clear.assert_called_once_with(0)
            self.assertEqual(libc.prctl.argtypes, [ctypes.c_int] + [ctypes.c_ulong] * 4)
            self.assertIs(libc.prctl.restype, ctypes.c_int)
            args = libc.prctl.call_args.args
            self.assertEqual(args[0], 0x59616D61)
            self.assertEqual(args[1].value, ctypes.c_ulong(-1).value)
            self.assertEqual(args[2:], (0, 0, 0))
            self.assertEqual((observed["result"], observed["errno"]), (result, errno))

    def test_go_poll_interval_timeout_and_no_network_or_process_launch(self):
        wait = PROBE["wait_for_go"]
        with (
            patch("pathlib.Path.is_file", side_effect=[False, False, True]),
            patch("time.monotonic", side_effect=[0, 0.01, 0.03]),
            patch("time.sleep") as sleep,
        ):
            wait()
            self.assertEqual(sleep.call_args_list, [call(0.02), call(0.02)])
        with (
            patch("pathlib.Path.is_file", return_value=False),
            patch("time.monotonic", side_effect=[0, 120]),
            patch("time.sleep") as sleep,
        ):
            with self.assertRaisesRegex(AssertionError, "120 seconds"):
                wait()
            sleep.assert_not_called()

    def test_phase_markers_are_empty_created_deleted_even_on_failure(self):
        phase = PROBE["phase"]
        with TemporaryDirectory() as directory:
            evidence = Path(directory)
            (evidence / "phases").mkdir()
            unlink = Path.unlink
            markers = []

            def remove(path):
                self.assertEqual(path.stat().st_size, 0)
                markers.append(path.name)
                unlink(path)

            with (
                patch.dict(phase.__wrapped__.__globals__, {"EVIDENCE": evidence}),
                patch.object(Path, "unlink", autospec=True, side_effect=remove),
            ):
                with self.assertRaisesRegex(RuntimeError, "test"):
                    with phase("query"):
                        raise RuntimeError("test")
            self.assertEqual(markers, ["query.begin", "query.end"])
            self.assertEqual(list((evidence / "phases").iterdir()), [])

    def test_origin_exception_is_only_exact_own_main_not_external_modules(self):
        audit = PROBE["origins"]
        own = str(PROBE["SOURCE"])
        with patch.dict(sys.modules, {"__main__": SimpleNamespace(__file__=own)}):
            benchmark = {
                "module_origins": lambda: {
                    "payload_modules": {},
                    "violations": {"__main__": own},
                }
            }
            self.assertEqual(audit(benchmark)["violations"], {})
            for violations in (
                {"__main__": own, "external": "/outside/foreign.py"},
                {"__main__": "/checks/foreign.py"},
                {"__main__": own, "__mp_main__": own},
            ):
                with self.assertRaisesRegex(AssertionError, "origins"):
                    audit(
                        {"module_origins": lambda: {"violations": deepcopy(violations)}}
                    )

    def test_native_wrapper_copies_config_returns_native_and_restores_factory(self):
        wrapper = PROBE["no_spill_connections"]
        for positional in (False, True):
            connection = Mock()
            connection.execute.return_value.fetchone.return_value = (
                "512.0 MiB",
                "256.0 MiB",
                1,
                "",
            )
            factory = Mock(return_value=connection)
            gold = SimpleNamespace(duckdb=SimpleNamespace(connect=factory))
            config = PROBE["LIMITS"] | {
                "temp_directory": "/tmp/nyc311-test/duckdb-spill"
            }
            before = deepcopy(config)
            report = {"workspaces": ["/tmp/nyc311-test"]}
            with patch.dict(
                wrapper.__wrapped__.__globals__, {"phase": lambda name: nullcontext()}
            ):
                with wrapper(gold, report, BENCHMARK):
                    for _ in range(4):
                        if positional:
                            result = gold.duckdb.connect(":memory:", False, config)
                        else:
                            result = gold.duckdb.connect(":memory:", config=config)
                        self.assertIs(result, connection)
            self.assertIs(gold.duckdb.connect, factory)
            self.assertEqual(config, before)
            connection.close.assert_not_called()
            for invoked in factory.call_args_list:
                native_config = (
                    invoked.args[2] if positional else invoked.kwargs["config"]
                )
                self.assertEqual(native_config, before | {"temp_directory": ""})
                self.assertIsNot(native_config, config)
            self.assertEqual(len(report["connections"]), 4)
            self.assertTrue(
                all(
                    row["effective"]["temp_directory"] == "" and not row["injected"]
                    for row in report["connections"]
                )
            )
            json.dumps(report)  # No live phase objects left in the report.

    def test_production_runner_flag_preserves_old_default(self):
        parse = RUNNER["parse_args"]
        argv = [
            "--work",
            "/tmp/opencode/nyc311-nospill-production-final86",
            "--case",
            "gold",
        ]
        self.assertFalse(parse(argv).production)
        self.assertTrue(parse(argv + ["--production"]).production)

    def test_production_runner_selects_ready_artifacts_without_fallback(self):
        main = RUNNER["main"]
        argv = [
            "--work",
            "/tmp/opencode/nyc311-nospill-production-final86",
            "--case",
            "gold",
            "--production",
        ]
        with (
            patch.dict(
                main.__globals__,
                {"digest": lambda path: "hash", "snapshot": lambda path: {}},
            ),
            patch("shutil.which", return_value="/usr/bin/strace"),
            patch.object(Path, "is_dir", autospec=True, return_value=True) as is_dir,
            patch.object(Path, "is_file", autospec=True, return_value=False) as is_file,
            patch.object(Path, "mkdir") as mkdir,
            patch("subprocess.run") as run,
        ):
            with self.assertRaisesRegex(ValueError, "ready ZIP/inputs missing"):
                main(argv)
        is_dir.assert_called_once_with(
            Path("/tmp/opencode/nyc311-benchmark-ready/inputs")
        )
        is_file.assert_called_once_with(
            Path("/tmp/opencode/nyc311-lambda-gold-ready/nyc311-gold.zip")
        )
        mkdir.assert_not_called()
        run.assert_not_called()

    def test_production_runner_rejects_historical_work_baseline_without_writes(self):
        main = RUNNER["main"]
        with (
            patch.dict(
                main.__globals__,
                {"digest": lambda path: "hash", "snapshot": lambda path: {}},
            ),
            patch("shutil.which", return_value="/usr/bin/strace"),
            patch.object(Path, "is_dir", return_value=True),
            patch.object(Path, "is_file", return_value=True),
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "read_text", return_value="{}"),
            patch.object(Path, "write_text") as write,
            patch("subprocess.run") as run,
        ):
            with self.assertRaisesRegex(AssertionError, "Never reuse a historical"):
                main(
                    [
                        "--work",
                        "/tmp/opencode/old-spike",
                        "--case",
                        "gold",
                        "--production",
                    ]
                )
        write.assert_not_called()
        run.assert_not_called()

    def test_production_native_observer_never_rewrites_or_proxies(self):
        wrapper = PROBE["no_spill_connections"]
        for positional in (False, True):
            for fault in (False, True):
                connection = Mock()
                connection.execute.return_value.fetchone.return_value = (
                    "512.0 MiB",
                    1,
                    "",
                )
                factory = Mock(return_value=connection)
                gold = SimpleNamespace(duckdb=SimpleNamespace(connect=factory))
                config = PROBE["PRODUCTION_LIMITS"] | (
                    {"memory_limit": "1B"} if fault else {}
                )
                before = dict(config)
                report = {}
                with patch.dict(
                    wrapper.__wrapped__.__globals__,
                    {"phase": lambda name: nullcontext()},
                ):
                    with wrapper(gold, report, BENCHMARK, int(fault), production=True):
                        for _ in range(1 if fault else 4):
                            if positional:
                                result = gold.duckdb.connect(":memory:", False, config)
                            else:
                                result = gold.duckdb.connect(":memory:", config=config)
                            self.assertIs(result, connection)
                            invoked = factory.call_args
                            self.assertIs(
                                invoked.args[2]
                                if positional
                                else invoked.kwargs["config"],
                                config,
                            )
                self.assertIs(gold.duckdb.connect, factory)
                self.assertEqual(config, before)
                connection.close.assert_not_called()
                for row in report["connections"]:
                    self.assertEqual(row["requested_config"], row["native_config"])
                    self.assertFalse(row["config_rewritten"])
                    self.assertTrue(row["native_kwargs_unchanged"])
                    if fault:
                        connection.execute.assert_not_called()
                        self.assertNotIn("effective", row)
                        self.assertIn(
                            "declared config only", row["effective_unavailable"]
                        )
                    else:
                        self.assertEqual(
                            set(row["effective"]), set(PROBE["PRODUCTION_LIMITS"])
                        )
                        self.assertNotIn(
                            "max_temp_directory_size",
                            connection.execute.call_args.args[0],
                        )
                json.dumps(report)

    def test_production_native_observer_rejects_empty_partial_or_spill_configs(self):
        wrapper = PROBE["no_spill_connections"]
        for config in (
            {},
            {"threads": 1},
            PROBE["PRODUCTION_LIMITS"] | {"temp_directory": "/tmp/spill"},
            PROBE["PRODUCTION_LIMITS"] | {"max_temp_directory_size": "256MiB"},
        ):
            factory = Mock()
            gold = SimpleNamespace(duckdb=SimpleNamespace(connect=factory))
            with wrapper(gold, {}, BENCHMARK, production=True):
                with self.assertRaisesRegex(AssertionError, "three-key"):
                    gold.duckdb.connect(":memory:", config=config)
            factory.assert_not_called()
            self.assertIs(gold.duckdb.connect, factory)

    def test_production_negative_uses_declared_policy_without_unused_temp_cap(self):
        synthetic = PROBE["synthetic"]
        runtime = ModuleType("nyc311.lambda_runtime")
        runtime.DUCKDB_LIMITS = dict(PROBE["PRODUCTION_LIMITS"])
        connection = Mock()
        connection.execute.side_effect = [
            SimpleNamespace(fetchone=lambda: ("32.0 MiB", 1, "")),
            NativeOOM("native allocation failure"),
        ]
        duckdb = SimpleNamespace(
            connect=Mock(return_value=connection), OutOfMemoryException=NativeOOM
        )
        report = {"elapsed": {}}
        with (
            patch.dict(sys.modules, {"nyc311.lambda_runtime": runtime}),
            patch.dict(
                synthetic.__globals__,
                {"phase": lambda name: nullcontext(), "inventory": lambda root: {}},
            ),
            patch.object(Path, "exists", return_value=False),
        ):
            synthetic(
                SimpleNamespace(case="negative", production=True),
                report,
                duckdb,
                {"TmpSampler": FakeSampler},
                BENCHMARK,
            )
        self.assertEqual(
            duckdb.connect.call_args.kwargs["config"],
            runtime.DUCKDB_LIMITS | {"memory_limit": "32MiB"},
        )
        self.assertEqual(runtime.DUCKDB_LIMITS, PROBE["PRODUCTION_LIMITS"])
        self.assertEqual(
            report["query_sha256"], hashlib.sha256(PROBE["QUERY"].encode()).hexdigest()
        )
        self.assertFalse(
            report["policy_provenance"]["config_rewritten_at_native_connect"]
        )

    def test_multiprocessing_alias_is_allowed_only_for_the_exact_main_object(self):
        audit = PROBE["origins"]
        own = str(PROBE["SOURCE"])
        main = SimpleNamespace(__file__=own)
        with patch.dict(sys.modules, {"__main__": main, "__mp_main__": main}):
            observed = audit(
                {
                    "module_origins": lambda: {
                        "violations": {"__main__": own, "__mp_main__": own},
                        "payload_modules": {},
                    }
                }
            )
        self.assertEqual(observed["violations"], {})

    def test_failure_injection_is_native_config_not_a_fake_raised_exception(self):
        wrapper = PROBE["no_spill_connections"]
        for fail_at in (1, 2):
            for boundary in ("connect", "settings"):
                connection = Mock()
                connection.execute.return_value.fetchone.return_value = (
                    "512.0 MiB",
                    "256.0 MiB",
                    1,
                    "",
                )
                native_error = NativeOOM("native allocation failure")

                def native(*args, **kwargs):
                    if kwargs["config"]["memory_limit"] == "1B":
                        if boundary == "connect":
                            raise native_error
                        connection.execute.side_effect = native_error
                    return connection

                factory = Mock(side_effect=native)
                gold = SimpleNamespace(duckdb=SimpleNamespace(connect=factory))
                config = PROBE["LIMITS"] | {
                    "temp_directory": "/tmp/nyc311-test/duckdb-spill"
                }
                report = {"workspaces": ["/tmp/nyc311-test"]}
                with patch.dict(
                    wrapper.__wrapped__.__globals__,
                    {"phase": lambda name: nullcontext()},
                ):
                    with self.assertRaises(NativeOOM) as caught:
                        with wrapper(gold, report, BENCHMARK, fail_at):
                            for _ in range(fail_at):
                                gold.duckdb.connect(":memory:", config=config)
                self.assertIs(caught.exception, native_error)
                self.assertIs(gold.duckdb.connect, factory)
                self.assertEqual(len(report["connections"]), fail_at)
                self.assertEqual(
                    report["connections"][-1]["native_config"],
                    config | {"memory_limit": "1B", "temp_directory": ""},
                )
                self.assertEqual(
                    connection.close.call_count, int(boundary == "settings")
                )
                self.assertEqual(config["memory_limit"], "512MiB")

    def test_native_wrapper_rejects_ignored_live_settings_and_closes(self):
        wrapper = PROBE["no_spill_connections"]
        connection = Mock()
        connection.execute.return_value.fetchone.return_value = (
            "512.0 MiB",
            "256.0 MiB",
            1,
            "/tmp/fallback",
        )
        factory = Mock(return_value=connection)
        gold = SimpleNamespace(duckdb=SimpleNamespace(connect=factory))
        report = {"workspaces": ["/tmp/nyc311-test"]}
        with patch.dict(
            wrapper.__wrapped__.__globals__, {"phase": lambda name: nullcontext()}
        ):
            with self.assertRaisesRegex(AssertionError, "live no-spill"):
                with wrapper(gold, report, BENCHMARK):
                    gold.duckdb.connect(
                        ":memory:",
                        config=PROBE["LIMITS"]
                        | {"temp_directory": "/tmp/nyc311-test/duckdb-spill"},
                    )
        connection.close.assert_called_once()
        self.assertIs(gold.duckdb.connect, factory)

    def test_synthetic_query_and_negative_verdict_are_fail_closed(self):
        synthetic = PROBE["synthetic"]
        for error in (
            NativeOOM("No temporary directory; spilling disabled"),
            NativeOOM("another native allocation description"),
            RuntimeError("wrong type"),
            NativeOOM("PermissionDenied Errno13"),
            None,
        ):
            connection = Mock()
            reader = MagicMock()
            reader.__enter__.return_value = [SimpleNamespace(num_rows=350000)]

            def execute(query):
                if query == PROBE["QUERY"]:
                    if error:
                        raise error
                    return SimpleNamespace(to_arrow_reader=Mock(return_value=reader))
                return SimpleNamespace(
                    fetchone=lambda: ("32.0 MiB", "256.0 MiB", 1, "")
                )

            connection.execute.side_effect = execute
            duckdb = SimpleNamespace(
                connect=Mock(return_value=connection), OutOfMemoryException=NativeOOM
            )
            report = {"elapsed": {}}
            with (
                patch.dict(
                    synthetic.__globals__,
                    {"phase": lambda name: nullcontext(), "inventory": lambda root: {}},
                ),
                patch.object(Path, "exists", return_value=False),
            ):
                if isinstance(error, NativeOOM) and "Permission" not in str(error):
                    synthetic(
                        SimpleNamespace(case="negative"),
                        report,
                        duckdb,
                        {"TmpSampler": FakeSampler},
                        BENCHMARK,
                    )
                    self.assertEqual(
                        report["outcome"], "expected_native_resource_rejection"
                    )
                    self.assertEqual(report["expected_error"]["message"], str(error))
                else:
                    with self.assertRaises(AssertionError):
                        synthetic(
                            SimpleNamespace(case="negative"),
                            report,
                            duckdb,
                            {"TmpSampler": FakeSampler},
                            BENCHMARK,
                        )
            connection.close.assert_called_once()
            self.assertEqual(
                duckdb.connect.call_args.kwargs["config"],
                PROBE["LIMITS"] | {"memory_limit": "32MiB", "temp_directory": ""},
            )
            self.assertIn("query_seconds", report["elapsed"])
            self.assertIn("resources_after_work", report)

    def test_positive_requires_physical_regular_file_blocks_and_350000_rows(self):
        synthetic = PROBE["synthetic"]
        for blocks in (0, 8192):
            connection, reader = Mock(), MagicMock()
            reader.__enter__.return_value = [SimpleNamespace(num_rows=350000)]
            connection.execute.side_effect = [
                SimpleNamespace(
                    fetchone=lambda: ("32.0 MiB", "256.0 MiB", 1, "/tmp/positive-spill")
                ),
                SimpleNamespace(to_arrow_reader=Mock(return_value=reader)),
            ]
            files = {
                "spill": [stat.S_IFREG, 100000, blocks],
                "dir": [stat.S_IFDIR, 4096, 4096],
            }
            report = {"elapsed": {}}
            with (
                patch.dict(
                    synthetic.__globals__,
                    {
                        "phase": lambda name: nullcontext(),
                        "inventory": Mock(side_effect=[files, files, files, {}]),
                    },
                ),
                patch.object(Path, "exists", return_value=False),
                patch.object(Path, "mkdir"),
                patch.object(Path, "rmdir") as remove,
            ):
                args = (
                    SimpleNamespace(case="positive"),
                    report,
                    SimpleNamespace(connect=Mock(return_value=connection)),
                    {"TmpSampler": FakeSampler},
                    BENCHMARK,
                )
                if blocks:
                    synthetic(*args)
                    self.assertEqual(report["outcome"], "completed_with_physical_spill")
                else:
                    with self.assertRaisesRegex(AssertionError, "physical spill"):
                        synthetic(*args)
                remove.assert_called_once()
            self.assertEqual(report["physical_spill_peak_bytes_lower_bound"], blocks)
            connection.close.assert_called_once()

    def test_prctl_failure_reports_errno_exits_nonzero_and_never_imports_payload(self):
        main = PROBE["main"]
        benchmark = BENCHMARK | {"guard_network": Mock(), "resources": lambda: {}}
        with TemporaryDirectory() as directory:
            evidence = Path(directory)
            (evidence / "phases").mkdir()
            with (
                patch.dict(
                    main.__globals__,
                    {
                        "EVIDENCE": evidence,
                        "trace_permission": lambda: {"result": -1, "errno": 1},
                        "origins": lambda benchmark: {},
                    },
                ),
                patch("runpy.run_path", return_value=benchmark),
                patch("pathlib.Path.mkdir"),
                patch("os.chdir"),
                patch.dict("os.environ"),
                patch.object(sys, "path", list(sys.path)),
                patch("sys.stdout", new_callable=io.StringIO) as stdout,
                patch("importlib.import_module") as native_import,
            ):
                result = main(
                    ["--case", "negative", "--run-id", "nospill-negative-one"]
                )
            self.assertEqual(result, 1)
            native_import.assert_not_called()
            self.assertFalse((evidence / "ready.json").exists())
            report = json.loads((evidence / "report.json").read_bytes())
            self.assertEqual(report["ptrace_permission"]["errno"], 1)
            self.assertEqual(
                report["failure"]["check"], "PR_SET_PTRACER_ANY permission failed"
            )
            self.assertEqual(json.loads(stdout.getvalue())["status"], "failed")

    def test_failure_publication_audit_delegates_progress_and_rejects_complete(self):
        work = PROBE["gold_work"]
        for case, production in product(("fail-first", "fail-late"), (False, True)):
            for corrupt_complete in (False, True):
                with (
                    self.subTest(
                        case=case,
                        corrupt_complete=corrupt_complete,
                        production=production,
                    ),
                    TemporaryDirectory() as directory,
                ):
                    evidence = Path(directory)
                    root = evidence / "objects/nyc311/published"
                    (root / "control/gold").mkdir(parents=True)
                    run_id = f"nospill-{case}-one"
                    fail_at = 1 if case == "fail-first" else 2
                    native_error = NativeOOM("controlled native error")
                    common = ModuleType("nyc311.lambda_common")
                    common.create_s3_client = Mock()
                    handler_module = ModuleType("nyc311.lambda_gold")
                    task = evidence / "payload" if production else PROBE["TASK"]
                    handler_module.SQL_DIRECTORY = task / "sql"
                    handler_module.temporary_workspace = Mock()
                    gold = ModuleType("nyc311.gold")
                    gold.duckdb = SimpleNamespace(
                        OutOfMemoryException=KeyError if production else NativeOOM
                    )
                    gold.GoldError = type("GoldError", (Exception,), {})
                    runtime = ModuleType("nyc311.lambda_runtime")
                    runtime.DUCKDB_LIMITS = dict(PROBE["PRODUCTION_LIMITS"])
                    if production:
                        (task / "sql").mkdir(parents=True)
                        for name in ("demand_daily", "closure_duration"):
                            (task / "sql" / f"{name}.sql").write_text("frozen SQL")
                        gold.__file__ = str(task / "gold.py")
                        Path(gold.__file__).write_text(
                            "def _compute_query(table, product, query, *, duckdb_config):\n"
                            "    if duckdb_config['memory_limit'] == '1B':\n"
                            "        return connection.execute(query)\n"
                            "    return 'prepared'\n"
                        )

                    def publish(*args, on_progress, **kwargs):
                        manifest = {"status": "incomplete", "products": {}}
                        on_progress(manifest)
                        try:
                            for product in ("demand_daily", "closure_duration")[
                                :fail_at
                            ]:
                                store._compute_query(
                                    "original-input",
                                    product,
                                    "frozen SQL",
                                    duckdb_config=dict(runtime.DUCKDB_LIMITS)
                                    if production
                                    else {"threads": 1},
                                )
                                key = f"gold/run={run_id}/{product}/part.parquet"
                                manifest["products"][product] = {"key": key}
                                on_progress(manifest)
                        except (NativeOOM, KeyError):
                            manifest["status"] = (
                                "complete" if corrupt_complete else "failed"
                            )
                            on_progress(manifest)
                            raise

                    handler_module.publish_gold = publish
                    delegated = []

                    def handler(event, context):
                        client = common.create_s3_client()

                        def progress(manifest):
                            delegated.append(deepcopy(manifest))
                            if manifest["status"] == "incomplete":
                                for descriptor in manifest["products"].values():
                                    client.put_object(
                                        Bucket="fixture-bucket",
                                        Key=f"nyc311/published/{descriptor['key']}",
                                        Body=b"prepared",
                                    )
                            client.put_object(
                                Bucket="fixture-bucket",
                                Key=f"nyc311/published/control/gold/{run_id}.json",
                                Body=json.dumps(manifest).encode(),
                            )

                        return handler_module.publish_gold(on_progress=progress)

                    handler_module.handler = handler
                    package = ModuleType("nyc311")
                    package.gold, package.lambda_common, package.lambda_gold = (
                        gold,
                        common,
                        handler_module,
                    )
                    store = ModuleType("nyc311.gold_store")

                    def load(base, target, **kwargs):
                        if target == run_id:
                            raise gold.GoldError("Only complete runs can be consumed")
                        return {}, {}

                    store.load_gold = Mock(side_effect=load)
                    original_compute = store._compute_query = Mock(
                        side_effect=(["prepared"] if fail_at == 2 else [])
                        + [native_error]
                    )
                    if production:
                        # A builtin raises without extra Python frames, as native execute does.
                        namespace = {
                            "connection": SimpleNamespace(execute={}.__getitem__)
                        }
                        exec(
                            compile(
                                Path(gold.__file__).read_text(), gold.__file__, "exec"
                            ),
                            namespace,
                        )
                        original_compute = store._compute_query = Mock(
                            wraps=namespace["_compute_query"]
                        )
                    report = {"elapsed": {}, "seed_sha256": {}}
                    phases = []

                    @contextmanager
                    def observed_phase(name):
                        phases.append((name, "begin"))
                        try:
                            yield
                        finally:
                            phases.append((name, "end"))

                    def connections(gold, report, benchmark, fail_at, **options):
                        self.assertEqual(
                            options, {"production": True} if production else {}
                        )
                        report["connections"] = [
                            {"injected": i == fail_at} for i in range(1, fail_at + 1)
                        ]
                        return nullcontext()

                    factory, workspace = (
                        common.create_s3_client,
                        handler_module.temporary_workspace,
                    )
                    with (
                        patch.dict(
                            sys.modules,
                            {
                                "nyc311": package,
                                "nyc311.lambda_common": common,
                                "nyc311.lambda_gold": handler_module,
                                "nyc311.gold": gold,
                                "nyc311.gold_store": store,
                                "nyc311.lambda_runtime": runtime,
                                "botocore.exceptions": SimpleNamespace(
                                    ClientError=Exception
                                ),
                            },
                        ),
                        patch.dict(
                            work.__globals__,
                            {
                                "EVIDENCE": evidence,
                                "phase": observed_phase,
                                "no_spill_connections": connections,
                                "TASK": task,
                            },
                        ),
                    ):
                        if corrupt_complete:
                            with self.assertRaisesRegex(
                                AssertionError, "No complete authorization"
                            ):
                                work(
                                    SimpleNamespace(
                                        case=case, run_id=run_id, production=production
                                    ),
                                    report,
                                    HELPER | {"TmpSampler": FakeSampler},
                                    BENCHMARK,
                                )
                        else:
                            work(
                                SimpleNamespace(
                                    case=case, run_id=run_id, production=production
                                ),
                                report,
                                HELPER | {"TmpSampler": FakeSampler},
                                BENCHMARK,
                            )
                            self.assertEqual(
                                report["outcome"], "expected_native_failure_closed"
                            )
                            self.assertTrue(report["failed_target_load_rejected"])
                            self.assertEqual(store.load_gold.call_count, 2)
                            if production:
                                self.assertEqual(
                                    report["native_oom_boundary"],
                                    "business_execute_query",
                                )
                                self.assertEqual(
                                    report["expected_error"]["traceback"][-1]["line"], 3
                                )
                                self.assertEqual(
                                    report["fault_injection"]["declared_config"],
                                    runtime.DUCKDB_LIMITS | {"memory_limit": "1B"},
                                )
                                self.assertEqual(
                                    runtime.DUCKDB_LIMITS, PROBE["PRODUCTION_LIMITS"]
                                )
                    self.assertEqual(
                        [m["status"] for m in delegated],
                        [p["status"] for p in report["progress"]],
                    )
                    self.assertIs(common.create_s3_client, factory)
                    self.assertIs(handler_module.temporary_workspace, workspace)
                    self.assertIs(handler_module.publish_gold, publish)
                    self.assertIs(store._compute_query, original_compute)
                    products = ("demand_daily", "closure_duration")[:fail_at]
                    self.assertEqual(
                        original_compute.call_args_list,
                        [
                            call(
                                "original-input",
                                product,
                                "frozen SQL",
                                duckdb_config=(
                                    runtime.DUCKDB_LIMITS
                                    | ({"memory_limit": "1B"} if i == fail_at else {})
                                )
                                if production
                                else {"threads": 1},
                            )
                            for i, product in enumerate(products, 1)
                        ],
                    )
                    self.assertEqual(
                        report["strict_compute_windows"],
                        [
                            {"phase": f"native-product-{index}", "product": product}
                            for index, product in enumerate(products, 1)
                        ],
                    )
                    for index in range(1, fail_at + 1):
                        self.assertEqual(
                            [
                                event
                                for event in phases
                                if event[0] == f"native-product-{index}"
                            ],
                            [
                                (f"native-product-{index}", "begin"),
                                (f"native-product-{index}", "end"),
                            ],
                        )
                    self.assertTrue(
                        all(entry["persisted"] for entry in report["put_attempts"])
                    )

    def test_ready_precedes_go_and_native_imports_with_network_guard_and_home(self):
        main = PROBE["main"]
        guard = Mock()
        benchmark = BENCHMARK | {"guard_network": guard, "resources": lambda: {}}
        with TemporaryDirectory() as directory:
            evidence = Path(directory)
            (evidence / "phases").mkdir()

            def permission():
                self.assertFalse((evidence / "ready.json").exists())
                return {"result": 0, "errno": 0}

            def wait():
                guard.assert_called_once()
                ready = json.loads((evidence / "ready.json").read_bytes())
                self.assertEqual(ready["pid"], os.getpid())
                self.assertFalse(ready["native_imports_started"])
                self.assertTrue(ready["production_policy_verification"])
                self.assertEqual(os.environ["HOME"], "/tmp/home")
                self.assertEqual(os.environ["TMPDIR"], "/tmp")
                self.assertEqual(sys.path[0], "/var/task")
                native_import.assert_not_called()
                raise RuntimeError("Controlled test stop at GO barrier")

            with (
                patch.dict(
                    main.__globals__,
                    {
                        "EVIDENCE": evidence,
                        "trace_permission": permission,
                        "wait_for_go": wait,
                        "origins": lambda benchmark: {},
                    },
                ),
                patch("runpy.run_path", return_value=benchmark),
                patch("pathlib.Path.mkdir"),
                patch("os.chdir") as chdir,
                patch.dict("os.environ"),
                patch.object(sys, "path", list(sys.path)),
                patch("sys.stdout", new_callable=io.StringIO),
                patch("importlib.import_module") as native_import,
            ):
                self.assertEqual(
                    main(
                        [
                            "--case",
                            "gold",
                            "--run-id",
                            "nospill-gold-one",
                            "--production",
                        ]
                    ),
                    1,
                )
                chdir.assert_called_once_with("/tmp/work")
            native_import.assert_not_called()
            report = json.loads((evidence / "report.json").read_bytes())
            self.assertEqual(report["failure"]["type"], "RuntimeError")
            self.assertTrue(report["production_policy_verification"])


if __name__ == "__main__":
    unittest.main()
