"""Stdlib fakes only: no Docker, handlers, native libraries or volume data."""

import io
import json
import runpy
import sys
from copy import deepcopy
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNNER = runpy.run_path(str(ROOT / "scripts/run_lambda_benchmarks.py"))
PROBE = runpy.run_path(str(ROOT / "scripts/benchmark_lambda.py"))
CONSTANTS = {
    "PROFILES": {
        stage: {
            "memory_mib": memory,
            "timeout_seconds": 900,
            "temporary_storage_mib": 512,
            "benchmark_cpus": cpu,
        }
        for stage, memory, cpu in (
            ("ingest", 512, 0.25),
            ("curate", 1024, 0.5),
            ("gold", 2048, 1.0),
        )
    },
    "SOURCE_LIMITS": {
        "page_size": 5000,
        "max_attempts": 3,
        "connect_timeout": 5.0,
        "read_timeout": 30.0,
        "max_backoff": 10.0,
        "task_deadline": 600.0,
    },
    "SDK_LIMITS": {"connect_timeout": 5, "read_timeout": 20, "total_max_attempts": 3},
    "DUCKDB_LIMITS": {
        "memory_limit": "512MiB",
        "threads": 1,
        "temp_directory": "",
    },
    "APPLICATION_SECONDS": 720,
    "FAILURE_REPORTING_SECONDS": 60,
    "LAMBDA_RESERVE_SECONDS": 180,
}


def test_runner_cli_is_opt_in_and_resource_flags_preserve_85():
    parse = RUNNER["parse_args"]
    with patch.dict(
        RUNNER["main"].__globals__, {"load_profiles": Mock(side_effect=AssertionError)}
    ):
        args = parse(["--work", "/tmp/opencode/trial85"])
        assert not args.profiled and args.artifact_suffix == "optimized"
    assert RUNNER["resource_flags"]() == ["--memory=2g", "--memory-swap=2g", "--cpus=2"]
    for stage, expected in (("ingest", "0.25"), ("curate", "0.5"), ("gold", "1")):
        profile = CONSTANTS["PROFILES"][stage]
        flags = RUNNER["resource_flags"](profile)
        assert flags == [
            f"--memory={profile['memory_mib']}M",
            f"--memory-swap={profile['memory_mib']}M",
            f"--cpus={expected}",
        ]
    for suffix in ("optimized", "bounded", "ready"):
        args = parse(
            [
                "--work",
                "/tmp/opencode/trial86",
                "--profiled",
                "--artifact-suffix",
                suffix,
            ]
        )
        assert args.profiled and args.artifact_suffix == suffix
    with patch("sys.stderr", new_callable=io.StringIO):
        for suffix in ("bounded", "ready", "../escape"):
            with pytest.raises(SystemExit):
                parse(["--work", "/tmp/opencode/new", "--artifact-suffix", suffix])
    base = ["--stage", "gold", "--run-id", "trial86", "--output", "/evidence"]
    assert not PROBE["parse_args"](base).profiled
    assert PROBE["parse_args"](base + ["--profiled"]).profiled
    assert PROBE["parse_args"](base + ["--task86"]).profiled


def test_current_host_runtime_metadata_uses_production_no_spill_policy():
    runtime = RUNNER["load_profiles"]()
    assert runtime["module_sha256"] == RUNNER["digest"](
        ROOT / "src/nyc311/lambda_runtime.py"
    )
    for key in ("PROFILES", "SOURCE_LIMITS", "SDK_LIMITS", "DUCKDB_LIMITS"):
        assert runtime["constants"][key] == CONSTANTS[key]


def test_lazy_host_profiles_and_payload_fingerprint(tmp_path):
    load = RUNNER["load_profiles"]
    with patch.dict(load.__globals__, {"ROOT": tmp_path}):
        with pytest.raises(FileNotFoundError):
            load()
        module = tmp_path / "src/nyc311/lambda_runtime.py"
        module.parent.mkdir(parents=True)
        module.write_text(
            "\n".join(f"{key} = {value!r}" for key, value in CONSTANTS.items())
        )
        runtime = load()
        assert runtime["constants"] == CONSTANTS
        assert runtime["module_sha256"] == RUNNER["digest"](module)
        bad = deepcopy(CONSTANTS)
        bad["SOURCE_LIMITS"]["max_attempts"] = 5
        with patch("runpy.run_path", return_value=bad), pytest.raises(ValueError):
            load()
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    manifest = inputs / "manifest.json"
    manifest.write_text(json.dumps({"runtime_profile": runtime}))
    settings = PROBE["profiled_settings"]
    with patch.dict(settings.__globals__, {"INPUTS": inputs, "TASK": tmp_path / "src"}):
        for stage, profile in CONSTANTS["PROFILES"].items():
            report = {"local_budgets": {}}
            assert settings(stage, report) == profile
            assert report["runtime_profile"] == runtime
            budgets = report["local_budgets"]
            assert budgets["profiled"]
            assert budgets["memory_bytes"] == profile["memory_mib"] * 1024**2
            assert budgets["cpu"] == profile["benchmark_cpus"]
            assert (
                budgets["application_seconds"]
                == budgets["work_seconds"] + budgets["failure_publication_seconds"]
                == 720
            )
            assert budgets["lambda_reserve_seconds"] == 180
        with (
            patch("runpy.run_path", return_value=bad),
            pytest.raises(AssertionError, match="constants"),
        ):
            settings("gold", {"local_budgets": {}})
        historical = deepcopy(CONSTANTS)
        historical["DUCKDB_LIMITS"].pop("temp_directory")
        historical["DUCKDB_LIMITS"]["max_temp_directory_size"] = "256MiB"
        module.write_text(
            "\n".join(f"{key} = {value!r}" for key, value in historical.items())
        )
        with pytest.raises(AssertionError, match="SHA-256"):
            settings("gold", {"local_budgets": {}})


@pytest.mark.parametrize(
    "limits",
    [
        {"memory_limit": "512MiB", "threads": 1},
        {"memory_limit": "512MiB", "threads": 1, "max_temp_directory_size": "256MiB"},
        CONSTANTS["DUCKDB_LIMITS"] | {"max_temp_directory_size": "256MiB"},
        CONSTANTS["DUCKDB_LIMITS"] | {"temp_directory": "/tmp/duckdb-spill"},
        CONSTANTS["DUCKDB_LIMITS"] | {"memory_limit": "1GiB"},
        CONSTANTS["DUCKDB_LIMITS"] | {"threads": 2},
    ],
)
def test_profile_loader_rejects_historical_or_changed_policy(limits, tmp_path):
    load = RUNNER["load_profiles"]
    constants = CONSTANTS | {"DUCKDB_LIMITS": limits}
    with (
        patch.dict(
            load.__globals__, {"ROOT": tmp_path, "digest": Mock(return_value="sha")}
        ),
        patch("runpy.run_path", return_value=constants),
        pytest.raises(ValueError, match="DuckDB limits"),
    ):
        load()


def test_profile_cgroups_context_and_explicit_budget_comparisons():
    verify = PROBE["verify_profile_cgroup"]
    profile = CONSTANTS["PROFILES"]["ingest"]
    counters = {"memory.max": str(512 * 1024**2), "cpu.max": "25000 100000"}
    verify(profile, counters)
    for change in ({"memory.max": str(2 * 1024**3)}, {"cpu.max": "200000 100000"}):
        with pytest.raises(AssertionError):
            verify(profile, counters | change)
    context_class = PROBE["LambdaContext"]
    with patch.dict(
        context_class.get_remaining_time_in_millis.__globals__,
        {"perf_counter": Mock(side_effect=[100, 115, 1001])},
    ):
        context = context_class(100, 900)
        assert context.get_remaining_time_in_millis() == 900000
        assert context.get_remaining_time_in_millis() == 885000
        assert context.get_remaining_time_in_millis() == 0
    report = {
        "local_budgets": {"memory_80pct_bytes": 400, "tmp_80pct_bytes": 400},
        "elapsed": {
            "handler_seconds": 700,
            "stage_seconds_including_imports_and_setup": 721,
        },
        "resources_after_handler": {
            "process_highwater_rss_bytes": 399,
            "cgroup": {"memory.peak": "401"},
        },
        "tmp_handler_peaks": {"allocated_bytes": 399},
    }
    PROBE["compare_profile_budgets"](report)
    comparisons = report["local_budgets"]["comparisons"]
    assert not comparisons["stage_within_720_seconds"]
    assert comparisons["handler_within_720_seconds"]
    assert not comparisons["memory_20pct_headroom"]
    assert comparisons["tmp_observed_20pct_headroom"]
    report["resources_after_handler"]["cgroup"]["memory.peak"] = {
        "unavailable": "OSError"
    }
    PROBE["compare_profile_budgets"](report)
    assert report["local_budgets"]["comparisons"]["memory_20pct_headroom"] is None


@pytest.mark.parametrize("seconds, expected", [(720, True), (720.001, False)])
def test_profile_time_comparison_keeps_exact_boundary(seconds, expected):
    report = {
        "local_budgets": {"memory_80pct_bytes": 400, "tmp_80pct_bytes": 400},
        "elapsed": {
            "handler_seconds": seconds,
            "stage_seconds_including_imports_and_setup": seconds,
        },
        "resources_after_handler": {
            "process_highwater_rss_bytes": 400,
            "cgroup": {"memory.peak": "400"},
        },
        "tmp_handler_peaks": {"allocated_bytes": 400},
    }
    PROBE["compare_profile_budgets"](report)
    comparisons = report["local_budgets"]["comparisons"]
    assert comparisons["handler_within_720_seconds"] is expected
    assert comparisons["stage_within_720_seconds"] is expected
    assert comparisons["memory_20pct_headroom"] is True
    assert comparisons["tmp_observed_20pct_headroom"] is True


def test_live_duckdb_observer_preserves_native_connections_and_restores_factory():
    package, gold = ModuleType("nyc311"), ModuleType("nyc311.gold")
    package.gold = gold
    native = Mock()
    native.execute.return_value.fetchone.return_value = (
        "512.0 MiB",
        1,
        "",
    )
    factory = Mock(return_value=native)
    gold.duckdb = SimpleNamespace(connect=factory)
    config = deepcopy(CONSTANTS["DUCKDB_LIMITS"])
    report = {
        "runtime_profile": {"constants": CONSTANTS},
    }
    with patch.dict(sys.modules, {"nyc311": package, "nyc311.gold": gold}):
        with PROBE["observe_duckdb"](report):
            for _ in range(4):
                assert gold.duckdb.connect(":memory:", config=config) is native
        assert gold.duckdb.connect is factory
    assert factory.call_count == native.execute.call_count == 4
    assert all(
        call.args == (":memory:",) and call.kwargs == {"config": config}
        for call in factory.call_args_list
    )
    assert all(
        call.args
        == (
            "SELECT current_setting('memory_limit'), "
            "current_setting('threads'), current_setting('temp_directory')",
        )
        and not call.kwargs
        for call in native.execute.call_args_list
    )
    records = report["duckdb_settings"]["connections"]
    assert len(records) == 4
    assert records[0]["requested_config"] == config
    assert records[0]["effective"]["memory_limit"] == "512.0 MiB"
    assert records[0]["effective"] == {
        "memory_limit": "512.0 MiB",
        "threads": 1,
        "temp_directory": "",
    }
    assert config == CONSTANTS["DUCKDB_LIMITS"]
    native.close.assert_not_called()


def test_live_duckdb_observer_rejects_fallback_settings_and_missing_connections():
    package, gold = ModuleType("nyc311"), ModuleType("nyc311.gold")
    package.gold = gold
    for change in (
        {},
        {"memory_limit": "1GiB"},
        {"threads": 2},
        {"temp_directory": ".tmp"},
        {"temp_directory": "/tmp/nyc311-fake/duckdb-spill"},
        {"max_temp_directory_size": "256MiB"},
    ):
        native = Mock()
        config = CONSTANTS["DUCKDB_LIMITS"] | change
        native.execute.return_value.fetchone.return_value = (
            config["memory_limit"],
            config["threads"],
            config["temp_directory"],
        )
        if not change:
            # Requested settings are correct, but the connection ignored them.
            native.execute.return_value.fetchone.return_value = (
                "1.0 GiB",
                1,
                config["temp_directory"],
            )
        factory = Mock(return_value=native)
        gold.duckdb = SimpleNamespace(connect=factory)
        report = {
            "runtime_profile": {"constants": CONSTANTS},
        }
        with patch.dict(sys.modules, {"nyc311": package, "nyc311.gold": gold}):
            with pytest.raises(AssertionError), PROBE["observe_duckdb"](report):
                gold.duckdb.connect(":memory:", config=config)
            assert gold.duckdb.connect is factory
            native.close.assert_called_once()
            with (
                pytest.raises(AssertionError, match="four"),
                PROBE["observe_duckdb"](report),
            ):
                pass
            assert gold.duckdb.connect is factory


@pytest.mark.parametrize(
    "effective", [("512 MiB", 2, ""), ("512 MiB", 1, "/tmp/spill")]
)
def test_live_duckdb_observer_rejects_ignored_threads_or_no_spill_policy(effective):
    package, gold = ModuleType("nyc311"), ModuleType("nyc311.gold")
    package.gold = gold
    native = Mock()
    native.execute.return_value.fetchone.return_value = effective
    factory = Mock(return_value=native)
    gold.duckdb = SimpleNamespace(connect=factory)
    config = deepcopy(CONSTANTS["DUCKDB_LIMITS"])
    report = {"runtime_profile": {"constants": CONSTANTS}}
    with patch.dict(sys.modules, {"nyc311": package, "nyc311.gold": gold}):
        with (
            pytest.raises(AssertionError, match="live DuckDB no-spill"),
            PROBE["observe_duckdb"](report),
        ):
            gold.duckdb.connect(":memory:", False, config)
        assert gold.duckdb.connect is factory
    factory.assert_called_once_with(":memory:", False, config)
    assert config == CONSTANTS["DUCKDB_LIMITS"]
    native.close.assert_called_once()
