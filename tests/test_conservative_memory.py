"""Offline helper tests only: no handlers, native dependencies or containers."""

import runpy
from copy import deepcopy
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PROBE = runpy.run_path(str(ROOT / "scripts/benchmark_lambda.py"))
PROFILES = runpy.run_path(str(ROOT / "src/nyc311/lambda_runtime.py"))["PROFILES"]


def compare(snapshot, budget=400):
    report = {
        "local_budgets": {"memory_80pct_bytes": budget, "tmp_80pct_bytes": 400},
        "elapsed": {
            "handler_seconds": 700,
            "stage_seconds_including_imports_and_setup": 721,
        },
        "resources_after_handler": snapshot,
        "tmp_handler_peaks": {"allocated_bytes": 399},
    }
    original = deepcopy(snapshot)
    PROBE["compare_profile_budgets"](report)
    assert snapshot == original
    result = report["local_budgets"]["comparisons"]
    assert result["stage_elapsed_seconds"] == 721
    assert result["stage_within_720_seconds"] is False
    assert result["handler_within_720_seconds"] is True
    assert result["tmp_observed_peak_bytes"] == 399
    assert result["tmp_observed_20pct_headroom"] is True
    assert result["tmp_is_sampled_lower_bound"] is True
    return result


@pytest.mark.parametrize(
    ("rss", "cgroup", "expected_peak", "headroom"),
    [
        (401, "399", 401, False),
        (399, "401", 401, False),
        (399, "399", 399, True),
        (400, "400", 400, True),
        (401, "401", 401, False),
        (0, "0", 0, True),
        (0, "0001", 1, True),
    ],
)
def test_complete_memory_uses_maximum(rss, cgroup, expected_peak, headroom):
    result = compare(
        {"process_highwater_rss_bytes": rss, "cgroup": {"memory.peak": cgroup}}
    )
    assert result["process_highwater_rss_bytes"] == rss
    assert result["process_highwater_rss_available"] is True
    assert result["cgroup_memory_peak_bytes"] == int(cgroup)
    assert result["cgroup_memory_peak_available"] is True
    assert result["memory_metrics_complete"] is True
    assert result["observed_memory_peak_bytes"] == expected_peak
    assert result["memory_peak_bytes"] == expected_peak
    assert result["memory_20pct_headroom"] is headroom


@pytest.mark.parametrize(
    "rss", [None, True, False, -1, 1.0, float("nan"), float("inf"), "1", {}, []]
)
@pytest.mark.parametrize("known_peak", [0, 399, 401])
def test_invalid_rss_leaves_comparison_unknown(rss, known_peak):
    result = compare(
        {
            "process_highwater_rss_bytes": rss,
            "cgroup": {"memory.peak": str(known_peak)},
        }
    )
    assert result["process_highwater_rss_bytes"] is None
    assert result["process_highwater_rss_available"] is False
    assert result["cgroup_memory_peak_bytes"] == known_peak
    assert result["cgroup_memory_peak_available"] is True
    assert result["memory_metrics_complete"] is False
    assert result["observed_memory_peak_bytes"] == known_peak
    assert result["memory_peak_bytes"] is None
    assert result["memory_20pct_headroom"] is None


@pytest.mark.parametrize(
    "cgroup",
    [None, True, 1, 1.0, float("nan"), "", "-1", "1.0", "max", " 1", "NaN", {}],
)
@pytest.mark.parametrize("known_peak", [0, 399, 401])
def test_invalid_cgroup_leaves_comparison_unknown(cgroup, known_peak):
    result = compare(
        {
            "process_highwater_rss_bytes": known_peak,
            "cgroup": {"memory.peak": cgroup},
        }
    )
    assert result["process_highwater_rss_bytes"] == known_peak
    assert result["process_highwater_rss_available"] is True
    assert result["cgroup_memory_peak_bytes"] is None
    assert result["cgroup_memory_peak_available"] is False
    assert result["memory_metrics_complete"] is False
    assert result["observed_memory_peak_bytes"] == known_peak
    assert result["memory_peak_bytes"] is None
    assert result["memory_20pct_headroom"] is None


@pytest.mark.parametrize(
    ("snapshot", "rss", "cgroup"),
    [
        ({}, None, None),
        (None, None, None),
        ({"cgroup": {}}, None, None),
        ({"cgroup": {"memory.peak": "401"}}, None, 401),
        ({"process_highwater_rss_bytes": 401}, 401, None),
        ({"process_highwater_rss_bytes": 0, "cgroup": None}, 0, None),
        ({"process_highwater_rss_bytes": 401, "cgroup": []}, 401, None),
        (
            {
                "process_highwater_rss_bytes": -1,
                "cgroup": {"memory.peak": {"unavailable": "OSError"}},
            },
            None,
            None,
        ),
    ],
)
def test_missing_and_unavailable_metrics_are_not_zero(snapshot, rss, cgroup):
    result = compare(snapshot)
    assert result["process_highwater_rss_bytes"] == rss
    assert result["process_highwater_rss_available"] is (rss is not None)
    assert result["cgroup_memory_peak_bytes"] == cgroup
    assert result["cgroup_memory_peak_available"] is (cgroup is not None)
    assert result["memory_metrics_complete"] is False
    assert result["observed_memory_peak_bytes"] == max(
        (value for value in (rss, cgroup) if value is not None), default=None
    )
    assert result["memory_peak_bytes"] is None
    assert result["memory_20pct_headroom"] is None


@pytest.mark.parametrize("stage", PROFILES)
def test_each_stage_uses_current_budget(stage):
    memory_bytes = PROFILES[stage]["memory_mib"] * 1024**2
    snapshot = {
        "process_highwater_rss_bytes": memory_bytes,
        "cgroup": {"memory.peak": str(memory_bytes // 2)},
    }
    assert compare(snapshot, 0.8 * memory_bytes)["memory_20pct_headroom"] is False
    assert compare(snapshot, 0.8 * 2 * memory_bytes)["memory_20pct_headroom"] is True
