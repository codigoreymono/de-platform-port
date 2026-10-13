from datetime import date, datetime

import pytest

from nyc311.config import COHORT_END, COHORT_START, daily_windows, execution_config


def test_full_month_windows() -> None:
    windows = daily_windows(COHORT_START, COHORT_END)
    assert len(windows) == 30
    assert windows[0] == (date(2026, 9, 1), date(2026, 9, 2))
    assert windows[-1] == (date(2026, 9, 30), date(2026, 10, 1))
    assert all(left[1] == right[0] for left, right in zip(windows, windows[1:]))


def test_selected_day_window() -> None:
    assert daily_windows(date(2026, 9, 30), COHORT_END) == [
        (date(2026, 9, 30), COHORT_END)
    ]


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (COHORT_START, COHORT_START),
        (COHORT_END, COHORT_START),
        (date(2026, 8, 31), date(2026, 9, 2)),
        (date(2026, 9, 30), date(2026, 10, 2)),
        (date(2025, 9, 1), date(2025, 10, 1)),
        (datetime(2026, 9, 1), COHORT_END),
        (COHORT_START, datetime(2026, 10, 1)),
        ("2026-09-01", "2026-10-01"),
        (None, COHORT_END),
    ],
)
def test_invalid_windows(start: date, end: date) -> None:
    with pytest.raises(ValueError):
        daily_windows(start, end)


def test_configuration_defaults_are_sample_and_bounded() -> None:
    config = execution_config()
    assert config == {
        "mode": "sample",
        "page_size": 5000,
        "max_attempts": 5,
        "connect_timeout": 10.0,
        "read_timeout": 60.0,
        "max_backoff": 30.0,
        "task_deadline": 720.0,
    }
    assert execution_config(mode="published", page_size=2)["mode"] == "published"


@pytest.mark.parametrize("name", ["page_size", "max_attempts"])
@pytest.mark.parametrize("value", [0, -1, 1.5, True, float("inf"), None])
def test_invalid_integer_settings(name: str, value: object) -> None:
    with pytest.raises(ValueError):
        execution_config(**{name: value})


@pytest.mark.parametrize(
    "name", ["connect_timeout", "read_timeout", "max_backoff", "task_deadline"]
)
@pytest.mark.parametrize(
    "value", [0, -1, float("nan"), float("inf"), float("-inf"), True, "10", None]
)
def test_invalid_time_settings(name: str, value: object) -> None:
    with pytest.raises(ValueError):
        execution_config(**{name: value})


def test_limits_cannot_exceed_deadline() -> None:
    with pytest.raises(ValueError, match="deadline"):
        execution_config(task_deadline=5)


def test_invalid_mode() -> None:
    with pytest.raises(ValueError, match="Mode"):
        execution_config(mode="production")
