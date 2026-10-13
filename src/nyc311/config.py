"""Validated inputs for the fixed September cohort; no execution or I/O."""

from datetime import date, timedelta
from math import isfinite
from typing import Literal, TypedDict

COHORT_START = date(2026, 9, 1)
COHORT_END = date(2026, 10, 1)
BUSINESS_TIMEZONE = "America/New_York"

type Mode = Literal["sample", "published"]


class ExecutionConfig(TypedDict):
    mode: Mode
    page_size: int
    max_attempts: int
    connect_timeout: float
    read_timeout: float
    max_backoff: float
    task_deadline: float


def validate_window(start: date, end: date) -> None:
    """Require whole local calendar days in the half-open September interval."""
    if type(start) is not date or type(end) is not date:
        raise ValueError("Window boundaries must be dates, not timestamps or strings")
    if not COHORT_START <= start < end <= COHORT_END:
        raise ValueError("Window must satisfy 2026-09-01 <= start < end <= 2026-10-01")


def daily_windows(start: date, end: date) -> list[tuple[date, date]]:
    validate_window(start, end)
    windows = []
    for offset in range((end - start).days):
        day = start + timedelta(days=offset)
        windows.append((day, day + timedelta(days=1)))
    return windows


def execution_config(
    *,
    mode: Mode = "sample",
    page_size: int = 5000,
    max_attempts: int = 5,
    connect_timeout: float = 10.0,
    read_timeout: float = 60.0,
    max_backoff: float = 30.0,
    task_deadline: float = 720.0,
) -> ExecutionConfig:
    if mode not in ("sample", "published"):
        raise ValueError("Mode must be sample or published")
    for name, value in (("page_size", page_size), ("max_attempts", max_attempts)):
        if type(value) is not int or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    for name, value in (
        ("connect_timeout", connect_timeout),
        ("read_timeout", read_timeout),
        ("max_backoff", max_backoff),
        ("task_deadline", task_deadline),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{name} must be a finite positive number")
    if max(connect_timeout, read_timeout, max_backoff) > task_deadline:
        raise ValueError("Per-request limits must not exceed the task deadline")
    return {
        "mode": mode,
        "page_size": page_size,
        "max_attempts": max_attempts,
        "connect_timeout": float(connect_timeout),
        "read_timeout": float(read_timeout),
        "max_backoff": float(max_backoff),
        "task_deadline": float(task_deadline),
    }
