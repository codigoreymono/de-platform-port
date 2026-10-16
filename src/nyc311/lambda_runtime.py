"""Measured Lambda resource policy, separate from data and analytical contracts."""

from contextlib import contextmanager
from math import isfinite
from time import monotonic

PROFILES = {
    "ingest": {
        "memory_mib": 512,
        "timeout_seconds": 900,
        "temporary_storage_mib": 512,
        "benchmark_cpus": 0.25,
    },
    "curate": {
        "memory_mib": 1024,
        "timeout_seconds": 900,
        "temporary_storage_mib": 512,
        "benchmark_cpus": 0.5,
    },
    "gold": {
        "memory_mib": 2048,
        "timeout_seconds": 900,
        "temporary_storage_mib": 512,
        "benchmark_cpus": 1.0,
    },
}
SOURCE_LIMITS = {
    "page_size": 5000,
    "max_attempts": 3,
    "connect_timeout": 5.0,
    "read_timeout": 30.0,
    "max_backoff": 10.0,
    "task_deadline": 600.0,
}
SDK_LIMITS = {"connect_timeout": 5, "read_timeout": 20, "total_max_attempts": 3}
DUCKDB_LIMITS = {
    "memory_limit": "512MiB",
    "threads": 1,
    "temp_directory": "",
}
APPLICATION_SECONDS = 720
FAILURE_REPORTING_SECONDS = 60
LAMBDA_RESERVE_SECONDS = 180


class InvocationDeadlineExceeded(TimeoutError):
    """Cooperative work/reporting budget expired; this is not a hard interrupt."""


class InvocationBudget:
    def __init__(self, stage: str, context: object) -> None:
        PROFILES[stage]
        allowance = APPLICATION_SECONDS
        if context is not None:
            remaining = context.get_remaining_time_in_millis() / 1000
            if not isfinite(remaining):
                raise ValueError("Lambda remaining time must be finite")
            allowance = min(allowance, remaining - LAMBDA_RESERVE_SECONDS)
        self.deadline = monotonic() + allowance
        self.work_deadline = self.deadline - FAILURE_REPORTING_SECONDS
        self.reporting = False
        self.check()

    def remaining(self) -> float:
        deadline = self.deadline if self.reporting else self.work_deadline
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise InvocationDeadlineExceeded("Lambda invocation budget exhausted")
        return remaining

    def check(self) -> None:
        self.remaining()

    def source_config(self, config: dict) -> dict:
        # Do not restart extraction's clock after earlier provenance/S3 work.
        result = dict(config)
        result["task_deadline"] = min(config["task_deadline"], self.remaining())
        for name in ("connect_timeout", "read_timeout", "max_backoff"):
            result[name] = min(config[name], result["task_deadline"])
        return result

    @contextmanager
    def failure_reporting(self):
        previous = self.reporting
        self.reporting = True
        try:
            self.check()
            yield
        finally:
            self.reporting = previous
