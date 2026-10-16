"""Small metadata validation and staged object transfers shared by Lambda stages."""

import json
import re
from datetime import date
from hashlib import sha256
from pathlib import Path

from nyc311.config import ExecutionConfig, daily_windows, execution_config
from nyc311.lambda_runtime import SDK_LIMITS, SOURCE_LIMITS, InvocationBudget
from nyc311.s3_store import S3Store, _relative_key
from nyc311.storage import manifest_key, output_root, write_json

PAYLOAD_LIMIT = 256 * 1024


def bounded_payload(value: dict[str, object]) -> dict[str, object]:
    if len(json.dumps(value, allow_nan=False).encode()) >= PAYLOAD_LIMIT:
        raise ValueError("Stage payload must be metadata below 256 KiB")
    return value


def settings(event: dict[str, object]) -> ExecutionConfig:
    bounded_payload(event)
    allowed = {
        "bucket",
        "prefix",
        "mode",
        "run_id",
        "config",
        "operation",
        "start_date",
        "end_date",
        "bronze_manifest_key",
        "days",
        "catalog",
        "stage",
        "status",
        "record_count",
        "silver_manifest_key",
        "row_count",
    }
    if event.keys() - allowed:
        raise ValueError("Stage inputs accept only execution metadata and references")
    if not isinstance(event["run_id"], str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_-]*", event["run_id"]
    ):
        raise ValueError("Run identifier must be a safe path component")
    if (
        not isinstance(event["bucket"], str)
        or not event["bucket"]
        or "/" in event["bucket"]
    ):
        raise ValueError("Bucket must be a name")
    mode = event.get("mode", "published")
    config = event.get("config", {})
    if not isinstance(config, dict) or "mode" in config:
        raise ValueError("Stage config must not override storage mode")
    selected = execution_config(mode=mode, **(SOURCE_LIMITS | config))
    if any(selected[name] > maximum for name, maximum in SOURCE_LIMITS.items()):
        raise ValueError(
            "Lambda execution settings may shorten, not exceed, approved limits"
        )
    return selected


def daily_event(event: dict[str, object]) -> date:
    if not all(isinstance(event.get(name), str) for name in ("start_date", "end_date")):
        raise ValueError("Daily tasks require ISO date strings")
    start, end = (
        date.fromisoformat(event["start_date"]),
        date.fromisoformat(event["end_date"]),
    )
    if len(daily_windows(start, end)) != 1:
        raise ValueError("Daily Lambda tasks require one NYC local creation day")
    if start.isoformat() != event["start_date"] or end.isoformat() != event["end_date"]:
        raise ValueError("Daily tasks require canonical YYYY-MM-DD dates")
    return start


def create_s3_client():
    # Construct only during explicitly invoked AWS execution, never at import time.
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        config=Config(
            connect_timeout=SDK_LIMITS["connect_timeout"],
            read_timeout=SDK_LIMITS["read_timeout"],
            retries={
                "mode": "standard",
                "total_max_attempts": SDK_LIMITS["total_max_attempts"],
            },
        ),
    )


def create_glue_client():
    # Construct only during explicitly invoked AWS execution, never at import time.
    import boto3
    from botocore.config import Config

    return boto3.client(
        "glue",
        config=Config(
            connect_timeout=SDK_LIMITS["connect_timeout"],
            read_timeout=SDK_LIMITS["read_timeout"],
            retries={
                "mode": "standard",
                "total_max_attempts": SDK_LIMITS["total_max_attempts"],
            },
        ),
    )


def namespace(
    event: dict[str, object],
    config: ExecutionConfig,
    *,
    budget: InvocationBudget | None = None,
) -> S3Store:
    prefix = event.get("prefix", "")
    if not isinstance(prefix, str):
        raise ValueError("S3 prefix must be text")
    scoped = f"{prefix}/{config['mode']}" if prefix else config["mode"]
    # Validate prefix before constructing the real SDK client.
    _relative_key(scoped)
    if budget is not None:
        budget.check()
    return S3Store(
        create_s3_client(),
        event["bucket"],
        scoped,
        check=budget.check if budget else None,
    )


def result(
    event: dict[str, object], stage: str, **references: object
) -> dict[str, object]:
    return bounded_payload(
        {
            "bucket": event["bucket"],
            "prefix": event.get("prefix", ""),
            "mode": event.get("mode", "published"),
            "run_id": event["run_id"],
            **{
                name: event[name]
                for name in ("days", "config", "operation")
                if name in event
            },
            "stage": stage,
            "status": "complete",
            **references,
        }
    )


def download_bronze(store: S3Store, base: Path, key: str, mode: str):
    from nyc311.bronze import _page_key, load_bronze

    manifest = store.read_json(key)
    if (
        manifest["kind"] != "bronze"
        or manifest["mode"] != mode
        or manifest["status"] != "complete"
        or manifest_key(manifest) != key
    ):
        raise ValueError(
            "Only complete matching Bronze can be downloaded for processing"
        )
    root = output_root(base, mode)
    for index, page in enumerate(manifest["pages"], 1):
        if page["key"] != _page_key(manifest, index):
            raise ValueError("Bronze references must stay within their extraction")
        store.download(page["key"], root)
    write_json(root, key, manifest)
    return load_bronze(base, key, mode=mode)[0]


def verify_remote_files(store: S3Store, references: list[tuple[str, str]]) -> None:
    for key, expected in references:
        if sha256(store.read(key)).hexdigest() != expected:
            raise ValueError("Uploaded S3 object does not match its manifest digest")
