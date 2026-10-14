"""Manual single-writer orchestration; business logic stays in the layer modules."""

from collections.abc import Sequence
from contextlib import ExitStack
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx

from nyc311.bronze import ingest_day, load_bronze
from nyc311.config import (
    COHORT_END,
    COHORT_START,
    ExecutionConfig,
    daily_windows,
    execution_config,
)
from nyc311.gold_store import load_gold, publish_gold
from nyc311.manifests import RunManifest
from nyc311.silver_store import publish_silver
from nyc311.source import create_client
from nyc311.storage import (
    manifest_key,
    object_path,
    output_root,
    read_json,
    write_json_atomic,
)


class PipelineError(RuntimeError):
    """A manual run has invalid inputs or inconsistent recovery provenance."""


def _previous_count(root: Path, day: date) -> int | None:
    key = f"control/silver/{day.isoformat()}.json"
    if not object_path(root, key).exists():
        return None
    try:
        partition = read_json(root, key)
        if partition["kind"] != "silver" or manifest_key(partition) != key:
            raise PipelineError(
                "Previous Silver provenance does not match the requested day"
            )
        source = read_json(root, partition["source_manifest_key"])
        count = source["record_count"]
        if (
            source["kind"] != "bronze"
            or source["status"] != "complete"
            or source["mode"] != partition["mode"]
            or source["start_date"] != day.isoformat()
            or manifest_key(source) != partition["source_manifest_key"]
            or type(count) is not int
            or count < 0
        ):
            raise PipelineError("Previous source count is not reliable")
        return count
    except (KeyError, ValueError, TypeError, OSError):
        raise PipelineError(
            "Previous source provenance is invalid; recover from complete Bronze"
        ) from None


def _sample_gold_days(root: Path, processed: list[date]) -> list[date]:
    selected = set(processed)
    for path in (root / "control/silver").glob("*.json"):
        day = date.fromisoformat(path.stem)
        daily_windows(day, day + timedelta(days=1))
        selected.add(day)
    return sorted(selected)


def run_pipeline(
    base: Path,
    run_id: str,
    start: date,
    end: date,
    *,
    config: ExecutionConfig | None = None,
    client: httpx.Client | None = None,
    bronze_keys: Sequence[str] | None = None,
) -> RunManifest:
    """Process days sequentially, rebuild Gold once, and persist progress atomically.

    Caller-supplied HTTP clients are not closed here. New run IDs are required for
    manual retries; no concurrent runs or automatic rollback are supported.
    """
    windows = daily_windows(start, end)
    if bronze_keys is not None and client is not None:
        raise PipelineError("Offline replay cannot accept an HTTP client")
    settings = execution_config(**(config or {}))
    root = output_root(base, settings["mode"])
    run: RunManifest = {
        "kind": "run",
        "mode": settings["mode"],
        "run_id": run_id,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "status": "running",
        "started_at_utc": datetime.now(UTC).isoformat(),
        "finished_at_utc": None,
        "completed_days": [],
        "output_keys": [],
        "error": None,
        "action": "extract" if bronze_keys is None else "replay",
        "stage": "bronze",
        "active_day": None,
        "bronze_keys": [] if bronze_keys is None else list(bronze_keys),
        "gold_days": [],
        "gold_manifest_key": None,
    }
    key = manifest_key(run)
    if (
        object_path(root, key).exists()
        or object_path(root, f"control/bronze/{run_id}").exists()
        or object_path(root, f"bronze/run={run_id}").exists()
        or object_path(root, f"control/gold/{run_id}.json").exists()
        or object_path(root, f"gold/run={run_id}").exists()
    ):
        raise FileExistsError("Run already exists; retry with a new run identifier")
    write_json_atomic(root, key, run)
    try:
        replay = {}
        if bronze_keys is not None:
            # Validate the whole replay plan before replacing any Silver partition.
            for reference in bronze_keys:
                manifest, _ = load_bronze(base, reference, mode=settings["mode"])
                day = date.fromisoformat(manifest["start_date"])
                if day in replay:
                    raise PipelineError(
                        "Replay requires exactly one complete Bronze extraction per day"
                    )
                replay[day] = reference
            if set(replay) != {day for day, _ in windows}:
                raise PipelineError(
                    "Replay references must cover exactly the requested date window"
                )
        with ExitStack() as stack:
            active_client = None
            if bronze_keys is None:
                active_client = (
                    client
                    if client is not None
                    else stack.enter_context(create_client(settings))
                )
            for day, _ in windows:
                run["active_day"] = day.isoformat()
                run["stage"] = "bronze"
                write_json_atomic(root, key, run)
                if bronze_keys is None:
                    bronze = ingest_day(
                        active_client,
                        day,
                        base,
                        run_id,
                        settings,
                        previous_count=_previous_count(root, day),
                    )
                    bronze_key = manifest_key(bronze)
                    run["bronze_keys"].append(bronze_key)
                else:
                    bronze_key = replay[day]
                run["output_keys"].append(bronze_key)
                run["stage"] = "silver"
                write_json_atomic(root, key, run)
                silver = publish_silver(
                    base, bronze_key, mode=settings["mode"], run_id=run_id
                )
                run["output_keys"].append(manifest_key(silver))
                run["completed_days"].append(day.isoformat())
                write_json_atomic(root, key, run)
        run["stage"] = "gold"
        run["active_day"] = None
        days = (
            _sample_gold_days(root, [day for day, _ in windows])
            if settings["mode"] == "sample"
            else None
        )
        run["gold_days"] = (
            [day.isoformat() for day in days]
            if days is not None
            else [day.isoformat() for day, _ in daily_windows(COHORT_START, COHORT_END)]
        )
        write_json_atomic(root, key, run)
        gold = publish_gold(base, run_id, mode=settings["mode"], days=days)
        load_gold(base, run_id, mode=settings["mode"])
        run["gold_manifest_key"] = manifest_key(gold)
        run["gold_days"] = gold["covered_days"]
        run["output_keys"].append(manifest_key(gold))
        run["stage"] = "finished"
        run["status"] = "succeeded"
        run["finished_at_utc"] = datetime.now(UTC).isoformat()
        write_json_atomic(root, key, run)
    except BaseException as error:
        run["status"] = "failed"
        run["error"] = type(error).__name__
        run["finished_at_utc"] = datetime.now(UTC).isoformat()
        try:
            write_json_atomic(root, key, run)
        except OSError:
            pass
        raise
    return run
