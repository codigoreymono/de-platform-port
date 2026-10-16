"""Daily ingestion entry point; no analytical dependencies or business-rule copies."""

import gzip
from contextlib import nullcontext
from hashlib import sha256

from nyc311 import lambda_common as common
from nyc311.bronze import ingest_day
from nyc311.lambda_runtime import InvocationBudget
from nyc311.s3_store import temporary_workspace
from nyc311.source import create_client
from nyc311.storage import manifest_key, object_path, output_root


def handler(event: dict[str, object], context: object) -> dict[str, object]:
    budget = InvocationBudget("ingest", context)
    config = common.settings(event)
    day = common.daily_event(event)
    operation = event.get("operation", "extract")
    if operation not in ("extract", "replay"):
        raise ValueError("Ingest operation must be extract or replay")
    if operation == "replay" and not isinstance(event.get("bronze_manifest_key"), str):
        raise ValueError("Replay requires an explicit Bronze manifest key")
    store = common.namespace(event, config, budget=budget)
    with temporary_workspace() as base:
        root = output_root(base, config["mode"])
        key = (
            event["bronze_manifest_key"]
            if operation == "replay"
            else f"control/bronze/{event['run_id']}/{day.isoformat()}.json"
        )
        if operation == "replay" or store.exists(key):
            manifest = common.download_bronze(store, base, key, config["mode"])
            if (
                manifest["start_date"] != event["start_date"]
                or manifest["end_date"] != event["end_date"]
            ):
                raise ValueError("Existing Bronze does not match the requested window")
        else:
            if store.exists(
                f"bronze/run={event['run_id']}/created_date={day.isoformat()}/page-0001.json.gz"
            ):
                raise FileExistsError(
                    "Bronze objects already exist without a complete extraction"
                )
            uploaded = set()

            def progress(manifest) -> None:
                # Upload original pages incrementally; failed work survives /tmp cleanup.
                for page in manifest["pages"]:
                    path = object_path(root, page["key"])
                    if (
                        manifest["status"] != "failed"
                        and page["key"] not in uploaded
                        and path.is_file()
                    ):
                        store.upload(root, page["key"])
                        uploaded.add(page["key"])
                if manifest["status"] == "complete":
                    for page in manifest["pages"]:
                        content = gzip.decompress(store.read(page["key"]))
                        if sha256(content).hexdigest() != page["sha256"]:
                            raise ValueError("Remote Bronze page checksum mismatch")
                with (
                    budget.failure_reporting()
                    if manifest["status"] == "failed"
                    else nullcontext()
                ):
                    store.write_json(manifest_key(manifest), manifest)

            previous = None
            silver_key = f"control/silver/{day.isoformat()}.json"
            if store.exists(silver_key):
                partition = store.read_json(silver_key)
                if (
                    partition["kind"] != "silver"
                    or partition["mode"] != config["mode"]
                    or manifest_key(partition) != silver_key
                ):
                    raise ValueError(
                        "Previous Silver provenance does not match the requested day"
                    )
                source = store.read_json(partition["source_manifest_key"])
                if (
                    source["kind"] != "bronze"
                    or source["status"] != "complete"
                    or source["mode"] != config["mode"]
                    or source["start_date"] != event["start_date"]
                    or source["end_date"] != event["end_date"]
                    or manifest_key(source) != partition["source_manifest_key"]
                    or type(source["record_count"]) is not int
                    or source["record_count"] < 0
                ):
                    raise ValueError("Previous source provenance is not reliable")
                previous = source["record_count"]
            config = budget.source_config(config)
            with create_client(config) as client:
                manifest = ingest_day(
                    client,
                    day,
                    base,
                    event["run_id"],
                    config,
                    previous_count=previous,
                    on_progress=progress,
                    absolute_deadline=budget.work_deadline,
                )
        budget.check()
        return common.result(
            event,
            "ingest",
            start_date=manifest["start_date"],
            end_date=manifest["end_date"],
            bronze_manifest_key=key,
            record_count=manifest["record_count"],
        )
