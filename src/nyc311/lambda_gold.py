"""Whole-cohort Gold entry point, with optional Glue Catalog republication."""

from contextlib import nullcontext
from datetime import date
from pathlib import Path

from nyc311 import catalog
from nyc311 import lambda_common as common
from nyc311.config import COHORT_END, COHORT_START, daily_windows, validate_window
from nyc311.gold import PRODUCT_SCHEMAS
from nyc311.gold_store import load_gold, publish_gold
from nyc311.lambda_runtime import DUCKDB_LIMITS, InvocationBudget
from nyc311.s3_store import temporary_workspace
from nyc311.storage import manifest_key, output_root, write_json

# ZIP resources live beside the nyc311 package, independently of the working directory.
SQL_DIRECTORY = Path(__file__).resolve().parent.parent / "sql"


def handler(event: dict[str, object], context: object) -> dict[str, object]:
    budget = InvocationBudget("gold", context)
    config = common.settings(event)
    requested = event.get("catalog")
    database = None if requested is None else catalog.catalog_database(requested)
    expected = [day for day, _ in daily_windows(COHORT_START, COHORT_END)]
    values = event.get("days")
    if values is not None and (
        not isinstance(values, list)
        or not all(isinstance(value, str) for value in values)
    ):
        raise ValueError("Gold days must be a list of ISO date strings")
    selected = (
        expected if values is None else [date.fromisoformat(value) for value in values]
    )
    for day in selected:
        validate_window(day, COHORT_END)
    if not selected or len(set(selected)) != len(selected):
        raise ValueError("Gold requires distinct selected creation days")
    selected.sort()
    if config["mode"] == "published" and selected != expected:
        raise ValueError("Official Gold requires all September days")
    store = common.namespace(event, config, budget=budget)
    key = f"control/gold/{event['run_id']}.json"
    with temporary_workspace() as base:
        root = output_root(base, config["mode"])
        if store.exists(key):
            existing = store.read_json(key)
            if existing["status"] != "complete" or existing["covered_days"] != [
                day.isoformat() for day in selected
            ]:
                raise ValueError(
                    "Existing Gold run is incomplete or belongs to another coverage"
                )
            for descriptor in existing["products"].values():
                store.download(descriptor["key"], root)
            write_json(root, key, existing)
            manifest, _ = load_gold(base, event["run_id"], mode=config["mode"])
        else:
            for product in PRODUCT_SCHEMAS:
                if store.exists(f"gold/run={event['run_id']}/{product}/part.parquet"):
                    raise FileExistsError(
                        "Gold artifacts already exist without a complete run"
                    )
            for day in selected:
                silver_key = f"control/silver/{day.isoformat()}.json"
                partition = store.read_json(silver_key)
                if (
                    partition["kind"] != "silver"
                    or partition["mode"] != config["mode"]
                    or partition["status"] != "complete"
                    or manifest_key(partition) != silver_key
                ):
                    raise ValueError(
                        "Gold requires verified complete Silver input days"
                    )
                for field in ("partition_key", "quality_report_key", "quarantine_key"):
                    store.download(partition[field], root)
                store.download(partition["source_manifest_key"], root)
                write_json(root, silver_key, partition)
            uploaded = set()

            def progress(manifest) -> None:
                if manifest["status"] != "failed":
                    for descriptor in manifest["products"].values():
                        if descriptor["key"] not in uploaded:
                            store.upload(root, descriptor["key"])
                            uploaded.add(descriptor["key"])
                if manifest["status"] == "complete":
                    common.verify_remote_files(
                        store,
                        [
                            (value["key"], value["sha256"])
                            for value in manifest["products"].values()
                        ],
                    )
                with (
                    budget.failure_reporting()
                    if manifest["status"] == "failed"
                    else nullcontext()
                ):
                    store.write_json(key, manifest)

            manifest = publish_gold(
                base,
                event["run_id"],
                mode=config["mode"],
                days=selected,
                sql_directory=SQL_DIRECTORY,
                on_progress=progress,
                duckdb_config=dict(DUCKDB_LIMITS),
            )
        budget.check()
        if database is not None:
            catalog.publish_gold_locations(
                common.create_glue_client(),
                database=database,
                bucket=event["bucket"],
                prefix=event.get("prefix", ""),
                mode=config["mode"],
                run_id=event["run_id"],
            )
        return common.result(
            event,
            "gold",
            gold_manifest_key=key,
            covered_days=manifest["covered_days"],
            products={
                name: {"key": value["key"], "row_count": value["row_count"]}
                for name, value in manifest["products"].items()
            },
        )
