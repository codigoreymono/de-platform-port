"""Daily curation entry point using existing local transformation/publication logic."""

from nyc311 import lambda_common as common
from nyc311.lambda_runtime import InvocationBudget
from nyc311.s3_store import temporary_workspace
from nyc311.silver_store import publish_silver
from nyc311.storage import manifest_key, output_root


def handler(event: dict[str, object], context: object) -> dict[str, object]:
    budget = InvocationBudget("curate", context)
    config = common.settings(event)
    common.daily_event(event)
    if not isinstance(event.get("bronze_manifest_key"), str):
        raise ValueError("Curation requires an explicit complete Bronze reference")
    store = common.namespace(event, config, budget=budget)
    with temporary_workspace() as base:
        bronze = common.download_bronze(
            store, base, event["bronze_manifest_key"], config["mode"]
        )
        if (
            bronze["start_date"] != event["start_date"]
            or bronze["end_date"] != event["end_date"]
        ):
            raise ValueError("Bronze input does not match the requested daily task")
        manifest = publish_silver(
            base,
            event["bronze_manifest_key"],
            mode=config["mode"],
            run_id=event["run_id"],
        )
        root = output_root(base, config["mode"])
        key = manifest_key(manifest)
        budget.check()
        incomplete = dict(manifest, status="incomplete")
        # PutObject replaces exactly one object. Never emulate POSIX rename via copy/delete.
        store.write_json(key, incomplete)
        try:
            fields = ("quality_report", "quarantine", "partition")
            references = []
            for field in fields:
                artifact = manifest[f"{field}_key"]
                store.upload(root, artifact)
                references.append((artifact, manifest[f"{field}_sha256"]))
            common.verify_remote_files(store, references)
            store.write_json(key, manifest)
        except Exception as error:
            failed = dict(incomplete, status="failed", error=type(error).__name__)
            try:
                with budget.failure_reporting():
                    store.write_json(key, failed)
            except Exception:
                pass
            raise
        budget.check()
        return common.result(
            event,
            "curate",
            start_date=manifest["start_date"],
            end_date=manifest["end_date"],
            silver_manifest_key=key,
            row_count=manifest["row_count"],
        )
