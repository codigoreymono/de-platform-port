import io
import json
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import httpx
import pytest
from botocore.exceptions import ClientError

from nyc311 import (
    catalog,
    lambda_common,
    lambda_curate,
    lambda_gold,
    lambda_ingest,
    source,
)
from nyc311.catalog import CatalogError
from nyc311.cli import fixture_transport
from nyc311.gold import GoldError
from nyc311.gold_store import load_gold, publish_gold
from nyc311.s3_store import S3Store, temporary_workspace
from nyc311.silver_store import load_silver, publish_silver
from nyc311.storage import output_root, read_json

ROOT = Path(__file__).resolve().parents[1]


class MemoryS3:
    """SDK-shaped, unsigned in-memory service; no credentials or transport exist."""

    def __init__(self) -> None:
        self.objects = {}
        self.calls = []
        self.fail_key = None
        self.fail_get_key = None

    def head_object(self, *, Bucket: str, Key: str) -> dict:
        self.calls.append(("head", Key))
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        self.calls.append(("get", Key))
        if Key == self.fail_get_key:
            raise ClientError({"Error": {"Code": "InternalError"}}, "GetObject")
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> dict:
        self.calls.append(("put", Key))
        if Key == self.fail_key:
            raise ClientError({"Error": {"Code": "InternalError"}}, "PutObject")
        self.objects[Key] = Body
        return {}


@pytest.fixture
def simulated_aws(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    client = MemoryS3()
    spaces = []

    @contextmanager
    def workspace():
        with temporary_workspace(tmp_path) as path:
            spaces.append(path)
            yield path

    monkeypatch.setattr(lambda_common, "create_s3_client", lambda: client)
    monkeypatch.setattr(lambda_gold, "SQL_DIRECTORY", ROOT / "sql")
    for module in (lambda_ingest, lambda_curate, lambda_gold):
        monkeypatch.setattr(module, "temporary_workspace", workspace)
    real_client = source.create_client
    monkeypatch.setattr(
        lambda_ingest,
        "create_client",
        lambda config: real_client(
            config,
            app_token="",
            transport=fixture_transport(ROOT / "data/samples/gold-metrics.json"),
        ),
    )
    yield client, spaces
    assert all(not path.exists() for path in spaces)


def daily_event(day: str = "2026-09-01", run_id: str = "lambda-run") -> dict:
    return {
        "bucket": "fixture-bucket",
        "prefix": "nyc311",
        "mode": "sample",
        "run_id": run_id,
        "start_date": day,
        "end_date": "2026-09-02" if day == "2026-09-01" else "2026-09-03",
        "config": {"page_size": 3, "max_attempts": 1},
    }


def test_handlers_compose_and_preserve_local_logical_outputs(
    simulated_aws, tmp_path: Path
) -> None:
    client, spaces = simulated_aws
    results = []
    for day in ("2026-09-01", "2026-09-02"):
        event = daily_event(day)
        ingested = lambda_ingest.handler(event, None)
        results.append(lambda_curate.handler({**event, **ingested}, None))
        assert len(json.dumps(event).encode()) < 2048
        assert len(json.dumps(ingested).encode()) < 2048
        assert len(json.dumps(results[-1]).encode()) < 2048
    gold = lambda_gold.handler(
        {
            "bucket": "fixture-bucket",
            "prefix": "nyc311",
            "mode": "sample",
            "run_id": "lambda-gold",
            "days": ["2026-09-01", "2026-09-02"],
        },
        None,
    )
    assert results[0]["row_count"] == 7
    assert results[1]["row_count"] == 5
    assert gold["status"] == "complete"
    assert len(gold["products"]) == 4
    assert len(json.dumps(gold).encode()) < 2048 < lambda_common.PAYLOAD_LIMIT
    assert len(spaces) == len(set(spaces)) == 5
    assert all(not space.exists() for space in spaces)
    store = S3Store(client, "fixture-bucket", "nyc311/sample")
    with temporary_workspace(tmp_path) as base:
        root = output_root(base, "sample")
        manifest = store.read_json(gold["gold_manifest_key"])
        store.download(gold["gold_manifest_key"], root)
        for product in manifest["products"].values():
            store.download(product["key"], root)
        loaded, tables = load_gold(base, "lambda-gold")
        assert loaded == manifest
        assert sum(tables["demand_daily"]["request_count"].to_pylist()) == 12
        assert sum(tables["channel_usage"]["request_count"].to_pylist()) == 12
        silver = store.read_json(results[0]["silver_manifest_key"])
        store.download(results[0]["silver_manifest_key"], root)
        for field in ("partition_key", "quality_report_key", "quarantine_key"):
            store.download(silver[field], root)
        assert load_silver(base, date(2026, 9, 1))[1].num_rows == 7
        # Same saved observations through ordinary local publication, not new HTTP.
        local = tmp_path / "local-parity"
        for day in ("2026-09-01", "2026-09-02"):
            bronze_key = f"control/bronze/lambda-run/{day}.json"
            lambda_common.download_bronze(store, local, bronze_key, "sample")
            local_manifest = publish_silver(local, bronze_key, run_id="lambda-run")
            assert local_manifest == store.read_json(f"control/silver/{day}.json")
            local_root = output_root(local, "sample")
            assert read_json(
                local_root, local_manifest["quality_report_key"]
            ) == store.read_json(local_manifest["quality_report_key"])
            store.download(f"control/silver/{day}.json", root)
            for field in ("partition_key", "quality_report_key", "quarantine_key"):
                store.download(local_manifest[field], root)
            assert load_silver(base, date.fromisoformat(day))[1].equals(
                load_silver(local, date.fromisoformat(day))[1], check_metadata=True
            )
        local_gold = publish_gold(
            local, "lambda-gold", days=[date(2026, 9, 1), date(2026, 9, 2)]
        )
        for field in ("input_partitions", "quality_exclusions", "observations_by_day"):
            assert local_gold[field] == manifest[field]
        _, local_tables = load_gold(local, "lambda-gold")
        assert all(
            tables[name].equals(local_tables[name], check_metadata=True)
            for name in tables
        )


def test_ingest_replay_and_complete_retry_never_make_http_calls(
    simulated_aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, _ = simulated_aws
    event = daily_event()
    first = lambda_ingest.handler(event, None)

    def no_http(*args, **kwargs):
        raise AssertionError("Completed retry and replay must use saved Bronze")

    monkeypatch.setattr(lambda_ingest, "create_client", no_http)
    assert lambda_ingest.handler(event, None) == first
    replay = lambda_ingest.handler(
        {
            **event,
            "run_id": "replayed",
            "operation": "replay",
            "bronze_manifest_key": first["bronze_manifest_key"],
        },
        None,
    )
    assert replay["record_count"] == 7
    assert replay["bronze_manifest_key"] == first["bronze_manifest_key"]
    assert replay["run_id"] == "replayed"


def test_results_compose_directly_without_restoring_execution_metadata(
    simulated_aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    event = {**daily_event(), "days": ["2026-09-01"], "operation": "extract"}
    ingested = lambda_ingest.handler(event, None)
    curated = lambda_curate.handler(ingested, None)
    gold = lambda_gold.handler(curated, None)
    assert gold["covered_days"] == event["days"]
    assert gold["config"] == event["config"]
    replay_event = {
        **event,
        "operation": "replay",
        "run_id": "direct-replay",
        "bronze_manifest_key": ingested["bronze_manifest_key"],
    }

    def no_http(*args, **kwargs):
        raise AssertionError("Replay result must remain a replay on retry")

    monkeypatch.setattr(lambda_ingest, "create_client", no_http)
    replayed = lambda_ingest.handler(replay_event, None)
    assert lambda_ingest.handler(replayed, None) == replayed


def test_s3_publication_invalidation_precedes_data_and_failure_blocks_consumption(
    simulated_aws,
) -> None:
    client, _ = simulated_aws
    event = daily_event()
    ingested = lambda_ingest.handler(event, None)
    curate_event = {**event, "bronze_manifest_key": ingested["bronze_manifest_key"]}
    first = lambda_curate.handler(curate_event, None)
    key = "nyc311/sample/control/silver/2026-09-01.json"
    data_key = "nyc311/sample/silver/created_date=2026-09-01/part.parquet"
    before = client.objects[data_key]
    client.fail_key = data_key
    with pytest.raises(ClientError):
        lambda_curate.handler(curate_event, None)
    assert client.objects[data_key] == before
    assert json.loads(client.objects[key])["status"] == "failed"
    client.fail_key = None
    recovered = lambda_curate.handler(curate_event, None)
    assert recovered == first
    puts = [name for operation, name in client.calls if operation == "put"]
    assert puts.index(key) < puts.index(data_key)


def test_bronze_upload_failure_retains_failed_remote_diagnostics_and_cleans_tmp(
    simulated_aws,
) -> None:
    client, _ = simulated_aws
    client.fail_key = (
        "nyc311/sample/bronze/run=lambda-run/created_date=2026-09-01/page-0002.json.gz"
    )
    with pytest.raises(ClientError):
        lambda_ingest.handler(daily_event(), None)
    marker = json.loads(
        client.objects["nyc311/sample/control/bronze/lambda-run/2026-09-01.json"]
    )
    assert marker["status"] != "complete"
    assert (
        "nyc311/sample/bronze/run=lambda-run/created_date=2026-09-01/page-0001.json.gz"
        in client.objects
    )


def test_gold_upload_failure_does_not_mark_run_complete(simulated_aws) -> None:
    client, _ = simulated_aws
    event = daily_event()
    ingested = lambda_ingest.handler(event, None)
    lambda_curate.handler(
        {**event, "bronze_manifest_key": ingested["bronze_manifest_key"]}, None
    )
    client.fail_key = "nyc311/sample/gold/run=failed-gold/channel_usage/part.parquet"
    gold_event = {
        "bucket": "fixture-bucket",
        "prefix": "nyc311",
        "mode": "sample",
        "run_id": "failed-gold",
        "days": ["2026-09-01"],
    }
    with pytest.raises(ClientError):
        lambda_gold.handler(gold_event, None)
    assert (
        json.loads(client.objects["nyc311/sample/control/gold/failed-gold.json"])[
            "status"
        ]
        == "failed"
    )


def test_silver_invalidation_failure_preserves_all_previous_artifacts(
    simulated_aws,
) -> None:
    client, _ = simulated_aws
    event = daily_event()
    ingested = lambda_ingest.handler(event, None)
    curate_event = {**event, **ingested}
    lambda_curate.handler(curate_event, None)
    before = dict(client.objects)
    client.calls.clear()
    client.fail_key = "nyc311/sample/control/silver/2026-09-01.json"
    with pytest.raises(ClientError):
        lambda_curate.handler(curate_event, None)
    assert client.objects == before
    assert [key for operation, key in client.calls if operation == "put"] == [
        client.fail_key
    ]


@pytest.mark.parametrize("stage", ["ingest", "curate", "gold"])
def test_failed_completion_write_withholds_success_and_recovers_manually(
    simulated_aws, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    client, _ = simulated_aws
    event = daily_event()
    if stage != "ingest":
        event.update(lambda_ingest.handler(event, None))
    if stage == "gold":
        lambda_curate.handler(event, None)
        event = {
            "bucket": "fixture-bucket",
            "prefix": "nyc311",
            "mode": "sample",
            "run_id": "completion-gold",
            "days": ["2026-09-01"],
        }
    module = {"ingest": lambda_ingest, "curate": lambda_curate, "gold": lambda_gold}[
        stage
    ]
    marker = {
        "ingest": "control/bronze/lambda-run/2026-09-01.json",
        "curate": "control/silver/2026-09-01.json",
        "gold": "control/gold/completion-gold.json",
    }[stage]
    original = client.put_object

    def fail_complete(*, Bucket, Key, Body):
        if (
            Key == f"nyc311/sample/{marker}"
            and json.loads(Body)["status"] == "complete"
        ):
            raise ClientError({"Error": {"Code": "InternalError"}}, "PutObject")
        return original(Bucket=Bucket, Key=Key, Body=Body)

    monkeypatch.setattr(client, "put_object", fail_complete)
    with pytest.raises(ClientError):
        module.handler(event, None)
    assert json.loads(client.objects[f"nyc311/sample/{marker}"])["status"] == "failed"
    monkeypatch.setattr(client, "put_object", original)
    if stage != "curate":
        with pytest.raises(ValueError):
            module.handler(event, None)
        event["run_id"] = "new-attempt"
    assert module.handler(event, None)["status"] == "complete"


@pytest.mark.parametrize("stage", ["ingest", "gold"])
def test_failure_diagnostics_cannot_mask_original_upload_error(
    simulated_aws, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    client, _ = simulated_aws
    event = daily_event()
    if stage == "gold":
        ingested = lambda_ingest.handler(event, None)
        lambda_curate.handler({**event, **ingested}, None)
        event = {
            "bucket": "fixture-bucket",
            "prefix": "nyc311",
            "mode": "sample",
            "run_id": "diagnostic-gold",
            "days": ["2026-09-01"],
        }
    original = client.put_object

    def fail(*, Bucket, Key, Body):
        if (stage == "ingest" and "/bronze/" in Key) or (
            stage == "gold" and "/gold/run=" in Key
        ):
            raise RuntimeError("Original artifact failure")
        if "/control/" in Key and json.loads(Body)["status"] == "failed":
            raise ValueError("Secondary diagnostic failure")
        return original(Bucket=Bucket, Key=Key, Body=Body)

    monkeypatch.setattr(client, "put_object", fail)
    with pytest.raises(RuntimeError, match="Original artifact failure"):
        (lambda_ingest if stage == "ingest" else lambda_gold).handler(event, None)


def test_gold_complete_retry_is_historical_and_verifies_frozen_artifacts(
    simulated_aws,
) -> None:
    client, _ = simulated_aws
    event = daily_event()
    ingested = lambda_ingest.handler(event, None)
    lambda_curate.handler({**event, **ingested}, None)
    gold_event = {
        "bucket": "fixture-bucket",
        "prefix": "nyc311",
        "mode": "sample",
        "run_id": "historical-gold",
        "days": ["2026-09-01"],
    }
    result = lambda_gold.handler(gold_event, None)
    client.objects["nyc311/sample/control/silver/2026-09-01.json"] = (
        b'{"status": "failed"}'
    )
    client.calls.clear()
    assert lambda_gold.handler(gold_event, None) == result
    assert not any("/silver/" in key for _, key in client.calls)
    client.objects[
        "nyc311/sample/gold/run=historical-gold/demand_daily/part.parquet"
    ] = b"corrupted"
    with pytest.raises(GoldError):
        lambda_gold.handler(gold_event, None)


@pytest.mark.parametrize("stage", ["ingest", "curate", "gold"])
@pytest.mark.parametrize("damage", ["read_failure", "checksum"])
def test_damaged_remote_inputs_fail_without_new_publication(
    simulated_aws, stage: str, damage: str
) -> None:
    client, _ = simulated_aws
    event = daily_event()
    ingested = lambda_ingest.handler(event, None)
    lambda_curate.handler({**event, **ingested}, None)
    key = (
        "nyc311/sample/bronze/run=lambda-run/created_date=2026-09-01/page-0001.json.gz"
    )
    if stage == "gold":
        key = "nyc311/sample/silver/created_date=2026-09-01/part.parquet"
        event = {
            "bucket": "fixture-bucket",
            "prefix": "nyc311",
            "mode": "sample",
            "run_id": "bad-input-gold",
            "days": ["2026-09-01"],
        }
    elif stage == "curate":
        event.update(ingested)
    if damage == "checksum":
        client.objects[key] = b"corrupted"
    else:
        client.fail_get_key = key
    before = dict(client.objects)
    module = {"ingest": lambda_ingest, "curate": lambda_curate, "gold": lambda_gold}[
        stage
    ]
    with pytest.raises((ClientError, RuntimeError)):
        module.handler(event, None)
    assert client.objects == before


@pytest.mark.parametrize("status", [None, "incomplete", "failed"])
def test_gold_rejects_unavailable_silver_before_any_product_write(
    simulated_aws, status
) -> None:
    client, _ = simulated_aws
    if status is not None:
        event = daily_event()
        ingested = lambda_ingest.handler(event, None)
        lambda_curate.handler({**event, **ingested}, None)
        key = "nyc311/sample/control/silver/2026-09-01.json"
        marker = json.loads(client.objects[key])
        marker["status"] = status
        client.objects[key] = json.dumps(marker).encode()
    before = dict(client.objects)
    gold_event = {
        "bucket": "fixture-bucket",
        "prefix": "nyc311",
        "mode": "sample",
        "run_id": "unavailable-silver",
        "days": ["2026-09-01"],
    }
    with pytest.raises((ClientError, ValueError)):
        lambda_gold.handler(gold_event, None)
    assert client.objects == before


def test_empty_fresh_source_cannot_replace_a_previously_populated_day(
    simulated_aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, _ = simulated_aws
    event = daily_event()
    ingested = lambda_ingest.handler(event, None)
    lambda_curate.handler({**event, **ingested}, None)
    original = dict(client.objects)
    monkeypatch.setattr(
        lambda_ingest,
        "create_client",
        lambda config: source.create_client(
            config,
            app_token="",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=[{"n": "0"}])
            ),
        ),
    )
    with pytest.raises(source.ExtractionError):
        lambda_ingest.handler({**event, "run_id": "empty-refresh"}, None)
    assert all(client.objects[key] == content for key, content in original.items())
    assert (
        json.loads(
            client.objects["nyc311/sample/control/bronze/empty-refresh/2026-09-01.json"]
        )["status"]
        == "failed"
    )


def test_fresh_selected_day_refresh_replaces_absent_rows_and_replay_is_logical(
    simulated_aws, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, _ = simulated_aws
    for day in ("2026-09-01", "2026-09-02"):
        event = daily_event(day)
        lambda_curate.handler({**event, **lambda_ingest.handler(event, None)}, None)
    unchanged = {
        key: content for key, content in client.objects.items() if "2026-09-02" in key
    }
    rows = json.loads((ROOT / "data/samples/gold-metrics.json").read_bytes())[1:]
    rows[0]["status"] = "Unmapped Status"
    fixture = tmp_path / "updated.json"
    fixture.write_text(json.dumps(rows))
    monkeypatch.setattr(
        lambda_ingest,
        "create_client",
        lambda config: source.create_client(
            config, app_token="", transport=fixture_transport(fixture)
        ),
    )
    event = daily_event(run_id="fresh-update")
    ingested = lambda_ingest.handler(event, None)
    assert lambda_curate.handler({**event, **ingested}, None)["row_count"] == 6
    store = S3Store(client, "fixture-bucket", "nyc311/sample")
    with temporary_workspace(tmp_path) as base:
        root = output_root(base, "sample")
        marker = store.read_json("control/silver/2026-09-01.json")
        store.download("control/silver/2026-09-01.json", root)
        for field in ("partition_key", "quality_report_key", "quarantine_key"):
            store.download(marker[field], root)
        _, before = load_silver(base, date(2026, 9, 1))
        assert "sample-gold-01" not in before["unique_key"].to_pylist()
        assert (
            next(
                row
                for row in before.to_pylist()
                if row["unique_key"] == "sample-gold-02"
            )["status"]
            == "UNMAPPED STATUS"
        )
        replay = lambda_ingest.handler(
            {
                **event,
                "run_id": "replay-update",
                "operation": "replay",
                "bronze_manifest_key": ingested["bronze_manifest_key"],
            },
            None,
        )
        lambda_curate.handler(replay, None)
        marker = store.read_json("control/silver/2026-09-01.json")
        store.download("control/silver/2026-09-01.json", root)
        for field in ("partition_key", "quality_report_key", "quarantine_key"):
            store.download(marker[field], root)
        assert before.equals(
            load_silver(base, date(2026, 9, 1))[1], check_metadata=True
        )
    assert all(client.objects[key] == content for key, content in unchanged.items())


@pytest.mark.parametrize(
    "damage",
    ["window", "payload", "coverage", "records", "prefix", "date_format", "days_type"],
)
def test_invalid_metadata_fails_before_any_sdk_client_construction(
    monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    def no_sdk():
        raise AssertionError("Invalid events must not construct AWS clients")

    monkeypatch.setattr(lambda_common, "create_s3_client", no_sdk)
    event = daily_event()
    if damage == "window":
        event["end_date"] = "2026-09-03"
    elif damage == "payload":
        event["data"] = "x" * lambda_common.PAYLOAD_LIMIT
    elif damage == "coverage":
        event["mode"] = "published"
        event["days"] = ["2026-09-01"]
    elif damage == "records":
        event["records"] = [{"unique_key": "123"}]
    elif damage == "prefix":
        event["prefix"] = "../other-project"
    elif damage == "date_format":
        event["start_date"] = "20260901"
    else:
        event["days"] = "2026-09-01"
    with pytest.raises(ValueError):
        (
            lambda_gold.handler
            if damage in ("coverage", "days_type")
            else lambda_ingest.handler
        )(event, None)


def catalog_gold_event(run_id: str = "catalog-gold") -> dict:
    return {
        "bucket": "fixture-bucket",
        "prefix": "nyc311",
        "mode": "sample",
        "run_id": run_id,
        "days": ["2026-09-01"],
        "catalog": {"database": "nyc311"},
    }


def test_gold_publishes_to_catalog_only_after_artifacts(
    simulated_aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, _ = simulated_aws
    event = daily_event()
    ingested = lambda_ingest.handler(event, None)
    lambda_curate.handler({**event, **ingested}, None)
    calls = []
    monkeypatch.setattr(lambda_common, "create_glue_client", lambda: "glue-client")
    monkeypatch.setattr(
        catalog,
        "publish_gold_locations",
        lambda client, **kwargs: calls.append((client, kwargs)) or {},
    )
    result = lambda_gold.handler(catalog_gold_event(), None)
    assert result["status"] == "complete"
    assert calls == [
        (
            "glue-client",
            {
                "database": "nyc311",
                "bucket": "fixture-bucket",
                "prefix": "nyc311",
                "mode": "sample",
                "run_id": "catalog-gold",
            },
        )
    ]


def test_catalog_failure_leaves_gold_published_but_run_unsuccessful(
    simulated_aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, _ = simulated_aws
    event = daily_event()
    ingested = lambda_ingest.handler(event, None)
    lambda_curate.handler({**event, **ingested}, None)

    def fail(*args, **kwargs):
        raise CatalogError("Glue table is unavailable")

    monkeypatch.setattr(lambda_common, "create_glue_client", lambda: object())
    monkeypatch.setattr(catalog, "publish_gold_locations", fail)
    with pytest.raises(CatalogError):
        lambda_gold.handler(catalog_gold_event("catalog-failure"), None)
    manifest = json.loads(
        client.objects["nyc311/sample/control/gold/catalog-failure.json"]
    )
    assert manifest["status"] == "complete"
    assert all(
        f"nyc311/sample/{descriptor['key']}" in client.objects
        for descriptor in manifest["products"].values()
    )


@pytest.mark.parametrize(
    "catalog_request",
    [{"database": "not valid"}, {}, {"database": "nyc311", "extra": "x"}],
)
def test_invalid_catalog_request_fails_before_sdk_construction(
    monkeypatch: pytest.MonkeyPatch, catalog_request: dict
) -> None:
    def no_sdk():
        raise AssertionError("Invalid events must not construct AWS clients")

    monkeypatch.setattr(lambda_common, "create_s3_client", no_sdk)
    monkeypatch.setattr(lambda_common, "create_glue_client", no_sdk)
    event = {**daily_event(), "catalog": catalog_request}
    with pytest.raises(ValueError):
        lambda_gold.handler(event, None)
