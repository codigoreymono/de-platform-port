import gzip
import json
from collections.abc import Callable
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from nyc311 import gold_store as store
from nyc311.config import COHORT_END, COHORT_START, daily_windows
from nyc311.gold import PRODUCT_SCHEMAS, GoldError, prepare_gold_product
from nyc311.manifests import BronzeManifest
from nyc311.silver_store import publish_silver
from nyc311.source import SourcePage
from nyc311.storage import (
    manifest_key,
    object_path,
    output_root,
    read_json,
    write_bytes,
    write_json,
)

DAY = COHORT_START
SAMPLES = Path(__file__).resolve().parents[1] / "data" / "samples"
type FixtureInput = Callable[..., tuple[BronzeManifest, list[SourcePage]]]


def save_input(
    base: Path,
    fixture: FixtureInput,
    day: date,
    records: list[dict[str, object]],
    *,
    mode: str = "sample",
    observed: str = "2026-10-13T12:00:00Z",
) -> None:
    manifest, pages = fixture(records, day=day, observed=observed)
    manifest["mode"] = mode
    root = output_root(base, mode)
    for page, reference in zip(pages, manifest["pages"]):
        write_bytes(root, reference["key"], gzip.compress(page["content"], mtime=0))
    write_json(root, manifest_key(manifest), manifest)
    publish_silver(base, manifest_key(manifest), mode=mode)


def metrics_input(base: Path, fixture: FixtureInput) -> list[date]:
    records = json.loads((SAMPLES / "gold-metrics.json").read_bytes())
    days = [DAY, date(2026, 9, 2)]
    for day, observed in zip(days, ["2026-10-13T12:00:00Z", "2026-10-20T12:00:00Z"]):
        values = [
            row for row in records if row["created_date"].startswith(day.isoformat())
        ]
        save_input(base, fixture, day, values, observed=observed)
    return days


def test_joint_sample_publication_and_integrity_metadata(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    days = metrics_input(tmp_path, silver_input)
    manifest = store.publish_gold(tmp_path, "complete", days=days)
    loaded, tables = store.load_gold(tmp_path, "complete")
    assert loaded == manifest
    assert manifest["status"] == "complete"
    assert manifest["coverage"] == manifest["mode"] == "sample"
    assert manifest["covered_days"] == [day.isoformat() for day in days]
    assert len(manifest["input_partitions"]) == 2
    assert set(tables) == set(manifest["products"]) == set(PRODUCT_SCHEMAS)
    assert manifest["observation_min_utc"] == "2026-10-13T12:00:00+00:00"
    assert manifest["observation_max_utc"] == "2026-10-20T12:00:00+00:00"
    assert (
        manifest["observations_by_day"]["2026-09-02"]["min_utc"]
        == manifest["observation_max_utc"]
    )
    assert datetime.fromisoformat(manifest["generated_at_utc"]).tzinfo == UTC
    assert (
        manifest["quality_exclusions"]["accepted"]
        == manifest["quality_exclusions"]["received"]
        == 12
    )
    assert manifest["quality_exclusions"]["duration_exclusions"] == {
        "closed_without_valid_duration": 4,
        "not_closed": 3,
    }
    assert sum(tables["demand_daily"]["request_count"].to_pylist()) == 12
    assert sum(tables["channel_usage"]["request_count"].to_pylist()) == 12
    assert sum(tables["outstanding_requests"]["outstanding_count"].to_pylist()) == 2
    assert sum(tables["outstanding_requests"]["unknown_status_count"].to_pylist()) == 1
    assert sum(tables["closure_duration"]["valid_closed_count"].to_pylist()) == 5
    root = output_root(tmp_path, "sample")
    for product, descriptor in manifest["products"].items():
        path = object_path(root, descriptor["key"])
        assert descriptor["sha256"] == sha256(path.read_bytes()).hexdigest()
        assert pq.ParquetFile(path).read().equals(tables[product], check_metadata=True)
        assert descriptor["row_count"] == tables[product].num_rows
    assert list(root.rglob("*.tmp")) == []
    assert not output_root(tmp_path, "published").exists()


def test_one_verified_input_snapshot_is_used_for_all_four_products(
    tmp_path: Path, silver_input: FixtureInput, monkeypatch: pytest.MonkeyPatch
) -> None:
    days = metrics_input(tmp_path, silver_input)
    real_load, real_compute = store.load_gold_inputs, store._compute_query
    loads = []
    tables = []

    def load(*args: object, **kwargs: object):
        loads.append(True)
        return real_load(*args, **kwargs)

    def compute(table: object, product: str, query: str):
        tables.append(id(table))
        return real_compute(table, product, query)

    monkeypatch.setattr(store, "load_gold_inputs", load)
    monkeypatch.setattr(store, "_compute_query", compute)
    store.publish_gold(tmp_path, "snapshot", days=days)
    assert loads == [True]
    assert len(tables) == 4 and len(set(tables)) == 1


def test_official_publication_requires_all_days_and_keeps_samples_separate(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    save_input(tmp_path, silver_input, DAY, [], mode="published")
    with pytest.raises(GoldError):
        store.publish_gold(tmp_path, "missing", mode="published")
    root = output_root(tmp_path, "published")
    assert not (root / "gold").exists()
    assert not (root / "control/gold").exists()
    for day, _ in daily_windows(COHORT_START, COHORT_END):
        if day != DAY:
            save_input(tmp_path, silver_input, day, [], mode="published")
    manifest = store.publish_gold(tmp_path, "official", mode="published")
    assert manifest["coverage"] == "full_month"
    assert len(manifest["covered_days"]) == 30
    assert all(
        table.num_rows == 0
        for table in store.load_gold(tmp_path, "official", mode="published")[1].values()
    )
    assert manifest["observation_min_utc"] == manifest["observation_max_utc"]
    with pytest.raises(GoldError):
        store.load_gold(tmp_path, "official", mode="sample")


def test_empty_and_all_rejected_days_retain_source_observation_provenance(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    record = json.loads((SAMPLES / "updated-request.json").read_bytes())[0]
    record["created_date"] = "invalid-date"
    save_input(tmp_path, silver_input, DAY, [record], observed="2026-10-13T12:00:00Z")
    second = date(2026, 9, 2)
    save_input(tmp_path, silver_input, second, [], observed="2026-10-20T12:00:00Z")
    manifest = store.publish_gold(tmp_path, "empty", days=[DAY, second])
    assert manifest["quality_exclusions"]["accepted"] == 0
    assert manifest["quality_exclusions"]["quarantined"] == 1
    assert manifest["quality_exclusions"]["received"] == 1
    assert manifest["observation_min_utc"] != manifest["observation_max_utc"]
    assert all(
        table.num_rows == 0 for table in store.load_gold(tmp_path, "empty")[1].values()
    )


@pytest.mark.parametrize("failure", [GoldError, OSError, KeyboardInterrupt])
def test_partial_run_is_not_published_and_previous_run_remains_valid(
    tmp_path: Path,
    silver_input: FixtureInput,
    monkeypatch: pytest.MonkeyPatch,
    failure: type[BaseException],
) -> None:
    days = metrics_input(tmp_path, silver_input)
    previous = store.publish_gold(tmp_path, "previous", days=days)
    real_write = store._write_prepared_product

    def stop(
        root: Path, inputs: object, product: str, run_id: str, result: object
    ) -> Path:
        if product == "outstanding_requests":
            raise failure("sensitive diagnostic must not be stored")
        return real_write(root, inputs, product, run_id, result)

    with monkeypatch.context() as patch:
        patch.setattr(store, "_write_prepared_product", stop)
        with pytest.raises(failure):
            store.publish_gold(tmp_path, "partial", days=days)
    root = output_root(tmp_path, "sample")
    partial = read_json(root, "control/gold/partial.json")
    assert partial["status"] != "complete"
    assert len(partial["products"]) == 2
    assert "sensitive diagnostic" not in json.dumps(partial)
    with pytest.raises(GoldError, match="Only complete"):
        store.load_gold(tmp_path, "partial")
    assert store.load_gold(tmp_path, "previous")[0] == previous
    with pytest.raises(FileExistsError):
        store.publish_gold(tmp_path, "partial", days=days)
    store.publish_gold(tmp_path, "retry-new-id", days=days)
    assert store.load_gold(tmp_path, "retry-new-id")[0]["status"] == "complete"


@pytest.mark.parametrize("failure", [OSError, KeyboardInterrupt])
def test_interrupted_complete_marker_does_not_authorize_four_prepared_files(
    tmp_path: Path,
    silver_input: FixtureInput,
    monkeypatch: pytest.MonkeyPatch,
    failure: type[BaseException],
) -> None:
    days = metrics_input(tmp_path, silver_input)
    real_write = store.write_json_atomic

    def stop(root: Path, key: str, value: dict[str, object]) -> None:
        if value["status"] == "complete":
            raise failure("commit interrupted")
        real_write(root, key, value)

    monkeypatch.setattr(store, "write_json_atomic", stop)
    with pytest.raises(failure):
        store.publish_gold(tmp_path, "commit", days=days)
    manifest = read_json(output_root(tmp_path, "sample"), "control/gold/commit.json")
    assert len(manifest["products"]) == 4
    assert manifest["status"] != "complete"
    with pytest.raises(GoldError, match="Only complete"):
        store.load_gold(tmp_path, "commit")


@pytest.mark.parametrize("product", list(PRODUCT_SCHEMAS))
def test_missing_or_corrupt_product_blocks_joint_consumption(
    tmp_path: Path, silver_input: FixtureInput, product: str
) -> None:
    days = metrics_input(tmp_path, silver_input)
    manifest = store.publish_gold(tmp_path, "damage", days=days)
    path = object_path(
        output_root(tmp_path, "sample"), manifest["products"][product]["key"]
    )
    path.write_bytes(b"changed")
    with pytest.raises(GoldError, match="checksum"):
        store.load_gold(tmp_path, "damage")
    path.unlink()
    with pytest.raises(GoldError, match="unavailable"):
        store.load_gold(tmp_path, "damage")


def test_existing_managed_run_cannot_be_overwritten_or_individually_prepared(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    days = metrics_input(tmp_path, silver_input)
    manifest = store.publish_gold(tmp_path, "immutable", days=days)
    with pytest.raises(FileExistsError):
        store.publish_gold(tmp_path, "immutable", days=days)
    with pytest.raises(GoldError, match="managed Gold"):
        prepare_gold_product(tmp_path, "demand_daily", "immutable", days=days)
    assert store.load_gold(tmp_path, "immutable")[0] == manifest


def test_old_gold_remains_readable_after_new_silver_observations(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    days = metrics_input(tmp_path, silver_input)
    manifest = store.publish_gold(tmp_path, "historical", days=days)
    _, original = store.load_gold(tmp_path, "historical")
    save_input(tmp_path, silver_input, DAY, [], observed="2026-10-25T12:00:00Z")
    loaded, tables = store.load_gold(tmp_path, "historical")
    assert loaded == manifest
    assert all(
        tables[name].equals(table, check_metadata=True)
        for name, table in original.items()
    )


def test_final_verification_rejects_product_lost_before_complete_marker(
    tmp_path: Path, silver_input: FixtureInput, monkeypatch: pytest.MonkeyPatch
) -> None:
    days = metrics_input(tmp_path, silver_input)
    real_verify = store._verify_run

    def lose_product(root: Path, manifest: dict[str, object]):
        object_path(root, manifest["products"]["channel_usage"]["key"]).unlink()
        return real_verify(root, manifest)

    monkeypatch.setattr(store, "_verify_run", lose_product)
    with pytest.raises(GoldError, match="unavailable"):
        store.publish_gold(tmp_path, "lost", days=days)
    marker = read_json(output_root(tmp_path, "sample"), "control/gold/lost.json")
    assert marker["status"] == "failed"
    with pytest.raises(GoldError, match="Only complete"):
        store.load_gold(tmp_path, "lost")


def test_individually_prepared_files_are_not_joint_publication(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    days = metrics_input(tmp_path, silver_input)
    for product in PRODUCT_SCHEMAS:
        prepare_gold_product(tmp_path, product, "prepared-only", days=days)
    with pytest.raises(GoldError, match="unavailable"):
        store.load_gold(tmp_path, "prepared-only")
    with pytest.raises(FileExistsError):
        store.publish_gold(tmp_path, "prepared-only", days=days)


@pytest.mark.parametrize("damage", ["missing-product", "coverage", "row-count"])
def test_inconsistent_complete_manifest_is_rejected(
    tmp_path: Path, silver_input: FixtureInput, damage: str
) -> None:
    days = metrics_input(tmp_path, silver_input)
    manifest = store.publish_gold(tmp_path, "invalid", days=days)
    if damage == "missing-product":
        del manifest["products"]["outstanding_requests"]
    elif damage == "coverage":
        manifest["covered_days"] = [DAY.isoformat()]
    else:
        manifest["products"]["channel_usage"]["row_count"] += 1
    write_json(output_root(tmp_path, "sample"), manifest_key(manifest), manifest)
    with pytest.raises(GoldError):
        store.load_gold(tmp_path, "invalid")


@pytest.mark.parametrize(
    "damage", ["missing-input-fields", "invalid-input-digest", "missing-exclusions"]
)
def test_frozen_provenance_is_required_without_reading_current_silver(
    tmp_path: Path, silver_input: FixtureInput, damage: str
) -> None:
    days = metrics_input(tmp_path, silver_input)
    manifest = store.publish_gold(tmp_path, "provenance", days=days)
    if damage == "missing-input-fields":
        manifest["input_partitions"][0] = {
            name: manifest["input_partitions"][0][name]
            for name in (
                "kind",
                "status",
                "mode",
                "start_date",
                "end_date",
                "row_count",
            )
        }
    elif damage == "invalid-input-digest":
        manifest["input_partitions"][0]["quality_report_sha256"] = "not-a-digest"
    else:
        del manifest["quality_exclusions"]["duration_exclusions"]
    write_json(output_root(tmp_path, "sample"), manifest_key(manifest), manifest)
    with pytest.raises(GoldError):
        store.load_gold(tmp_path, "provenance")
