import gzip
import json
from collections.abc import Callable
from datetime import date
from hashlib import sha256
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc311 import silver_store as store
from nyc311 import source
from nyc311.manifests import BronzeManifest, SilverManifest
from nyc311.silver import SILVER_SCHEMA, SilverError
from nyc311.source import SourcePage
from nyc311.storage import (
    manifest_key,
    object_path,
    output_root,
    read_json,
    write_bytes,
    write_json,
)

DAY = date(2026, 9, 1)
SAMPLES = Path(__file__).resolve().parents[1] / "data" / "samples"
type FixtureInput = Callable[..., tuple[BronzeManifest, list[SourcePage]]]


def rows(name: str = "pagination-001.json") -> list[dict[str, object]]:
    return json.loads((SAMPLES / name).read_bytes())


def saved_bronze(
    base: Path,
    fixture: FixtureInput,
    records: list[dict[str, object]],
    run_id: str = "source-001",
    day: date = DAY,
    mode: str = "sample",
) -> str:
    manifest, pages = fixture(records, day=day)
    manifest["run_id"] = run_id
    manifest["mode"] = mode
    root = output_root(base, mode)
    reference = manifest["pages"][0]
    reference["key"] = (
        f"bronze/run={run_id}/created_date={day.isoformat()}/page-0001.json.gz"
    )
    write_bytes(root, reference["key"], gzip.compress(pages[0]["content"], mtime=0))
    write_json(root, manifest_key(manifest), manifest)
    return manifest_key(manifest)


def initial_partition(base: Path, fixture: FixtureInput) -> SilverManifest:
    key = saved_bronze(base, fixture, rows())
    return store.publish_silver(base, key)


def test_publish_partition_auxiliary_files_and_complete_manifest(
    tmp_path: Path, silver_input: FixtureInput, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = saved_bronze(tmp_path, silver_input, rows())

    def no_http(*args: object, **kwargs: object) -> None:
        raise AssertionError("Publication must not invoke HTTP")

    monkeypatch.setattr(source, "_request", no_http)
    result = store.publish_silver(tmp_path, key, run_id="processing-001")
    root = output_root(tmp_path, "sample")
    assert result["status"] == "complete"
    assert result["partition_key"] == "silver/created_date=2026-09-01/part.parquet"
    loaded, table = store.load_silver(tmp_path, DAY)
    assert loaded == result
    assert table.schema.equals(SILVER_SCHEMA, check_metadata=True)
    assert table.num_rows == result["row_count"] == 2
    report = read_json(root, result["quality_report_key"])
    assert report["accepted"] == 2
    assert report["source_manifest_key"] == key
    assert (
        json.loads(
            gzip.decompress(object_path(root, result["quarantine_key"]).read_bytes())
        )
        == []
    )
    assert read_json(root, manifest_key(result)) == result
    footer = pq.ParquetFile(object_path(root, result["partition_key"])).metadata
    assert footer.row_group(0).column(0).compression == "SNAPPY"
    assert list(root.rglob("*.tmp")) == []
    assert not (root / "gold").exists()


def test_verified_empty_partition_is_written_with_stable_schema(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    key = saved_bronze(tmp_path, silver_input, [])
    manifest = store.publish_silver(tmp_path, key)
    loaded, table = store.load_silver(tmp_path, DAY)
    assert loaded == manifest
    assert manifest["row_count"] == table.num_rows == 0
    assert table.schema.equals(SILVER_SCHEMA, check_metadata=True)
    assert object_path(
        output_root(tmp_path, "sample"), manifest["partition_key"]
    ).exists()


def test_rejected_records_and_quality_are_persisted(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    invalid = dict(rows()[1], created_date="invalid-date")
    key = saved_bronze(tmp_path, silver_input, [rows()[0], invalid])
    manifest = store.publish_silver(tmp_path, key)
    root = output_root(tmp_path, "sample")
    _, table = store.load_silver(tmp_path, DAY)
    assert table.num_rows == 1
    quarantine = json.loads(
        gzip.decompress(object_path(root, manifest["quarantine_key"]).read_bytes())
    )
    assert quarantine[0]["original"] == invalid
    assert quarantine[0]["reason_codes"] == ["invalid_created_date"]
    report = read_json(root, manifest["quality_report_key"])
    assert (
        report["received"]
        == report["accepted"]
        + report["quarantined"]
        + report["identical_duplicates_discarded"]
    )


@pytest.mark.parametrize("mode", ["sample", "published"])
def test_modes_remain_separate(
    tmp_path: Path, silver_input: FixtureInput, mode: str
) -> None:
    key = saved_bronze(tmp_path, silver_input, rows(), mode=mode)
    manifest = store.publish_silver(tmp_path, key, mode=mode)
    assert store.load_silver(tmp_path, DAY, mode=mode)[0] == manifest
    other = "published" if mode == "sample" else "sample"
    assert not output_root(tmp_path, other).exists()


@pytest.mark.parametrize("failure", [OSError, KeyboardInterrupt])
def test_partial_parquet_write_preserves_previous_publication(
    tmp_path: Path,
    silver_input: FixtureInput,
    monkeypatch: pytest.MonkeyPatch,
    failure: type[BaseException],
) -> None:
    original = initial_partition(tmp_path, silver_input)
    root = output_root(tmp_path, "sample")
    original_data = object_path(root, original["partition_key"]).read_bytes()
    original_marker = object_path(root, manifest_key(original)).read_bytes()

    def interrupted_write(table: object, path: Path, **kwargs: object) -> None:
        path.write_bytes(b"PAR1 partial write")
        raise failure("interrupted")

    monkeypatch.setattr(store.pq, "write_table", interrupted_write)
    with pytest.raises(failure):
        store.publish_silver(tmp_path, original["source_manifest_key"])
    assert object_path(root, original["partition_key"]).read_bytes() == original_data
    assert object_path(root, manifest_key(original)).read_bytes() == original_marker
    assert store.load_silver(tmp_path, DAY)[0] == original
    assert list(root.rglob("*.tmp")) == []


def test_failed_invalidation_does_not_replace_any_current_artifact(
    tmp_path: Path, silver_input: FixtureInput, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = initial_partition(tmp_path, silver_input)
    root = output_root(tmp_path, "sample")
    keys = [
        manifest_key(original),
        original["partition_key"],
        original["quality_report_key"],
        original["quarantine_key"],
    ]
    before = {key: object_path(root, key).read_bytes() for key in keys}
    real_write = store.write_json_atomic

    def deny_invalidation(root: Path, key: str, value: dict[str, object]) -> None:
        if value["status"] == "incomplete":
            raise OSError("cannot invalidate")
        real_write(root, key, value)

    monkeypatch.setattr(store, "write_json_atomic", deny_invalidation)
    with pytest.raises(OSError):
        store.publish_silver(tmp_path, original["source_manifest_key"])
    assert before == {key: object_path(root, key).read_bytes() for key in keys}
    assert store.load_silver(tmp_path, DAY)[0] == original
    assert list(root.rglob("*.tmp")) == []


@pytest.mark.parametrize("artifact", ["quality", "quarantine", "partition"])
def test_failed_replacement_blocks_consumption_until_retry(
    tmp_path: Path,
    silver_input: FixtureInput,
    monkeypatch: pytest.MonkeyPatch,
    artifact: str,
) -> None:
    original = initial_partition(tmp_path, silver_input)
    root = output_root(tmp_path, "sample")
    old_data = object_path(root, original["partition_key"]).read_bytes()
    targets = {
        "quality": original["quality_report_key"],
        "quarantine": original["quarantine_key"],
        "partition": original["partition_key"],
    }
    target = object_path(root, targets[artifact])
    real_replace = Path.replace

    def failed_replace(path: Path, destination: Path) -> Path:
        if Path(destination) == target:
            raise OSError("replacement failed")
        return real_replace(path, destination)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "replace", failed_replace)
        with pytest.raises(OSError):
            store.publish_silver(tmp_path, original["source_manifest_key"])
    marker = read_json(root, manifest_key(original))
    assert marker["status"] == "failed"
    assert marker["error"] == "OSError"
    assert object_path(root, original["partition_key"]).read_bytes() == old_data
    with pytest.raises(SilverError, match="Only complete"):
        store.load_silver(tmp_path, DAY)
    restored = store.publish_silver(tmp_path, original["source_manifest_key"])
    assert store.load_silver(tmp_path, DAY)[0] == restored
    assert list(root.rglob("*.tmp")) == []


@pytest.mark.parametrize("failure", [OSError, KeyboardInterrupt])
def test_failure_between_parquet_replacement_and_complete_marker_is_not_consumable(
    tmp_path: Path,
    silver_input: FixtureInput,
    monkeypatch: pytest.MonkeyPatch,
    failure: type[BaseException],
) -> None:
    original = initial_partition(tmp_path, silver_input)
    refreshed_rows = rows("updated-request.json")
    key = saved_bronze(tmp_path, silver_input, refreshed_rows, run_id="source-002")
    real_write = store.write_json_atomic

    def interrupt_marker(root: Path, key: str, value: dict[str, object]) -> None:
        if value["status"] == "complete":
            raise failure("cannot commit complete marker")
        real_write(root, key, value)

    with monkeypatch.context() as patch:
        patch.setattr(store, "write_json_atomic", interrupt_marker)
        with pytest.raises(failure):
            store.publish_silver(tmp_path, key)
    root = output_root(tmp_path, "sample")
    actual_data = pq.ParquetFile(object_path(root, original["partition_key"])).read()
    assert actual_data.num_rows == 1
    assert actual_data["status"].to_pylist() == ["CLOSED"]
    assert read_json(root, manifest_key(original))["status"] != "complete"
    with pytest.raises(SilverError, match="Only complete"):
        store.load_silver(tmp_path, DAY)
    store.publish_silver(tmp_path, key)
    assert store.load_silver(tmp_path, DAY)[1].equals(actual_data)


def test_atomic_manifest_partial_write_preserves_incomplete_marker(
    tmp_path: Path, silver_input: FixtureInput, monkeypatch: pytest.MonkeyPatch
) -> None:
    from nyc311 import storage

    original = initial_partition(tmp_path, silver_input)
    real_write = storage.write_json

    def partial_metadata(root: Path, key: str, value: dict[str, object]) -> None:
        if value["status"] == "complete":
            write_bytes(root, key, b'{"partial":')
            raise OSError("partial write")
        real_write(root, key, value)

    monkeypatch.setattr(storage, "write_json", partial_metadata)
    with pytest.raises(OSError):
        store.publish_silver(tmp_path, original["source_manifest_key"])
    root = output_root(tmp_path, "sample")
    assert read_json(root, manifest_key(original))["status"] == "failed"
    with pytest.raises(SilverError):
        store.load_silver(tmp_path, DAY)
    assert list(root.rglob("*.tmp")) == []


@pytest.mark.parametrize(
    "artifact", ["partition_key", "quality_report_key", "quarantine_key"]
)
def test_missing_or_changed_artifact_is_not_consumable(
    tmp_path: Path, silver_input: FixtureInput, artifact: str
) -> None:
    manifest = initial_partition(tmp_path, silver_input)
    path = object_path(output_root(tmp_path, "sample"), manifest[artifact])
    path.write_bytes(b"changed")
    with pytest.raises(SilverError, match="checksum"):
        store.load_silver(tmp_path, DAY)
    path.unlink()
    with pytest.raises(SilverError, match="unavailable"):
        store.load_silver(tmp_path, DAY)


def test_raw_parquet_without_complete_manifest_is_rejected(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    manifest = initial_partition(tmp_path, silver_input)
    root = output_root(tmp_path, "sample")
    object_path(root, manifest_key(manifest)).unlink()
    assert object_path(root, manifest["partition_key"]).exists()
    with pytest.raises(SilverError, match="unavailable"):
        store.load_silver(tmp_path, DAY)


def test_old_completion_marker_is_invalidated_before_any_artifact_replacement(
    tmp_path: Path, silver_input: FixtureInput, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = initial_partition(tmp_path, silver_input)
    root = output_root(tmp_path, "sample")
    targets = {
        object_path(root, original[field])
        for field in ("partition_key", "quality_report_key", "quarantine_key")
    }
    replacements = []
    real_replace = Path.replace

    def observe_replacement(path: Path, destination: Path) -> Path:
        destination = Path(destination)
        if destination in targets:
            assert read_json(root, manifest_key(original))["status"] == "incomplete"
            with pytest.raises(SilverError, match="Only complete"):
                store.load_silver(tmp_path, DAY)
            assert path.parent == destination.parent
            assert path.name.endswith(".tmp")
            replacements.append(destination)
        return real_replace(path, destination)

    monkeypatch.setattr(Path, "replace", observe_replacement)
    store.publish_silver(tmp_path, original["source_manifest_key"])
    assert set(replacements) == targets


def test_failed_complete_and_failed_status_writes_leave_incomplete_marker(
    tmp_path: Path, silver_input: FixtureInput, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = initial_partition(tmp_path, silver_input)
    real_write = store.write_json_atomic

    def incomplete_only(root: Path, key: str, value: dict[str, object]) -> None:
        if value["status"] != "incomplete":
            raise OSError("cannot persist final state")
        real_write(root, key, value)

    monkeypatch.setattr(store, "write_json_atomic", incomplete_only)
    with pytest.raises(OSError):
        store.publish_silver(tmp_path, original["source_manifest_key"])
    marker = read_json(output_root(tmp_path, "sample"), manifest_key(original))
    assert marker["status"] == "incomplete"
    with pytest.raises(SilverError, match="Only complete"):
        store.load_silver(tmp_path, DAY)


def test_replay_replaces_one_file_without_duplicate_accumulation(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    original = initial_partition(tmp_path, silver_input)
    _, expected = store.load_silver(tmp_path, DAY)
    root = output_root(tmp_path, "sample")
    expected_report = read_json(root, original["quality_report_key"])
    for _ in range(3):
        current = store.publish_silver(tmp_path, original["source_manifest_key"])
        loaded, actual = store.load_silver(tmp_path, DAY)
        assert actual.equals(expected, check_metadata=True)
        assert actual.num_rows == 2
        assert current == loaded
        assert read_json(root, current["quality_report_key"]) == expected_report
    assert list((root / "silver").rglob("*.parquet")) == [
        object_path(root, original["partition_key"])
    ]
    assert list(root.rglob("*.tmp")) == []


def test_new_observation_replaces_old_status_and_provenance(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    original = initial_partition(tmp_path, silver_input)
    new_rows = [rows("updated-request.json")[0], rows()[1]]
    key = saved_bronze(tmp_path, silver_input, new_rows, run_id="source-002")
    current = store.publish_silver(tmp_path, key)
    _, table = store.load_silver(tmp_path, DAY)
    by_key = {row["unique_key"]: row for row in table.to_pylist()}
    assert table.num_rows == 2
    assert by_key["sample-0001"]["status"] == "CLOSED"
    assert by_key["sample-0001"]["closed_date_local"] is not None
    assert by_key["sample-0001"]["duration_eligible"] is True
    assert by_key["sample-0002"]["status"] == "IN PROGRESS"
    assert all(row["source_run_id"] == "source-002" for row in by_key.values())
    assert current["source_manifest_key"] == key
    assert current["partition_key"] == original["partition_key"]


def test_absent_observation_is_removed_not_merged_with_previous_partition(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    initial_partition(tmp_path, silver_input)
    key = saved_bronze(
        tmp_path, silver_input, rows("updated-request.json"), run_id="source-002"
    )
    store.publish_silver(tmp_path, key)
    _, table = store.load_silver(tmp_path, DAY)
    assert table["unique_key"].to_pylist() == ["sample-0001"]


def test_latest_rejected_observation_does_not_keep_stale_silver_row(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    initial_partition(tmp_path, silver_input)
    invalid = dict(rows()[0], created_date="not-a-date")
    key = saved_bronze(
        tmp_path, silver_input, [invalid, rows()[1]], run_id="source-rejected"
    )
    current = store.publish_silver(tmp_path, key)
    root = output_root(tmp_path, "sample")
    _, table = store.load_silver(tmp_path, DAY)
    assert table["unique_key"].to_pylist() == ["sample-0002"]
    rejected = json.loads(
        gzip.decompress(object_path(root, current["quarantine_key"]).read_bytes())
    )
    assert rejected[0]["original"] == invalid
    assert read_json(root, current["quality_report_key"])["quarantined"] == 1


def test_all_rejected_refresh_replaces_populated_partition_with_typed_empty_file(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    initial_partition(tmp_path, silver_input)
    rejected = [dict(row, created_date="not-a-date") for row in rows()]
    key = saved_bronze(tmp_path, silver_input, rejected, run_id="source-all-rejected")
    current = store.publish_silver(tmp_path, key)
    _, table = store.load_silver(tmp_path, DAY)
    assert current["status"] == "complete"
    assert current["row_count"] == table.num_rows == 0
    assert table.schema.equals(SILVER_SCHEMA, check_metadata=True)
    report = read_json(output_root(tmp_path, "sample"), current["quality_report_key"])
    assert report["quarantined"] == report["received"] == 2


def test_reused_processing_identifier_clears_old_quarantine_safely(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    records = rows()
    records[0]["created_date"] = "bad"
    old_key = saved_bronze(tmp_path, silver_input, records, run_id="source-invalid")
    old = store.publish_silver(tmp_path, old_key, run_id="processing")
    root = output_root(tmp_path, "sample")
    assert read_json(root, old["quality_report_key"])["quarantined"] == 1
    new_key = saved_bronze(tmp_path, silver_input, rows(), run_id="source-valid")
    current = store.publish_silver(tmp_path, new_key, run_id="processing")
    assert current["quality_report_key"] == old["quality_report_key"]
    assert current["quarantine_key"] == old["quarantine_key"]
    assert (
        json.loads(
            gzip.decompress(object_path(root, current["quarantine_key"]).read_bytes())
        )
        == []
    )
    assert read_json(root, current["quality_report_key"])["quarantined"] == 0
    assert store.load_silver(tmp_path, DAY)[1].num_rows == 2


def test_refresh_does_not_modify_another_creation_day(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    original = initial_partition(tmp_path, silver_input)
    other_day = date(2026, 9, 2)
    other_row = dict(
        rows()[0], unique_key="sample-other-day", created_date="2026-09-02T00:00:00"
    )
    other_key = saved_bronze(
        tmp_path, silver_input, [other_row], run_id="source-other", day=other_day
    )
    other = store.publish_silver(tmp_path, other_key)
    root = output_root(tmp_path, "sample")
    keys = [
        manifest_key(other),
        other["partition_key"],
        other["quality_report_key"],
        other["quarantine_key"],
    ]
    snapshot = {key: object_path(root, key).read_bytes() for key in keys}
    store.publish_silver(tmp_path, original["source_manifest_key"])
    assert snapshot == {key: object_path(root, key).read_bytes() for key in keys}
    assert store.load_silver(tmp_path, other_day)[1].num_rows == 1
    with duckdb.connect(":memory:") as connection:
        result = connection.execute(
            "SELECT created_date, count(*) FROM read_parquet(?) GROUP BY created_date ORDER BY created_date",
            [str(root / "silver" / "created_date=*" / "part.parquet")],
        ).fetchall()
    assert result == [(DAY, 2), (other_day, 1)]


def test_invalid_new_bronze_preserves_last_successful_publication(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    original = initial_partition(tmp_path, silver_input)
    root = output_root(tmp_path, "sample")
    data = object_path(root, original["partition_key"]).read_bytes()
    marker = object_path(root, manifest_key(original)).read_bytes()
    key = saved_bronze(tmp_path, silver_input, rows(), run_id="source-incomplete")
    incoming = read_json(root, key)
    incoming["status"] = "incomplete"
    write_json(root, key, incoming)
    with pytest.raises(RuntimeError):
        store.publish_silver(tmp_path, key)
    assert object_path(root, original["partition_key"]).read_bytes() == data
    assert object_path(root, manifest_key(original)).read_bytes() == marker
    assert store.load_silver(tmp_path, DAY)[0] == original


@pytest.mark.parametrize(
    "damage", ["schema", "duplicates", "day", "row_count", "accounting", "quarantine"]
)
def test_semantically_inconsistent_artifacts_are_rejected_even_with_valid_hashes(
    tmp_path: Path, silver_input: FixtureInput, damage: str
) -> None:
    manifest = initial_partition(tmp_path, silver_input)
    root = output_root(tmp_path, "sample")
    partition = object_path(root, manifest["partition_key"])
    if damage in ("schema", "duplicates", "day"):
        table = pq.ParquetFile(partition).read()
        if damage == "schema":
            table = table.drop_columns(["status"])
        elif damage == "duplicates":
            table = pa.concat_tables([table.slice(0, 1), table.slice(0, 1)])
        else:
            table = table.set_column(
                table.schema.get_field_index("created_date"),
                table.schema.field("created_date"),
                pa.array([date(2026, 9, 2)] * table.num_rows, type=pa.date32()),
            )
        pq.write_table(table, partition, compression="snappy")
        manifest["partition_sha256"] = sha256(partition.read_bytes()).hexdigest()
    elif damage == "row_count":
        manifest["row_count"] = 3
    elif damage == "accounting":
        report = read_json(root, manifest["quality_report_key"])
        report["received"] = 999
        write_json(root, manifest["quality_report_key"], report)
        manifest["quality_report_sha256"] = sha256(
            object_path(root, manifest["quality_report_key"]).read_bytes()
        ).hexdigest()
    else:
        path = object_path(root, manifest["quarantine_key"])
        path.write_bytes(gzip.compress(b"[{}]", mtime=0))
        manifest["quarantine_sha256"] = sha256(path.read_bytes()).hexdigest()
    write_json(root, manifest_key(manifest), manifest)
    with pytest.raises(SilverError):
        store.load_silver(tmp_path, DAY)
