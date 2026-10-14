from pathlib import Path

import pytest

from nyc311.manifests import (
    BronzeManifest,
    Manifest,
    QualityReport,
    RunManifest,
    SilverManifest,
)
from nyc311.storage import (
    manifest_key,
    object_path,
    output_root,
    read_bytes,
    read_json,
    write_bytes,
    write_json,
)


def sample_manifests() -> list[Manifest]:
    bronze: BronzeManifest = {
        "kind": "bronze",
        "mode": "sample",
        "run_id": "fixture-001",
        "start_date": "2026-09-01",
        "end_date": "2026-09-02",
        "dataset_id": "erm2-nwe9",
        "selected_fields": ["unique_key", "created_date", "status"],
        "order": "unique_key ASC",
        "page_size": 2,
        "pages": [
            {
                "key": "bronze/run=fixture-001/created_date=2026-09-01/page-0001.json.gz",
                "row_count": 2,
                "observed_at_utc": "2026-10-13T12:00:00Z",
                "last_cursor": "sample-0002",
                "sha256": "0" * 64,
            }
        ],
        "record_count": 2,
        "source_count_before": 2,
        "source_count_after": 2,
        "status": "complete",
    }
    silver: SilverManifest = {
        "kind": "silver",
        "mode": "sample",
        "run_id": "fixture-001",
        "start_date": "2026-09-01",
        "end_date": "2026-09-02",
        "source_manifest_key": manifest_key(bronze),
        "partition_key": "silver/created_date=2026-09-01/part.parquet",
        "quality_report_key": "quality/run=fixture-001/2026-09-01.json",
        "quarantine_key": "quarantine/run=fixture-001/2026-09-01.json.gz",
        "partition_sha256": "0" * 64,
        "quality_report_sha256": "0" * 64,
        "quarantine_sha256": "0" * 64,
        "row_count": 2,
        "schema_version": "1",
        "transformation_version": "1",
        "status": "complete",
    }
    quality: QualityReport = {
        "kind": "quality",
        "mode": "sample",
        "run_id": "fixture-001",
        "start_date": "2026-09-01",
        "end_date": "2026-09-02",
        "received": 2,
        "accepted": 2,
        "quarantined": 0,
        "identical_duplicates_discarded": 0,
        "warnings": {"unknown_status": 1},
        "rejection_reasons": {},
        "source_manifest_key": manifest_key(bronze),
        "category_counts": {"UNKNOWN": 2},
        "status_counts": {"UNKNOWN": 2},
        "duration_exclusions": {"not_closed": 2, "closed_without_valid_duration": 0},
    }
    run: RunManifest = {
        "kind": "run",
        "mode": "sample",
        "run_id": "fixture-001",
        "start_date": "2026-09-01",
        "end_date": "2026-10-01",
        "status": "running",
        "started_at_utc": "2026-10-13T12:00:00Z",
        "finished_at_utc": None,
        "completed_days": ["2026-09-01"],
        "output_keys": [],
        "error": None,
        "action": "extract",
        "stage": "bronze",
        "active_day": None,
        "bronze_keys": [],
        "gold_days": [],
        "gold_manifest_key": None,
    }
    return [bronze, silver, quality, run]


@pytest.mark.parametrize("manifest", sample_manifests())
def test_manifest_round_trip(tmp_path: Path, manifest: Manifest) -> None:
    root = output_root(tmp_path, manifest["mode"])
    key = manifest_key(manifest)
    write_json(root, key, manifest)
    assert read_json(root, key) == manifest
    assert object_path(root, key).relative_to(root).parts[0] in ("control", "quality")
    assert not (output_root(tmp_path, "published") / key).exists()


def test_modes_are_separate_without_creating_directories(tmp_path: Path) -> None:
    sample = output_root(tmp_path, "sample")
    published = output_root(tmp_path, "published")
    assert sample != published
    assert not sample.exists()
    assert not published.exists()
    key = "silver/created_date=2026-09-01/part.parquet"
    write_bytes(sample, key, b"sample-only")
    assert read_bytes(sample, key) == b"sample-only"
    assert not object_path(published, key).exists()


def test_sample_root_cannot_alias_published(tmp_path: Path) -> None:
    published = output_root(tmp_path, "published")
    published.mkdir()
    (tmp_path / "sample").symlink_to(published, target_is_directory=True)
    with pytest.raises(ValueError, match="symbolic link"):
        output_root(tmp_path, "sample")
    assert list(published.iterdir()) == []


@pytest.mark.parametrize(
    "key", ["", ".", "..", "../escape", "a/../b", "/absolute", "a\\b"]
)
def test_unsafe_storage_keys(tmp_path: Path, key: str) -> None:
    with pytest.raises(ValueError):
        write_bytes(tmp_path, key, b"must not be written")
    assert list(tmp_path.iterdir()) == []


def test_symlink_cannot_escape_storage_root(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "link").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="inside"):
        write_bytes(root, "link/escape.json", b"must not be written")
    assert list(outside.iterdir()) == []


@pytest.mark.parametrize("run_id", ["", "../escape", "nested/run", "/absolute"])
def test_unsafe_run_identifier(run_id: str) -> None:
    manifest = sample_manifests()[0]
    manifest["run_id"] = run_id
    with pytest.raises(ValueError, match="identifier"):
        manifest_key(manifest)


def test_daily_manifest_cannot_cover_multiple_days() -> None:
    manifest = sample_manifests()[0]
    manifest["end_date"] = "2026-09-03"
    with pytest.raises(ValueError, match="one day"):
        manifest_key(manifest)


def test_manifest_window_stays_in_september() -> None:
    manifest = sample_manifests()[0]
    manifest["start_date"] = "2026-08-31"
    with pytest.raises(ValueError, match="Window"):
        manifest_key(manifest)


def test_nonfinite_json_is_not_written(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        write_json(tmp_path, "control/test.json", {"value": float("nan")})
    assert list(tmp_path.iterdir()) == []


def test_json_reader_requires_an_object(tmp_path: Path) -> None:
    write_bytes(tmp_path, "control/test.json", b"[]")
    with pytest.raises(ValueError, match="object"):
        read_json(tmp_path, "control/test.json")


@pytest.mark.parametrize(
    "content",
    [
        b'{"value": NaN}',
        b'{"value": Infinity}',
        b'{"value": -Infinity}',
        b'{"value": 1e400}',
        b'{"nested": [NaN]}',
        b'{"status": "failed", "status": "complete"}',
        b'{"nested": {"value": 1, "value": 2}}',
    ],
)
def test_json_reader_rejects_ambiguous_values(tmp_path: Path, content: bytes) -> None:
    write_bytes(tmp_path, "control/test.json", content)
    with pytest.raises(ValueError):
        read_json(tmp_path, "control/test.json")


def test_missing_object_is_an_explicit_error(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_bytes(tmp_path, "control/missing.json")


def test_invalid_storage_mode(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Mode"):
        output_root(tmp_path, "other")
