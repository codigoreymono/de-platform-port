import gzip
import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path

import httpx
import pytest

from nyc311 import bronze, source
from nyc311.config import ExecutionConfig, execution_config
from nyc311.manifests import BronzeManifest
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


def mock_client(
    pages: list[bytes | int],
    config: ExecutionConfig,
    requests: list[httpx.Request],
    counts: list[int] | None = None,
) -> httpx.Client:
    total = sum(len(json.loads(page)) for page in pages if isinstance(page, bytes))
    pending_counts = iter(counts if counts is not None else [total, total])
    pending_pages = iter(pages)

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.params["$select"] == "count(*) as n":
            return httpx.Response(200, json=[{"n": str(next(pending_counts))}])
        page = next(pending_pages)
        if isinstance(page, int):
            return httpx.Response(page, text="failure must not be persisted")
        return httpx.Response(200, content=page)

    return source.create_client(
        config, app_token="fixture-secret", transport=httpx.MockTransport(respond)
    )


def original_pages() -> list[bytes]:
    return [
        (SAMPLES / name).read_bytes()
        for name in ("pagination-001.json", "pagination-002.json")
    ]


def save_fixture(base: Path, run_id: str = "fixture-001") -> BronzeManifest:
    settings = execution_config(page_size=2, max_attempts=1)
    with mock_client(original_pages(), settings, []) as client:
        return bronze.ingest_day(client, DAY, base, run_id, settings)


def test_complete_bronze_preserves_original_bytes_and_metadata(tmp_path: Path) -> None:
    manifest = save_fixture(tmp_path)
    root = output_root(tmp_path, "sample")
    assert read_json(root, manifest_key(manifest)) == manifest
    assert manifest["status"] == "complete"
    assert (
        manifest["record_count"]
        == manifest["source_count_before"]
        == manifest["source_count_after"]
        == 3
    )
    assert manifest["dataset_id"] == source.DATASET_ID
    assert manifest["selected_fields"] == list(source.SELECTED_FIELDS)
    assert manifest["order"] == "unique_key ASC"
    assert manifest["page_size"] == 2
    assert "fixture-secret" not in json.dumps(manifest)
    assert "headers" not in manifest
    assert len(manifest["pages"]) == 2
    assert len({page["key"] for page in manifest["pages"]}) == 2
    for index, (reference, content) in enumerate(
        zip(manifest["pages"], original_pages()), 1
    ):
        path = object_path(root, reference["key"])
        assert path.name == f"page-{index:04d}.json.gz"
        assert gzip.decompress(path.read_bytes()) == content
        assert reference["sha256"] == sha256(content).hexdigest()
        assert reference["row_count"] == len(json.loads(content))
        assert datetime.fromisoformat(reference["observed_at_utc"]).tzinfo == UTC
    saved = list((root / "bronze").rglob("*.json.gz"))
    assert set(saved) == {object_path(root, page["key"]) for page in manifest["pages"]}
    assert not (root / "silver").exists()
    assert not (root / "gold").exists()


def test_empty_day_retains_original_empty_page(tmp_path: Path) -> None:
    settings = execution_config(max_attempts=1)
    with mock_client([b" [ ] \n"], settings, []) as client:
        manifest = bronze.ingest_day(client, DAY, tmp_path, "empty", settings)
    assert manifest["status"] == "complete"
    assert manifest["record_count"] == 0
    assert manifest["pages"][0]["last_cursor"] is None
    path = object_path(output_root(tmp_path, "sample"), manifest["pages"][0]["key"])
    assert gzip.decompress(path.read_bytes()) == b" [ ] \n"


def test_exact_page_size_preserves_terminal_page(tmp_path: Path) -> None:
    settings = execution_config(page_size=2, max_attempts=1)
    with mock_client([original_pages()[0], b"[]"], settings, []) as client:
        manifest = bronze.ingest_day(client, DAY, tmp_path, "exact", settings)
    assert manifest["status"] == "complete"
    assert [page["row_count"] for page in manifest["pages"]] == [2, 0]
    assert manifest["pages"][-1]["last_cursor"] == "sample-0002"


@pytest.mark.parametrize("mode", ["sample", "published"])
def test_storage_modes_remain_separate(tmp_path: Path, mode: str) -> None:
    settings = execution_config(mode=mode, page_size=2, max_attempts=1)
    with mock_client(original_pages(), settings, []) as client:
        manifest = bronze.ingest_day(client, DAY, tmp_path, "mode-check", settings)
    assert object_path(output_root(tmp_path, mode), manifest_key(manifest)).exists()
    other = "published" if mode == "sample" else "sample"
    assert not output_root(tmp_path, other).exists()


def test_existing_attempt_is_not_overwritten(tmp_path: Path) -> None:
    manifest = save_fixture(tmp_path)
    root = output_root(tmp_path, "sample")
    before = object_path(root, manifest_key(manifest)).read_bytes()
    requests = []
    settings = execution_config(page_size=2)
    with mock_client(original_pages(), settings, requests) as client:
        with pytest.raises(FileExistsError, match="new run identifier"):
            bronze.ingest_day(client, DAY, tmp_path, "fixture-001", settings)
    assert requests == []
    assert object_path(root, manifest_key(manifest)).read_bytes() == before


@pytest.mark.parametrize("run_id", ["", "../outside", "a/b"])
def test_invalid_run_identifier_has_no_io(tmp_path: Path, run_id: str) -> None:
    requests = []
    settings = execution_config()
    with mock_client([], settings, requests) as client:
        with pytest.raises(ValueError):
            bronze.ingest_day(client, DAY, tmp_path, run_id, settings)
    assert requests == []
    assert list(tmp_path.iterdir()) == []


def test_invalid_day_has_no_io(tmp_path: Path) -> None:
    requests = []
    settings = execution_config()
    with mock_client([], settings, requests) as client:
        with pytest.raises(ValueError):
            bronze.ingest_day(client, date(2026, 10, 1), tmp_path, "invalid", settings)
    assert requests == []
    assert list(tmp_path.iterdir()) == []


def test_missing_file_blocks_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = execution_config(page_size=2, max_attempts=1)
    original_write = bronze.write_bytes
    removed = []

    def lose_first_file(root: Path, key: str, content: bytes) -> None:
        original_write(root, key, content)
        if not removed:
            object_path(root, key).unlink()
            removed.append(key)

    monkeypatch.setattr(bronze, "write_bytes", lose_first_file)
    with mock_client(original_pages(), settings, []) as client:
        with pytest.raises(bronze.BronzeError, match="unavailable"):
            bronze.ingest_day(client, DAY, tmp_path, "missing", settings)
    state = read_json(
        output_root(tmp_path, "sample"), "control/bronze/missing/2026-09-01.json"
    )
    assert state["status"] == "failed"
    assert state["error"] == "BronzeError"


def test_corrupt_file_blocks_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_write = bronze.write_bytes
    monkeypatch.setattr(
        bronze,
        "write_bytes",
        lambda root, key, content: original_write(root, key, b"not-gzip"),
    )
    settings = execution_config(page_size=2, max_attempts=1)
    with mock_client(original_pages(), settings, []) as client:
        with pytest.raises(bronze.BronzeError):
            bronze.ingest_day(client, DAY, tmp_path, "corrupt", settings)
    assert (
        read_json(
            output_root(tmp_path, "sample"), "control/bronze/corrupt/2026-09-01.json"
        )["status"]
        == "failed"
    )


def test_offline_replay_preserves_bytes_and_observation_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = save_fixture(tmp_path)

    def no_source_calls(*args: object, **kwargs: object) -> None:
        raise AssertionError("Replay must not invoke the source")

    monkeypatch.setattr(bronze, "extract_day", no_source_calls)
    monkeypatch.setattr(source, "create_client", no_source_calls)
    monkeypatch.setattr(source, "_request", no_source_calls)
    loaded, result = bronze.load_bronze(tmp_path, manifest_key(manifest))
    assert loaded == manifest
    assert result["complete"] is True
    assert result["record_count"] == 3
    assert result["source_count_before"] == result["source_count_after"] == 3
    assert [page["content"] for page in result["pages"]] == original_pages()
    assert [page["observed_at_utc"] for page in result["pages"]] == [
        page["observed_at_utc"] for page in manifest["pages"]
    ]
    assert bronze.load_bronze(tmp_path, manifest_key(manifest)) == (loaded, result)


def test_api_failure_preserves_saved_pages_and_failure_manifest(tmp_path: Path) -> None:
    requests = []
    settings = execution_config(page_size=2, max_attempts=1)
    first = original_pages()[0]
    with mock_client([first, 503], settings, requests, counts=[3]) as client:
        with pytest.raises(source.ExtractionError, match="exhausted"):
            bronze.ingest_day(client, DAY, tmp_path, "interrupted", settings)
    root = output_root(tmp_path, "sample")
    key = "control/bronze/interrupted/2026-09-01.json"
    manifest = read_json(root, key)
    assert manifest["status"] == "failed"
    assert manifest["error"] == "ExtractionError"
    assert manifest["source_count_before"] == 3
    assert manifest["source_count_after"] is None
    assert manifest["record_count"] == 2
    assert len(manifest["pages"]) == 1
    assert (
        gzip.decompress(object_path(root, manifest["pages"][0]["key"]).read_bytes())
        == first
    )
    assert len(requests) == 3
    with pytest.raises(bronze.BronzeError, match="Only complete"):
        bronze.load_bronze(tmp_path, key)


def test_changed_counts_preserve_both_observations_and_all_pages(
    tmp_path: Path,
) -> None:
    settings = execution_config(page_size=2, max_attempts=1)
    with mock_client(original_pages(), settings, [], counts=[3, 4]) as client:
        with pytest.raises(source.ExtractionError, match="Inconsistent"):
            bronze.ingest_day(client, DAY, tmp_path, "changed-count", settings)
    key = "control/bronze/changed-count/2026-09-01.json"
    manifest = read_json(output_root(tmp_path, "sample"), key)
    assert manifest["source_count_before"] == 3
    assert manifest["source_count_after"] == 4
    assert len(manifest["pages"]) == 2
    assert manifest["status"] == "failed"
    with pytest.raises(bronze.BronzeError, match="Only complete"):
        bronze.load_bronze(tmp_path, key)


def test_storage_failure_stops_http_and_preserves_diagnostic_references(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_write = bronze.write_bytes

    def fail_second_page(root: Path, key: str, content: bytes) -> None:
        if "page-0002" in key:
            raise OSError("fixture-secret must not enter saved diagnostics")
        original_write(root, key, content)

    monkeypatch.setattr(bronze, "write_bytes", fail_second_page)
    settings = execution_config(page_size=2, max_attempts=1)
    requests = []
    with mock_client(original_pages(), settings, requests) as client:
        with pytest.raises(OSError):
            bronze.ingest_day(client, DAY, tmp_path, "disk-failure", settings)
    root = output_root(tmp_path, "sample")
    key = "control/bronze/disk-failure/2026-09-01.json"
    state = read_json(root, key)
    assert state["status"] == "failed"
    assert state["error"] == "OSError"
    assert "fixture-secret" not in json.dumps(state)
    assert len(state["pages"]) == 2
    assert object_path(root, state["pages"][0]["key"]).exists()
    assert not object_path(root, state["pages"][1]["key"]).exists()
    assert len(requests) == 3
    with pytest.raises(bronze.BronzeError, match="Only complete"):
        bronze.load_bronze(tmp_path, key)


def test_keyboard_interruption_keeps_incomplete_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_write = bronze.write_bytes

    def interrupt(root: Path, key: str, content: bytes) -> None:
        if "page-0002" in key:
            raise KeyboardInterrupt
        original_write(root, key, content)

    monkeypatch.setattr(bronze, "write_bytes", interrupt)
    settings = execution_config(page_size=2, max_attempts=1)
    with mock_client(original_pages(), settings, []) as client:
        with pytest.raises(KeyboardInterrupt):
            bronze.ingest_day(client, DAY, tmp_path, "keyboard", settings)
    root = output_root(tmp_path, "sample")
    key = "control/bronze/keyboard/2026-09-01.json"
    manifest = read_json(root, key)
    assert manifest["status"] == "incomplete"
    assert len(manifest["pages"]) == 2
    assert object_path(root, manifest["pages"][0]["key"]).exists()
    with pytest.raises(bronze.BronzeError, match="Only complete"):
        bronze.load_bronze(tmp_path, key)


def test_completion_marker_write_failure_does_not_publish_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_write = bronze.write_json

    def fail_completion(root: Path, key: str, value: dict[str, object]) -> None:
        if value["status"] == "complete":
            raise OSError("cannot write completion marker")
        original_write(root, key, value)

    monkeypatch.setattr(bronze, "write_json", fail_completion)
    settings = execution_config(page_size=2, max_attempts=1)
    with mock_client(original_pages(), settings, []) as client:
        with pytest.raises(OSError):
            bronze.ingest_day(client, DAY, tmp_path, "marker-failure", settings)
    key = "control/bronze/marker-failure/2026-09-01.json"
    assert read_json(output_root(tmp_path, "sample"), key)["status"] == "failed"
    with pytest.raises(bronze.BronzeError):
        bronze.load_bronze(tmp_path, key)


@pytest.mark.parametrize("damage", ["missing", "invalid-gzip", "changed-bytes"])
def test_replay_rejects_missing_or_damaged_page(tmp_path: Path, damage: str) -> None:
    manifest = save_fixture(tmp_path)
    path = object_path(output_root(tmp_path, "sample"), manifest["pages"][0]["key"])
    if damage == "missing":
        path.unlink()
    elif damage == "invalid-gzip":
        path.write_bytes(b"invalid-gzip")
    else:
        path.write_bytes(gzip.compress(b'[{"unique_key":"modified"}]'))
    with pytest.raises(bronze.BronzeError):
        bronze.load_bronze(tmp_path, manifest_key(manifest))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "incomplete"),
        ("status", "failed"),
        ("status", "other"),
        ("kind", "silver"),
        ("mode", "published"),
        ("record_count", True),
        ("source_count_before", -1),
        ("source_count_after", None),
        ("pages", []),
        ("page_size", 0),
        ("page_size", True),
        ("dataset_id", "other"),
        ("selected_fields", ["unique_key"]),
        ("order", "created_date DESC"),
    ],
)
def test_replay_rejects_invalid_manifest_fields(
    tmp_path: Path, field: str, value: object
) -> None:
    manifest = save_fixture(tmp_path)
    key = manifest_key(manifest)
    manifest[field] = value
    write_json(output_root(tmp_path, "sample"), key, manifest)
    with pytest.raises(bronze.BronzeError):
        bronze.load_bronze(tmp_path, key)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("key", "../outside.json.gz"),
        ("key", "bronze/run=other/created_date=2026-09-01/page-0001.json.gz"),
        ("row_count", True),
        ("row_count", 1),
        ("sha256", "0" * 64),
        ("last_cursor", "wrong"),
        ("observed_at_utc", "2026-10-13T12:00:00"),
        ("observed_at_utc", "2026-10-13T12:00:00+02:00"),
    ],
)
def test_replay_rejects_invalid_page_metadata(
    tmp_path: Path, field: str, value: object
) -> None:
    manifest = save_fixture(tmp_path)
    key = manifest_key(manifest)
    manifest["pages"][0][field] = value
    write_json(output_root(tmp_path, "sample"), key, manifest)
    with pytest.raises(bronze.BronzeError):
        bronze.load_bronze(tmp_path, key)


def test_replay_rejects_duplicate_page_references(tmp_path: Path) -> None:
    manifest = save_fixture(tmp_path)
    manifest["pages"][1]["key"] = manifest["pages"][0]["key"]
    write_json(output_root(tmp_path, "sample"), manifest_key(manifest), manifest)
    with pytest.raises(bronze.BronzeError, match="unique and ordered"):
        bronze.load_bronze(tmp_path, manifest_key(manifest))


def test_replay_rejects_inconsistent_manifest_totals(tmp_path: Path) -> None:
    manifest = save_fixture(tmp_path)
    manifest["record_count"] = manifest["source_count_before"] = manifest[
        "source_count_after"
    ] = 2
    write_json(output_root(tmp_path, "sample"), manifest_key(manifest), manifest)
    with pytest.raises(bronze.BronzeError, match="extraction count"):
        bronze.load_bronze(tmp_path, manifest_key(manifest))


def test_replay_rejects_unknown_manifest_reference(tmp_path: Path) -> None:
    with pytest.raises(bronze.BronzeError, match="unavailable"):
        bronze.load_bronze(tmp_path, "control/bronze/missing/2026-09-01.json")


@pytest.mark.parametrize("failure", [KeyboardInterrupt, OSError])
def test_partial_manifest_update_preserves_last_valid_diagnostic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: type[BaseException]
) -> None:
    original_write = bronze.write_json

    def partial_update(root: Path, key: str, value: dict[str, object]) -> None:
        if len(value["pages"]) == 2:
            write_bytes(root, key, b'{"partial":')
            raise failure("partial metadata write")
        original_write(root, key, value)

    monkeypatch.setattr(bronze, "write_json", partial_update)
    settings = execution_config(page_size=2, max_attempts=1)
    with mock_client(original_pages(), settings, []) as client:
        with pytest.raises(failure):
            bronze.ingest_day(client, DAY, tmp_path, "partial-metadata", settings)
    root = output_root(tmp_path, "sample")
    key = "control/bronze/partial-metadata/2026-09-01.json"
    manifest = read_json(root, key)
    assert manifest["status"] == "incomplete"
    assert manifest["source_count_before"] == 3
    assert manifest["record_count"] == 2
    assert len(manifest["pages"]) == 1
    assert (
        gzip.decompress(object_path(root, manifest["pages"][0]["key"]).read_bytes())
        == original_pages()[0]
    )
    assert list((root / "control").rglob("*.tmp")) == []
    with pytest.raises(bronze.BronzeError, match="Only complete"):
        bronze.load_bronze(tmp_path, key)


@pytest.mark.parametrize("keys", [["b", "a", "c"], ["a", "b", "a"], ["b", "c", "a"]])
def test_replay_rejects_invalid_text_order_with_valid_hashes(
    tmp_path: Path, keys: list[str]
) -> None:
    manifest = save_fixture(tmp_path)
    root = output_root(tmp_path, "sample")
    for reference, page_keys in zip(manifest["pages"], [keys[:2], keys[2:]]):
        content = json.dumps([{"unique_key": key} for key in page_keys]).encode()
        object_path(root, reference["key"]).write_bytes(gzip.compress(content))
        reference["sha256"] = sha256(content).hexdigest()
        reference["last_cursor"] = page_keys[-1]
    write_json(root, manifest_key(manifest), manifest)
    with pytest.raises(bronze.BronzeError):
        bronze.load_bronze(tmp_path, manifest_key(manifest))
