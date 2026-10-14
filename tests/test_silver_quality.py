import gzip
import json
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc311 import source
from nyc311.bronze import BronzeError
from nyc311.manifests import BronzeManifest
from nyc311.silver import SILVER_SCHEMA, SilverError, transform_bronze, transform_day
from nyc311.source import SourcePage
from nyc311.storage import manifest_key, output_root, write_bytes, write_json

SAMPLES = Path(__file__).resolve().parents[1] / "data" / "samples"
type FixtureInput = Callable[..., tuple[BronzeManifest, list[SourcePage]]]


def rows(name: str) -> list[dict[str, object]]:
    return json.loads((SAMPLES / name).read_bytes())


def test_quality_fixture_quarantine_and_partial_defects(
    silver_input: FixtureInput,
) -> None:
    originals = rows("quality-cases.json")
    manifest, pages = silver_input(originals)
    result = transform_day(manifest, pages)
    assert result["quality_report"]["received"] == 8
    assert result["quality_report"]["accepted"] == 5
    assert result["quality_report"]["quarantined"] == 3
    rejected = result["quarantine"]
    assert [record["original"] for record in rejected] == [
        originals[index] for index in (2, 3, 5)
    ]
    assert [record["source_row_number"] for record in rejected] == [3, 4, 6]
    assert [record["reason_codes"] for record in rejected] == [
        ["missing_or_invalid_identifier"],
        ["invalid_created_date"],
        ["created_date_outside_window"],
    ]
    by_key = {record["unique_key"]: record for record in result["table"].to_pylist()}
    assert by_key["sample-null-fields"]["incident_zip"] == "00123"
    assert by_key["sample-unknown-status"]["lifecycle_class"] == "unknown"
    assert by_key["sample-unknown-status"]["complaint_type"] == "Synthetic New Category"
    assert (
        "negative_closure_duration"
        in by_key["sample-negative-duration"]["quality_flags"]
    )
    assert (
        "unresolvable_closed_date"
        in by_key["sample-ambiguous-closure"]["quality_flags"]
    )
    assert (
        "closed_after_observation" in by_key["sample-future-closure"]["quality_flags"]
    )
    assert all(not record["duration_eligible"] for record in by_key.values())


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("Closed", "closed"),
        ("Open", "outstanding"),
        ("In Progress", "outstanding"),
        ("Pending", "outstanding"),
        ("Assigned", "outstanding"),
        ("Started", "outstanding"),
        ("Cancelled", "unknown"),
        ("New Status", "unknown"),
        (None, "unknown"),
    ],
)
def test_exact_lifecycle_mapping(
    silver_input: FixtureInput, status: str | None, expected: str
) -> None:
    record = dict(rows("updated-request.json")[0], status=status)
    manifest, pages = silver_input([record])
    result = transform_day(manifest, pages)["table"].to_pylist()[0]
    assert result["lifecycle_class"] == expected
    assert result["duration_eligible"] is (expected == "closed")
    if expected == "unknown":
        assert "unknown_status" in result["quality_flags"]


@pytest.mark.parametrize(
    ("name", "value", "flag"),
    [
        ("incident_zip", 123, "invalid_incident_zip"),
        ("incident_zip", "ABCDE", "invalid_incident_zip"),
        ("descriptor", {"bad": "type"}, "invalid_descriptor_type"),
        ("closed_date", "bad", "invalid_closed_date"),
        ("closed_date", None, "missing_closed_date"),
        (
            "resolution_action_updated_date",
            "bad",
            "invalid_resolution_action_updated_date",
        ),
        (":updated_at", "2026-10-13T12:00:00", "invalid_source_updated_at"),
    ],
)
def test_optional_defects_do_not_discard_requests(
    silver_input: FixtureInput, name: str, value: object, flag: str
) -> None:
    record = dict(rows("updated-request.json")[0])
    record[name] = value
    manifest, pages = silver_input([record])
    result = transform_day(manifest, pages)
    assert result["quality_report"]["accepted"] == 1
    assert result["quarantine"] == []
    assert flag in result["table"].to_pylist()[0]["quality_flags"]


def test_missing_optional_fields_do_not_reject_open_request(
    silver_input: FixtureInput,
) -> None:
    record = dict(rows("duplicates.json")[0])
    manifest, pages = silver_input([record])
    result = transform_day(manifest, pages)
    assert result["quality_report"]["accepted"] == 1
    row = result["table"].to_pylist()[0]
    assert row["closed_date_local"] is None
    assert row["lifecycle_class"] == "outstanding"
    assert "missing_closed_date" not in row["quality_flags"]


def test_invalid_required_values_and_multiple_reasons(
    silver_input: FixtureInput,
) -> None:
    record = dict(rows("pagination-001.json")[0], unique_key=123, created_date="bad")
    manifest, pages = silver_input([record])
    result = transform_day(manifest, pages)
    assert result["table"].num_rows == 0
    assert result["table"].schema.equals(SILVER_SCHEMA, check_metadata=True)
    assert result["quarantine"][0]["original"] == record
    assert result["quarantine"][0]["reason_codes"] == [
        "missing_or_invalid_identifier",
        "invalid_created_date",
    ]


def test_missing_required_column_fails_day(silver_input: FixtureInput) -> None:
    manifest, pages = silver_input([{"unique_key": "sample-missing"}])
    with pytest.raises(SilverError, match="required columns"):
        transform_day(manifest, pages)


@pytest.mark.parametrize("status", ["failed", "incomplete"])
def test_incomplete_input_is_rejected(silver_input: FixtureInput, status: str) -> None:
    manifest, pages = silver_input(rows("pagination-001.json"))
    manifest["status"] = status
    with pytest.raises(SilverError, match="Only complete"):
        transform_day(manifest, pages)


def test_inconsistent_provenance_is_rejected(silver_input: FixtureInput) -> None:
    manifest, pages = silver_input(rows("pagination-001.json"))
    pages[0]["observed_at_utc"] = "2026-12-02T12:00:00Z"
    with pytest.raises(SilverError, match="provenance"):
        transform_day(manifest, pages)


def test_transform_verified_local_bronze_without_http_or_publication(
    silver_input: FixtureInput, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, pages = silver_input(
        rows("pagination-001.json"), rows("pagination-002.json")
    )
    manifest["page_size"] = 2
    root = output_root(tmp_path, "sample")
    for page, reference in zip(pages, manifest["pages"]):
        write_bytes(root, reference["key"], gzip.compress(page["content"], mtime=0))
    write_json(root, manifest_key(manifest), manifest)
    snapshot = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}

    def no_http(*args: object, **kwargs: object) -> None:
        raise AssertionError("HTTP must not be used by Silver")

    monkeypatch.setattr(source, "_request", no_http)
    result = transform_bronze(tmp_path, manifest_key(manifest), run_id="processing-001")
    assert result["table"].num_rows == 3
    assert result["quality_report"]["run_id"] == "processing-001"
    assert not (root / "silver").exists()
    assert not (root / "quarantine").exists()
    assert not (root / "quality").exists()
    assert snapshot == {
        path: path.read_bytes() for path in root.rglob("*") if path.is_file()
    }
    manifest["status"] = "failed"
    write_json(root, manifest_key(manifest), manifest)
    with pytest.raises(BronzeError):
        transform_bronze(tmp_path, manifest_key(manifest))


@pytest.mark.parametrize("across_pages", [False, True])
def test_identical_duplicates_keep_first_provenance(
    silver_input: FixtureInput, across_pages: bool
) -> None:
    identical = rows("duplicates.json")[:2]
    groups = [[identical[0]], [identical[1]]] if across_pages else [identical]
    manifest, pages = silver_input(*groups)
    result = transform_day(manifest, pages)
    report = result["quality_report"]
    assert (
        report["received"],
        report["accepted"],
        report["quarantined"],
        report["identical_duplicates_discarded"],
    ) == (2, 1, 0, 1)
    record = result["table"].to_pylist()[0]
    assert record["source_page_key"] == manifest["pages"][0]["key"]
    assert record["source_row_number"] == 1
    assert report["warnings"]["unknown_borough"] == 1
    assert report["category_counts"] == {"UNKNOWN": 1}
    assert report["status_counts"] == {"OPEN": 1}


@pytest.mark.parametrize("across_pages", [False, True])
def test_conflicting_duplicates_fail_entire_day(
    silver_input: FixtureInput, across_pages: bool
) -> None:
    conflict = rows("duplicates.json")
    groups = [conflict[:2], conflict[2:]] if across_pages else [conflict]
    manifest, pages = silver_input(*groups)
    with pytest.raises(SilverError, match="Conflicting"):
        transform_day(manifest, pages)


def test_conflicts_do_not_disappear_after_text_normalization(
    silver_input: FixtureInput,
) -> None:
    original = rows("pagination-001.json")[0]
    different = dict(original, unique_key=f" {original['unique_key']} ")
    manifest, pages = silver_input([original, different])
    with pytest.raises(SilverError, match="Conflicting"):
        transform_day(manifest, pages)


def test_invalid_identifiers_do_not_merge_different_requests(
    silver_input: FixtureInput,
) -> None:
    original = rows("quality-cases.json")[2]
    different = dict(original, status="Closed")
    manifest, pages = silver_input([original, original, different])
    result = transform_day(manifest, pages)
    report = result["quality_report"]
    assert report["received"] == 3
    assert report["quarantined"] == 2
    assert report["identical_duplicates_discarded"] == 1
    assert [record["source_row_number"] for record in result["quarantine"]] == [1, 3]


def test_reports_reconcile_and_replay_is_deterministic(
    silver_input: FixtureInput,
) -> None:
    records = rows("quality-cases.json")
    manifest, pages = silver_input(records + [records[0]])
    first = transform_day(manifest, pages)
    second = transform_day(manifest, pages)
    report = first["quality_report"]
    assert (
        report["received"]
        == report["accepted"]
        + report["quarantined"]
        + report["identical_duplicates_discarded"]
    )
    assert sum(report["category_counts"].values()) == report["accepted"]
    assert sum(report["status_counts"].values()) == report["accepted"]
    assert report["duration_exclusions"] == {
        "not_closed": 2,
        "closed_without_valid_duration": 3,
    }
    assert report["source_manifest_key"] == manifest_key(manifest)
    assert first["table"].equals(second["table"])
    assert first["quarantine"] == second["quarantine"]
    assert report == second["quality_report"]
    json.dumps(report, allow_nan=False)
    json.dumps(first["quarantine"], allow_nan=False)
    sink = pa.BufferOutputStream()
    pq.write_table(first["table"], sink, compression="snappy")
    assert pq.read_table(pa.BufferReader(sink.getvalue())).equals(first["table"])


def test_verified_empty_day_has_typed_table_and_zero_accounting(
    silver_input: FixtureInput,
) -> None:
    manifest, pages = silver_input([])
    result = transform_day(manifest, pages)
    assert result["table"].num_rows == 0
    assert result["table"].schema.equals(SILVER_SCHEMA, check_metadata=True)
    assert result["quarantine"] == []
    report = result["quality_report"]
    assert all(
        report[name] == 0
        for name in (
            "received",
            "accepted",
            "quarantined",
            "identical_duplicates_discarded",
        )
    )
    assert (
        report["warnings"] == report["category_counts"] == report["status_counts"] == {}
    )


def test_duplicate_dictionary_order_is_not_a_conflict(
    silver_input: FixtureInput,
) -> None:
    original = rows("duplicates.json")[0]
    reversed_keys = dict(reversed(list(original.items())))
    manifest, pages = silver_input([original, reversed_keys])
    result = transform_day(manifest, pages)
    assert result["quality_report"]["identical_duplicates_discarded"] == 1


@pytest.mark.parametrize("run_id", ["", "../unsafe", "nested/id"])
def test_invalid_processing_identifier_is_rejected(
    silver_input: FixtureInput, run_id: str
) -> None:
    manifest, pages = silver_input(rows("pagination-001.json"))
    with pytest.raises(SilverError, match="Processing run"):
        transform_day(manifest, pages, run_id=run_id)


@pytest.mark.parametrize(
    "column", ["created_date", "closed_date", "resolution_action_updated_date"]
)
def test_extreme_unlocalizable_date_does_not_abort_valid_records(
    silver_input: FixtureInput, column: str
) -> None:
    valid = rows("updated-request.json")[0]
    extreme = dict(valid, unique_key="sample-extreme")
    extreme[column] = "9999-12-31T23:59:59"
    manifest, pages = silver_input([valid, extreme])
    result = transform_day(manifest, pages)
    accepted = {record["unique_key"]: record for record in result["table"].to_pylist()}
    assert accepted[valid["unique_key"]]["duration_eligible"] is True
    if column == "created_date":
        assert len(accepted) == 1
        assert result["quarantine"][0]["original"] == extreme
        assert "invalid_created_date" in result["quarantine"][0]["reason_codes"]
    else:
        assert len(accepted) == 2
        assert accepted["sample-extreme"][f"{column}_utc"] is None
        assert f"unresolvable_{column}" in accepted["sample-extreme"]["quality_flags"]


def test_date_fallback_keeps_missing_and_ambiguous_values_safe(
    silver_input: FixtureInput,
) -> None:
    valid = rows("updated-request.json")[0]
    records = [
        valid,
        dict(valid, unique_key="sample-extreme", closed_date="9999-12-31T23:59:59"),
        dict(valid, unique_key="sample-ambiguous", closed_date="2026-11-01T01:30:00"),
        dict(valid, unique_key="sample-missing", closed_date=None),
    ]
    manifest, pages = silver_input(records)
    result = transform_day(manifest, pages)
    assert result["table"].num_rows == 4
    assert (
        result["quality_report"]["duration_exclusions"]["closed_without_valid_duration"]
        == 3
    )


@pytest.mark.parametrize(
    "value", ["9999-12-31T23:59:59-05:00", "0001-01-01T00:00:00+05:00"]
)
def test_fixed_timestamp_outside_interoperable_calendar_is_flagged(
    silver_input: FixtureInput, value: str
) -> None:
    valid = rows("updated-request.json")[0]
    defective = dict(valid, unique_key="sample-fixed-range")
    defective[":updated_at"] = value
    manifest, pages = silver_input([valid, defective])
    accepted = transform_day(manifest, pages)["table"].to_pylist()
    assert len(accepted) == 2
    assert accepted[1]["source_updated_at_utc"] is None
    assert "invalid_source_updated_at" in accepted[1]["quality_flags"]


def test_year_zero_is_rejected_without_failing_other_records(
    silver_input: FixtureInput,
) -> None:
    valid = rows("updated-request.json")[0]
    invalid = dict(
        valid, unique_key="sample-year-zero", created_date="0000-01-01T00:00:00"
    )
    manifest, pages = silver_input([valid, invalid])
    result = transform_day(manifest, pages)
    assert result["table"].num_rows == 1
    assert result["quarantine"][0]["reason_codes"] == ["invalid_created_date"]
