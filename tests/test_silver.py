import json
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc311.silver import SILVER_SCHEMA, SilverError, normalize_page, to_silver_table

SAMPLES = Path(__file__).resolve().parents[1] / "data" / "samples"


def fixture_rows(name: str) -> list[dict[str, object]]:
    return json.loads((SAMPLES / name).read_bytes())


def normalized(
    rows: list[dict[str, object]], observed: str = "2026-12-02T12:00:00Z"
) -> pd.DataFrame:
    return normalize_page(
        rows,
        source_run_id="fixture-001",
        source_page_key="data/samples/fixture.json",
        observed_at_utc=observed,
    )


def test_explicit_schema_and_parquet_round_trip() -> None:
    table = to_silver_table(normalized(fixture_rows("pagination-001.json")))
    assert table.schema.equals(SILVER_SCHEMA, check_metadata=True)
    assert table.schema.field("unique_key").type == pa.string()
    assert table.schema.field("incident_zip").type == pa.string()
    assert table.schema.field("created_date").type == pa.date32()
    assert table.schema.field("observed_at_utc").type == pa.timestamp("us")
    assert table.schema.field("quality_flags").type == pa.list_(pa.string())
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy")
    restored = pq.read_table(pa.BufferReader(sink.getvalue()))
    assert restored.equals(table)
    assert restored.schema.equals(SILVER_SCHEMA, check_metadata=True)


def test_schema_is_stable_for_null_optional_fields_and_empty_input() -> None:
    minimal = [{"unique_key": "00123", "created_date": "2026-09-01T00:00:00"}]
    tables = [
        to_silver_table(normalized(rows))
        for rows in (minimal, fixture_rows("pagination-001.json"), [])
    ]
    assert all(
        table.schema.equals(SILVER_SCHEMA, check_metadata=True) for table in tables
    )
    assert tables[0]["unique_key"].to_pylist() == ["00123"]
    assert tables[0]["closed_date_local"].null_count == 1
    assert tables[-1].num_rows == 0


def test_text_normalization_is_conservative_and_zip_stays_text() -> None:
    row = fixture_rows("quality-cases.json")[0]
    row.update(
        {
            "agency": " dot ",
            "borough": " queens ",
            "status": " Open ",
            "complaint_type": "  Synthetic New Category  ",
            "descriptor": " Mixed CASE ",
        }
    )
    result = normalized([row]).iloc[0]
    assert result["agency"] == "DOT"
    assert result["borough"] == "QUEENS"
    assert result["status"] == "OPEN"
    assert result["complaint_type"] == "Synthetic New Category"
    assert result["descriptor"] == "Mixed CASE"
    assert result["incident_zip"] == "00123"
    assert row["agency"] == " dot "


def test_nyc_local_dates_and_utc_instants_are_distinct() -> None:
    result = normalized(fixture_rows("pagination-002.json")).iloc[0]
    assert result["created_date_local"] == pd.Timestamp("2026-09-01T23:59:59")
    assert result["created_date_utc"] == pd.Timestamp("2026-09-02T03:59:59")
    assert result["created_date"] == date(2026, 9, 1)
    assert result["closed_date_local"] == pd.Timestamp("2026-10-01T00:00:00")
    assert result["closed_date_utc"] == pd.Timestamp("2026-10-01T04:00:00")


def test_dst_changes_elapsed_time_and_ambiguous_times_are_not_guessed() -> None:
    rows = [
        dict(fixture_rows("pagination-001.json")[0], closed_date="2026-11-01T02:30:00")
    ]
    valid = normalized(rows).iloc[0]
    assert valid["closed_date_utc"] == pd.Timestamp("2026-11-01T07:30:00")
    ambiguous = normalized(fixture_rows("quality-cases.json")[6:7]).iloc[0]
    assert ambiguous["closed_date_local"] == pd.Timestamp("2026-11-01T01:30:00")
    assert pd.isna(ambiguous["closed_date_utc"])
    rows[0]["closed_date"] = "2026-03-08T02:30:00"
    assert pd.isna(normalized(rows).iloc[0]["closed_date_utc"])


@pytest.mark.parametrize(
    "value", ["not-a-date", 123, "2026-09-01T00:00:00Z", "2026-09-01T00:00:00+02:00"]
)
def test_business_dates_do_not_silently_accept_offsets_or_numbers(
    value: object,
) -> None:
    row = dict(fixture_rows("pagination-001.json")[0], closed_date=value)
    result = normalized([row]).iloc[0]
    assert pd.isna(result["closed_date_local"])
    assert pd.isna(result["closed_date_utc"])


def test_fixed_source_timestamp_converts_its_offset_to_utc() -> None:
    row = dict(fixture_rows("pagination-001.json")[0])
    row[":updated_at"] = "2026-10-13T14:00:00+02:00"
    assert normalized([row]).iloc[0]["source_updated_at_utc"] == pd.Timestamp(
        "2026-10-13T12:00:00"
    )


@pytest.mark.parametrize(
    "observed", ["bad", "2026-10-13T12:00:00", "2026-10-13T12:00:00+02:00"]
)
def test_invalid_observation_metadata_fails(observed: str) -> None:
    with pytest.raises(SilverError, match="explicitly UTC"):
        normalized(fixture_rows("pagination-001.json"), observed)


@pytest.mark.parametrize("rows", [[{"status": "Open"}], ["not-a-record"]])
def test_incompatible_page_structure_fails(rows: list[dict[str, object]]) -> None:
    with pytest.raises(SilverError):
        normalized(rows)


def test_provenance_and_observation_are_preserved() -> None:
    result = normalized(fixture_rows("pagination-001.json"))
    assert result["source_row_number"].tolist() == [1, 2]
    assert result["source_run_id"].tolist() == ["fixture-001", "fixture-001"]
    assert (
        result["observed_at_utc"].tolist() == [pd.Timestamp("2026-12-02T12:00:00")] * 2
    )
    assert result["transformation_version"].tolist() == ["1", "1"]
