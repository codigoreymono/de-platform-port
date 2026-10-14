import gzip
import json
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from nyc311.config import COHORT_END, COHORT_START, daily_windows
from nyc311.gold import (
    CHANNEL_SCHEMA,
    CLOSURE_SCHEMA,
    DEMAND_SCHEMA,
    OUTSTANDING_SCHEMA,
    GoldError,
    compute_product,
    load_gold_inputs,
    prepare_gold_product,
)
from nyc311.manifests import BronzeManifest, SilverManifest
from nyc311.silver import SILVER_SCHEMA
from nyc311.silver_store import publish_silver
from nyc311.source import SourcePage
from nyc311.storage import (
    manifest_key,
    object_path,
    output_root,
    write_bytes,
    write_json,
)

DAY = COHORT_START
SAMPLES = Path(__file__).resolve().parents[1] / "data" / "samples"
type FixtureInput = Callable[..., tuple[BronzeManifest, list[SourcePage]]]


def save_silver(
    base: Path,
    fixture: FixtureInput,
    records: list[dict[str, object]],
    *,
    day: date = DAY,
    mode: str = "sample",
    observed: str = "2026-10-13T12:00:00Z",
) -> SilverManifest:
    manifest, pages = fixture(records, day=day, observed=observed)
    manifest["mode"] = mode
    root = output_root(base, mode)
    for page, reference in zip(pages, manifest["pages"]):
        write_bytes(root, reference["key"], gzip.compress(page["content"], mtime=0))
    write_json(root, manifest_key(manifest), manifest)
    return publish_silver(base, manifest_key(manifest), mode=mode)


def test_official_input_requires_all_30_verified_days(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    for day, _ in daily_windows(COHORT_START, COHORT_END):
        save_silver(tmp_path, silver_input, [], day=day, mode="published")
    result = load_gold_inputs(tmp_path, mode="published")
    assert len(result["days"]) == len(result["silver_manifests"]) == 30
    assert result["table"].num_rows == 0
    assert result["table"].schema.equals(SILVER_SCHEMA, check_metadata=True)
    assert not (output_root(tmp_path, "published") / "gold").exists()
    for product in ("demand_daily", "closure_duration"):
        path = prepare_gold_product(tmp_path, product, "full-month", mode="published")
        table = pq.ParquetFile(path).read()
        assert table.num_rows == 0
        assert table.schema.metadata[b"coverage"] == b"full_month"
        assert table.schema.metadata[b"publication_state"] == b"prepared"
    assert not (output_root(tmp_path, "published") / "control/gold").exists()


def test_missing_official_days_are_reported(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    save_silver(tmp_path, silver_input, [], mode="published")
    with pytest.raises(GoldError) as error:
        load_gold_inputs(tmp_path, mode="published")
    assert "2026-09-02" in str(error.value)
    assert "2026-09-30" in str(error.value)
    assert not (output_root(tmp_path, "published") / "gold").exists()


def test_published_mode_cannot_bypass_coverage_by_selecting_one_day(
    tmp_path: Path,
) -> None:
    with pytest.raises(GoldError, match="Full September"):
        load_gold_inputs(tmp_path, mode="published", days=[DAY])
    assert list(tmp_path.iterdir()) == []


def test_sample_days_are_explicit_and_never_used_as_published_input(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    records = json.loads((SAMPLES / "pagination-001.json").read_bytes())
    save_silver(tmp_path, silver_input, records)
    result = load_gold_inputs(tmp_path, days=[DAY])
    assert result["mode"] == "sample"
    assert result["days"] == [DAY]
    assert result["table"].num_rows == 2
    with pytest.raises(GoldError, match="invalid Silver"):
        load_gold_inputs(tmp_path, mode="published")
    assert not output_root(tmp_path, "published").exists()


@pytest.mark.parametrize("status", ["incomplete", "failed"])
def test_incomplete_selected_partition_blocks_gold(
    tmp_path: Path, silver_input: FixtureInput, status: str
) -> None:
    manifest = save_silver(tmp_path, silver_input, [])
    manifest["status"] = status
    write_json(output_root(tmp_path, "sample"), manifest_key(manifest), manifest)
    with pytest.raises(GoldError, match="2026-09-01"):
        load_gold_inputs(tmp_path, days=[DAY])


def test_missing_artifact_in_complete_partition_blocks_gold(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    manifest = save_silver(tmp_path, silver_input, [])
    object_path(
        output_root(tmp_path, "sample"), manifest["quality_report_key"]
    ).unlink()
    with pytest.raises(GoldError, match="2026-09-01"):
        load_gold_inputs(tmp_path, days=[DAY])


def test_cross_partition_identifiers_must_be_unique(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    record = json.loads((SAMPLES / "pagination-001.json").read_bytes())[0]
    save_silver(tmp_path, silver_input, [record])
    second_day = date(2026, 9, 2)
    second = dict(record, created_date="2026-09-02T00:00:00")
    save_silver(tmp_path, silver_input, [second], day=second_day)
    with pytest.raises(GoldError, match="unique across"):
        load_gold_inputs(tmp_path, days=[DAY, second_day])


@pytest.mark.parametrize("days", [[], [DAY, DAY]])
def test_invalid_selection_fails_without_writes(
    tmp_path: Path, days: list[date]
) -> None:
    with pytest.raises(GoldError):
        load_gold_inputs(tmp_path, days=days)
    assert list(tmp_path.iterdir()) == []


def metric_records() -> list[dict[str, object]]:
    return json.loads((SAMPLES / "gold-metrics.json").read_bytes())


def metrics_silver(base: Path, fixture: FixtureInput) -> list[date]:
    days = [date(2026, 9, 1), date(2026, 9, 2)]
    for day in days:
        records = [
            record
            for record in metric_records()
            if record["created_date"].startswith(day.isoformat())
        ]
        save_silver(base, fixture, records, day=day)
    return days


def test_demand_known_counts_unknown_buckets_and_denominators(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    days = metrics_silver(tmp_path, silver_input)
    inputs = load_gold_inputs(tmp_path, days=days)
    result = compute_product(inputs["table"], "demand_daily")
    assert result.schema.equals(DEMAND_SCHEMA)
    values = {
        (row["created_date"], row["borough"], row["complaint_type"]): row
        for row in result.to_pylist()
    }
    assert sum(row["request_count"] for row in values.values()) == 12
    street = values[(DAY, "MANHATTAN", "Street Condition")]
    assert street["request_count"] == 3
    assert street["category_share"] == 0.75
    assert values[(DAY, "MANHATTAN", "Noise - Residential")]["category_share"] == 0.25
    assert values[(DAY, "QUEENS", "Street Condition")]["request_count"] == 2
    assert values[(days[1], "UNKNOWN", "UNKNOWN")]["request_count"] == 1
    assert values[(days[1], "UNKNOWN", "UNKNOWN")]["category_share"] == 0.5


def test_prepared_demand_parquet_is_labeled_sample_without_publication_marker(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    days = metrics_silver(tmp_path, silver_input)
    path = prepare_gold_product(tmp_path, "demand_daily", "metrics", days=days)
    root = output_root(tmp_path, "sample")
    assert path == root / "gold/run=metrics/demand_daily/part.parquet"
    result = pq.ParquetFile(path).read()
    assert result.schema.metadata[b"mode"] == b"sample"
    assert result.schema.metadata[b"coverage"] == b"sample"
    assert result.schema.metadata[b"publication_state"] == b"prepared"
    assert json.loads(result.schema.metadata[b"covered_days"]) == [
        day.isoformat() for day in days
    ]
    assert sum(result["request_count"].to_pylist()) == 12
    assert not output_root(tmp_path, "published").exists()
    assert not (root / "control/gold").exists()


def test_demand_empty_input_has_explicit_empty_schema(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    save_silver(tmp_path, silver_input, [])
    table = load_gold_inputs(tmp_path, days=[DAY])["table"]
    result = compute_product(table, "demand_daily")
    assert result.num_rows == 0
    assert result.schema.equals(DEMAND_SCHEMA)


def test_incomplete_official_month_cannot_write_product(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    save_silver(tmp_path, silver_input, [], mode="published")
    with pytest.raises(GoldError):
        prepare_gold_product(tmp_path, "demand_daily", "blocked", mode="published")
    assert not (output_root(tmp_path, "published") / "gold").exists()


def test_closure_known_population_continuous_quantiles_and_later_month_closures(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    days = metrics_silver(tmp_path, silver_input)
    inputs = load_gold_inputs(tmp_path, days=days)
    result = compute_product(inputs["table"], "closure_duration")
    assert result.schema.equals(CLOSURE_SCHEMA)
    assert result.num_rows == 2
    values = {row["agency"]: row for row in result.to_pylist()}
    # DOT hours [1, 3, 9, 696] include the valid October closure of a September request.
    assert values["DOT"]["valid_closed_count"] == 4
    assert values["DOT"]["median_closure_hours"] == pytest.approx(6.0)
    assert values["DOT"]["p90_closure_hours"] == pytest.approx(489.9)
    assert values["NYPD"]["valid_closed_count"] == 1
    assert (
        values["NYPD"]["median_closure_hours"]
        == values["NYPD"]["p90_closure_hours"]
        == 0.0
    )
    assert sum(result["valid_closed_count"].to_pylist()) == 5


def test_day_one_continuous_p90_is_interpolated(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    metrics_silver(tmp_path, silver_input)
    inputs = load_gold_inputs(tmp_path, days=[DAY])
    values = compute_product(inputs["table"], "closure_duration").to_pylist()
    dot = next(row for row in values if row["agency"] == "DOT")
    assert dot["valid_closed_count"] == 3
    assert dot["median_closure_hours"] == 3.0
    assert dot["p90_closure_hours"] == pytest.approx(7.8)


def test_closure_parquet_round_trip_and_demand_keeps_ineligible_requests(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    days = metrics_silver(tmp_path, silver_input)
    path = prepare_gold_product(tmp_path, "closure_duration", "metrics", days=days)
    result = pq.ParquetFile(path).read()
    expected = compute_product(
        load_gold_inputs(tmp_path, days=days)["table"], "closure_duration"
    )
    assert result.replace_schema_metadata(None).equals(expected)
    assert result.schema.metadata[b"mode"] == b"sample"
    assert result.schema.metadata[b"publication_state"] == b"prepared"
    demand = compute_product(
        load_gold_inputs(tmp_path, days=days)["table"], "demand_daily"
    )
    assert sum(demand["request_count"].to_pylist()) == 12


def test_no_eligible_closures_has_empty_typed_result(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    records = [
        record
        for record in metric_records()
        if record["unique_key"]
        in ("sample-gold-05", "sample-gold-06", "sample-gold-07")
    ]
    save_silver(tmp_path, silver_input, records)
    inputs = load_gold_inputs(tmp_path, days=[DAY])
    result = compute_product(inputs["table"], "closure_duration")
    assert result.num_rows == 0
    assert result.schema.equals(CLOSURE_SCHEMA)
    assert (
        sum(
            compute_product(inputs["table"], "demand_daily")[
                "request_count"
            ].to_pylist()
        )
        == 3
    )


def test_duration_uses_microseconds_without_integer_second_truncation(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    record = dict(metric_records()[0], closed_date="2026-09-01T00:00:00.000001")
    save_silver(tmp_path, silver_input, [record])
    table = load_gold_inputs(tmp_path, days=[DAY])["table"]
    result = compute_product(table, "closure_duration").to_pylist()[0]
    assert result["median_closure_hours"] == pytest.approx(1 / 3600000000, abs=1e-18)
    assert result["p90_closure_hours"] == pytest.approx(1 / 3600000000, abs=1e-18)


def test_duration_uses_utc_elapsed_time_across_dst(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    record = dict(metric_records()[0], closed_date="2026-11-01T02:30:00")
    save_silver(tmp_path, silver_input, [record], observed="2026-11-02T12:00:00Z")
    table = load_gold_inputs(tmp_path, days=[DAY])["table"]
    result = compute_product(table, "closure_duration").to_pylist()[0]
    assert result["valid_closed_count"] == 1
    assert result["median_closure_hours"] == 61 * 24 + 3.5


def test_outstanding_fixture_separates_confirmed_and_unknown_states(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    days = metrics_silver(tmp_path, silver_input)
    table = load_gold_inputs(tmp_path, days=days)["table"]
    result = compute_product(table, "outstanding_requests")
    assert result.schema.equals(OUTSTANDING_SCHEMA)
    assert sum(result["outstanding_count"].to_pylist()) == 2
    assert sum(result["unknown_status_count"].to_pylist()) == 1
    unknown = next(
        row for row in result.to_pylist() if row["lifecycle_class"] == "unknown"
    )
    assert unknown["agency"] == "NYPD"
    assert unknown["outstanding_count"] == 0
    assert unknown["unknown_status_count"] == 1
    assert all(row["age_band"] == "30+" for row in result.to_pylist())
    assert compute_product(table, "outstanding_requests").equals(result)


@pytest.mark.parametrize(
    ("age", "band"),
    [
        (timedelta(0), "[0,7)"),
        (timedelta(days=7, microseconds=-1), "[0,7)"),
        (timedelta(days=7), "[7,14)"),
        (timedelta(days=14, microseconds=-1), "[7,14)"),
        (timedelta(days=14), "[14,30)"),
        (timedelta(days=30, microseconds=-1), "[14,30)"),
        (timedelta(days=30), "30+"),
        (timedelta(microseconds=-1), "UNKNOWN"),
    ],
)
def test_outstanding_age_uses_observation_and_exact_boundaries(
    tmp_path: Path, silver_input: FixtureInput, age: timedelta, band: str
) -> None:
    record = dict(metric_records()[4])
    observed = datetime(2026, 9, 1, 4, tzinfo=UTC) + age
    save_silver(tmp_path, silver_input, [record], observed=observed.isoformat())
    inputs = load_gold_inputs(tmp_path, days=[DAY])
    result = compute_product(inputs["table"], "outstanding_requests").to_pylist()[0]
    assert result["age_band"] == band
    assert result["outstanding_count"] == 1
    assert result["unknown_status_count"] == 0


def test_outstanding_parquet_uses_saved_observation_not_generation_time(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    save_silver(
        tmp_path, silver_input, [metric_records()[4]], observed="2026-09-08T04:00:00Z"
    )
    first = prepare_gold_product(
        tmp_path, "outstanding_requests", "age-first", days=[DAY]
    )
    second = prepare_gold_product(
        tmp_path, "outstanding_requests", "age-second", days=[DAY]
    )
    assert (
        pq.ParquetFile(first)
        .read()
        .equals(pq.ParquetFile(second).read(), check_metadata=True)
    )
    assert pq.ParquetFile(first).read()["age_band"].to_pylist() == ["[7,14)"]


def test_channel_counts_shares_and_missing_or_new_categories(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    records = [
        record
        for record in metric_records()
        if record["created_date"].startswith(DAY.isoformat())
    ]
    channels = ["PHONE", "ONLINE", None, "ONLINE", "PHONE", "MOBILE", "PHONE"]
    for record, channel in zip(records, channels):
        record["open_data_channel_type"] = channel
    save_silver(tmp_path, silver_input, records)
    table = load_gold_inputs(tmp_path, days=[DAY])["table"]
    result = compute_product(table, "channel_usage")
    assert result.schema.equals(CHANNEL_SCHEMA)
    assert sum(result["request_count"].to_pylist()) == 7
    values = {(row["borough"], row["channel"]): row for row in result.to_pylist()}
    assert values[("MANHATTAN", "PHONE")]["request_count"] == 2
    assert values[("MANHATTAN", "PHONE")]["channel_share"] == 0.5
    assert values[("MANHATTAN", "ONLINE")]["channel_share"] == 0.5
    assert values[("UNKNOWN", "UNKNOWN")]["request_count"] == 1
    assert values[("UNKNOWN", "UNKNOWN")]["channel_share"] == 1.0
    new = dict(records[0], open_data_channel_type=" new_channel ")
    save_silver(tmp_path, silver_input, [new])
    new_result = compute_product(
        load_gold_inputs(tmp_path, days=[DAY])["table"], "channel_usage"
    )
    assert new_result["channel"].to_pylist() == ["NEW_CHANNEL"]
    assert new_result["channel_share"].to_pylist() == [1.0]


def test_channel_parquet_keeps_unknown_population_and_reconciles(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    days = metrics_silver(tmp_path, silver_input)
    path = prepare_gold_product(tmp_path, "channel_usage", "channels", days=days)
    table = pq.ParquetFile(path).read()
    assert sum(table["request_count"].to_pylist()) == 12
    values = {
        (row["created_date"], row["borough"], row["channel"]): (
            row["request_count"],
            row["channel_share"],
        )
        for row in table.to_pylist()
    }
    assert values == {
        (DAY, "MANHATTAN", "ONLINE"): (2, 0.5),
        (DAY, "MANHATTAN", "PHONE"): (2, 0.5),
        (DAY, "QUEENS", "MOBILE"): (1, 0.5),
        (DAY, "QUEENS", "UNKNOWN"): (1, 0.5),
        (DAY, "UNKNOWN", "UNKNOWN"): (1, 1.0),
        (days[1], "MANHATTAN", "MOBILE"): (1, 0.5),
        (days[1], "MANHATTAN", "UNKNOWN"): (1, 0.5),
        (days[1], "QUEENS", "PHONE"): (1, 1.0),
        (days[1], "UNKNOWN", "MOBILE"): (1, 0.5),
        (days[1], "UNKNOWN", "ONLINE"): (1, 0.5),
    }
    assert table.schema.metadata[b"coverage"] == b"sample"


@pytest.mark.parametrize("product", ["outstanding_requests", "channel_usage"])
def test_new_products_preserve_empty_schema(
    tmp_path: Path, silver_input: FixtureInput, product: str
) -> None:
    save_silver(tmp_path, silver_input, [])
    table = load_gold_inputs(tmp_path, days=[DAY])["table"]
    result = compute_product(table, product)
    assert result.num_rows == 0
    expected = (
        OUTSTANDING_SCHEMA if product == "outstanding_requests" else CHANNEL_SCHEMA
    )
    assert result.schema.equals(expected)


def test_incorrect_channel_shares_are_rejected_despite_balanced_totals(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    from nyc311.gold import _compute_query

    records = [
        dict(row, open_data_channel_type=channel, borough="MANHATTAN")
        for row, channel in zip(metric_records()[:3], ["PHONE", "PHONE", "ONLINE"])
    ]
    save_silver(tmp_path, silver_input, records)
    table = load_gold_inputs(tmp_path, days=[DAY])["table"]
    query = """
        SELECT created_date, borough, open_data_channel_type AS channel,
               count(*) AS request_count, 0.5::DOUBLE AS channel_share
        FROM silver GROUP BY created_date, borough, open_data_channel_type
    """
    with pytest.raises(GoldError, match="invalid counts, shares"):
        _compute_query(table, "channel_usage", query)


def test_incorrect_age_bands_are_rejected_despite_balanced_lifecycle_totals(
    tmp_path: Path, silver_input: FixtureInput
) -> None:
    from nyc311.gold import _compute_query

    record = dict(metric_records()[4])
    save_silver(tmp_path, silver_input, [record], observed="2026-09-02T04:00:00Z")
    table = load_gold_inputs(tmp_path, days=[DAY])["table"]
    query = """
        SELECT agency, borough, lifecycle_class, '30+' AS age_band,
               count(*) AS outstanding_count, 0::BIGINT AS unknown_status_count
        FROM silver WHERE lifecycle_class = 'outstanding'
        GROUP BY agency, borough, lifecycle_class
    """
    with pytest.raises(GoldError, match="counts do not reconcile"):
        _compute_query(table, "outstanding_requests", query)
