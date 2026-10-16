"""Verified Gold inputs and shared analytical computation/product preparation."""

import json
import re
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from math import isclose, isfinite
from pathlib import Path
from typing import Literal, TypedDict

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from nyc311.config import COHORT_END, COHORT_START, Mode, daily_windows, validate_window
from nyc311.lambda_runtime import DUCKDB_LIMITS
from nyc311.manifests import SilverManifest
from nyc311.silver_contract import SILVER_SCHEMA, SilverError
from nyc311.silver_store import load_silver
from nyc311.storage import object_path, output_root, temporary_object

type Product = Literal[
    "demand_daily", "closure_duration", "outstanding_requests", "channel_usage"
]
DEFAULT_SQL_DIRECTORY = Path(__file__).resolve().parents[2] / "sql"
DEMAND_SCHEMA = pa.schema(
    [
        pa.field("created_date", pa.date32(), nullable=False),
        pa.field("borough", pa.string(), nullable=False),
        pa.field("complaint_type", pa.string(), nullable=False),
        pa.field("request_count", pa.int64(), nullable=False),
        pa.field("category_share", pa.float64(), nullable=False),
    ]
)
CLOSURE_SCHEMA = pa.schema(
    [
        pa.field("agency", pa.string(), nullable=False),
        pa.field("complaint_type", pa.string(), nullable=False),
        pa.field("valid_closed_count", pa.int64(), nullable=False),
        pa.field("median_closure_hours", pa.float64(), nullable=False),
        pa.field("p90_closure_hours", pa.float64(), nullable=False),
    ]
)
OUTSTANDING_SCHEMA = pa.schema(
    [
        pa.field("agency", pa.string(), nullable=False),
        pa.field("borough", pa.string(), nullable=False),
        pa.field("lifecycle_class", pa.string(), nullable=False),
        pa.field("age_band", pa.string(), nullable=False),
        pa.field("outstanding_count", pa.int64(), nullable=False),
        pa.field("unknown_status_count", pa.int64(), nullable=False),
    ]
)
PRODUCT_SCHEMAS = {
    "demand_daily": DEMAND_SCHEMA,
    "closure_duration": CLOSURE_SCHEMA,
    "outstanding_requests": OUTSTANDING_SCHEMA,
}
CHANNEL_SCHEMA = pa.schema(
    [
        pa.field("created_date", pa.date32(), nullable=False),
        pa.field("borough", pa.string(), nullable=False),
        pa.field("channel", pa.string(), nullable=False),
        pa.field("request_count", pa.int64(), nullable=False),
        pa.field("channel_share", pa.float64(), nullable=False),
    ]
)
PRODUCT_SCHEMAS["channel_usage"] = CHANNEL_SCHEMA


class GoldError(RuntimeError):
    """Gold inputs lack coverage or consistency, or a product fails validation."""


class GoldInputs(TypedDict):
    table: pa.Table
    silver_manifests: list[SilverManifest]
    mode: Mode
    days: list[date]


def load_gold_inputs(
    base: Path, *, mode: Mode = "sample", days: Sequence[date] | None = None
) -> GoldInputs:
    """Published mode always requires September; sample can select explicit days.

    All selected partitions must be complete and verified, even if empty.
    Reading occurs under the existing no-refresh-during-consumption contract.
    """
    output_root(base, mode)
    expected = [start for start, _ in daily_windows(COHORT_START, COHORT_END)]
    selected = list(expected if days is None else days)
    for day in selected:
        validate_window(day, COHORT_END)
    if not selected or len(set(selected)) != len(selected):
        raise GoldError(
            "Select distinct September days, including validated empty days"
        )
    selected.sort()
    if mode == "published" and selected != expected:
        missing = sorted(set(expected) - set(selected))
        raise GoldError(
            "Full September coverage is required; missing selection: "
            + ", ".join(day.isoformat() for day in missing)
        )
    manifests: list[SilverManifest] = []
    tables = []
    invalid = []
    for day in selected:
        try:
            manifest, table = load_silver(base, day, mode=mode)
        except SilverError:
            invalid.append(day.isoformat())
            continue
        manifests.append(manifest)
        tables.append(table)
    if invalid:
        raise GoldError(
            "Missing, incomplete, or invalid Silver days: " + ", ".join(invalid)
        )
    table = pa.concat_tables(tables)
    identifiers = table["unique_key"].to_pylist()
    if len(set(identifiers)) != table.num_rows:
        raise GoldError("Request identifiers must be unique across selected partitions")
    return {
        "table": table,
        "silver_manifests": manifests,
        "mode": mode,
        "days": selected,
    }


def _age_band(created: datetime | None, observed: datetime | None) -> str:
    """Independent aggregate validation, without changing Silver lifecycle/quality."""
    if created is None or observed is None:
        return "UNKNOWN"
    delta = observed - created
    age_us = delta.days * 86400000000 + delta.seconds * 1000000 + delta.microseconds
    if age_us < 0:
        return "UNKNOWN"
    for days, band in ((7, "[0,7)"), (14, "[7,14)"), (30, "[14,30)")):
        if age_us < days * 86400000000:
            return band
    return "30+"


def _validate_result(source: pa.Table, result: pa.Table, product: Product) -> None:
    if any(
        result[field.name].null_count for field in result.schema if not field.nullable
    ):
        raise GoldError("Gold result contains null required values")
    if product == "outstanding_requests":
        columns = [
            source[name].to_pylist()
            for name in (
                "agency",
                "borough",
                "lifecycle_class",
                "created_date_utc",
                "observed_at_utc",
            )
        ]
        expected = Counter(
            (agency, borough, lifecycle, _age_band(created, observed))
            for agency, borough, lifecycle, created, observed in zip(*columns)
            if lifecycle in ("outstanding", "unknown")
        )
        received = Counter()
        grains = set()
        for row in result.to_pylist():
            group = (row["agency"], row["borough"], row["lifecycle_class"])
            grain = (*group, row["age_band"])
            outstanding, unknown = row["outstanding_count"], row["unknown_status_count"]
            if (
                grain in grains
                or row["age_band"]
                not in ("[0,7)", "[7,14)", "[14,30)", "30+", "UNKNOWN")
                or outstanding < 0
                or unknown < 0
                or outstanding + unknown <= 0
                or row["lifecycle_class"] not in ("outstanding", "unknown")
                or (row["lifecycle_class"] == "outstanding" and unknown != 0)
                or (row["lifecycle_class"] == "unknown" and outstanding != 0)
            ):
                raise GoldError(
                    "Outstanding aggregation mixes lifecycle counts or has invalid grain"
                )
            grains.add(grain)
            received[grain] += outstanding + unknown
        if received != expected:
            raise GoldError("Outstanding and unknown counts do not reconcile to Silver")
        return
    if product == "closure_duration":
        columns = [
            source[name].to_pylist()
            for name in (
                "agency",
                "complaint_type",
                "lifecycle_class",
                "duration_eligible",
            )
        ]
        expected = Counter(
            (agency, category)
            for agency, category, lifecycle, eligible in zip(*columns)
            if lifecycle == "closed" and eligible
        )
        received = Counter()
        for row in result.to_pylist():
            group = (row["agency"], row["complaint_type"])
            count = row["valid_closed_count"]
            median, p90 = row["median_closure_hours"], row["p90_closure_hours"]
            if (
                group in received
                or count <= 0
                or not isfinite(median)
                or not isfinite(p90)
                or not 0 <= median <= p90
            ):
                raise GoldError(
                    "Closure aggregation has invalid counts, quantiles, or grain"
                )
            received[group] = count
        if received != expected:
            raise GoldError(
                "Closure counts do not reconcile to eligible Silver requests"
            )
        return
    expected = Counter(
        zip(source["created_date"].to_pylist(), source["borough"].to_pylist())
    )
    source_dimension = (
        "complaint_type" if product == "demand_daily" else "open_data_channel_type"
    )
    expected_grains = Counter(
        zip(
            source["created_date"].to_pylist(),
            source["borough"].to_pylist(),
            source[source_dimension].to_pylist(),
        )
    )
    received = Counter()
    shares: dict[tuple[date, str], float] = defaultdict(float)
    grains = set()
    for row in result.to_pylist():
        group = (row["created_date"], row["borough"])
        dimension = "complaint_type" if product == "demand_daily" else "channel"
        share_column = (
            "category_share" if product == "demand_daily" else "channel_share"
        )
        grain = (*group, row[dimension])
        count, share = row["request_count"], row[share_column]
        denominator = expected[group]
        if (
            grain in grains
            or count <= 0
            or count != expected_grains[grain]
            or denominator <= 0
            or not isfinite(share)
            or not 0 < share <= 1
            or not isclose(share, count / denominator, rel_tol=1e-9)
        ):
            raise GoldError("Daily aggregation has invalid counts, shares, or grain")
        grains.add(grain)
        received[group] += count
        shares[group] += share
    if received != expected or any(
        not isclose(value, 1.0, rel_tol=1e-9) for value in shares.values()
    ):
        raise GoldError("Daily counts or shares do not reconcile to accepted Silver")


def compute_product(
    table: pa.Table,
    product: Product,
    *,
    sql_directory: Path | None = None,
    duckdb_config: dict[str, object] | None = None,
) -> pa.Table:
    """Execute versioned SQL over verified Silver; packaging can supply its SQL directory."""
    if product not in PRODUCT_SCHEMAS:
        raise GoldError("Unsupported Gold product")
    if not table.schema.equals(SILVER_SCHEMA, check_metadata=True):
        raise GoldError("Gold requires the explicit Silver schema")
    directory = DEFAULT_SQL_DIRECTORY if sql_directory is None else sql_directory
    query = (directory / f"{product}.sql").read_text(encoding="utf-8")
    if duckdb_config is not None:
        return _compute_query(table, product, query, duckdb_config=duckdb_config)
    return _compute_query(table, product, query)


def _compute_query(
    table: pa.Table,
    product: Product,
    query: str,
    *,
    duckdb_config: dict[str, object] | None = None,
) -> pa.Table:
    """Execute unchanged SQL without spill; overrides may only lower the memory cap."""
    if product not in PRODUCT_SCHEMAS or not table.schema.equals(
        SILVER_SCHEMA, check_metadata=True
    ):
        raise GoldError(
            "Gold requires a supported product and the explicit Silver schema"
        )
    if duckdb_config is not None:
        if (
            not isinstance(duckdb_config, dict)
            or set(duckdb_config) - DUCKDB_LIMITS.keys()
        ):
            raise GoldError("DuckDB configuration may contain only resource settings")
        for name, value in duckdb_config.items():
            if name == "threads":
                valid = type(value) is int and value == DUCKDB_LIMITS[name]
            elif name == "temp_directory":
                valid = type(value) is str and value == ""
            else:
                match = isinstance(value, str) and re.fullmatch(
                    r"([1-9][0-9]*)\s*(?:([KMGT])(i?))?B", value, re.IGNORECASE
                )
                valid = bool(match) and (
                    int(match[1])
                    * (1024 if match[3] else 1000)
                    ** "BKMGT".index((match[2] or "B").upper())
                    <= 512 * 1024**2
                )
            if not valid:
                raise GoldError("DuckDB resource setting has an unsafe value")
    config = dict(DUCKDB_LIMITS)
    if duckdb_config is not None:
        config.update(duckdb_config)
    with duckdb.connect(":memory:", config=config) as connection:
        connection.register("silver", table)
        result = connection.execute(query).to_arrow_table()
    result = result.cast(PRODUCT_SCHEMAS[product], safe=True)
    _validate_result(table, result, product)
    return result


def prepare_gold_product(
    base: Path,
    product: Product,
    run_id: str,
    *,
    mode: Mode = "sample",
    days: Sequence[date] | None = None,
    sql_directory: Path | None = None,
    duckdb_config: dict[str, object] | None = None,
) -> Path:
    """Write one prepared Parquet, not the four-product publication of task 5.6."""
    if product not in PRODUCT_SCHEMAS:
        raise GoldError("Unsupported Gold product")
    if not isinstance(run_id, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_-]*", run_id
    ):
        raise GoldError("Gold run identifier must be a safe path component")
    root = output_root(base, mode)
    if object_path(root, f"control/gold/{run_id}.json").exists():
        raise GoldError(
            "A managed Gold run cannot be changed by individual preparation"
        )
    inputs = load_gold_inputs(base, mode=mode, days=days)
    if duckdb_config is None:
        result = compute_product(inputs["table"], product, sql_directory=sql_directory)
    else:
        result = compute_product(
            inputs["table"],
            product,
            sql_directory=sql_directory,
            duckdb_config=duckdb_config,
        )
    return _write_prepared_product(root, inputs, product, run_id, result)


def _write_prepared_product(
    root: Path, inputs: GoldInputs, product: Product, run_id: str, result: pa.Table
) -> Path:
    """The run manifest, not this immutable preparation footer, authorizes publication."""
    mode = inputs["mode"]
    result = result.replace_schema_metadata(
        {
            b"product": product.encode(),
            b"mode": mode.encode(),
            b"coverage": b"sample" if mode == "sample" else b"full_month",
            b"covered_days": json.dumps(
                [day.isoformat() for day in inputs["days"]]
            ).encode(),
            b"coverage_start": inputs["days"][0].isoformat().encode(),
            b"coverage_end": (inputs["days"][-1] + timedelta(days=1))
            .isoformat()
            .encode(),
            b"publication_state": b"prepared",
        }
    )
    key = f"gold/run={run_id}/{product}/part.parquet"
    with temporary_object(root, key) as temporary:
        pq.write_table(result, temporary, compression="snappy")
        restored = pq.ParquetFile(temporary).read()
        if not restored.equals(result, check_metadata=True):
            raise GoldError("Prepared Gold Parquet does not preserve analytical values")
        temporary.replace(object_path(root, key))
    return object_path(root, key)
