"""Immutable local four-product Gold runs, consumed only through complete manifests."""

import json
import re
from collections import Counter
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from nyc311.config import COHORT_END, COHORT_START, Mode, daily_windows, validate_window
from nyc311.gold import (
    DEFAULT_SQL_DIRECTORY,
    PRODUCT_SCHEMAS,
    GoldError,
    GoldInputs,
    Product,
    _compute_query,
    _write_prepared_product,
    load_gold_inputs,
)
from nyc311.manifests import GoldManifest
from nyc311.silver import SCHEMA_VERSION, TRANSFORMATION_VERSION
from nyc311.storage import (
    decode_json,
    manifest_key,
    object_path,
    output_root,
    read_bytes,
    read_json,
    write_json_atomic,
)

GOLD_VERSION = "1"


def _run_key(run_id: str) -> str:
    if not isinstance(run_id, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_-]*", run_id
    ):
        raise GoldError("Gold run identifier must be a safe path component")
    return f"control/gold/{run_id}.json"


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() != timedelta(0):
        raise GoldError("Gold observation and generation timestamps must be UTC")
    return parsed.astimezone(UTC)


def _provenance(
    root: Path, inputs: GoldInputs
) -> tuple[dict[str, object], dict[str, dict[str, str]]]:
    """Capture source observation ranges even for empty or fully quarantined days."""
    scalar_counts = Counter()
    frequencies = {
        name: Counter()
        for name in ("warnings", "rejection_reasons", "duration_exclusions")
    }
    observations = {}
    try:
        for partition in inputs["silver_manifests"]:
            content = read_bytes(root, partition["quality_report_key"])
            if sha256(content).hexdigest() != partition["quality_report_sha256"]:
                raise GoldError("Input quality report changed during Gold preparation")
            report = decode_json(content)
            for name in (
                "received",
                "accepted",
                "quarantined",
                "identical_duplicates_discarded",
            ):
                scalar_counts[name] += report[name]
            for name in frequencies:
                frequencies[name].update(report[name])
            source = read_json(root, partition["source_manifest_key"])
            if (
                source["kind"] != "bronze"
                or source["status"] != "complete"
                or source["mode"] != inputs["mode"]
                or source["start_date"] != partition["start_date"]
                or source["end_date"] != partition["end_date"]
                or source["record_count"] != report["received"]
                or manifest_key(source) != partition["source_manifest_key"]
            ):
                raise GoldError("Gold source provenance disagrees with verified Silver")
            stamps = [_utc(page["observed_at_utc"]) for page in source["pages"]]
            if not stamps:
                raise GoldError(
                    "Source observation provenance must include empty pages"
                )
            observations[partition["start_date"]] = {
                "min_utc": min(stamps).isoformat(),
                "max_utc": max(stamps).isoformat(),
            }
    except (KeyError, TypeError, ValueError, OSError):
        raise GoldError(
            "Gold source or quality provenance is invalid or unavailable"
        ) from None
    exclusions: dict[str, object] = dict(scalar_counts)
    exclusions.update(
        {name: dict(sorted(values.items())) for name, values in frequencies.items()}
    )
    return exclusions, observations


def _verify_run(root: Path, manifest: GoldManifest) -> dict[str, pa.Table]:
    """Verify a frozen run; never compare it with Silver that may since have refreshed."""
    try:
        manifest_key(manifest)
        if (
            manifest["kind"] != "gold"
            or manifest["transformation_version"] != GOLD_VERSION
            or manifest["start_date"] != COHORT_START.isoformat()
            or manifest["end_date"] != COHORT_END.isoformat()
            or set(manifest["products"]) != set(PRODUCT_SCHEMAS)
        ):
            raise GoldError("Gold manifest must describe the four September products")
        selected = [date.fromisoformat(value) for value in manifest["covered_days"]]
        for day in selected:
            validate_window(day, COHORT_END)
        if not selected or selected != sorted(set(selected)):
            raise GoldError(
                "Gold coverage must contain distinct ordered September days"
            )
        expected = [day for day, _ in daily_windows(COHORT_START, COHORT_END)]
        if manifest["mode"] == "published" and (
            selected != expected or manifest["coverage"] != "full_month"
        ):
            raise GoldError(
                "Official Gold publication requires complete September coverage"
            )
        if manifest["mode"] == "sample" and manifest["coverage"] != "sample":
            raise GoldError("Sample Gold cannot claim official coverage")
        _utc(manifest["generated_at_utc"])
        if set(manifest["observations_by_day"]) != set(manifest["covered_days"]):
            raise GoldError(
                "Gold observation metadata does not cover every selected day"
            )
        minima, maxima = [], []
        for value in manifest["observations_by_day"].values():
            lower, upper = _utc(value["min_utc"]), _utc(value["max_utc"])
            if lower > upper:
                raise GoldError("Gold observation range is invalid")
            minima.append(lower)
            maxima.append(upper)
        if _utc(manifest["observation_min_utc"]) != min(minima) or _utc(
            manifest["observation_max_utc"]
        ) != max(maxima):
            raise GoldError("Overall Gold observation range disagrees with input days")
        partitions = manifest["input_partitions"]
        if [value["start_date"] for value in partitions] != manifest["covered_days"]:
            raise GoldError("Gold input provenance disagrees with its coverage")
        for partition, day in zip(partitions, selected):
            if (
                partition["kind"] != "silver"
                or partition["status"] != "complete"
                or partition["mode"] != manifest["mode"]
                or partition["end_date"] != (day + timedelta(days=1)).isoformat()
                or type(partition["row_count"]) is not int
                or partition["row_count"] < 0
                or partition["schema_version"] != SCHEMA_VERSION
                or partition["transformation_version"] != TRANSFORMATION_VERSION
                or manifest_key(partition) != f"control/silver/{day.isoformat()}.json"
                or partition["partition_key"]
                != f"silver/created_date={day.isoformat()}/part.parquet"
                or partition["quality_report_key"]
                != f"quality/run={partition['run_id']}/{day.isoformat()}.json"
                or partition["quarantine_key"]
                != f"quarantine/run={partition['run_id']}/{day.isoformat()}.json.gz"
            ):
                raise GoldError(
                    "Gold must reference verified complete input partitions"
                )
            for field in (
                "partition_sha256",
                "quality_report_sha256",
                "quarantine_sha256",
            ):
                if not re.fullmatch(r"[0-9a-f]{64}", partition[field]):
                    raise GoldError(
                        "Frozen Silver provenance must contain artifact digests"
                    )
            if not re.fullmatch(
                r"control/bronze/[A-Za-z0-9][A-Za-z0-9_-]*/"
                + re.escape(day.isoformat())
                + r"\.json",
                partition["source_manifest_key"],
            ):
                raise GoldError(
                    "Frozen Silver provenance must identify its Bronze source"
                )
        quality = manifest["quality_exclusions"]
        counts = [
            quality[name]
            for name in (
                "received",
                "accepted",
                "quarantined",
                "identical_duplicates_discarded",
            )
        ]
        if any(type(count) is not int or count < 0 for count in counts) or counts[
            0
        ] != sum(counts[1:]):
            raise GoldError("Gold quality exclusions do not reconcile")
        if counts[1] != sum(value["row_count"] for value in partitions):
            raise GoldError(
                "Gold accepted count disagrees with its frozen Silver inputs"
            )
        for name in ("warnings", "rejection_reasons", "duration_exclusions"):
            frequencies = quality[name]
            if not isinstance(frequencies, dict) or any(
                not isinstance(code, str) or type(count) is not int or count < 0
                for code, count in frequencies.items()
            ):
                raise GoldError(
                    "Gold quality exclusions must retain valid reason mappings"
                )
        tables = {}
        for product, schema in PRODUCT_SCHEMAS.items():
            descriptor = manifest["products"][product]
            if (
                descriptor["key"]
                != f"gold/run={manifest['run_id']}/{product}/part.parquet"
            ):
                raise GoldError("Gold product reference points outside its run")
            content = read_bytes(root, descriptor["key"])
            if sha256(content).hexdigest() != descriptor["sha256"]:
                raise GoldError("Gold product checksum mismatch")
            if not re.fullmatch(r"[0-9a-f]{64}", descriptor["sql_sha256"]):
                raise GoldError("Gold SQL provenance must contain a SHA-256 digest")
            table = pq.ParquetFile(pa.BufferReader(content)).read()
            if (
                not table.schema.equals(schema)
                or type(descriptor["row_count"]) is not int
                or table.num_rows != descriptor["row_count"]
            ):
                raise GoldError(
                    "Gold product schema or row count disagrees with its manifest"
                )
            metadata = table.schema.metadata
            if (
                metadata[b"mode"].decode() != manifest["mode"]
                or metadata[b"product"].decode() != product
            ):
                raise GoldError("Gold product mode or name disagrees with its run")
            if (
                json.loads(metadata[b"covered_days"]) != manifest["covered_days"]
                or metadata[b"coverage"].decode() != manifest["coverage"]
            ):
                raise GoldError("Gold product coverage disagrees with its run")
            tables[product] = table
        if (
            sum(tables["demand_daily"]["request_count"].to_pylist()) != counts[1]
            or sum(tables["channel_usage"]["request_count"].to_pylist()) != counts[1]
        ):
            raise GoldError(
                "Published Gold population totals disagree with accepted Silver"
            )
        return tables
    except (KeyError, TypeError, ValueError, OSError, pa.ArrowException):
        raise GoldError(
            "Gold publication metadata or artifacts are invalid or unavailable"
        ) from None


def publish_gold(
    base: Path,
    run_id: str,
    *,
    mode: Mode = "sample",
    days: Sequence[date] | None = None,
    sql_directory: Path | None = None,
) -> GoldManifest:
    """Publish one immutable four-product run; retry failed work with a new run_id."""
    key = _run_key(run_id)
    root = output_root(base, mode)
    if (
        object_path(root, key).exists()
        or object_path(root, f"gold/run={run_id}").exists()
    ):
        raise FileExistsError("Gold run already exists; use a new run identifier")
    inputs = load_gold_inputs(base, mode=mode, days=days)
    exclusions, observations = _provenance(root, inputs)
    directory = DEFAULT_SQL_DIRECTORY if sql_directory is None else sql_directory
    # Execute exactly the SQL texts whose hashes are recorded, even if files change later.
    queries = {
        product: (directory / f"{product}.sql").read_text(encoding="utf-8")
        for product in PRODUCT_SCHEMAS
    }
    manifest: GoldManifest = {
        "kind": "gold",
        "mode": mode,
        "run_id": run_id,
        "start_date": COHORT_START.isoformat(),
        "end_date": COHORT_END.isoformat(),
        "covered_days": [day.isoformat() for day in inputs["days"]],
        "coverage": "sample" if mode == "sample" else "full_month",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "transformation_version": GOLD_VERSION,
        "input_partitions": inputs["silver_manifests"],
        "observations_by_day": observations,
        "observation_min_utc": min(
            _utc(value["min_utc"]) for value in observations.values()
        ).isoformat(),
        "observation_max_utc": max(
            _utc(value["max_utc"]) for value in observations.values()
        ).isoformat(),
        "quality_exclusions": exclusions,
        "products": {},
        "status": "incomplete",
    }
    write_json_atomic(root, key, manifest)
    try:
        for name, query in queries.items():
            product = cast(Product, name)
            result = _compute_query(inputs["table"], product, query)
            path = _write_prepared_product(root, inputs, product, run_id, result)
            manifest["products"][product] = {
                "key": path.relative_to(root).as_posix(),
                "row_count": result.num_rows,
                "sha256": sha256(path.read_bytes()).hexdigest(),
                "sql_sha256": sha256(query.encode()).hexdigest(),
            }
            write_json_atomic(root, key, manifest)
        _verify_run(root, manifest)
        manifest["generated_at_utc"] = datetime.now(UTC).isoformat()
        manifest["status"] = "complete"
        write_json_atomic(root, key, manifest)
    except Exception as error:
        manifest["status"] = "failed"
        manifest["error"] = type(error).__name__
        try:
            write_json_atomic(root, key, manifest)
        except OSError:
            pass
        raise
    return manifest


def load_gold(
    base: Path, run_id: str, *, mode: Mode = "sample"
) -> tuple[GoldManifest, dict[str, pa.Table]]:
    """Read a historical complete run without depending on current mutable Silver."""
    root = output_root(base, mode)
    key = _run_key(run_id)
    try:
        manifest = cast(GoldManifest, read_json(root, key))
        if (
            manifest["kind"] != "gold"
            or manifest["mode"] != mode
            or key != manifest_key(manifest)
        ):
            raise GoldError("Gold run reference or mode does not match")
        if manifest["status"] != "complete":
            raise GoldError("Only complete four-product Gold runs can be consumed")
    except (KeyError, TypeError, ValueError, OSError):
        raise GoldError("Gold manifest is invalid or unavailable") from None
    return manifest, _verify_run(root, manifest)
