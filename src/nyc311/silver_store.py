"""Single-writer local Silver publication and manifest-gated consumption."""

import gzip
import json
import zlib
from contextlib import ExitStack
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from nyc311.config import COHORT_END, Mode, validate_window
from nyc311.manifests import SilverManifest
from nyc311.silver_contract import (
    SCHEMA_VERSION,
    SILVER_SCHEMA,
    TRANSFORMATION_VERSION,
    SilverError,
)
from nyc311.storage import (
    decode_json,
    manifest_key,
    object_path,
    output_root,
    read_bytes,
    read_json,
    temporary_object,
    write_json_atomic,
)


def _artifact_keys(day: str, run_id: str) -> tuple[str, str, str]:
    return (
        f"silver/created_date={day}/part.parquet",
        f"quality/run={run_id}/{day}.json",
        f"quarantine/run={run_id}/{day}.json.gz",
    )


def _validate_table(table: pa.Table, day: date, row_count: int) -> None:
    if not table.schema.equals(SILVER_SCHEMA, check_metadata=True):
        raise SilverError("Published Parquet does not match the Silver schema")
    if type(row_count) is not int or row_count < 0 or table.num_rows != row_count:
        raise SilverError("Published Parquet row count disagrees with its manifest")
    if any(
        table[field.name].null_count for field in SILVER_SCHEMA if not field.nullable
    ):
        raise SilverError("Published Silver has null required values")
    identifiers = table["unique_key"].to_pylist()
    if len(set(identifiers)) != row_count:
        raise SilverError("Published Silver has duplicate identifiers")
    if any(value != day for value in table["created_date"].to_pylist()):
        raise SilverError("Published Silver has records outside its creation partition")


def _validate_quality(
    report: dict[str, object], quarantine: object, manifest: SilverManifest
) -> None:
    if (
        report["kind"] != "quality"
        or report["mode"] != manifest["mode"]
        or report["run_id"] != manifest["run_id"]
        or report["start_date"] != manifest["start_date"]
        or report["end_date"] != manifest["end_date"]
        or report["source_manifest_key"] != manifest["source_manifest_key"]
    ):
        raise SilverError("Silver quality report disagrees with publication provenance")
    counts = [
        report[name]
        for name in (
            "received",
            "accepted",
            "quarantined",
            "identical_duplicates_discarded",
        )
    ]
    if any(type(count) is not int or count < 0 for count in counts):
        raise SilverError("Silver quality counts must be nonnegative integers")
    if (
        counts[0] != sum(counts[1:])
        or counts[1] != manifest["row_count"]
        or not isinstance(quarantine, list)
        or len(quarantine) != counts[2]
        or not all(isinstance(record, dict) for record in quarantine)
    ):
        raise SilverError("Silver quality and quarantine counts do not reconcile")


def _read_partition(root: Path, manifest: SilverManifest) -> pa.Table:
    """Verify all referenced artifacts; completion status is gated by the caller."""
    try:
        key = manifest_key(manifest)
        day = date.fromisoformat(manifest["start_date"])
        expected = _artifact_keys(day.isoformat(), manifest["run_id"])
        actual = (
            manifest["partition_key"],
            manifest["quality_report_key"],
            manifest["quarantine_key"],
        )
        if actual != expected or manifest["kind"] != "silver":
            raise SilverError(
                "Silver artifact references disagree with the daily layout"
            )
        if (
            manifest["schema_version"] != SCHEMA_VERSION
            or manifest["transformation_version"] != TRANSFORMATION_VERSION
        ):
            raise SilverError("Unsupported Silver schema or transformation version")
        contents = []
        for artifact, digest in zip(
            actual,
            (
                manifest["partition_sha256"],
                manifest["quality_report_sha256"],
                manifest["quarantine_sha256"],
            ),
        ):
            content = read_bytes(root, artifact)
            if sha256(content).hexdigest() != digest:
                raise SilverError("Silver artifact checksum mismatch")
            contents.append(content)
        # ParquetFile avoids Hive inference conflicting with the physical date column.
        table = pq.ParquetFile(pa.BufferReader(contents[0])).read()
        _validate_table(table, day, manifest["row_count"])
        report = decode_json(contents[1])
        quarantine = json.loads(gzip.decompress(contents[2]))
        _validate_quality(report, quarantine, manifest)
        if key != f"control/silver/{day.isoformat()}.json":
            raise SilverError("Silver manifest does not identify a daily partition")
        return table
    except (
        KeyError,
        TypeError,
        ValueError,
        OSError,
        EOFError,
        zlib.error,
        pa.ArrowException,
    ):
        raise SilverError(
            "Silver publication artifacts are invalid or unavailable"
        ) from None


def publish_silver(
    base: Path, bronze_key: str, *, mode: Mode = "sample", run_id: str | None = None
) -> SilverManifest:
    """Prepare first, invalidate the marker, replace artifacts, then mark complete.

    On failure after invalidation, retry this function with complete saved Bronze.
    It never restores an old completion marker over potentially replaced data.
    """
    from nyc311.silver import transform_bronze

    curated = transform_bronze(base, bronze_key, mode=mode, run_id=run_id)
    report = curated["quality_report"]
    day = date.fromisoformat(report["start_date"])
    _validate_table(curated["table"], day, report["accepted"])
    partition_key, report_key, quarantine_key = _artifact_keys(
        day.isoformat(), report["run_id"]
    )
    manifest: SilverManifest = {
        "kind": "silver",
        "mode": mode,
        "run_id": report["run_id"],
        "start_date": report["start_date"],
        "end_date": report["end_date"],
        "source_manifest_key": report["source_manifest_key"],
        "partition_key": partition_key,
        "quality_report_key": report_key,
        "quarantine_key": quarantine_key,
        "partition_sha256": "",
        "quality_report_sha256": "",
        "quarantine_sha256": "",
        "row_count": curated["table"].num_rows,
        "schema_version": SCHEMA_VERSION,
        "transformation_version": TRANSFORMATION_VERSION,
        "status": "incomplete",
    }
    key = manifest_key(manifest)
    root = output_root(base, mode)
    with ExitStack() as stack:
        staged = [
            stack.enter_context(temporary_object(root, artifact))
            for artifact in (partition_key, report_key, quarantine_key)
        ]
        pq.write_table(curated["table"], staged[0], compression="snappy")
        restored = pq.ParquetFile(staged[0]).read()
        _validate_table(restored, day, manifest["row_count"])
        if not restored.equals(curated["table"], check_metadata=True):
            raise SilverError(
                "Prepared Parquet does not preserve the transformed values"
            )
        staged[1].write_bytes(
            (json.dumps(report, indent=2, allow_nan=False) + "\n").encode()
        )
        staged[2].write_bytes(
            gzip.compress(
                json.dumps(curated["quarantine"], allow_nan=False).encode(), mtime=0
            )
        )
        _validate_quality(
            decode_json(staged[1].read_bytes()),
            json.loads(gzip.decompress(staged[2].read_bytes())),
            manifest,
        )
        for path, field in zip(
            staged, ("partition_sha256", "quality_report_sha256", "quarantine_sha256")
        ):
            manifest[field] = sha256(path.read_bytes()).hexdigest()
        # If invalidation fails, no existing data or auxiliary artifact is replaced.
        write_json_atomic(root, key, manifest)
        try:
            # Auxiliary files are also replaced only after invalidating the old marker.
            staged[1].replace(object_path(root, report_key))
            staged[2].replace(object_path(root, quarantine_key))
            staged[0].replace(object_path(root, partition_key))
            _read_partition(root, manifest)
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


def load_silver(
    base: Path, day: date, *, mode: Mode = "sample"
) -> tuple[SilverManifest, pa.Table]:
    """Read a complete daily publication, never an unmarked Parquet file."""
    validate_window(day, COHORT_END)
    root = output_root(base, mode)
    key = f"control/silver/{day.isoformat()}.json"
    try:
        manifest = cast(SilverManifest, read_json(root, key))
        if manifest["kind"] != "silver" or manifest["mode"] != mode:
            raise SilverError("Silver kind or storage mode does not match")
        if manifest["status"] != "complete":
            raise SilverError("Only complete Silver partitions can be consumed")
        if key != manifest_key(manifest):
            raise SilverError("Silver manifest reference disagrees with its provenance")
    except (KeyError, TypeError, ValueError, OSError):
        raise SilverError("Silver manifest is invalid or unavailable") from None
    return manifest, _read_partition(root, manifest)
