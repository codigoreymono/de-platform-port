"""In-memory Silver transformations. No partition publication or replacement."""

import json
import re
from collections import Counter
from copy import deepcopy
from datetime import date, datetime, timedelta
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import TypedDict

import pandas as pd
import pyarrow as pa

from nyc311.bronze import load_bronze
from nyc311.config import BUSINESS_TIMEZONE, Mode, validate_window
from nyc311.manifests import BronzeManifest, QualityReport, QuarantineRecord
from nyc311.source import SourcePage
from nyc311.storage import manifest_key

SCHEMA_VERSION = "1"
TRANSFORMATION_VERSION = "1"
TEXT_FIELDS = (
    "unique_key",
    "agency",
    "agency_name",
    "complaint_type",
    "descriptor",
    "borough",
    "incident_zip",
    "status",
    "open_data_channel_type",
)
BUSINESS_DATES = ("created_date", "closed_date", "resolution_action_updated_date")
BOROUGHS = {"MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"}
CHANNELS = {"PHONE", "ONLINE", "MOBILE", "OTHER", "UNKNOWN"}
OUTSTANDING_STATUSES = {"OPEN", "IN PROGRESS", "PENDING", "ASSIGNED", "STARTED"}
LOCAL_DATE_PATTERN = (
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}(?:[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?)?"
)

SILVER_SCHEMA = pa.schema(
    [pa.field(name, pa.string(), nullable=name != "unique_key") for name in TEXT_FIELDS]
    + [
        pa.field(f"{name}_local", pa.timestamp("us"), nullable=name != "created_date")
        for name in BUSINESS_DATES
    ]
    + [
        pa.field(f"{name}_utc", pa.timestamp("us"), nullable=name != "created_date")
        for name in BUSINESS_DATES
    ]
    + [
        pa.field("source_updated_at_utc", pa.timestamp("us")),
        pa.field("observed_at_utc", pa.timestamp("us"), nullable=False),
        pa.field("created_date", pa.date32(), nullable=False),
        pa.field("lifecycle_class", pa.string(), nullable=False),
        pa.field("duration_eligible", pa.bool_(), nullable=False),
        pa.field(
            "quality_flags", pa.list_(pa.field("element", pa.string())), nullable=False
        ),
        pa.field("source_run_id", pa.string(), nullable=False),
        pa.field("source_page_key", pa.string(), nullable=False),
        pa.field("source_row_number", pa.int64(), nullable=False),
        pa.field("transformation_version", pa.string(), nullable=False),
    ],
    metadata={
        b"schema_version": SCHEMA_VERSION.encode(),
        b"business_timezone": BUSINESS_TIMEZONE.encode(),
        b"utc_convention": b"UTC-normalized, timezone-free microsecond timestamps",
    },
)


class SilverError(RuntimeError):
    """A daily input has incompatible structure, provenance, or conflicting rows."""


class CuratedDay(TypedDict):
    table: pa.Table
    quarantine: list[QuarantineRecord]
    quality_report: QualityReport


def _text(values: pd.Series) -> pd.Series:
    cleaned = values.map(
        lambda value: value.strip() if isinstance(value, str) else pd.NA
    )
    return cleaned.astype("string").replace("", pd.NA)


def _nyc_instant(value: pd.Timestamp) -> pd.Timestamp | None:
    if pd.isna(value):
        return None
    try:
        localized = value.tz_localize(
            BUSINESS_TIMEZONE, ambiguous="NaT", nonexistent="NaT"
        )
        if pd.isna(localized):
            return None
        return localized.tz_convert("UTC").tz_localize(None).as_unit("us")
    except (OverflowError, ValueError, NotImplementedError):
        return None


def _local_dates(values: pd.Series) -> tuple[pd.Series, pd.Series]:
    text = _text(values)
    local = pd.to_datetime(
        text.where(text.str.fullmatch(LOCAL_DATE_PATTERN, na=False)),
        format="ISO8601",
        errors="coerce",
    ).dt.as_unit("us")
    local = local.where(local.dt.year.between(1, 9999))
    try:
        utc = (
            local.dt.tz_localize(BUSINESS_TIMEZONE, ambiguous="NaT", nonexistent="NaT")
            .dt.tz_convert("UTC")
            .dt.tz_localize(None)
            .dt.as_unit("us")
        )
    except (OverflowError, ValueError, NotImplementedError):
        # A single out-of-range conversion must not invalidate the other records.
        utc = pd.Series(
            [_nyc_instant(value) for value in local],
            index=local.index,
            dtype="datetime64[us]",
        )
    return local, utc


def _fixed_dates(values: pd.Series) -> pd.Series:
    text = _text(values)
    aware = text.str.contains(r"(?:Z|[+-][0-9]{2}:[0-9]{2})$", regex=True, na=False)
    utc = (
        pd.to_datetime(text.where(aware), format="ISO8601", errors="coerce", utc=True)
        .dt.tz_localize(None)
        .dt.as_unit("us")
    )
    return utc.where(utc.dt.year.between(1, 9999))


def normalize_page(
    rows: list[dict[str, object]],
    *,
    source_run_id: str,
    source_page_key: str,
    observed_at_utc: str,
) -> pd.DataFrame:
    """Normalize values and provenance; record acceptance is decided separately."""
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise SilverError("Source page must be a list of records")
    raw = pd.DataFrame(rows)
    if rows and not {"unique_key", "created_date"}.issubset(raw.columns):
        raise SilverError("Source page is missing required columns")
    if not isinstance(source_run_id, str) or not source_run_id.strip():
        raise SilverError("Source run identifier must be nonempty text")
    if not isinstance(source_page_key, str) or not source_page_key:
        raise SilverError("Source page reference must be nonempty text")
    key = PurePosixPath(source_page_key)
    if key.is_absolute() or ".." in key.parts or not key.parts:
        raise SilverError("Source page reference must be relative")
    try:
        observed = datetime.fromisoformat(observed_at_utc)
        if observed.utcoffset() != timedelta(0):
            raise ValueError
    except (ValueError, TypeError):
        raise SilverError("Observation timestamp must be explicitly UTC") from None
    frame = pd.DataFrame(index=raw.index)
    for name in TEXT_FIELDS:
        frame[name] = _text(raw.get(name, pd.Series(pd.NA, index=raw.index)))
    for name in ("agency", "borough", "status", "open_data_channel_type"):
        frame[name] = frame[name].str.upper()
    frame["borough"] = frame["borough"].where(
        frame["borough"].isin(BOROUGHS), "UNKNOWN"
    )
    for name in ("agency", "complaint_type", "status", "open_data_channel_type"):
        frame[name] = frame[name].fillna("UNKNOWN")
    zip_valid = frame["incident_zip"].str.fullmatch(r"[0-9]{5}", na=False)
    frame["incident_zip"] = frame["incident_zip"].where(zip_valid)
    for name in BUSINESS_DATES:
        values = raw.get(name, pd.Series(pd.NA, index=raw.index))
        frame[f"{name}_local"], frame[f"{name}_utc"] = _local_dates(values)
    frame["source_updated_at_utc"] = _fixed_dates(
        raw.get(":updated_at", pd.Series(pd.NA, index=raw.index))
    )
    frame["observed_at_utc"] = pd.Timestamp(observed).tz_localize(None).as_unit("us")
    frame["created_date"] = frame["created_date_local"].dt.date
    frame["lifecycle_class"] = "unknown"
    frame["duration_eligible"] = False
    frame["quality_flags"] = pd.Series(
        [[] for _ in rows], index=raw.index, dtype="object"
    )
    frame["source_run_id"] = source_run_id
    frame["source_page_key"] = source_page_key
    frame["source_row_number"] = range(1, len(rows) + 1)
    frame["transformation_version"] = TRANSFORMATION_VERSION
    return frame


def to_silver_table(frame: pd.DataFrame) -> pa.Table:
    table = pa.Table.from_pandas(
        frame, schema=SILVER_SCHEMA, preserve_index=False, safe=True
    )
    if any(
        table[field.name].null_count for field in SILVER_SCHEMA if not field.nullable
    ):
        raise SilverError("Required Silver columns cannot contain null values")
    return table.replace_schema_metadata(SILVER_SCHEMA.metadata)


def _present(value: object) -> bool:
    return value is not None and (not isinstance(value, str) or bool(value.strip()))


def _audit_page(
    frame: pd.DataFrame, rows: list[dict[str, object]], day: date
) -> list[list[str]]:
    """Annotate warnings and return critical rejection reasons, without discarding raw data."""
    flags = [[] for _ in rows]
    rejected = [[] for _ in rows]

    def mark(mask: pd.Series, code: str, *, critical: bool = False) -> None:
        target = rejected if critical else flags
        for index, applies in enumerate(mask.fillna(False).tolist()):
            if applies:
                target[index].append(code)

    mark(frame["unique_key"].isna(), "missing_or_invalid_identifier", critical=True)
    mark(frame["created_date_utc"].isna(), "invalid_created_date", critical=True)
    created = frame["created_date_local"]
    mark(
        (created < pd.Timestamp(day))
        | (created >= pd.Timestamp(day + timedelta(days=1))),
        "created_date_outside_window",
        critical=True,
    )
    frame["lifecycle_class"] = (
        frame["status"]
        .map(
            {
                "CLOSED": "closed",
                **{status: "outstanding" for status in OUTSTANDING_STATUSES},
            }
        )
        .fillna("unknown")
    )
    closed = frame["lifecycle_class"].eq("closed")
    mark(frame["lifecycle_class"].eq("unknown"), "unknown_status")
    mark(frame["borough"].eq("UNKNOWN"), "unknown_borough")
    mark(
        frame["open_data_channel_type"].eq("UNKNOWN")
        | ~frame["open_data_channel_type"].isin(CHANNELS),
        "unknown_channel",
    )
    for name in ("agency", "complaint_type"):
        mark(frame[name].eq("UNKNOWN"), f"missing_{name}")
    for name in TEXT_FIELDS:
        bad_type = pd.Series(
            [
                _present(row.get(name)) and not isinstance(row.get(name), str)
                for row in rows
            ],
            index=frame.index,
            dtype="bool",
        )
        if name != "unique_key":
            mark(bad_type, f"invalid_{name}_type")
    zip_present = pd.Series(
        [_present(row.get("incident_zip")) for row in rows],
        index=frame.index,
        dtype="bool",
    )
    mark(zip_present & frame["incident_zip"].isna(), "invalid_incident_zip")
    for name in ("closed_date", "resolution_action_updated_date"):
        supplied = pd.Series(
            [_present(row.get(name)) for row in rows], index=frame.index, dtype="bool"
        )
        mark(supplied & frame[f"{name}_local"].isna(), f"invalid_{name}")
        mark(
            frame[f"{name}_local"].notna() & frame[f"{name}_utc"].isna(),
            f"unresolvable_{name}",
        )
    updated = pd.Series(
        [_present(row.get(":updated_at")) for row in rows],
        index=frame.index,
        dtype="bool",
    )
    mark(updated & frame["source_updated_at_utc"].isna(), "invalid_source_updated_at")
    closed_supplied = pd.Series(
        [_present(row.get("closed_date")) for row in rows],
        index=frame.index,
        dtype="bool",
    )
    mark(closed & ~closed_supplied, "missing_closed_date")
    mark(~closed & frame["closed_date_local"].notna(), "closure_status_mismatch")
    mark(
        frame["closed_date_utc"] < frame["created_date_utc"],
        "negative_closure_duration",
    )
    mark(
        frame["closed_date_utc"] > frame["observed_at_utc"], "closed_after_observation"
    )
    mark(
        frame["created_date_utc"] > frame["observed_at_utc"],
        "created_after_observation",
    )
    frame["duration_eligible"] = (
        closed
        & frame["closed_date_utc"].notna()
        & frame["created_date_utc"].notna()
        & frame["closed_date_utc"].ge(frame["created_date_utc"])
        & frame["closed_date_utc"].le(frame["observed_at_utc"])
        & frame["created_date_utc"].le(frame["observed_at_utc"])
    )
    frame["quality_flags"] = pd.Series(flags, index=frame.index, dtype="object")
    return rejected


def _keep_unique(
    rows: list[dict[str, object]],
    identifiers: pd.Series,
    seen_rows: set[str],
    seen_identifiers: dict[str, str],
) -> list[bool]:
    """Keep the first occurrence only when original records are identical."""
    keep = []
    for record, identifier in zip(rows, identifiers):
        try:
            signature = json.dumps(
                record, sort_keys=True, separators=(",", ":"), allow_nan=False
            )
        except (TypeError, ValueError):
            raise SilverError(
                "Source record must contain JSON-compatible values"
            ) from None
        if pd.notna(identifier):
            previous = seen_identifiers.get(identifier)
            if previous is not None and previous != signature:
                raise SilverError("Conflicting records for one request identifier")
            seen_identifiers[identifier] = signature
        keep.append(signature not in seen_rows)
        seen_rows.add(signature)
    return keep


def transform_day(
    manifest: BronzeManifest, pages: list[SourcePage], *, run_id: str | None = None
) -> CuratedDay:
    """Pure transformation of complete inputs; fixtures can exercise defensive row rules.

    Use transform_bronze for persisted inputs so file integrity is verified first.
    No Silver, quarantine, or report files are published here.
    """
    if manifest["kind"] != "bronze" or manifest["status"] != "complete":
        raise SilverError("Only complete Bronze inputs can be transformed")
    manifest_key(manifest)
    processing_run = manifest["run_id"] if run_id is None else run_id
    if not isinstance(processing_run, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_-]*", processing_run
    ):
        raise SilverError("Processing run identifier must be a safe path component")
    if manifest["mode"] not in ("sample", "published"):
        raise SilverError("Invalid Bronze storage mode")
    day = date.fromisoformat(manifest["start_date"])
    end = date.fromisoformat(manifest["end_date"])
    validate_window(day, end)
    if (
        end - day != timedelta(days=1)
        or not pages
        or len(pages) != len(manifest["pages"])
    ):
        raise SilverError("Silver input must cover one complete day")
    counts = [
        manifest[name]
        for name in ("record_count", "source_count_before", "source_count_after")
    ]
    if (
        any(type(count) is not int or count < 0 for count in counts)
        or len(set(counts)) != 1
    ):
        raise SilverError("Bronze extraction counts must agree")
    accepted = []
    quarantine: list[QuarantineRecord] = []
    received = 0
    discarded = 0
    seen_rows: set[str] = set()
    seen_identifiers: dict[str, str] = {}
    for page, reference in zip(pages, manifest["pages"]):
        if (
            sha256(page["content"]).hexdigest() != reference["sha256"]
            or page["observed_at_utc"] != reference["observed_at_utc"]
            or page["row_count"] != reference["row_count"]
            or type(page["row_count"]) is not int
            or type(reference["row_count"]) is not int
            or page["row_count"] < 0
        ):
            raise SilverError(
                "Page content or observation disagrees with Bronze provenance"
            )
        try:
            rows = json.loads(page["content"])
        except (ValueError, UnicodeError):
            raise SilverError("Source page JSON is invalid") from None
        frame = normalize_page(
            rows,
            source_run_id=manifest["run_id"],
            source_page_key=reference["key"],
            observed_at_utc=page["observed_at_utc"],
        )
        if len(rows) != page["row_count"]:
            raise SilverError("Source page count is inconsistent")
        received += len(rows)
        reasons = _audit_page(frame, rows, day)
        keep = _keep_unique(rows, frame["unique_key"], seen_rows, seen_identifiers)
        discarded += keep.count(False)
        valid = [included and not codes for included, codes in zip(keep, reasons)]
        accepted.append(frame.loc[valid])
        for index, codes in enumerate(reasons):
            if codes and keep[index]:
                quarantine.append(
                    {
                        "source_run_id": manifest["run_id"],
                        "source_page_key": reference["key"],
                        "source_row_number": index + 1,
                        "original": deepcopy(rows[index]),
                        "reason_codes": codes,
                    }
                )
    if received != manifest["record_count"] or not accepted:
        raise SilverError("Received rows disagree with the complete Bronze input")
    frame = pd.concat(accepted, ignore_index=True)
    if (
        received != len(frame) + len(quarantine) + discarded
        or frame["unique_key"].duplicated().any()
    ):
        raise SilverError("Daily quality accounting or identifier uniqueness failed")
    report: QualityReport = {
        "kind": "quality",
        "mode": manifest["mode"],
        "run_id": processing_run,
        "start_date": manifest["start_date"],
        "end_date": manifest["end_date"],
        "received": received,
        "accepted": len(frame),
        "quarantined": len(quarantine),
        "identical_duplicates_discarded": discarded,
        "warnings": dict(
            sorted(
                Counter(
                    code for flags in frame["quality_flags"] for code in flags
                ).items()
            )
        ),
        "rejection_reasons": dict(
            sorted(
                Counter(
                    code for record in quarantine for code in record["reason_codes"]
                ).items()
            )
        ),
        "source_manifest_key": manifest_key(manifest),
        "category_counts": {
            key: int(value)
            for key, value in sorted(frame["complaint_type"].value_counts().items())
        },
        "status_counts": {
            key: int(value)
            for key, value in sorted(frame["status"].value_counts().items())
        },
        "duration_exclusions": {
            "not_closed": int(frame["lifecycle_class"].ne("closed").sum()),
            "closed_without_valid_duration": int(
                (
                    frame["lifecycle_class"].eq("closed") & ~frame["duration_eligible"]
                ).sum()
            ),
        },
    }
    return {
        "table": to_silver_table(frame),
        "quarantine": quarantine,
        "quality_report": report,
    }


def transform_bronze(
    base: Path, key: str, *, mode: Mode = "sample", run_id: str | None = None
) -> CuratedDay:
    manifest, extraction = load_bronze(base, key, mode=mode)
    return transform_day(manifest, extraction["pages"], run_id=run_id)
