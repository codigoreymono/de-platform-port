"""Silver schema and versions, without importing the Pandas transformation stage."""

import pyarrow as pa

from nyc311.config import BUSINESS_TIMEZONE

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
