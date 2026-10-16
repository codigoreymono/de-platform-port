"""Source metadata and pagination validation shared without importing HTTP libraries."""

from typing import Literal, TypedDict

DATASET_ID = "erm2-nwe9"
ENDPOINT = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"
SELECTED_FIELDS = (
    "unique_key",
    "created_date",
    "closed_date",
    "status",
    "complaint_type",
    "descriptor",
    "agency",
    "agency_name",
    "borough",
    "incident_zip",
    "open_data_channel_type",
    "resolution_action_updated_date",
    ":updated_at",
)


class ExtractionError(RuntimeError):
    """A source window could not be extracted safely."""


class SourcePage(TypedDict):
    content: bytes
    row_count: int
    observed_at_utc: str
    last_cursor: str | None


class DayExtraction(TypedDict):
    start_date: str
    end_date: str
    pages: list[SourcePage]
    record_count: int
    source_count_before: int
    source_count_after: int
    complete: Literal[True]


def validate_page(
    rows: object, page_size: int, cursor: str | None
) -> tuple[int, str | None]:
    if not isinstance(rows, list) or len(rows) > page_size:
        raise ExtractionError("Source page must be an array within the page limit")
    last_key = cursor
    for row in rows:
        if not isinstance(row, dict):
            raise ExtractionError("Source page must contain objects")
        key = row.get("unique_key")
        if not isinstance(key, str) or not key.strip():
            raise ExtractionError("Source cursor identifier must be nonempty text")
        if last_key is not None and key <= last_key:
            raise ExtractionError("Source cursor must advance in strict text order")
        last_key = key
    return len(rows), last_key
