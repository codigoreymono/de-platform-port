"""JSON-native contracts only; completion and publication belong to later stages.

Date boundaries are local ISO dates; technical timestamps are ISO UTC strings.
File references are relative storage keys, never credentials or request headers.
"""

from typing import Literal, NotRequired, TypedDict

from nyc311.config import Mode

type CompletionStatus = Literal["incomplete", "complete", "failed"]


class BronzePage(TypedDict):
    key: str
    row_count: int
    observed_at_utc: str
    last_cursor: str | None
    sha256: str


class BronzeManifest(TypedDict):
    kind: Literal["bronze"]
    mode: Mode
    run_id: str
    start_date: str
    end_date: str
    dataset_id: str
    selected_fields: list[str]
    order: str
    page_size: int
    pages: list[BronzePage]
    record_count: int
    source_count_before: int | None
    source_count_after: int | None
    status: CompletionStatus
    error: NotRequired[str]
    software_version: NotRequired[str]


class SilverManifest(TypedDict):
    kind: Literal["silver"]
    mode: Mode
    run_id: str
    start_date: str
    end_date: str
    source_manifest_key: str
    partition_key: str
    quality_report_key: str
    row_count: int
    schema_version: str
    transformation_version: str
    status: CompletionStatus


class QualityReport(TypedDict):
    kind: Literal["quality"]
    mode: Mode
    run_id: str
    start_date: str
    end_date: str
    received: int
    accepted: int
    quarantined: int
    identical_duplicates_discarded: int
    warnings: dict[str, int]
    rejection_reasons: dict[str, int]


class RunManifest(TypedDict):
    kind: Literal["run"]
    mode: Mode
    run_id: str
    start_date: str
    end_date: str
    status: Literal["running", "succeeded", "failed"]
    started_at_utc: str
    finished_at_utc: str | None
    completed_days: list[str]
    output_keys: list[str]
    error: str | None


type Manifest = BronzeManifest | SilverManifest | QualityReport | RunManifest
