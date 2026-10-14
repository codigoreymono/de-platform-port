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
    quarantine_key: str
    partition_sha256: str
    quality_report_sha256: str
    quarantine_sha256: str
    row_count: int
    schema_version: str
    transformation_version: str
    status: CompletionStatus
    error: NotRequired[str]


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
    source_manifest_key: str
    category_counts: dict[str, int]
    status_counts: dict[str, int]
    duration_exclusions: dict[str, int]


class QuarantineRecord(TypedDict):
    source_run_id: str
    source_page_key: str
    source_row_number: int
    original: dict[str, object]
    reason_codes: list[str]


class GoldProduct(TypedDict):
    key: str
    row_count: int
    sha256: str
    sql_sha256: str


class GoldManifest(TypedDict):
    kind: Literal["gold"]
    mode: Mode
    run_id: str
    start_date: str
    end_date: str
    covered_days: list[str]
    coverage: Literal["sample", "full_month"]
    generated_at_utc: str
    transformation_version: str
    input_partitions: list[SilverManifest]
    observations_by_day: dict[str, dict[str, str]]
    observation_min_utc: str
    observation_max_utc: str
    quality_exclusions: dict[str, object]
    products: dict[str, GoldProduct]
    status: CompletionStatus
    error: NotRequired[str]


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
    action: Literal["extract", "replay"]
    stage: Literal["bronze", "silver", "gold", "finished"]
    active_day: str | None
    bronze_keys: list[str]
    gold_days: list[str]
    gold_manifest_key: str | None


type Manifest = (
    BronzeManifest | SilverManifest | QualityReport | RunManifest | GoldManifest
)
