"""Local Bronze pages and extraction manifests, without Silver processing."""

from __future__ import annotations

import gzip
import json
import zlib
from collections.abc import Callable
from datetime import date, datetime, timedelta
from hashlib import sha256
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Literal, cast

from nyc311.config import (
    COHORT_END,
    ExecutionConfig,
    Mode,
    execution_config,
    validate_window,
)
from nyc311.manifests import BronzeManifest, BronzePage
from nyc311.source_contract import (
    DATASET_ID,
    SELECTED_FIELDS,
    DayExtraction,
    ExtractionError,
    SourcePage,
    validate_page,
)
from nyc311.storage import (
    manifest_key,
    object_path,
    output_root,
    read_bytes,
    read_json,
    write_bytes,
    write_json,
)

if TYPE_CHECKING:
    import httpx


class BronzeError(RuntimeError):
    """Saved Bronze is not safe to consume or declare complete."""


def _page_key(manifest: BronzeManifest, number: int) -> str:
    return (
        f"bronze/run={manifest['run_id']}/created_date={manifest['start_date']}/"
        f"page-{number:04d}.json.gz"
    )


def _write_manifest(root: Path, key: str, manifest: BronzeManifest) -> None:
    """Replace only Bronze metadata, keeping the prior diagnostic on write failure."""
    path = object_path(root, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with NamedTemporaryFile(
            dir=path.parent, suffix=".tmp", delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
        write_json(root, temporary_path.relative_to(root).as_posix(), manifest)
        temporary_path.replace(path)
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass


def _read_pages(root: Path, manifest: BronzeManifest) -> list[SourcePage]:
    """Verify files and extraction evidence without normalizing source records."""
    try:
        manifest_key(manifest)
        if (
            manifest["kind"] != "bronze"
            or manifest["dataset_id"] != DATASET_ID
            or manifest["selected_fields"] != list(SELECTED_FIELDS)
            or manifest["order"] != "unique_key ASC"
            or type(manifest["page_size"]) is not int
            or manifest["page_size"] <= 0
        ):
            raise BronzeError("Invalid Bronze source selection or page size")
        counts = [
            manifest[name]
            for name in ("record_count", "source_count_before", "source_count_after")
        ]
        if (
            any(type(count) is not int or count < 0 for count in counts)
            or len(set(counts)) != 1
        ):
            raise BronzeError("Bronze source counts must agree")
        references = manifest["pages"]
        if not isinstance(references, list) or not references:
            raise BronzeError("Bronze must reference its original pages")
        pages: list[SourcePage] = []
        cursor = None
        for index, reference in enumerate(references, start=1):
            if reference["key"] != _page_key(manifest, index):
                raise BronzeError("Bronze page references must be unique and ordered")
            content = gzip.decompress(read_bytes(root, reference["key"]))
            if sha256(content).hexdigest() != reference["sha256"]:
                raise BronzeError("Bronze page checksum mismatch")
            rows = json.loads(content)
            actual_count, cursor = validate_page(rows, manifest["page_size"], cursor)
            row_count = reference["row_count"]
            if type(row_count) is not int or actual_count != row_count:
                raise BronzeError(
                    "Bronze page row counts or JSON structure are invalid"
                )
            if index < len(references) and row_count != manifest["page_size"]:
                raise BronzeError("Bronze contains a premature short page")
            if index == len(references) and row_count >= manifest["page_size"]:
                raise BronzeError("Bronze is missing its terminal page")
            if reference["last_cursor"] != cursor:
                raise BronzeError("Bronze cursor metadata disagrees with its page")
            observed = reference["observed_at_utc"]
            if datetime.fromisoformat(observed).utcoffset() != timedelta(0):
                raise BronzeError("Bronze observation timestamps must be UTC")
            pages.append(
                {
                    "content": content,
                    "row_count": row_count,
                    "observed_at_utc": observed,
                    "last_cursor": reference["last_cursor"],
                }
            )
        if sum(page["row_count"] for page in pages) != manifest["record_count"]:
            raise BronzeError("Bronze files disagree with the extraction count")
        return pages
    except (
        KeyError,
        TypeError,
        ValueError,
        OSError,
        EOFError,
        zlib.error,
        ExtractionError,
    ):
        raise BronzeError(
            "Bronze metadata or page files are invalid or unavailable"
        ) from None


def ingest_day(
    client: httpx.Client,
    day: date,
    base: Path,
    run_id: str,
    config: ExecutionConfig | None = None,
    *,
    previous_count: int | None = None,
    on_progress: Callable[[BronzeManifest], None] | None = None,
    absolute_deadline: float | None = None,
) -> BronzeManifest:
    """Persist observations as they arrive; every attempt uses a new run identifier."""
    from nyc311.source import extract_day

    validate_window(day, COHORT_END)
    settings = execution_config(**(config or {}))
    if previous_count is not None and (
        type(previous_count) is not int or previous_count < 0
    ):
        raise ValueError("Previous count must be a nonnegative integer")
    manifest: BronzeManifest = {
        "kind": "bronze",
        "mode": settings["mode"],
        "run_id": run_id,
        "start_date": day.isoformat(),
        "end_date": (day + timedelta(days=1)).isoformat(),
        "dataset_id": DATASET_ID,
        "selected_fields": list(SELECTED_FIELDS),
        "order": "unique_key ASC",
        "page_size": settings["page_size"],
        "pages": [],
        "record_count": 0,
        "source_count_before": None,
        "source_count_after": None,
        "status": "incomplete",
    }
    try:
        manifest["software_version"] = version("nyc311-data-platform")
    except PackageNotFoundError:
        pass
    key = manifest_key(manifest)
    root = output_root(base, settings["mode"])
    directory = object_path(root, _page_key(manifest, 1)).parent
    if object_path(root, key).exists() or directory.exists():
        raise FileExistsError("Bronze attempt already exists; use a new run identifier")
    _write_manifest(root, key, manifest)
    if on_progress is not None:
        on_progress(manifest)

    def save_page(page: SourcePage) -> None:
        reference: BronzePage = {
            "key": _page_key(manifest, len(manifest["pages"]) + 1),
            "row_count": page["row_count"],
            "observed_at_utc": page["observed_at_utc"],
            "last_cursor": page["last_cursor"],
            "sha256": sha256(page["content"]).hexdigest(),
        }
        manifest["pages"].append(reference)
        manifest["record_count"] += page["row_count"]
        # Record the expected file before writing; failures remain explicitly incomplete.
        _write_manifest(root, key, manifest)
        write_bytes(root, reference["key"], gzip.compress(page["content"], mtime=0))
        if on_progress is not None:
            on_progress(manifest)

    def save_count(position: Literal["before", "after"], count: int) -> None:
        if position == "before":
            manifest["source_count_before"] = count
        else:
            manifest["source_count_after"] = count
        _write_manifest(root, key, manifest)
        if on_progress is not None:
            on_progress(manifest)

    try:
        extract_day(
            client,
            day,
            settings,
            previous_count=previous_count,
            on_page=save_page,
            on_count=save_count,
            **(
                {"absolute_deadline": absolute_deadline}
                if absolute_deadline is not None
                else {}
            ),
        )
        _read_pages(root, manifest)
        manifest["status"] = "complete"
        _write_manifest(root, key, manifest)
        if on_progress is not None:
            on_progress(manifest)
    except Exception as error:
        manifest["status"] = "failed"
        # Exception messages can contain credentials or host paths; store only the type.
        manifest["error"] = type(error).__name__
        try:
            _write_manifest(root, key, manifest)
            if on_progress is not None:
                on_progress(manifest)
        except Exception:
            pass
        raise
    return manifest


def load_bronze(
    base: Path, key: str, *, mode: Mode = "sample"
) -> tuple[BronzeManifest, DayExtraction]:
    """Load an explicitly selected complete extraction with no HTTP or writes."""
    root = output_root(base, mode)
    try:
        manifest = cast(BronzeManifest, read_json(root, key))
        if manifest["kind"] != "bronze" or manifest["mode"] != mode:
            raise BronzeError("Bronze kind or storage mode does not match")
        if manifest["status"] != "complete":
            raise BronzeError("Only complete Bronze extractions can be replayed")
        if key != manifest_key(manifest):
            raise BronzeError("Bronze manifest reference disagrees with its provenance")
    except (KeyError, TypeError, ValueError, OSError):
        raise BronzeError("Bronze manifest is invalid or unavailable") from None
    pages = _read_pages(root, manifest)
    result: DayExtraction = {
        "start_date": manifest["start_date"],
        "end_date": manifest["end_date"],
        "pages": pages,
        "record_count": manifest["record_count"],
        "source_count_before": cast(int, manifest["source_count_before"]),
        "source_count_after": cast(int, manifest["source_count_after"]),
        "complete": True,
    }
    return manifest, result
