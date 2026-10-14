import json
import os
import socket
from collections.abc import Callable
from datetime import date, timedelta
from hashlib import sha256
from pathlib import Path
from typing import NoReturn

import pytest

from nyc311.manifests import BronzeManifest
from nyc311.source import DATASET_ID, SELECTED_FIELDS, SourcePage


@pytest.fixture(autouse=True)
def offline_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Fail closed on real network use and avoid host AWS credential discovery."""

    def deny_network(*args: object, **kwargs: object) -> NoReturn:
        raise RuntimeError("Real network access is disabled in offline tests")

    for name in (
        "create_connection",
        "getaddrinfo",
        "gethostbyname",
        "gethostbyname_ex",
        "gethostbyaddr",
        "getnameinfo",
    ):
        monkeypatch.setattr(socket, name, deny_network)
    for name in ("connect", "connect_ex", "sendto"):
        monkeypatch.setattr(socket.socket, name, deny_network)
    if hasattr(socket.socket, "sendmsg"):
        monkeypatch.setattr(socket.socket, "sendmsg", deny_network)

    for name in list(os.environ):
        if name.startswith("AWS_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-aws-config"))


@pytest.fixture
def silver_input() -> Callable[..., tuple[BronzeManifest, list[SourcePage]]]:
    """In-memory fixture context; malformed records test defensive Silver rules."""

    def make(
        *groups: list[dict[str, object]],
        observed: str = "2026-10-13T12:00:00Z",
        day: date = date(2026, 9, 1),
    ) -> tuple[BronzeManifest, list[SourcePage]]:
        pages: list[SourcePage] = []
        references = []
        for index, rows in enumerate(groups, 1):
            content = json.dumps(rows, sort_keys=True, allow_nan=False).encode()
            cursor = rows[-1].get("unique_key") if rows else None
            pages.append(
                {
                    "content": content,
                    "row_count": len(rows),
                    "observed_at_utc": observed,
                    "last_cursor": cursor,
                }
            )
            references.append(
                {
                    "key": f"bronze/run=silver-fixture/created_date={day.isoformat()}/page-{index:04d}.json.gz",
                    "row_count": len(rows),
                    "observed_at_utc": observed,
                    "last_cursor": cursor,
                    "sha256": sha256(content).hexdigest(),
                }
            )
        count = sum(len(rows) for rows in groups)
        manifest: BronzeManifest = {
            "kind": "bronze",
            "mode": "sample",
            "run_id": "silver-fixture",
            "start_date": day.isoformat(),
            "end_date": (day + timedelta(days=1)).isoformat(),
            "dataset_id": DATASET_ID,
            "selected_fields": list(SELECTED_FIELDS),
            "order": "unique_key ASC",
            "page_size": max((len(rows) for rows in groups), default=0) + 1,
            "pages": references,
            "record_count": count,
            "source_count_before": count,
            "source_count_after": count,
            "status": "complete",
        }
        return manifest, pages

    return make
