import json
import os
import socket
from collections import Counter
from pathlib import Path

import httpx
import pytest

SAMPLES = Path(__file__).resolve().parents[1] / "data" / "samples"


def sample_rows(name: str) -> list[dict[str, object]]:
    rows = json.loads((SAMPLES / name).read_text(encoding="utf-8"))
    assert isinstance(rows, list)
    assert all(isinstance(row, dict) for row in rows)
    return rows


def test_fixtures_are_small_and_synthetic() -> None:
    files = sorted(SAMPLES.glob("*.json"))
    assert len(files) == 6
    assert sum(path.stat().st_size for path in files) < 16_384
    rows = [row for path in files for row in sample_rows(path.name)]
    assert len(rows) == 27
    assert all(
        row["unique_key"] is None or row["unique_key"].startswith("sample-")
        for row in rows
    )
    assert all("incident_address" not in row for row in rows)


def test_cli_demo_fixture_contains_representative_channels_and_omissions() -> None:
    rows = sample_rows("gold-metrics.json")
    counts = Counter(row.get("open_data_channel_type") for row in rows)
    assert counts == {"ONLINE": 3, "PHONE": 3, "MOBILE": 3, None: 3}
    assert {
        row["unique_key"] for row in rows if "open_data_channel_type" not in row
    } == {"sample-gold-03", "sample-gold-07", "sample-gold-11"}


def test_pagination_fixture_layout() -> None:
    first = sample_rows("pagination-001.json")
    final = sample_rows("pagination-002.json")
    assert len(first) == 2
    assert len(final) == 1
    keys = [row["unique_key"] for row in first + final]
    assert keys == sorted(set(keys))
    assert all(row["created_date"].startswith("2026-09-01T") for row in first + final)
    assert "closed_date" not in first[0]
    assert first[1]["closed_date"] is None
    assert final[0]["closed_date"].startswith("2026-10-")


def test_update_fixture_keeps_identity_and_creation() -> None:
    original = sample_rows("pagination-001.json")[0]
    updated = sample_rows("updated-request.json")[0]
    assert original["unique_key"] == updated["unique_key"]
    assert original["created_date"] == updated["created_date"]
    assert original["status"] == "Open"
    assert updated["status"] == "Closed"
    assert updated["closed_date"].startswith("2026-10-")


def test_quality_fixture_cases() -> None:
    rows = sample_rows("quality-cases.json")
    by_key = {row["unique_key"]: row for row in rows}
    assert by_key["sample-null-fields"]["borough"] is None
    assert by_key["sample-null-fields"]["incident_zip"] == "00123"
    assert by_key["sample-unknown-status"]["status"] == "Synthetic Unmapped Status"
    assert None in by_key
    assert by_key["sample-invalid-date"]["created_date"] == "not-a-date"
    assert by_key["sample-negative-duration"]["closed_date"].startswith("2026-08-")
    assert by_key["sample-wrong-window"]["created_date"].startswith("2026-09-02T")
    assert (
        by_key["sample-ambiguous-closure"]["closed_date"] == "2026-11-01T01:30:00.000"
    )
    assert by_key["sample-future-closure"]["closed_date"].startswith("2026-12-")


def test_duplicate_fixture_cases() -> None:
    first, identical, conflict = sample_rows("duplicates.json")
    assert first == identical
    assert first["unique_key"] == conflict["unique_key"]
    assert first != conflict


def test_mock_http_transport_works_without_network() -> None:
    rows = sample_rows("pagination-001.json")
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=rows))
    with httpx.Client(transport=transport) as client:
        response = client.get("https://fixture.invalid/requests")
    assert response.json() == rows


def test_real_network_is_blocked() -> None:
    for name in (
        "getaddrinfo",
        "gethostbyname",
        "gethostbyname_ex",
        "gethostbyaddr",
        "getnameinfo",
    ):
        with pytest.raises(RuntimeError, match="offline tests"):
            getattr(socket, name)("fixture.invalid", 443)
    with pytest.raises(RuntimeError, match="offline tests"):
        socket.create_connection(("192.0.2.1", 443))
    with socket.socket() as connection:
        with pytest.raises(RuntimeError, match="offline tests"):
            connection.connect(("192.0.2.1", 443))
        with pytest.raises(RuntimeError, match="offline tests"):
            connection.connect_ex(("192.0.2.1", 443))
        with pytest.raises(RuntimeError, match="offline tests"):
            connection.sendto(b"blocked", ("192.0.2.1", 443))
        if hasattr(connection, "sendmsg"):
            with pytest.raises(RuntimeError, match="offline tests"):
                connection.sendmsg([b"blocked"], [], 0, ("192.0.2.1", 443))


def test_host_aws_configuration_is_not_used() -> None:
    assert "AWS_ACCESS_KEY_ID" not in os.environ
    assert "AWS_SECRET_ACCESS_KEY" not in os.environ
    assert "AWS_SESSION_TOKEN" not in os.environ
    assert "AWS_PROFILE" not in os.environ
    assert os.environ["AWS_EC2_METADATA_DISABLED"] == "true"
    assert not Path(os.environ["AWS_SHARED_CREDENTIALS_FILE"]).exists()
    assert not Path(os.environ["AWS_CONFIG_FILE"]).exists()
