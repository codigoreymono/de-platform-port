import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest

from nyc311.config import execution_config
from nyc311.source import ENDPOINT, SELECTED_FIELDS, ExtractionError, extract_day

DAY = date(2026, 9, 1)
SAMPLES = Path(__file__).resolve().parents[1] / "data" / "samples"


def client_for_pages(
    pages: list[object], requests: list[httpx.Request]
) -> httpx.Client:
    responses = iter(pages)
    count = sum(len(page) for page in pages if isinstance(page, list))

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.params["$select"] == "count(*) as n":
            return httpx.Response(200, json=[{"n": str(count)}])
        requests.append(request)
        return httpx.Response(200, json=next(responses))

    return httpx.Client(transport=httpx.MockTransport(respond), trust_env=False)


def test_daily_pagination_preserves_original_pages() -> None:
    payloads = [
        json.loads((SAMPLES / name).read_bytes())
        for name in ("pagination-001.json", "pagination-002.json")
    ]
    requests = []
    with client_for_pages(payloads, requests) as client:
        result = extract_day(client, DAY, execution_config(page_size=2))
    assert result["record_count"] == 3
    assert result["start_date"] == "2026-09-01"
    assert result["end_date"] == "2026-09-02"
    assert [json.loads(page["content"]) for page in result["pages"]] == payloads
    assert [page["row_count"] for page in result["pages"]] == [2, 1]
    assert result["pages"][-1]["last_cursor"] == "sample-0003"
    for page in result["pages"]:
        assert datetime.fromisoformat(page["observed_at_utc"]).tzinfo == UTC
    first = requests[0].url.params
    assert first["$select"] == ",".join(SELECTED_FIELDS)
    assert first["$order"] == "unique_key ASC"
    assert first["$limit"] == "2"
    assert first["$where"] == (
        "created_date >= '2026-09-01T00:00:00' AND created_date < '2026-09-02T00:00:00'"
    )
    assert requests[1].url.params["$where"] == (
        first["$where"] + " AND unique_key > 'sample-0002'"
    )
    assert all(str(request.url).startswith(ENDPOINT) for request in requests)
    assert all("$offset" not in request.url.params for request in requests)


def test_exact_page_size_requires_terminal_empty_page() -> None:
    requests = []
    with client_for_pages(
        [[{"unique_key": "a"}, {"unique_key": "b"}], []], requests
    ) as client:
        result = extract_day(client, DAY, execution_config(page_size=2))
    assert len(requests) == 2
    assert [page["row_count"] for page in result["pages"]] == [2, 0]
    assert result["record_count"] == 2


def test_cursor_uses_text_not_numeric_order() -> None:
    requests = []
    with client_for_pages(
        [[{"unique_key": "10"}, {"unique_key": "2"}], []], requests
    ) as client:
        result = extract_day(client, DAY, execution_config(page_size=2))
    assert result["record_count"] == 2
    assert "unique_key > '2'" in requests[1].url.params["$where"]


def test_cursor_literal_is_escaped() -> None:
    requests = []
    with client_for_pages([[{"unique_key": "a'b"}], []], requests) as client:
        extract_day(client, DAY, execution_config(page_size=1))
    assert "unique_key > 'a''b'" in requests[1].url.params["$where"]


@pytest.mark.parametrize(
    "pages",
    [
        [[{"unique_key": "b"}, {"unique_key": "a"}]],
        [[{"unique_key": "a"}, {"unique_key": "a"}]],
        [
            [
                {"unique_key": "a", "status": "Open"},
                {"unique_key": "a", "status": "Closed"},
            ]
        ],
        [[{"unique_key": "a"}], [{"unique_key": "a"}]],
        [[{"unique_key": "b"}], [{"unique_key": "a"}]],
    ],
)
def test_nonadvancing_or_conflicting_identifiers_fail(pages: list[object]) -> None:
    requests = []
    size = 1 if len(pages) > 1 else 2
    with client_for_pages(pages, requests) as client:
        with pytest.raises(ExtractionError, match="strict text order"):
            extract_day(client, DAY, execution_config(page_size=size))


@pytest.mark.parametrize("key", [None, 1, True, "", " "])
def test_invalid_cursor_identifier(key: object) -> None:
    with client_for_pages([[{"unique_key": key}]], []) as client:
        with pytest.raises(ExtractionError, match="nonempty text"):
            extract_day(client, DAY)


@pytest.mark.parametrize("payload", [{"error": "bad"}, None, ["not an object"]])
def test_invalid_page_envelope(payload: object) -> None:
    with client_for_pages([payload], []) as client:
        with pytest.raises(ExtractionError):
            extract_day(client, DAY)


def test_oversized_page_fails() -> None:
    with client_for_pages([[{"unique_key": "a"}, {"unique_key": "b"}]], []) as client:
        with pytest.raises(ExtractionError, match="page limit"):
            extract_day(client, DAY, execution_config(page_size=1))


def test_invalid_json_fails() -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, content=b"not-json")
    )
    with httpx.Client(transport=transport) as client:
        with pytest.raises(ExtractionError, match="invalid JSON"):
            extract_day(client, DAY)


def test_last_september_day_uses_october_exclusive_boundary() -> None:
    requests = []
    with client_for_pages([[]], requests) as client:
        extract_day(client, date(2026, 9, 30))
    assert "created_date < '2026-10-01T00:00:00'" in requests[0].url.params["$where"]


@pytest.mark.parametrize(
    "day", [date(2026, 8, 31), date(2026, 10, 1), datetime(2026, 9, 1)]
)
def test_invalid_day_makes_no_request(day: date) -> None:
    requests = []
    with client_for_pages([], requests) as client:
        with pytest.raises(ValueError):
            extract_day(client, day)
    assert requests == []


def test_client_can_be_reused_for_multiple_days() -> None:
    requests = []
    with client_for_pages([[], []], requests) as client:
        extract_day(client, DAY)
        extract_day(client, date(2026, 9, 2))
        assert not client.is_closed
    assert "2026-09-02T00:00:00" in requests[-1].url.params["$where"]


def client_for_counts(
    counts: list[object], pages: list[object], requests: list[httpx.Request]
) -> httpx.Client:
    count_payloads = iter(counts)
    page_payloads = iter(pages)

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        values = (
            count_payloads
            if request.url.params["$select"] == "count(*) as n"
            else page_payloads
        )
        return httpx.Response(200, json=next(values))

    return httpx.Client(transport=httpx.MockTransport(respond), trust_env=False)


def test_count_checks_use_original_daily_window() -> None:
    requests = []
    counts = [[{"n": "2"}], [{"n": 2}]]
    with client_for_counts(
        counts, [[{"unique_key": "a"}, {"unique_key": "b"}], []], requests
    ) as client:
        result = extract_day(client, DAY, execution_config(page_size=2))
    assert result["complete"] is True
    assert result["source_count_before"] == result["source_count_after"] == 2
    count_requests = [
        request
        for request in requests
        if request.url.params["$select"] == "count(*) as n"
    ]
    assert len(count_requests) == 2
    assert requests[0] is count_requests[0]
    assert requests[-1] is count_requests[-1]
    assert (
        count_requests[0].url.params["$where"] == count_requests[1].url.params["$where"]
    )
    assert "unique_key >" not in count_requests[-1].url.params["$where"]


@pytest.mark.parametrize(("before", "after", "rows"), [(2, 3, 2), (3, 3, 1), (1, 1, 0)])
def test_inconsistent_counts_block_completion(
    before: int, after: int, rows: int
) -> None:
    payload = [{"unique_key": f"key-{index}"} for index in range(rows)]
    with client_for_counts(
        [[{"n": str(before)}], [{"n": str(after)}]], [payload], []
    ) as client:
        with pytest.raises(
            ExtractionError, match=f"before={before}, after={after}, extracted={rows}"
        ):
            extract_day(client, DAY)


def test_previously_populated_day_cannot_become_unexplained_empty() -> None:
    requests = []
    with client_for_counts([[{"n": "0"}]], [], requests) as client:
        with pytest.raises(ExtractionError, match="Unexplained empty"):
            extract_day(client, DAY, previous_count=10)
    assert len(requests) == 1


def test_initial_verified_empty_day_can_complete() -> None:
    with client_for_counts([[{"n": "0"}], [{"n": "0"}]], [[]], []) as client:
        result = extract_day(client, DAY)
    assert result["complete"] is True
    assert result["record_count"] == 0
    assert result["source_count_before"] == result["source_count_after"] == 0


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {},
        [{"count": "1"}],
        [{"n": "1"}, {"n": "1"}],
        [{"n": -1}],
        [{"n": True}],
        [{"n": 1.5}],
        [{"n": "-1"}],
        [{"n": "1.5"}],
        [{"n": "NaN"}],
        [{"n": ""}],
    ],
)
def test_invalid_count_response(payload: object) -> None:
    requests = []
    with client_for_counts([payload], [], requests) as client:
        with pytest.raises(ExtractionError, match="nonnegative integer"):
            extract_day(client, DAY)
    assert len(requests) == 1


@pytest.mark.parametrize("value", [-1, True, "1", float("nan")])
def test_invalid_previous_count_makes_no_request(value: object) -> None:
    requests = []
    with client_for_counts([], [], requests) as client:
        with pytest.raises(ValueError, match="Previous count"):
            extract_day(client, DAY, previous_count=value)
    assert requests == []


def test_raw_json_bytes_are_not_normalized() -> None:
    content = b' [ { "unique_key" : "a", "status" : "Open" } ]\n'

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.params["$select"] == "count(*) as n":
            return httpx.Response(200, json=[{"n": "1"}])
        return httpx.Response(200, content=content)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        result = extract_day(client, DAY)
    assert result["pages"][0]["content"] == content
