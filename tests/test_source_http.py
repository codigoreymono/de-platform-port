import gzip
import logging
import traceback
from collections.abc import Iterator
from datetime import UTC, date, datetime

import httpx
import pytest

from nyc311 import source
from nyc311.config import ExecutionConfig, execution_config
from nyc311.source import ExtractionError


@pytest.fixture(autouse=True)
def fake_time(monkeypatch: pytest.MonkeyPatch) -> tuple[list[float], list[float]]:
    now = [100.0]
    sleeps = []

    def advance(delay: float) -> None:
        sleeps.append(delay)
        now[0] += delay

    monkeypatch.setattr(source, "monotonic", lambda: now[0])
    monkeypatch.setattr(source, "sleep", advance)
    monkeypatch.setattr(source, "uniform", lambda lower, upper: upper)
    return now, sleeps


def client_for_events(
    events: list[int | Exception | httpx.Response], requests: list[httpx.Request]
) -> httpx.Client:
    pending = iter(events)

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        event = next(pending)
        if isinstance(event, Exception):
            raise event
        if isinstance(event, httpx.Response):
            return event
        return httpx.Response(event, json=[])

    return httpx.Client(transport=httpx.MockTransport(respond), trust_env=False)


def request(
    client: httpx.Client, config: ExecutionConfig | None = None
) -> httpx.Response:
    settings = config or execution_config()
    return source._request(
        client, {}, settings, source.monotonic() + settings["task_deadline"]
    )


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_http_status_is_retried(
    status: int, fake_time: tuple[list[float], list[float]]
) -> None:
    requests = []
    with client_for_events([status, 200], requests) as client:
        assert request(client).status_code == 200
    assert len(requests) == 2
    assert fake_time[1] == [1.0]


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError,
        httpx.ReadTimeout,
        httpx.WriteError,
        httpx.RemoteProtocolError,
    ],
)
def test_transient_transport_failure_is_retried(
    error: type[httpx.TransportError], fake_time: tuple[list[float], list[float]]
) -> None:
    requests = []
    with client_for_events([error("connection failed"), 200], requests) as client:
        assert request(client).status_code == 200
    assert len(requests) == 2
    assert fake_time[1] == [1.0]


def test_backoff_grows_and_is_capped(
    fake_time: tuple[list[float], list[float]],
) -> None:
    requests = []
    with client_for_events([503, 503, 503, 503, 200], requests) as client:
        request(client, execution_config(max_backoff=2))
    assert len(requests) == 5
    assert fake_time[1] == [1.0, 2.0, 2.0, 2.0]


def test_backoff_uses_jitter(
    monkeypatch: pytest.MonkeyPatch, fake_time: tuple[list[float], list[float]]
) -> None:
    monkeypatch.setattr(source, "uniform", lambda lower, upper: upper / 2)
    with client_for_events([503, 200], []) as client:
        request(client)
    assert fake_time[1] == [0.5]


@pytest.mark.parametrize("event", [503, httpx.ConnectError("failure")])
def test_exhaustion_stops_at_max_attempts(
    event: int | Exception, fake_time: tuple[list[float], list[float]]
) -> None:
    requests = []
    with client_for_events([event, event, event], requests) as client:
        with pytest.raises(ExtractionError, match="retry budget exhausted"):
            request(client, execution_config(max_attempts=3))
    assert len(requests) == 3
    assert fake_time[1] == [1.0, 2.0]


@pytest.mark.parametrize("status", [301, 400, 401, 403, 404, 422, 501])
def test_permanent_status_is_not_retried(
    status: int, fake_time: tuple[list[float], list[float]]
) -> None:
    requests = []
    with client_for_events([status], requests) as client:
        with pytest.raises(ExtractionError, match=str(status)):
            request(client)
    assert len(requests) == 1
    assert fake_time[1] == []


def test_local_protocol_failure_is_not_retried() -> None:
    requests = []
    with client_for_events(
        [httpx.LocalProtocolError("invalid header")], requests
    ) as client:
        with pytest.raises(ExtractionError, match="configuration or protocol"):
            request(client)
    assert len(requests) == 1


def test_retry_after_can_exceed_backoff_cap(
    fake_time: tuple[list[float], list[float]],
) -> None:
    response = httpx.Response(429, headers={"Retry-After": "45"})
    with client_for_events([response, 200], []) as client:
        request(client, execution_config(max_backoff=2))
    assert fake_time[1] == [45.0]


@pytest.mark.parametrize("value", ["-1", "nonsense", "NaN", "Infinity", "1.5"])
def test_invalid_retry_after_uses_backoff(
    value: str, fake_time: tuple[list[float], list[float]]
) -> None:
    with client_for_events(
        [httpx.Response(429, headers={"Retry-After": value}), 200], []
    ) as client:
        request(client)
    assert fake_time[1] == [1.0]


def test_retry_after_http_date() -> None:
    now = datetime(2026, 10, 13, 12, 0, 0, tzinfo=UTC)
    assert source._retry_after("Tue, 13 Oct 2026 12:00:05 GMT", now) == 5
    assert source._retry_after("Tue, 13 Oct 2026 11:59:55 GMT", now) == 0
    assert source._retry_after("0", now) == 0


def test_timeout_configuration_and_remaining_budget() -> None:
    requests = []
    settings = execution_config()
    with client_for_events([200, 200], requests) as client:
        request(client, settings)
        source._request(client, {}, settings, source.monotonic() + 2)
    assert requests[0].extensions["timeout"] == {
        "connect": 10.0,
        "read": 60.0,
        "write": 60.0,
        "pool": 10.0,
    }
    assert requests[1].extensions["timeout"] == {
        "connect": 2.0,
        "read": 2.0,
        "write": 2.0,
        "pool": 2.0,
    }


def test_retry_delay_cannot_exhaust_task_budget(
    fake_time: tuple[list[float], list[float]],
) -> None:
    requests = []
    with client_for_events(
        [httpx.Response(429, headers={"Retry-After": "1000"})], requests
    ) as client:
        with pytest.raises(ExtractionError, match="budget"):
            request(client)
    assert len(requests) == 1
    assert fake_time[1] == []


def test_expired_deadline_makes_no_request() -> None:
    requests = []
    with client_for_events([], requests) as client:
        with pytest.raises(ExtractionError, match="deadline"):
            source._request(client, {}, execution_config(), source.monotonic())
    assert requests == []


def test_success_after_deadline_is_not_accepted(
    fake_time: tuple[list[float], list[float]],
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        fake_time[0][0] += 721
        return httpx.Response(200, json=[])

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ExtractionError, match="deadline"):
            request(client)


def test_slow_body_is_stopped_between_chunks(
    fake_time: tuple[list[float], list[float]],
) -> None:
    chunks_read = []
    closed = []

    class SlowBody(httpx.SyncByteStream):
        def __iter__(self) -> Iterator[bytes]:
            for index in range(8):
                fake_time[0][0] += 2
                chunks_read.append(index)
                yield b" "

        def close(self) -> None:
            closed.append(True)

    transport = httpx.MockTransport(lambda sent: httpx.Response(200, stream=SlowBody()))
    with httpx.Client(transport=transport) as client:
        with pytest.raises(ExtractionError, match="deadline"):
            source._request(client, {}, execution_config(), source.monotonic() + 5)
    assert chunks_read == [0, 1, 2]
    assert closed == [True]
    assert fake_time[0][0] == 106


def test_streamed_compressed_json_keeps_decoded_response_bytes() -> None:
    content = b" [ ] \n"
    transport = httpx.MockTransport(
        lambda sent: httpx.Response(
            200,
            headers={"Content-Encoding": "gzip"},
            stream=httpx.ByteStream(gzip.compress(content)),
        )
    )
    with httpx.Client(transport=transport) as client:
        response = request(client)
    assert response.content == content
    assert response.json() == []


def test_slow_gzip_header_is_stopped_before_decoder_produces_bytes(
    fake_time: tuple[list[float], list[float]],
) -> None:
    consumed = []
    closed = []
    compressed = gzip.compress(b"[]")

    class SlowGzip(httpx.SyncByteStream):
        def __iter__(self) -> Iterator[bytes]:
            for index, value in enumerate(compressed):
                fake_time[0][0] += 2
                consumed.append(index)
                yield bytes([value])

        def close(self) -> None:
            closed.append(True)

    transport = httpx.MockTransport(
        lambda sent: httpx.Response(
            200, headers={"Content-Encoding": "gzip"}, stream=SlowGzip()
        )
    )
    with httpx.Client(transport=transport) as client:
        with pytest.raises(ExtractionError, match="deadline"):
            source._request(client, {}, execution_config(), source.monotonic() + 5)
    assert consumed == [0, 1, 2]
    assert closed == [True]


def test_daily_deadline_is_shared_by_count_and_page_requests(
    fake_time: tuple[list[float], list[float]],
) -> None:
    requests = []

    def respond(sent: httpx.Request) -> httpx.Response:
        requests.append(sent)
        fake_time[0][0] += 241
        if sent.url.params["$select"] == "count(*) as n":
            return httpx.Response(200, json=[{"n": "1"}])
        return httpx.Response(200, json=[{"unique_key": "a"}])

    with source.create_client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ExtractionError, match="deadline"):
            source.extract_day(client, date(2026, 9, 1), execution_config(page_size=1))
    assert len(requests) == 3


def test_retried_page_is_returned_only_once() -> None:
    events = iter(
        [
            (200, [{"n": "1"}]),
            (503, []),
            (200, [{"unique_key": "a"}]),
            (200, [{"n": "1"}]),
        ]
    )

    def respond(sent: httpx.Request) -> httpx.Response:
        status, payload = next(events)
        return httpx.Response(status, json=payload)

    with source.create_client(transport=httpx.MockTransport(respond)) as client:
        result = source.extract_day(client, date(2026, 9, 1))
    assert result["complete"] is True
    assert result["record_count"] == 1
    assert len(result["pages"]) == 1


def test_overslept_backoff_stops_before_next_request(
    monkeypatch: pytest.MonkeyPatch, fake_time: tuple[list[float], list[float]]
) -> None:
    monkeypatch.setattr(source, "sleep", lambda delay: fake_time[0].__setitem__(0, 900))
    requests = []
    with client_for_events([503], requests) as client:
        with pytest.raises(ExtractionError, match="deadline"):
            request(client)
    assert len(requests) == 1


@pytest.fixture(autouse=True)
def no_host_app_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NYC311_APP_TOKEN", raising=False)


@pytest.mark.parametrize(
    ("environment", "explicit", "expected"),
    [
        (None, None, None),
        ("environment-secret", None, "environment-secret"),
        ("environment-secret", "explicit-secret", "explicit-secret"),
        ("environment-secret", "", None),
    ],
)
def test_optional_token_is_only_a_header(
    monkeypatch: pytest.MonkeyPatch,
    environment: str | None,
    explicit: str | None,
    expected: str | None,
) -> None:
    if environment is not None:
        monkeypatch.setenv("NYC311_APP_TOKEN", environment)
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.params["$select"] == "count(*) as n":
            return httpx.Response(200, json=[{"n": "0"}])
        return httpx.Response(200, json=[])

    with source.create_client(
        app_token=explicit, transport=httpx.MockTransport(respond)
    ) as client:
        result = source.extract_day(client, date(2026, 9, 1))
    assert len(requests) == 3
    for sent in requests:
        assert sent.headers.get("X-App-Token") == expected
        assert sent.headers["Accept"] == "application/json"
        assert "app_token" not in str(sent.url)
        assert "secret" not in str(sent.url)
    assert "secret" not in repr(result)
    assert "headers" not in repr(result)


@pytest.mark.parametrize(
    "token", ["secret\nvalue", "secret\rvalue", "secret\x00", "secret\u00e9", 123]
)
def test_invalid_token_error_does_not_echo_input(token: str) -> None:
    with pytest.raises(ValueError) as error:
        source.create_client(app_token=token)
    assert "secret" not in str(error.value)


@pytest.mark.parametrize("kind", ["http", "network", "protocol", "json"])
def test_diagnostics_do_not_leak_tokens_or_error_bodies(
    kind: str, caplog: pytest.LogCaptureFixture
) -> None:
    secret = "sensitive-app-token-001"
    caplog.set_level(logging.DEBUG)

    def respond(request: httpx.Request) -> httpx.Response:
        if kind == "network":
            raise httpx.ConnectError(f"header contains {secret}", request=request)
        if kind == "protocol":
            raise httpx.LocalProtocolError(f"header contains {secret}", request=request)
        if kind == "json":
            return httpx.Response(200, content=f"invalid {secret}".encode())
        return httpx.Response(403, text=f"body echoes {secret}")

    settings = execution_config(max_attempts=2)
    with source.create_client(
        settings, app_token=secret, transport=httpx.MockTransport(respond)
    ) as client:
        with pytest.raises(ExtractionError) as error:
            source.extract_day(client, date(2026, 9, 1), settings)
    assert secret not in str(error.value)
    assert secret not in repr(error.value)
    assert secret not in "".join(traceback.format_exception(error.value))
    assert secret not in caplog.text


def test_retry_and_completion_logs_are_bounded_metadata(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="nyc311.source")
    events = iter([503, 200, 200, 200])

    def respond(request: httpx.Request) -> httpx.Response:
        status = next(events)
        payload = (
            [{"n": "0"}] if request.url.params["$select"] == "count(*) as n" else []
        )
        return httpx.Response(status, json=payload)

    with source.create_client(transport=httpx.MockTransport(respond)) as client:
        source.extract_day(client, date(2026, 9, 1))
    assert "retry attempt=1 reason=HTTP status 503" in caplog.text
    assert "complete day=2026-09-01 records=0 pages=1" in caplog.text


def test_client_does_not_forward_token_on_redirect() -> None:
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(302, headers={"Location": "https://other.invalid/"})

    with source.create_client(
        app_token="secret", transport=httpx.MockTransport(respond)
    ) as client:
        with pytest.raises(ExtractionError, match="302"):
            source.extract_day(client, date(2026, 9, 1))
    assert len(requests) == 1
    assert requests[0].url.host == "data.cityofnewyork.us"
