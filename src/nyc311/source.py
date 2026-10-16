"""Daily NYC 311 extraction; no persistence or downstream transformations."""

import logging
import os
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from math import isfinite
from random import uniform
from time import monotonic, sleep
from typing import Literal

import httpx

from nyc311.config import COHORT_END, ExecutionConfig, execution_config, validate_window
from nyc311.source_contract import (
    DATASET_ID as DATASET_ID,
)
from nyc311.source_contract import (
    ENDPOINT,
    SELECTED_FIELDS,
    DayExtraction,
    ExtractionError,
    SourcePage,
)
from nyc311.source_contract import (
    validate_page as validate_page,
)

logger = logging.getLogger(__name__)


def create_client(
    config: ExecutionConfig | None = None,
    *,
    app_token: str | None = None,
    transport: httpx.BaseTransport | None = None,
) -> httpx.Client:
    """None reads NYC311_APP_TOKEN; an explicit empty string disables it.

    The caller closes this client, normally using it as a context manager.
    """
    settings = execution_config(**(config or {}))
    token = os.environ.get("NYC311_APP_TOKEN", "") if app_token is None else app_token
    if not isinstance(token, str) or any(
        ord(char) < 32 or ord(char) > 126 for char in token
    ):
        raise ValueError("App Token must be printable ASCII header text")
    headers = {"Accept": "application/json"}
    if token.strip():
        headers["X-App-Token"] = token.strip()
    return httpx.Client(
        headers=headers,
        timeout=httpx.Timeout(
            settings["read_timeout"],
            connect=settings["connect_timeout"],
            pool=settings["connect_timeout"],
        ),
        follow_redirects=False,
        trust_env=False,
        transport=transport,
    )


def _retry_after(value: str, now: datetime) -> float | None:
    value = value.strip()
    if value.isascii() and value.isdecimal():
        return float(value)
    try:
        retry_at = parsedate_to_datetime(value)
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        return max(0.0, (retry_at - now).total_seconds())
    except (ValueError, TypeError, OverflowError):
        return None


def _request(
    client: httpx.Client,
    params: dict[str, str],
    config: ExecutionConfig,
    deadline: float,
) -> httpx.Response:
    backoff = min(1.0, config["max_backoff"])
    for attempt in range(1, config["max_attempts"] + 1):
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise ExtractionError("Daily extraction deadline exceeded")
        timeout = httpx.Timeout(
            min(config["read_timeout"], remaining),
            connect=min(config["connect_timeout"], remaining),
            pool=min(config["connect_timeout"], remaining),
        )
        response = None
        try:
            with client.stream(
                "GET", ENDPOINT, params=params, timeout=timeout, follow_redirects=False
            ) as streamed:
                if streamed.is_success:
                    if streamed.is_stream_consumed:
                        # Preloaded responses (including MockTransport) are decoded.
                        content = streamed.content
                        headers = {
                            name: value
                            for name, value in streamed.headers.items()
                            if name.lower()
                            not in ("content-encoding", "content-length")
                        }
                    else:
                        raw = bytearray()
                        for chunk in streamed.iter_raw():
                            if monotonic() >= deadline:
                                raise ExtractionError(
                                    "Daily extraction deadline exceeded"
                                )
                            raw.extend(chunk)
                        content = bytes(raw)
                        headers = dict(streamed.headers)
                    response = httpx.Response(
                        streamed.status_code, headers=headers, content=content
                    )
                else:
                    response = streamed
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError):
            problem = "transport failure"
        except httpx.RequestError:
            raise ExtractionError(
                "Source request configuration or protocol failure"
            ) from None
        if monotonic() >= deadline:
            raise ExtractionError("Daily extraction deadline exceeded")
        if response is not None:
            if response.is_success:
                return response
            problem = f"HTTP status {response.status_code}"
            if response.status_code not in (429, 500, 502, 503, 504):
                raise ExtractionError(f"Source {problem}")
        if attempt == config["max_attempts"]:
            raise ExtractionError(f"Source {problem}; retry budget exhausted")
        delay = uniform(0.0, backoff)
        if response is not None:
            advised = _retry_after(
                response.headers.get("Retry-After", ""), datetime.now(UTC)
            )
            if advised is not None:
                delay = max(delay, advised)
        if delay >= deadline - monotonic():
            raise ExtractionError("Retry delay exceeds daily extraction budget")
        logger.warning(
            "NYC 311 retry attempt=%d reason=%s delay_seconds=%.3f",
            attempt,
            problem,
            delay,
        )
        sleep(delay)
        backoff = min(config["max_backoff"], backoff * 2)
    raise AssertionError("Validated retry budget must allow at least one attempt")


def _json(response: httpx.Response) -> object:
    try:
        return response.json()
    except (ValueError, UnicodeError):
        raise ExtractionError("Source returned invalid JSON") from None


def _source_count(
    client: httpx.Client, where: str, config: ExecutionConfig, deadline: float
) -> int:
    value = _json(
        _request(
            client, {"$select": "count(*) as n", "$where": where}, config, deadline
        )
    )
    if isinstance(value, list) and len(value) == 1 and isinstance(value[0], dict):
        count = value[0].get("n")
        if type(count) is int and count >= 0:
            return count
        if isinstance(count, str) and count.isascii() and count.isdecimal():
            try:
                return int(count)
            except ValueError:
                pass
    raise ExtractionError("Source count must be one nonnegative integer")


def extract_day(
    client: httpx.Client,
    day: date,
    config: ExecutionConfig | None = None,
    *,
    previous_count: int | None = None,
    on_page: Callable[[SourcePage], None] | None = None,
    on_count: Callable[[Literal["before", "after"], int], None] | None = None,
    absolute_deadline: float | None = None,
) -> DayExtraction:
    """Return original pages and optionally report observations as they arrive.

    Callback failures propagate without HTTP retries. The caller owns the client.
    """
    validate_window(day, COHORT_END)
    end = day + timedelta(days=1)
    settings = execution_config(**(config or {}))
    if previous_count is not None and (
        type(previous_count) is not int or previous_count < 0
    ):
        raise ValueError("Previous count must be a nonnegative integer")
    deadline = monotonic() + settings["task_deadline"]
    if absolute_deadline is not None:
        if not isfinite(absolute_deadline):
            raise ValueError("Absolute extraction deadline must be finite")
        deadline = min(deadline, absolute_deadline)
    where = (
        f"created_date >= '{day.isoformat()}T00:00:00' "
        f"AND created_date < '{end.isoformat()}T00:00:00'"
    )
    before = _source_count(client, where, settings, deadline)
    if on_count is not None:
        on_count("before", before)
    if previous_count and before == 0:
        raise ExtractionError(
            "Unexplained empty replacement of a previously populated day"
        )
    pages: list[SourcePage] = []
    cursor: str | None = None
    count = 0
    while True:
        page_where = where
        if cursor is not None:
            escaped = cursor.replace("'", "''")
            page_where += f" AND unique_key > '{escaped}'"
        response = _request(
            client,
            {
                "$select": ",".join(SELECTED_FIELDS),
                "$where": page_where,
                "$order": "unique_key ASC",
                "$limit": str(settings["page_size"]),
            },
            settings,
            deadline,
        )
        row_count, last_key = validate_page(
            _json(response), settings["page_size"], cursor
        )
        page: SourcePage = {
            "content": response.content,
            "row_count": row_count,
            "observed_at_utc": datetime.now(UTC).isoformat(),
            "last_cursor": last_key,
        }
        pages.append(page)
        if on_page is not None:
            on_page(page)
        count += row_count
        cursor = last_key
        if row_count < settings["page_size"]:
            break
    after = _source_count(client, where, settings, deadline)
    if on_count is not None:
        on_count("after", after)
    if before != after or count != before:
        raise ExtractionError(
            f"Inconsistent source counts: before={before}, after={after}, extracted={count}"
        )
    if monotonic() >= deadline:
        raise ExtractionError("Daily extraction deadline exceeded")
    logger.info(
        "NYC 311 complete day=%s records=%d pages=%d",
        day.isoformat(),
        count,
        len(pages),
    )
    return {
        "start_date": day.isoformat(),
        "end_date": end.isoformat(),
        "pages": pages,
        "record_count": count,
        "source_count_before": before,
        "source_count_after": after,
        "complete": True,
    }
