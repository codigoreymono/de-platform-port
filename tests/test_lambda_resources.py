import io
import json
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import boto3
import httpx
import pytest
from botocore.config import Config
from test_lambda_stages import MemoryS3, daily_event

from nyc311 import (
    bronze,
    lambda_common,
    lambda_curate,
    lambda_gold,
    lambda_ingest,
    lambda_runtime,
    source,
)
from nyc311.cli import fixture_transport
from nyc311.config import execution_config
from nyc311.lambda_runtime import InvocationBudget, InvocationDeadlineExceeded
from nyc311.s3_store import S3Store, temporary_workspace
from nyc311.storage import output_root, read_json

STAGES = (lambda_ingest, lambda_curate, lambda_gold)
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def clock(monkeypatch):
    now = [100.0]

    def advance(delay):
        now[0] += delay

    monkeypatch.setattr(lambda_runtime, "monotonic", lambda: now[0])
    monkeypatch.setattr(source, "monotonic", lambda: now[0])
    monkeypatch.setattr(source, "sleep", advance)
    monkeypatch.setattr(source, "uniform", lambda lower, upper: upper)
    return now


@pytest.fixture
def aws(monkeypatch, tmp_path):
    client = MemoryS3()
    spaces = []

    @contextmanager
    def workspace():
        with temporary_workspace(tmp_path) as path:
            spaces.append(path)
            yield path

    monkeypatch.setattr(lambda_common, "create_s3_client", lambda: client)
    monkeypatch.setattr(lambda_gold, "SQL_DIRECTORY", ROOT / "sql")
    for module in STAGES:
        monkeypatch.setattr(module, "temporary_workspace", workspace)
    monkeypatch.setattr(
        lambda_ingest,
        "create_client",
        lambda config: source.create_client(
            config,
            app_token="",
            transport=fixture_transport(ROOT / "data/samples/gold-metrics.json"),
        ),
    )
    yield client
    assert spaces
    assert all(not path.exists() for path in spaces)


@pytest.mark.parametrize(
    ("stage", "memory", "cpus"),
    [("ingest", 512, 0.25), ("curate", 1024, 0.5), ("gold", 2048, 1.0)],
)
def test_approved_resource_profiles(stage, memory, cpus):
    assert lambda_runtime.PROFILES[stage] == {
        "memory_mib": memory,
        "timeout_seconds": 900,
        "temporary_storage_mib": 512,
        "benchmark_cpus": cpus,
    }
    assert lambda_runtime.SOURCE_LIMITS == {
        "page_size": 5000,
        "max_attempts": 3,
        "connect_timeout": 5.0,
        "read_timeout": 30.0,
        "max_backoff": 10.0,
        "task_deadline": 600.0,
    }
    assert lambda_runtime.DUCKDB_LIMITS == {
        "memory_limit": "512MiB",
        "threads": 1,
        "temp_directory": "",
    }


@pytest.mark.parametrize("stage", ["ingest", "curate", "gold"])
@pytest.mark.parametrize(
    ("milliseconds", "allowance"),
    [(None, 720), (900_000, 720), (1_200_000, 720), (500_000, 320)],
)
def test_budget_uses_monotonic_application_cap_and_context_reserve(
    clock, stage, milliseconds, allowance
):
    context = (
        None
        if milliseconds is None
        else SimpleNamespace(get_remaining_time_in_millis=lambda: milliseconds)
    )
    budget = InvocationBudget(stage, context)
    assert budget.deadline == 100 + allowance
    assert budget.work_deadline == 100 + allowance - 60
    assert budget.remaining() == allowance - 60
    clock[0] += 7
    assert budget.remaining() == allowance - 67
    clock[0] = budget.work_deadline
    with pytest.raises(InvocationDeadlineExceeded):
        budget.check()
    with pytest.raises(InvocationDeadlineExceeded):
        budget.remaining()


@pytest.mark.parametrize("milliseconds", [0, 180_000, 240_000])
@pytest.mark.parametrize("module", STAGES)
def test_handlers_reject_exhausted_context_at_entry(
    clock, monkeypatch, module, milliseconds
):
    def no_settings(event):
        pytest.fail("An exhausted invocation must stop before event processing")

    monkeypatch.setattr(lambda_common, "settings", no_settings)
    context = SimpleNamespace(get_remaining_time_in_millis=lambda: milliseconds)
    with pytest.raises(InvocationDeadlineExceeded):
        module.handler({}, context)


@pytest.mark.parametrize("remaining", [float("inf"), float("nan")])
def test_budget_rejects_nonfinite_context(clock, remaining):
    context = SimpleNamespace(get_remaining_time_in_millis=lambda: remaining)
    with pytest.raises(ValueError, match="finite"):
        InvocationBudget("ingest", context)


@pytest.mark.parametrize("milliseconds", [None, 500_000])
def test_failure_reporting_uses_only_application_grace_and_restores_state(
    clock, milliseconds
):
    context = (
        None
        if milliseconds is None
        else SimpleNamespace(get_remaining_time_in_millis=lambda: milliseconds)
    )
    budget = InvocationBudget("curate", context)
    clock[0] = budget.work_deadline
    with pytest.raises(InvocationDeadlineExceeded):
        budget.check()
    with pytest.raises(RuntimeError, match="diagnostic failure"):
        with budget.failure_reporting():
            assert budget.remaining() == 60
            with budget.failure_reporting():
                clock[0] += 1
                assert budget.remaining() == 59
            assert budget.reporting is True
            raise RuntimeError("diagnostic failure")
    assert budget.reporting is False
    with pytest.raises(InvocationDeadlineExceeded):
        budget.check()
    clock[0] = budget.deadline
    with pytest.raises(InvocationDeadlineExceeded):
        with budget.failure_reporting():
            pytest.fail("Failure reporting must not borrow the Lambda reserve")
    assert budget.reporting is False
    fresh = InvocationBudget("curate", None)
    assert fresh.reporting is False
    assert fresh.remaining() == 660


def test_source_config_clamps_remaining_time_without_mutating_or_widening(clock):
    budget = InvocationBudget("ingest", None)
    config = execution_config(**lambda_runtime.SOURCE_LIMITS)
    original = dict(config)
    assert budget.source_config(config) == config
    clock[0] = budget.work_deadline - 2
    selected = budget.source_config(config)
    assert config == original
    assert selected == {
        **config,
        "task_deadline": 2,
        "connect_timeout": 2,
        "read_timeout": 2,
        "max_backoff": 2,
    }
    shorter = execution_config(
        task_deadline=1, connect_timeout=0.1, read_timeout=0.2, max_backoff=0.3
    )
    assert budget.source_config(shorter) == shorter
    clock[0] = budget.work_deadline
    with pytest.raises(InvocationDeadlineExceeded):
        budget.source_config(config)


@pytest.mark.parametrize("module", STAGES)
@pytest.mark.parametrize("name", list(lambda_runtime.SOURCE_LIMITS))
def test_settings_cannot_widen_caps_before_client_construction(
    monkeypatch, module, name
):
    def no_client():
        pytest.fail("Invalid settings must not construct an SDK client")

    monkeypatch.setattr(lambda_common, "create_s3_client", no_client)
    event = daily_event()
    event["config"] = {name: lambda_runtime.SOURCE_LIMITS[name] + 1}
    with pytest.raises(ValueError, match="approved limits"):
        module.handler(event, None)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("page_size", True),
        ("page_size", 2.0),
        ("max_attempts", "2"),
        ("connect_timeout", False),
        ("read_timeout", "5"),
        ("max_backoff", -1),
        ("task_deadline", None),
    ],
)
def test_settings_validate_types_before_client_construction(monkeypatch, name, value):
    def no_client():
        pytest.fail("Invalid settings must not construct an SDK client")

    monkeypatch.setattr(lambda_common, "create_s3_client", no_client)
    with pytest.raises(ValueError):
        lambda_ingest.handler({**daily_event(), "config": {name: value}}, None)


def test_settings_defaults_and_shorter_typed_limits():
    event = {**daily_event(), "config": {}}
    assert lambda_common.settings(event) == execution_config(
        mode="sample", **lambda_runtime.SOURCE_LIMITS
    )
    shorter = {
        "page_size": 10,
        "max_attempts": 1,
        "connect_timeout": 1,
        "read_timeout": 2,
        "max_backoff": 1,
        "task_deadline": 3,
    }
    assert lambda_common.settings({**event, "config": shorter}) == execution_config(
        mode="sample", **shorter
    )


def test_sdk_client_receives_explicit_bounded_botocore_config(monkeypatch):
    calls = []
    sentinel = object()

    def client(service, **kwargs):
        calls.append((service, kwargs))
        return sentinel

    monkeypatch.setattr(boto3, "client", client)
    assert lambda_common.create_s3_client() is sentinel
    assert len(calls) == 1
    service, kwargs = calls[0]
    assert service == "s3"
    assert set(kwargs) == {"config"}
    config = kwargs["config"]
    assert isinstance(config, Config)
    assert config.connect_timeout == 5
    assert config.read_timeout == 20
    assert config.retries == {"mode": "standard", "total_max_attempts": 3}


@pytest.mark.parametrize("operation", ["head", "missing_head", "put"])
def test_s3_checks_after_io_and_refuses_all_subsequent_expired_io(
    clock, monkeypatch, operation
):
    client = MemoryS3()
    client.objects["object"] = b"original"
    budget = InvocationBudget("ingest", None)
    store = S3Store(client, "fixture-bucket", check=budget.check)
    method = "put_object" if operation == "put" else "head_object"
    original = getattr(client, method)

    def expire(**kwargs):
        clock[0] = budget.work_deadline
        return original(**kwargs)

    monkeypatch.setattr(client, method, expire)
    with pytest.raises(InvocationDeadlineExceeded):
        if operation == "put":
            store.write("object", b"updated")
        else:
            store.exists("missing" if operation == "missing_head" else "object")
    calls = list(client.calls)
    assert len(calls) == 1
    for action in (
        lambda: store.exists("object"),
        lambda: store.write("object", b"forbidden"),
        lambda: store.read("object"),
    ):
        with pytest.raises(InvocationDeadlineExceeded):
            action()
    assert client.calls == calls


@pytest.mark.parametrize("expire_during", ["get", "chunk"])
def test_s3_stream_closes_when_budget_expires_and_reads_only_one_mib_chunks(
    clock, expire_during
):
    budget = InvocationBudget("ingest", None)
    sizes = []
    gets = []

    class Body(io.BytesIO):
        def read(self, size=-1):
            sizes.append(size)
            clock[0] = budget.work_deadline
            return super().read(size)

    body = Body(b"x" * (2 * 1024 * 1024))

    def get_object(**kwargs):
        gets.append(kwargs)
        if expire_during == "get":
            clock[0] = budget.work_deadline
        return {"Body": body}

    store = S3Store(
        SimpleNamespace(get_object=get_object), "fixture-bucket", check=budget.check
    )
    with pytest.raises(InvocationDeadlineExceeded):
        store.read("object")
    assert body.closed
    assert sizes == ([1024 * 1024] if expire_during == "chunk" else [])
    with pytest.raises(InvocationDeadlineExceeded):
        store.read("object")
    assert gets == [{"Bucket": "fixture-bucket", "Key": "object"}]


@pytest.mark.parametrize("caller", ["source", "bronze"])
@pytest.mark.parametrize("absolute_offset", [0, -1])
def test_expired_absolute_extraction_deadline_prevents_http(
    clock, tmp_path, caller, absolute_offset
):
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, json=[{"n": "0"}])

    config = execution_config(**lambda_runtime.SOURCE_LIMITS)
    with source.create_client(
        config, app_token="", transport=httpx.MockTransport(respond)
    ) as client:
        with pytest.raises(source.ExtractionError, match="deadline"):
            kwargs = {"absolute_deadline": clock[0] + absolute_offset}
            if caller == "source":
                source.extract_day(client, date(2026, 9, 1), config, **kwargs)
            else:
                bronze.ingest_day(
                    client, date(2026, 9, 1), tmp_path, "expired", config, **kwargs
                )
    assert requests == []
    if caller == "bronze":
        manifest = read_json(
            output_root(tmp_path, "sample"), "control/bronze/expired/2026-09-01.json"
        )
        assert manifest["status"] == "failed"


@pytest.mark.parametrize("caller", ["source", "bronze"])
@pytest.mark.parametrize("expiry", ["page", "final_count"])
@pytest.mark.parametrize("earlier", ["absolute", "local"])
def test_extraction_keeps_earliest_deadline_across_progress_callbacks(
    clock, tmp_path, caller, expiry, earlier
):
    requests = []
    progress = []
    start = clock[0]
    config = execution_config(
        task_deadline=4 if earlier == "local" else 600,
        connect_timeout=1,
        read_timeout=2,
        max_backoff=1,
    )

    def respond(request):
        requests.append(request)
        is_count = request.url.params["$select"] == "count(*) as n"
        return httpx.Response(200, json=[{"n": "0"}] if is_count else [])

    def advance():
        clock[0] = start + 4

    def on_page(page):
        if expiry == "page":
            advance()

    def on_count(position, count):
        if expiry == "final_count" and position == "after":
            advance()

    def on_progress(manifest):
        progress.append(manifest["status"])
        if (expiry == "page" and manifest["pages"]) or (
            expiry == "final_count" and manifest["source_count_after"] is not None
        ):
            advance()

    with source.create_client(
        config, app_token="", transport=httpx.MockTransport(respond)
    ) as client:
        kwargs = {"absolute_deadline": start + (4 if earlier == "absolute" else 600)}
        with pytest.raises(source.ExtractionError, match="deadline"):
            if caller == "source":
                source.extract_day(
                    client,
                    date(2026, 9, 1),
                    config,
                    on_page=on_page,
                    on_count=on_count,
                    **kwargs,
                )
            else:
                bronze.ingest_day(
                    client,
                    date(2026, 9, 1),
                    tmp_path,
                    "callback",
                    config,
                    on_progress=on_progress,
                    **kwargs,
                )
    assert len(requests) == (2 if expiry == "page" else 3)
    assert requests[0].extensions["timeout"]["read"] == 2
    if caller == "bronze":
        assert progress[-1] == "failed"
        assert "complete" not in progress


def test_ingest_passes_original_work_deadline_after_prior_s3_work(
    clock, aws, monkeypatch
):
    original_head = aws.head_object
    captured = []
    original_ingest = lambda_ingest.ingest_day

    def delayed_head(**kwargs):
        clock[0] += 30
        return original_head(**kwargs)

    def ingest(*args, **kwargs):
        captured.append((args[4], kwargs["absolute_deadline"]))
        return original_ingest(*args, **kwargs)

    monkeypatch.setattr(aws, "head_object", delayed_head)
    monkeypatch.setattr(lambda_ingest, "ingest_day", ingest)
    assert lambda_ingest.handler(daily_event(), None)["status"] == "complete"
    assert captured[0][1] == 760
    assert captured[0][0]["task_deadline"] == 570


@pytest.mark.parametrize("grace", [True, False])
def test_silver_expiry_after_invalidation_uses_only_failure_reporting_grace(
    clock, aws, monkeypatch, grace
):
    event = daily_event()
    event.update(lambda_ingest.handler(event, None))
    marker = "nyc311/sample/control/silver/2026-09-01.json"
    original_put = aws.put_object

    def expire_artifact(**kwargs):
        result = original_put(**kwargs)
        if "/quality/" in kwargs["Key"]:
            clock[0] = 760 if grace else 820
        return result

    monkeypatch.setattr(aws, "put_object", expire_artifact)
    aws.calls.clear()
    with pytest.raises(InvocationDeadlineExceeded):
        lambda_curate.handler(event, None)
    assert json.loads(aws.objects[marker])["status"] == (
        "failed" if grace else "incomplete"
    )
    puts = [key for operation, key in aws.calls if operation == "put"]
    assert puts[0] == marker
    assert len(puts) == (3 if grace else 2)


def test_silver_expiry_before_invalidation_preserves_previous_publication(
    clock, aws, monkeypatch
):
    event = daily_event()
    event.update(lambda_ingest.handler(event, None))
    lambda_curate.handler(event, None)
    before = dict(aws.objects)
    original_publish = lambda_curate.publish_silver

    def expired_publish(*args, **kwargs):
        manifest = original_publish(*args, **kwargs)
        clock[0] = 760
        return manifest

    monkeypatch.setattr(lambda_curate, "publish_silver", expired_publish)
    aws.calls.clear()
    with pytest.raises(InvocationDeadlineExceeded):
        lambda_curate.handler(event, None)
    assert aws.objects == before
    assert not any(operation == "put" for operation, _ in aws.calls)


@pytest.mark.parametrize("grace", [True, False])
def test_gold_expiry_at_completion_cannot_publish_complete(
    clock, aws, monkeypatch, grace
):
    event = daily_event()
    event.update(lambda_ingest.handler(event, None))
    lambda_curate.handler(event, None)
    original_verify = lambda_common.verify_remote_files
    original_publish = lambda_gold.publish_gold
    configs = []

    def expire_verification(*args, **kwargs):
        original_verify(*args, **kwargs)
        clock[0] = 760 if grace else 820

    def capture_config(*args, **kwargs):
        configs.append(kwargs["duckdb_config"])
        return original_publish(*args, **kwargs)

    monkeypatch.setattr(lambda_common, "verify_remote_files", expire_verification)
    monkeypatch.setattr(lambda_gold, "publish_gold", capture_config)
    marker = "nyc311/sample/control/gold/resource-gold.json"
    statuses = []
    original_put = aws.put_object

    def capture_marker(**kwargs):
        if kwargs["Key"] == marker:
            statuses.append(json.loads(kwargs["Body"])["status"])
        return original_put(**kwargs)

    monkeypatch.setattr(aws, "put_object", capture_marker)
    with pytest.raises(InvocationDeadlineExceeded):
        lambda_gold.handler(
            {**event, "run_id": "resource-gold", "days": ["2026-09-01"]}, None
        )
    assert "complete" not in statuses
    assert statuses[-1] == ("failed" if grace else "incomplete")
    assert json.loads(aws.objects[marker])["status"] == statuses[-1]
    config = configs[0]
    assert config == {
        "memory_limit": "512MiB",
        "threads": 1,
        "temp_directory": "",
    }
