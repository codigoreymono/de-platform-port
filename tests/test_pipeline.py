import json
import os
import subprocess
from collections import Counter
from datetime import date
from pathlib import Path

import httpx
import pytest

from nyc311 import cli, gold_store, pipeline, source
from nyc311.config import execution_config
from nyc311.gold_store import load_gold
from nyc311.silver_store import load_silver
from nyc311.storage import output_root, read_json

START = date(2026, 9, 1)
END = date(2026, 9, 3)
FIXTURE = Path(__file__).resolve().parents[1] / "data/samples/gold-metrics.json"


def simulated_client() -> httpx.Client:
    return source.create_client(
        execution_config(page_size=3),
        app_token="",
        transport=cli.fixture_transport(FIXTURE),
    )


def test_sequential_end_to_end_run_preserves_order_and_final_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = []
    real_ingest, real_silver, real_gold = (
        pipeline.ingest_day,
        pipeline.publish_silver,
        pipeline.publish_gold,
    )

    def ingest(*args: object, **kwargs: object):
        events.append(("bronze", args[1]))
        return real_ingest(*args, **kwargs)

    def silver(*args: object, **kwargs: object):
        result = real_silver(*args, **kwargs)
        events.append(("silver", date.fromisoformat(result["start_date"])))
        return result

    def gold(*args: object, **kwargs: object):
        events.append(("gold", None))
        return real_gold(*args, **kwargs)

    monkeypatch.setattr(pipeline, "ingest_day", ingest)
    monkeypatch.setattr(pipeline, "publish_silver", silver)
    monkeypatch.setattr(pipeline, "publish_gold", gold)
    with simulated_client() as client:
        result = pipeline.run_pipeline(
            tmp_path,
            "e2e",
            START,
            END,
            config=execution_config(page_size=3),
            client=client,
        )
        assert not client.is_closed
    assert events == [
        ("bronze", START),
        ("silver", START),
        ("bronze", date(2026, 9, 2)),
        ("silver", date(2026, 9, 2)),
        ("gold", None),
    ]
    assert result["status"] == "succeeded"
    assert result["stage"] == "finished"
    assert result["completed_days"] == ["2026-09-01", "2026-09-02"]
    assert len(result["bronze_keys"]) == 2
    assert result["gold_manifest_key"] == "control/gold/e2e.json"
    assert result["error"] is None
    root = output_root(tmp_path, "sample")
    assert read_json(root, "control/runs/e2e.json") == result
    gold_manifest, products = load_gold(tmp_path, "e2e")
    assert gold_manifest["status"] == "complete"
    assert sum(products["demand_daily"]["request_count"].to_pylist()) == 12
    assert load_silver(tmp_path, START)[1].num_rows == 7
    assert load_silver(tmp_path, date(2026, 9, 2))[1].num_rows == 5


def test_sample_cli_runs_the_whole_pipeline_without_live_http(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert (
        cli.main(
            [
                "sample",
                "--base",
                str(tmp_path),
                "--run-id",
                "sample-cli",
                "--page-size",
                "3",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "succeeded"
    assert result["mode"] == "sample"
    manifest, products = load_gold(tmp_path, "sample-cli")
    assert manifest["coverage"] == "sample"
    assert cli.DEFAULT_FIXTURE == FIXTURE
    counts = Counter()
    channels = products["channel_usage"].to_pylist()
    for row in channels:
        counts[row["channel"]] += row["request_count"]
    assert counts == {"ONLINE": 3, "PHONE": 3, "MOBILE": 3, "UNKNOWN": 3}
    manhattan = {
        row["channel"]: row["channel_share"]
        for row in channels
        if row["created_date"] == START and row["borough"] == "MANHATTAN"
    }
    assert manhattan == {"ONLINE": 0.5, "PHONE": 0.5}
    for day, absent in (
        (START, "sample-gold-03"),
        (START, "sample-gold-07"),
        (date(2026, 9, 2), "sample-gold-11"),
    ):
        silver = {
            row["unique_key"]: row for row in load_silver(tmp_path, day)[1].to_pylist()
        }
        assert silver[absent]["open_data_channel_type"] == "UNKNOWN"
    assert not output_root(tmp_path, "published").exists()


def test_module_entry_runs_via_uv_offline(tmp_path: Path) -> None:
    environment = dict(os.environ, UV_OFFLINE="1")
    result = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "-m",
            "nyc311",
            "sample",
            "--base",
            str(tmp_path),
            "--run-id",
            "module-cli",
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "succeeded"
    assert load_gold(tmp_path, "module-cli")[0]["status"] == "complete"


def test_live_extraction_requires_explicit_opt_in(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as error:
        cli.main(["extract", "--base", str(tmp_path), "--run-id", "not-live"])
    assert error.value.code == 2
    assert list(tmp_path.iterdir()) == []


def test_invalid_window_does_not_create_run_or_contact_source(tmp_path: Path) -> None:
    with simulated_client() as client:
        with pytest.raises(ValueError):
            pipeline.run_pipeline(
                tmp_path, "invalid", date(2026, 8, 31), END, client=client
            )
    assert list(tmp_path.iterdir()) == []


def test_existing_run_identifier_cannot_overwrite_history(tmp_path: Path) -> None:
    with simulated_client() as client:
        first = pipeline.run_pipeline(tmp_path, "immutable", START, END, client=client)
        with pytest.raises(FileExistsError):
            pipeline.run_pipeline(tmp_path, "immutable", START, END, client=client)
    assert (
        read_json(output_root(tmp_path, "sample"), "control/runs/immutable.json")
        == first
    )


def test_published_partial_window_is_not_misreported_as_success(tmp_path: Path) -> None:
    with simulated_client() as client:
        with pytest.raises(RuntimeError):
            pipeline.run_pipeline(
                tmp_path,
                "partial-month",
                START,
                END,
                config=execution_config(mode="published"),
                client=client,
            )
    root = output_root(tmp_path, "published")
    result = read_json(root, "control/runs/partial-month.json")
    assert result["status"] == "failed"
    assert result["stage"] == "gold"
    assert result["completed_days"] == ["2026-09-01", "2026-09-02"]
    assert result["gold_manifest_key"] is None
    assert not (root / "control/gold/partial-month.json").exists()


def test_standalone_bronze_id_collision_is_detected_before_any_silver_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline(tmp_path)
    root = output_root(tmp_path, "sample")
    snapshot = {
        path: path.read_bytes()
        for path in (root / "silver").rglob("*")
        if path.is_file()
    }
    with simulated_client() as client:
        pipeline.ingest_day(
            client, date(2026, 9, 2), tmp_path, "standalone", execution_config()
        )

    def no_silver(*args: object, **kwargs: object) -> None:
        raise AssertionError("Namespace collision must fail before any Silver changes")

    monkeypatch.setattr(pipeline, "publish_silver", no_silver)
    with simulated_client() as client:
        with pytest.raises(FileExistsError):
            pipeline.run_pipeline(tmp_path, "standalone", START, END, client=client)
    assert snapshot == {
        path: path.read_bytes()
        for path in (root / "silver").rglob("*")
        if path.is_file()
    }
    assert not (root / "control/runs/standalone.json").exists()


def test_cli_failure_exposes_owned_progress_and_recovery_references(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    fixture = cli.fixture_transport(FIXTURE)

    def fail_second_day(request: httpx.Request) -> httpx.Response:
        if "created_date >= '2026-09-02" in request.url.params["$where"]:
            return httpx.Response(503, text="sensitive-unavailable-body")
        return fixture.handle_request(request)

    def controlled_client(settings: object, **kwargs: object) -> httpx.Client:
        return source.create_client(
            settings, app_token="", transport=httpx.MockTransport(fail_second_day)
        )

    monkeypatch.setattr(cli, "create_client", controlled_client)
    assert (
        cli.main(
            [
                "sample",
                "--base",
                str(tmp_path),
                "--run-id",
                "partial-cli",
                "--max-attempts",
                "1",
            ]
        )
        == 1
    )
    output = capsys.readouterr().out
    result = json.loads(output)
    assert result["status"] == "failed"
    assert result["stage"] == "bronze"
    assert result["active_day"] == "2026-09-02"
    assert result["completed_days"] == ["2026-09-01"]
    assert result["bronze_keys"] == ["control/bronze/partial-cli/2026-09-01.json"]
    assert result["run_manifest_key"] == "control/runs/partial-cli.json"
    assert result["gold_manifest_key"] is None
    assert "sensitive-unavailable" not in output


def test_cli_id_collision_never_echoes_previous_success_as_new_result(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline(tmp_path)
    assert cli.main(["sample", "--base", str(tmp_path), "--run-id", "original"]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "failed"
    assert result["error"] == "FileExistsError"
    assert "gold_manifest_key" not in result
    assert "completed_days" not in result


def baseline(base: Path, run_id: str = "original"):
    with simulated_client() as client:
        return pipeline.run_pipeline(base, run_id, START, END, client=client)


def test_offline_replay_uses_complete_bronze_without_client_or_new_extraction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = baseline(tmp_path)
    root = output_root(tmp_path, "sample")
    snapshot = {
        path: path.read_bytes()
        for path in (root / "bronze").rglob("*")
        if path.is_file()
    }
    expected = {day: load_silver(tmp_path, day)[1] for day in (START, date(2026, 9, 2))}
    _, old_gold = load_gold(tmp_path, "original")

    def no_http(*args: object, **kwargs: object) -> None:
        raise AssertionError("Replay must not create or invoke an HTTP client")

    monkeypatch.setattr(pipeline, "create_client", no_http)
    monkeypatch.setattr(pipeline, "ingest_day", no_http)
    monkeypatch.setattr(source, "_request", no_http)
    result = pipeline.run_pipeline(
        tmp_path, "replay", START, END, bronze_keys=original["bronze_keys"]
    )
    assert result["status"] == "succeeded"
    assert result["action"] == "replay"
    assert snapshot == {
        path: path.read_bytes()
        for path in (root / "bronze").rglob("*")
        if path.is_file()
    }
    assert all(
        load_silver(tmp_path, day)[1].equals(table, check_metadata=True)
        for day, table in expected.items()
    )
    _, new_gold = load_gold(tmp_path, "replay")
    assert all(
        new_gold[name].equals(table, check_metadata=True)
        for name, table in old_gold.items()
    )


def test_replay_cli_works_with_explicit_storage_keys(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    original = baseline(tmp_path)
    result = cli.main(
        [
            "replay",
            "--base",
            str(tmp_path),
            "--run-id",
            "replay-cli",
            "--start",
            "2026-09-01",
            "--end",
            "2026-09-03",
            "--bronze",
            *original["bronze_keys"],
        ]
    )
    assert result == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "succeeded"
    assert output["action"] == "replay"


def test_selected_fresh_day_replaces_observations_and_rebuilds_whole_sample_gold(
    tmp_path: Path,
) -> None:
    baseline(tmp_path)
    root = output_root(tmp_path, "sample")
    untouched = {
        path: path.read_bytes()
        for path in root.rglob("*")
        if path.is_file() and "2026-09-02" in str(path)
    }
    record = json.loads(FIXTURE.read_bytes())[4]
    record["status"] = "Closed"
    record["closed_date"] = "2026-09-01T10:00:00"
    refresh_fixture = tmp_path / "refresh-fixture.json"
    refresh_fixture.write_text(json.dumps([record]))
    settings = execution_config(page_size=2)
    with source.create_client(
        settings, app_token="", transport=cli.fixture_transport(refresh_fixture)
    ) as client:
        result = pipeline.run_pipeline(
            tmp_path, "refresh", START, date(2026, 9, 2), config=settings, client=client
        )
    assert result["completed_days"] == ["2026-09-01"]
    assert result["gold_days"] == ["2026-09-01", "2026-09-02"]
    table = load_silver(tmp_path, START)[1]
    assert table.num_rows == 1
    assert table["status"].to_pylist() == ["CLOSED"]
    assert table["source_run_id"].to_pylist() == ["refresh"]
    assert all(path.read_bytes() == content for path, content in untouched.items())
    _, gold = load_gold(tmp_path, "refresh")
    assert sum(gold["demand_daily"]["request_count"].to_pylist()) == 6


@pytest.mark.parametrize("damage", ["missing-day", "duplicate-day", "incomplete"])
def test_replay_plan_fails_before_mutating_silver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    original = baseline(tmp_path)
    references = list(original["bronze_keys"])
    root = output_root(tmp_path, "sample")
    before = {
        path: path.read_bytes()
        for path in (root / "silver").rglob("*")
        if path.is_file()
    }
    if damage == "missing-day":
        references.pop()
    elif damage == "duplicate-day":
        references[1] = references[0]
    else:
        manifest = read_json(root, references[1])
        manifest["status"] = "incomplete"
        from nyc311.storage import write_json

        write_json(root, references[1], manifest)

    def no_silver(*args: object, **kwargs: object) -> None:
        raise AssertionError("Invalid replay plan must fail before Silver publication")

    monkeypatch.setattr(pipeline, "publish_silver", no_silver)
    with pytest.raises(RuntimeError):
        pipeline.run_pipeline(tmp_path, "bad-plan", START, END, bronze_keys=references)
    assert before == {
        path: path.read_bytes()
        for path in (root / "silver").rglob("*")
        if path.is_file()
    }
    assert read_json(root, "control/runs/bad-plan.json")["status"] == "failed"


def test_refresh_passes_previous_raw_count_and_blocks_unexplained_empty_source(
    tmp_path: Path,
) -> None:
    baseline(tmp_path)
    fixture = tmp_path / "empty-fixture.json"
    fixture.write_text("[]")
    root = output_root(tmp_path, "sample")
    old = load_silver(tmp_path, START)[1]
    with source.create_client(
        app_token="", transport=cli.fixture_transport(fixture)
    ) as client:
        with pytest.raises(source.ExtractionError, match="Unexplained empty"):
            pipeline.run_pipeline(
                tmp_path, "unexpected-empty", START, date(2026, 9, 2), client=client
            )
    assert load_silver(tmp_path, START)[1].equals(old)
    assert read_json(root, "control/runs/unexpected-empty.json")["stage"] == "bronze"
    assert read_json(root, "control/runs/unexpected-empty.json")["status"] == "failed"


def test_ingestion_failure_stops_later_days_and_recovers_from_remaining_window(
    tmp_path: Path,
) -> None:
    fixture = cli.fixture_transport(FIXTURE)
    requests = []

    def fail_day_two(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if "created_date >= '2026-09-02" in request.url.params["$where"]:
            return httpx.Response(503, text="unavailable")
        return fixture.handle_request(request)

    settings = execution_config(page_size=3, max_attempts=1)
    with source.create_client(
        settings, app_token="", transport=httpx.MockTransport(fail_day_two)
    ) as client:
        with pytest.raises(source.ExtractionError):
            pipeline.run_pipeline(
                tmp_path,
                "failed-source",
                START,
                date(2026, 9, 4),
                config=settings,
                client=client,
            )
    root = output_root(tmp_path, "sample")
    failed = read_json(root, "control/runs/failed-source.json")
    assert failed["status"] == "failed"
    assert failed["stage"] == "bronze"
    assert failed["active_day"] == "2026-09-02"
    assert failed["completed_days"] == ["2026-09-01"]
    assert failed["gold_manifest_key"] is None
    assert not any(
        "created_date >= '2026-09-03" in request.url.params["$where"]
        for request in requests
    )
    assert (
        read_json(root, "control/bronze/failed-source/2026-09-02.json")["status"]
        == "failed"
    )
    assert not (root / "control/gold/failed-source.json").exists()
    with simulated_client() as client:
        recovered = pipeline.run_pipeline(
            tmp_path, "remaining", date(2026, 9, 2), END, client=client
        )
    assert recovered["status"] == "succeeded"
    assert recovered["gold_days"] == ["2026-09-01", "2026-09-02"]
    assert (
        sum(
            load_gold(tmp_path, "remaining")[1]["demand_daily"][
                "request_count"
            ].to_pylist()
        )
        == 12
    )


@pytest.mark.parametrize("failure", [OSError, KeyboardInterrupt])
def test_silver_publication_interruption_recovers_by_offline_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: type[BaseException]
) -> None:
    root = output_root(tmp_path, "sample")
    target = root / "silver/created_date=2026-09-02/part.parquet"
    real_replace = Path.replace

    def stop_partition(path: Path, destination: Path) -> Path:
        if Path(destination) == target:
            raise failure("partition publication interrupted")
        return real_replace(path, destination)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "replace", stop_partition)
        with simulated_client() as client:
            with pytest.raises(failure):
                pipeline.run_pipeline(
                    tmp_path, "failed-silver", START, END, client=client
                )
    failed = read_json(root, "control/runs/failed-silver.json")
    assert failed["status"] == "failed"
    assert failed["stage"] == "silver"
    assert failed["active_day"] == "2026-09-02"
    assert failed["completed_days"] == ["2026-09-01"]
    assert len(failed["bronze_keys"]) == 2
    assert read_json(root, failed["bronze_keys"][1])["status"] == "complete"
    assert read_json(root, "control/silver/2026-09-02.json")["status"] != "complete"
    assert failed["gold_manifest_key"] is None
    recovered = pipeline.run_pipeline(
        tmp_path, "recovered-silver", START, END, bronze_keys=failed["bronze_keys"]
    )
    assert recovered["status"] == "succeeded"
    assert load_gold(tmp_path, "recovered-silver")[0]["status"] == "complete"
    assert list(root.rglob("*.tmp")) == []


def test_failed_gold_is_not_old_publication_and_replay_rebuilds_all_products(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline(tmp_path)
    root = output_root(tmp_path, "sample")
    _, old_products = load_gold(tmp_path, "original")
    real_write = gold_store._write_prepared_product

    def fail_third(
        root: Path, inputs: object, product: str, run_id: str, table: object
    ):
        if product == "outstanding_requests":
            raise OSError("sensitive-error-not-for-manifest")
        return real_write(root, inputs, product, run_id, table)

    with monkeypatch.context() as patch:
        patch.setattr(gold_store, "_write_prepared_product", fail_third)
        with simulated_client() as client:
            with pytest.raises(OSError):
                pipeline.run_pipeline(
                    tmp_path, "failed-gold", START, END, client=client
                )
    failed = read_json(root, "control/runs/failed-gold.json")
    assert failed["status"] == "failed"
    assert failed["stage"] == "gold"
    assert failed["completed_days"] == ["2026-09-01", "2026-09-02"]
    assert failed["gold_manifest_key"] is None
    assert not any(key.startswith("control/gold/") for key in failed["output_keys"])
    assert "sensitive-error" not in json.dumps(failed)
    assert read_json(root, "control/gold/failed-gold.json")["status"] == "failed"
    with pytest.raises(RuntimeError):
        load_gold(tmp_path, "failed-gold")
    assert load_gold(tmp_path, "original")[0]["status"] == "complete"
    result = pipeline.run_pipeline(
        tmp_path, "recovered-gold", START, END, bronze_keys=failed["bronze_keys"]
    )
    assert result["status"] == "succeeded"
    products = load_gold(tmp_path, "recovered-gold")[1]
    # Data and observation times match the failed run's complete saved Bronze.
    assert sum(products["demand_daily"]["request_count"].to_pylist()) == 12
    assert set(products) == set(old_products)


def test_complete_official_month_can_run_through_simulated_http(tmp_path: Path) -> None:
    with simulated_client() as client:
        result = pipeline.run_pipeline(
            tmp_path,
            "full-month",
            START,
            date(2026, 10, 1),
            config=execution_config(mode="published"),
            client=client,
        )
    assert result["status"] == "succeeded"
    assert len(result["completed_days"]) == len(result["gold_days"]) == 30
    gold, tables = load_gold(tmp_path, "full-month", mode="published")
    assert gold["coverage"] == "full_month"
    assert sum(tables["demand_daily"]["request_count"].to_pylist()) == 12
    assert not output_root(tmp_path, "sample").exists()


def test_cli_returns_nonzero_on_failure_without_echoing_sensitive_details(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise OSError("secret-that-must-not-be-printed")

    monkeypatch.setattr(cli, "run_pipeline", fail)
    assert cli.main(["sample", "--base", str(tmp_path), "--run-id", "cli-failed"]) == 1
    output = capsys.readouterr().out
    assert "secret-that" not in output
    assert json.loads(output)["status"] == "failed"


def test_failed_silver_day_is_not_silently_excluded_from_sample_gold(
    tmp_path: Path,
) -> None:
    original = baseline(tmp_path)
    root = output_root(tmp_path, "sample")
    from nyc311.storage import write_json

    failed_partition = read_json(root, "control/silver/2026-09-02.json")
    failed_partition["status"] = "failed"
    write_json(root, "control/silver/2026-09-02.json", failed_partition)
    with pytest.raises(RuntimeError):
        pipeline.run_pipeline(
            tmp_path,
            "do-not-narrow",
            START,
            date(2026, 9, 2),
            bronze_keys=original["bronze_keys"][:1],
        )
    result = read_json(root, "control/runs/do-not-narrow.json")
    assert result["status"] == "failed"
    assert result["stage"] == "gold"
    assert result["gold_days"] == ["2026-09-01", "2026-09-02"]
    assert result["gold_manifest_key"] is None
