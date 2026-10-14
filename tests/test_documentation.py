import re
import shlex
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from nyc311 import cli, source
from nyc311.config import COHORT_END, COHORT_START, daily_windows, execution_config
from nyc311.gold_store import load_gold
from nyc311.pipeline import run_pipeline

ROOT = Path(__file__).resolve().parents[1]


def fenced_blocks(path: Path, language: str) -> list[str]:
    return re.findall(
        rf"```{language}\n(.*?)\n```", path.read_text(encoding="utf-8"), re.DOTALL
    )


def test_readme_offline_commands_run_without_modification_except_storage_base(
    tmp_path: Path, capsys
) -> None:
    blocks = fenced_blocks(ROOT / "README.md", "bash")
    for command in ("sample", "replay"):
        block = next(
            value
            for value in blocks
            if value.startswith(f"uv run python -m nyc311 {command}")
        )
        arguments = shlex.split(block.replace("\\\n", " "))
        assert arguments[:5] == ["uv", "run", "python", "-m", "nyc311"]
        arguments[arguments.index("--base") + 1] = str(tmp_path)
        assert cli.main(arguments[5:]) == 0
        capsys.readouterr()
    first = load_gold(tmp_path, "sample-docs-001")[1]
    second = load_gold(tmp_path, "sample-docs-replay-001")[1]
    assert all(
        second[name].equals(table, check_metadata=True) for name, table in first.items()
    )


def test_real_month_command_is_documented_but_never_executed() -> None:
    blocks = fenced_blocks(ROOT / "docs/local-runbook.md", "bash")
    block = next(value for value in blocks if "--run-id september-2026-001" in value)
    arguments = shlex.split(block.replace("\\\n", " "))
    assert arguments[:6] == ["uv", "run", "python", "-m", "nyc311", "extract"]
    assert arguments[arguments.index("--mode") + 1] == "published"
    assert "--live" in arguments
    start = date.fromisoformat(arguments[arguments.index("--start") + 1])
    end = date.fromisoformat(arguments[arguments.index("--end") + 1])
    assert (start, end) == (COHORT_START, COHORT_END)
    assert len(daily_windows(start, end)) == 30


def test_documented_post_extraction_check_runs_on_simulated_full_month_only(
    tmp_path: Path, capsys
) -> None:
    settings = execution_config(mode="published")
    with source.create_client(
        settings, app_token="", transport=cli.fixture_transport(cli.DEFAULT_FIXTURE)
    ) as client:
        run_pipeline(
            tmp_path,
            "september-2026-001",
            COHORT_START,
            COHORT_END,
            config=settings,
            client=client,
        )
    block = next(
        value
        for value in fenced_blocks(ROOT / "docs/local-runbook.md", "bash")
        if value.startswith("uv run python - <<'PY'")
    )
    script = block.split("\n", 1)[1].rsplit("\nPY", 1)[0]
    script = script.replace('Path("data/local")', f"Path({str(tmp_path)!r})")
    exec(compile(script, "documented-post-check", "exec"), {})
    output = capsys.readouterr().out
    assert "Verified official Gold:" in output
    assert "Observation range:" in output
    # The same check also applies to a partial replay against complete month coverage.
    settings = execution_config(mode="published")
    partial = run_pipeline(
        tmp_path,
        "september-2026-refresh-check",
        COHORT_START,
        date(2026, 9, 2),
        config=settings,
        bronze_keys=["control/bronze/september-2026-001/2026-09-01.json"],
    )
    assert partial["completed_days"] == ["2026-09-01"]
    assert len(partial["gold_days"]) == 30
    refreshed_script = script.replace(
        '"september-2026-001"', '"september-2026-refresh-check"'
    )
    exec(compile(refreshed_script, "documented-refresh-post-check", "exec"), {})
    assert "Verified official Gold:" in capsys.readouterr().out


def test_documented_analytics_queries_reproduce_fixed_fixture_metrics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    class FixedObservation(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 13, 12, tzinfo=UTC)

    monkeypatch.setattr(source, "datetime", FixedObservation)
    assert (
        cli.main(
            [
                "sample",
                "--base",
                str(tmp_path),
                "--run-id",
                "sample-docs-001",
                "--page-size",
                "3",
            ]
        )
        == 0
    )
    capsys.readouterr()
    load_gold(tmp_path, "sample-docs-001")
    blocks = fenced_blocks(ROOT / "docs/analytics.md", "sql")
    assert len(blocks) == 7
    # Only replace the example's storage prefix; the documented SQL is executed as-is.
    setup = blocks[0].replace(
        "data/local/sample/gold/run=sample-docs-001",
        str(tmp_path / "sample/gold/run=sample-docs-001"),
    )
    with duckdb.connect(":memory:") as connection:
        connection.execute(setup)
        demand, closure, outstanding, channels, weighted, totals = [
            connection.execute(query).fetchall() for query in blocks[1:]
        ]
    assert sum(row[3] for row in demand) == 12
    assert closure == [
        ("DOT", "Street Condition", 4, 6.0, 489.9),
        ("NYPD", "Noise - Residential", 1, 0.0, 0.0),
    ]
    assert sum(row[4] for row in outstanding) == 2
    assert sum(row[5] for row in outstanding) == 1
    assert {row[3] for row in outstanding} == {"30+"}
    assert channels == [
        (date(2026, 9, 1), "MANHATTAN", "ONLINE", 2, 50.0),
        (date(2026, 9, 1), "MANHATTAN", "PHONE", 2, 50.0),
        (date(2026, 9, 1), "QUEENS", "MOBILE", 1, 50.0),
        (date(2026, 9, 1), "QUEENS", "UNKNOWN", 1, 50.0),
        (date(2026, 9, 1), "UNKNOWN", "UNKNOWN", 1, 100.0),
        (date(2026, 9, 2), "MANHATTAN", "MOBILE", 1, 50.0),
        (date(2026, 9, 2), "MANHATTAN", "UNKNOWN", 1, 50.0),
        (date(2026, 9, 2), "QUEENS", "PHONE", 1, 100.0),
        (date(2026, 9, 2), "UNKNOWN", "MOBILE", 1, 50.0),
        (date(2026, 9, 2), "UNKNOWN", "ONLINE", 1, 50.0),
    ]
    assert [row for row in weighted if row[0] == "MANHATTAN"] == [
        ("MANHATTAN", "MOBILE", 1, 16.67),
        ("MANHATTAN", "ONLINE", 2, 33.33),
        ("MANHATTAN", "PHONE", 2, 33.33),
        ("MANHATTAN", "UNKNOWN", 1, 16.67),
    ]
    assert totals == [(12, 12, 5, 2, 1)]
