from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_gold_store import FixtureInput, metrics_input

from nyc311 import gold, gold_store
from nyc311.gold import GoldInputs, Product
from nyc311.lambda_runtime import DUCKDB_LIMITS
from nyc311.silver_contract import SILVER_SCHEMA
from nyc311.storage import output_root, read_json


@pytest.fixture
def resource_config() -> dict[str, object]:
    return dict(DUCKDB_LIMITS)


@pytest.fixture
def gold_inputs(tmp_path: Path, silver_input: FixtureInput) -> GoldInputs:
    days = metrics_input(tmp_path, silver_input)
    return gold.load_gold_inputs(tmp_path, days=days)


@pytest.mark.parametrize("product", list(gold.PRODUCT_SCHEMAS))
def test_resource_settings_are_effective_and_preserve_all_metrics(
    tmp_path: Path,
    gold_inputs: GoldInputs,
    resource_config: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    product: Product,
) -> None:
    expected = gold.compute_product(gold_inputs["table"], product)
    real_connect = duckdb.connect
    connections = []
    spill = tmp_path / "duckdb-spill"
    assert not spill.exists()

    def connect(database: str, **kwargs: object) -> duckdb.DuckDBPyConnection:
        assert database == ":memory:"
        assert kwargs == {"config": resource_config}
        connection = real_connect(database, **kwargs)
        connections.append(connection)
        assert connection.execute(
            "SELECT current_setting('memory_limit'), "
            "current_setting('threads'), current_setting('temp_directory')"
        ).fetchone() == ("512.0 MiB", 1, "")
        return connection

    monkeypatch.setattr(gold.duckdb, "connect", connect)
    result = gold.compute_product(
        gold_inputs["table"], product, duckdb_config=resource_config
    )
    assert result.equals(expected, check_metadata=True)
    assert len(connections) == 1
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        connections[0].execute("SELECT 1")
    assert not spill.exists()


@pytest.mark.parametrize(
    ("duckdb_config", "memory_setting"),
    [
        (None, "512.0 MiB"),
        ({}, "512.0 MiB"),
        ({"memory_limit": "512MiB"}, "512.0 MiB"),
        ({"threads": 1}, "512.0 MiB"),
        ({"temp_directory": ""}, "512.0 MiB"),
        ({"memory_limit": "256MiB"}, "256.0 MiB"),
        ({"memory_limit": "536870912B"}, "512.0 MiB"),
        ({"memory_limit": "512MB"}, "488.2 MiB"),
        ({"memory_limit": "65536KiB"}, "64.0 MiB"),
        ({"memory_limit": "64000KB"}, "61.0 MiB"),
    ],
)
def test_defaults_and_partial_overrides_always_use_bounded_no_spill_settings(
    gold_inputs: GoldInputs,
    monkeypatch: pytest.MonkeyPatch,
    duckdb_config: dict[str, object] | None,
    memory_setting: str,
) -> None:
    real_connect = duckdb.connect
    calls = []
    unchanged_config = None if duckdb_config is None else duckdb_config.copy()

    def connect(database: str, **kwargs: object) -> duckdb.DuckDBPyConnection:
        calls.append((database, kwargs))
        connection = real_connect(database, **kwargs)
        assert connection.execute(
            "SELECT current_setting('memory_limit'), "
            "current_setting('threads'), current_setting('temp_directory')"
        ).fetchone() == (memory_setting, 1, "")
        return connection

    monkeypatch.setattr(gold.duckdb, "connect", connect)
    gold.compute_product(
        gold_inputs["table"], "demand_daily", duckdb_config=duckdb_config
    )
    expected_config = DUCKDB_LIMITS | (duckdb_config or {})
    assert calls == [(":memory:", {"config": expected_config})]
    assert calls[0][1]["config"] is not DUCKDB_LIMITS
    assert duckdb_config == unchanged_config


def test_publication_forwards_limits_to_each_frozen_query_and_preserves_outputs(
    tmp_path: Path,
    gold_inputs: GoldInputs,
    resource_config: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = gold_inputs["days"]
    original = gold_store.publish_gold(tmp_path, "default", days=days)
    _, expected = gold_store.load_gold(tmp_path, "default")
    real_compute = gold_store._compute_query
    seen = []
    unchanged_config = resource_config.copy()

    def compute(
        table: pa.Table,
        product: Product,
        query: str,
        *,
        duckdb_config: dict[str, object],
    ) -> pa.Table:
        assert duckdb_config is resource_config
        assert query == (gold.DEFAULT_SQL_DIRECTORY / f"{product}.sql").read_text(
            encoding="utf-8"
        )
        seen.append(product)
        return real_compute(table, product, query, duckdb_config=duckdb_config)

    monkeypatch.setattr(gold_store, "_compute_query", compute)
    manifest = gold_store.publish_gold(
        tmp_path, "bounded", days=days, duckdb_config=resource_config
    )
    loaded, tables = gold_store.load_gold(tmp_path, "bounded")
    assert loaded == manifest
    assert manifest["status"] == "complete"
    assert seen == list(gold.PRODUCT_SCHEMAS)
    assert resource_config == unchanged_config
    for name, expected_table in expected.items():
        assert tables[name].equals(expected_table, check_metadata=True)
        assert (
            manifest["products"][name]["sql_sha256"]
            == original["products"][name]["sql_sha256"]
        )
    for field in (
        "covered_days",
        "coverage",
        "input_partitions",
        "quality_exclusions",
        "observations_by_day",
        "observation_min_utc",
        "observation_max_utc",
    ):
        assert manifest[field] == original[field]


def test_preparation_forwards_resource_configuration(
    tmp_path: Path,
    gold_inputs: GoldInputs,
    resource_config: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = gold.compute_product(gold_inputs["table"], "demand_daily")
    real_compute = gold.compute_product
    calls = []

    def compute(
        table: pa.Table,
        product: Product,
        *,
        sql_directory: Path | None,
        duckdb_config: dict[str, object],
    ) -> pa.Table:
        calls.append(duckdb_config)
        return real_compute(
            table, product, sql_directory=sql_directory, duckdb_config=duckdb_config
        )

    monkeypatch.setattr(gold, "compute_product", compute)
    path = gold.prepare_gold_product(
        tmp_path,
        "demand_daily",
        "prepared",
        days=gold_inputs["days"],
        duckdb_config=resource_config,
    )
    assert calls == [resource_config]
    assert pq.ParquetFile(path).read().replace_schema_metadata(None).equals(expected)


@pytest.mark.parametrize(
    "config",
    [
        {"TimeZone": "Europe/London"},
        {"default_collation": "nocase"},
        {"allow_unsigned_extensions": True},
        {"memory_limit": "-1"},
        {"memory_limit": "unlimited"},
        {"memory_limit": "1iB"},
        {"memory_limit": 512},
        {"memory_limit": "0B"},
        {"memory_limit": "513MiB"},
        {"memory_limit": "536870913B"},
        {"memory_limit": "537MB"},
        {"memory_limit": "1GiB"},
        {"memory_limit": "1TB"},
        {"memory_limit": None},
        {"max_temp_directory_size": "256MiB"},
        {"max_temp_directory_size": "0MiB"},
        {"max_temp_directory_size": "256"},
        {"threads": 0},
        {"threads": -1},
        {"threads": True},
        {"threads": 1.5},
        {"threads": 2},
        {"threads": None},
        {"temp_directory": "/tmp/spill"},
        {"temp_directory": "relative/spill"},
        {"temp_directory": ".tmp"},
        {"temp_directory": "."},
        {"temp_directory": "None"},
        {"temp_directory": "NULL"},
        {"temp_directory": "\x00"},
        {"temp_directory": "/tmp/spill\x00"},
        {"temp_directory": None},
        {"temp_directory": Path("/tmp/spill")},
        {"temp_directory": False},
        {"temp_directory": b""},
        [],
        "",
    ],
)
def test_unsafe_configuration_is_rejected_before_connecting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, config: object
) -> None:
    def connect(*args: object, **kwargs: object) -> None:
        pytest.fail("Unsafe configuration must not open a DuckDB connection")

    monkeypatch.setattr(gold.duckdb, "connect", connect)
    with pytest.raises(gold.GoldError, match="DuckDB"):
        gold._compute_query(
            pa.Table.from_pylist([], schema=SILVER_SCHEMA),
            "demand_daily",
            "SELECT * FROM silver",
            duckdb_config=config,
        )
    assert list(tmp_path.iterdir()) == []


def test_resource_failure_closes_connection_and_does_not_authorize_publication(
    tmp_path: Path,
    gold_inputs: GoldInputs,
    resource_config: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resource_config["memory_limit"] = "1B"
    real_connect = duckdb.connect
    connections = []

    def connect(database: str, **kwargs: object) -> duckdb.DuckDBPyConnection:
        connection = real_connect(database, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(gold.duckdb, "connect", connect)
    statuses = []
    with pytest.raises(duckdb.OutOfMemoryException) as error:
        gold_store.publish_gold(
            tmp_path,
            "too-small",
            days=gold_inputs["days"],
            duckdb_config=resource_config,
            on_progress=lambda manifest: statuses.append(manifest["status"]),
        )
    frame = [entry for entry in error.traceback if entry.name == "_compute_query"][-1]
    assert str(frame.statement).strip() == (
        "result = connection.execute(query).to_arrow_table()"
    )
    assert frame.locals["query"] == (
        gold.DEFAULT_SQL_DIRECTORY / "demand_daily.sql"
    ).read_text(encoding="utf-8")
    assert statuses == ["incomplete", "failed"]
    assert len(connections) == 1
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        connections[0].execute("SELECT 1")
    manifest = read_json(output_root(tmp_path, "sample"), "control/gold/too-small.json")
    assert manifest["status"] == "failed"
    assert manifest["error"] == "OutOfMemoryException"
    assert manifest["products"] == {}
    assert not (tmp_path / "duckdb-spill").exists()
    with pytest.raises(gold.GoldError, match="Only complete"):
        gold_store.load_gold(tmp_path, "too-small")
