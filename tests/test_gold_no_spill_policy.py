from contextlib import contextmanager
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_gold_store import FixtureInput, metrics_input
from test_lambda_stages import MemoryS3

from nyc311 import gold, lambda_common, lambda_gold
from nyc311.lambda_runtime import DUCKDB_LIMITS
from nyc311.s3_store import S3Store, temporary_workspace
from nyc311.storage import output_root


def test_lambda_gold_passes_policy_copy_and_keeps_staging_without_spill(
    tmp_path: Path, silver_input: FixtureInput, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert DUCKDB_LIMITS == {
        "memory_limit": "512MiB",
        "threads": 1,
        "temp_directory": "",
    }
    days = metrics_input(tmp_path, silver_input)
    inputs = gold.load_gold_inputs(tmp_path, days=days)
    root = output_root(tmp_path, "sample")
    client = MemoryS3()
    store = S3Store(client, "fixture-bucket", "sample")
    for path in root.rglob("*"):
        if path.is_file():
            store.upload(root, path.relative_to(root).as_posix())
    spaces = []

    @contextmanager
    def workspace():
        with temporary_workspace(tmp_path) as path:
            spaces.append(path)
            yield path
            assert not (path / "duckdb-spill").exists()
            assert not (path / ".tmp").exists()

    real_publish = lambda_gold.publish_gold
    configs = []

    def publish(base: Path, *args: object, **kwargs: object):
        config = kwargs["duckdb_config"]
        assert config == DUCKDB_LIMITS
        assert config is not DUCKDB_LIMITS
        configs.append(config)
        staged = output_root(base, "sample")
        assert staged.is_dir()
        assert list(staged.glob("silver/**/*.parquet"))
        return real_publish(base, *args, **kwargs)

    monkeypatch.setattr(lambda_common, "create_s3_client", lambda: client)
    monkeypatch.setattr(lambda_gold, "temporary_workspace", workspace)
    monkeypatch.setattr(lambda_gold, "SQL_DIRECTORY", gold.DEFAULT_SQL_DIRECTORY)
    monkeypatch.setattr(lambda_gold, "publish_gold", publish)
    result = lambda_gold.handler(
        {
            "bucket": "fixture-bucket",
            "mode": "sample",
            "run_id": "no-spill",
            "days": [day.isoformat() for day in days],
        },
        None,
    )
    assert result["status"] == "complete"
    assert len(configs) == len(spaces) == 1
    assert not spaces[0].exists()
    manifest = store.read_json(result["gold_manifest_key"])
    assert manifest["status"] == "complete"
    assert set(manifest["products"]) == set(gold.PRODUCT_SCHEMAS)
    for product, descriptor in manifest["products"].items():
        content = client.objects[store.key(descriptor["key"])]
        actual = pq.ParquetFile(pa.BufferReader(content)).read()
        expected = gold.compute_product(inputs["table"], product)
        assert actual.replace_schema_metadata(None).equals(expected)
    assert DUCKDB_LIMITS == configs[0]
