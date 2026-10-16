from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from nyc311 import catalog
from nyc311.catalog import (
    GOLD_TABLES,
    CatalogError,
    catalog_database,
    gold_location,
    publish_gold_locations,
)

DATABASE = "nyc311"


def glue_table(name: str) -> dict:
    """A Glue GetTable response including read-only fields the update must drop."""
    return {
        "Name": name,
        "Description": f"{name} product",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
        "PartitionKeys": [],
        "CreateTime": datetime(2026, 9, 1, tzinfo=timezone.utc),
        "UpdateTime": datetime(2026, 9, 1, tzinfo=timezone.utc),
        "CreatedBy": "arn:aws:iam::000000000000:role/previous",
        "StorageDescriptor": {
            "Location": f"s3://bucket/nyc311/published/gold/run=old/{name}/",
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": (
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
            ),
            "SerdeInfo": {
                "SerializationLibrary": (
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                ),
                "Parameters": {"serialization.format": "1"},
            },
            "Columns": [{"Name": "created_date", "Type": "date"}],
        },
    }


class MemoryGlue:
    """SDK-shaped, credential-free Glue catalog; records every call."""

    def __init__(self, database: str = DATABASE) -> None:
        self.database = database
        self.calls = []
        self.fail_table = None
        self.tables = {name: glue_table(name) for name in GOLD_TABLES}

    def get_table(self, *, DatabaseName: str, Name: str) -> dict:
        self.calls.append(("get", DatabaseName, Name))
        if DatabaseName != self.database or Name not in self.tables:
            raise ClientError(
                {"Error": {"Code": "EntityNotFoundException"}}, "GetTable"
            )
        return {"Table": dict(self.tables[Name])}

    def update_table(self, *, DatabaseName: str, TableInput: dict) -> dict:
        self.calls.append(("update", DatabaseName, dict(TableInput)))
        if DatabaseName != self.database:
            raise ClientError(
                {"Error": {"Code": "EntityNotFoundException"}}, "UpdateTable"
            )
        if TableInput["Name"] == self.fail_table:
            raise ClientError({"Error": {"Code": "InternalError"}}, "UpdateTable")
        self.tables[TableInput["Name"]] = dict(TableInput)
        return {}


def test_publish_points_every_table_at_the_selected_run() -> None:
    client = MemoryGlue()
    updated = publish_gold_locations(
        client,
        database=DATABASE,
        bucket="data-bucket",
        prefix="nyc311",
        mode="published",
        run_id="september-2026-001",
    )
    assert updated == {
        product: (
            f"s3://data-bucket/nyc311/published/gold/run=september-2026-001/{product}/"
        )
        for product in GOLD_TABLES
    }
    updates = [call for call in client.calls if call[0] == "update"]
    assert [call[2]["Name"] for call in updates] == list(GOLD_TABLES)
    for _, _, table_input in updates:
        assert set(table_input) == {
            "Name",
            "Description",
            "TableType",
            "Parameters",
            "StorageDescriptor",
            "PartitionKeys",
        }
        assert table_input["Parameters"]["classification"] == "parquet"
        assert table_input["StorageDescriptor"]["Columns"] == [
            {"Name": "created_date", "Type": "date"}
        ]


def test_publish_without_prefix_uses_bucket_root() -> None:
    client = MemoryGlue()
    updated = publish_gold_locations(
        client,
        database=DATABASE,
        bucket="data-bucket",
        prefix="",
        mode="published",
        run_id="september-2026-001",
    )
    assert updated["demand_daily"] == (
        "s3://data-bucket/published/gold/run=september-2026-001/demand_daily/"
    )


def test_publish_can_target_a_subset_of_tables() -> None:
    client = MemoryGlue()
    updated = publish_gold_locations(
        client,
        database=DATABASE,
        bucket="data-bucket",
        prefix="nyc311",
        mode="published",
        run_id="september-2026-001",
        products=("demand_daily",),
    )
    assert set(updated) == {"demand_daily"}
    assert [call[2]["Name"] for call in client.calls if call[0] == "update"] == [
        "demand_daily"
    ]


def test_missing_table_fails_closed_before_any_update() -> None:
    client = MemoryGlue()
    del client.tables["closure_duration"]
    with pytest.raises(CatalogError, match="closure_duration"):
        publish_gold_locations(
            client,
            database=DATABASE,
            bucket="data-bucket",
            prefix="nyc311",
            mode="published",
            run_id="september-2026-001",
        )
    assert [call for call in client.calls if call[0] == "update"] == []


def test_failed_update_raises_and_is_reported() -> None:
    client = MemoryGlue()
    client.fail_table = "closure_duration"
    with pytest.raises(CatalogError, match="could not be updated"):
        publish_gold_locations(
            client,
            database=DATABASE,
            bucket="data-bucket",
            prefix="nyc311",
            mode="published",
            run_id="september-2026-001",
        )
    failed = client.calls[-1][2]["Name"]
    assert failed == "closure_duration"


def test_without_description_the_update_omits_it() -> None:
    client = MemoryGlue()
    client.tables["demand_daily"].pop("Description")
    publish_gold_locations(
        client,
        database=DATABASE,
        bucket="data-bucket",
        prefix="nyc311",
        mode="published",
        run_id="september-2026-001",
        products=("demand_daily",),
    )
    update = next(call for call in client.calls if call[0] == "update")
    assert "Description" not in update[2]


@pytest.mark.parametrize("mode", ["sample", "", None])
def test_only_the_published_namespace_is_catalogued(mode) -> None:
    with pytest.raises(ValueError, match="published"):
        gold_location("data-bucket", "nyc311", mode, "run-1", "demand_daily")


@pytest.mark.parametrize(
    "bucket,prefix,run_id,product",
    [
        ("", "nyc311", "run-1", "demand_daily"),
        ("bad/name", "nyc311", "run-1", "demand_daily"),
        ("data-bucket", "../escape", "run-1", "demand_daily"),
        ("data-bucket", "nyc311", "/absolute", "demand_daily"),
        ("data-bucket", "nyc311", "run-1", "unknown_product"),
    ],
)
def test_location_components_are_validated(bucket, prefix, run_id, product) -> None:
    with pytest.raises(ValueError):
        gold_location(bucket, prefix, "published", run_id, product)


def test_catalog_request_requires_exactly_a_database_name() -> None:
    assert catalog_database({"database": "nyc311"}) == "nyc311"
    for bad in (
        None,
        [],
        {},
        {"database": "nyc311", "run_id": "extra"},
        {"name": "nyc311"},
        {"database": "bad-name"},
        {"database": ""},
        {"database": 7},
    ):
        with pytest.raises(ValueError):
            catalog_database(bad)


def test_gold_tables_match_the_four_products() -> None:
    assert catalog.GOLD_TABLES == (
        "demand_daily",
        "closure_duration",
        "outstanding_requests",
        "channel_usage",
    )
