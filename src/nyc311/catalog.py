"""Point Glue Catalog Gold tables at one successful published run.

The catalog update is a pointer change performed only after the Gold artifacts
are already published and verified. A failure here never rewrites Gold; it only
leaves the run unsuccessful and the previous locations in place for recovery.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import TYPE_CHECKING

from nyc311.gold import PRODUCT_SCHEMAS
from nyc311.s3_store import _relative_key

if TYPE_CHECKING:
    from botocore.client import BaseClient

GOLD_TABLES: tuple[str, ...] = tuple(PRODUCT_SCHEMAS)
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")
_DATABASE = re.compile(r"[A-Za-z0-9_]{1,255}")


class CatalogError(RuntimeError):
    """The Glue Catalog cannot be pointed at a successful published run."""


def catalog_database(catalog: object) -> str:
    """Validate the optional catalog request carried by a Gold event."""
    if not isinstance(catalog, dict) or set(catalog) != {"database"}:
        raise ValueError("Catalog publication accepts only a database name")
    database = catalog["database"]
    if not isinstance(database, str) or not _DATABASE.fullmatch(database):
        raise ValueError("Catalog database name is invalid")
    return database


def gold_location(
    bucket: str, prefix: str, mode: str, run_id: str, product: str
) -> str:
    if not isinstance(bucket, str) or not bucket or "/" in bucket:
        raise ValueError("Bucket must be a name")
    if prefix:
        _relative_key(prefix)
    if mode != "published":
        raise ValueError("Only the official published namespace is catalogued")
    if not isinstance(run_id, str) or not _NAME.fullmatch(run_id):
        raise ValueError("Run identifier must be a safe path component")
    if product not in GOLD_TABLES:
        raise ValueError("Only the four whole-cohort Gold products are catalogued")
    base = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"
    return f"{base}/{mode}/gold/run={run_id}/{product}/"


def publish_gold_locations(
    client: BaseClient,
    *,
    database: str,
    bucket: str,
    prefix: str,
    mode: str,
    run_id: str,
    products: Sequence[str] = GOLD_TABLES,
) -> dict[str, str]:
    """Update each Gold table location to the selected run's product prefix."""
    if not isinstance(database, str) or not _DATABASE.fullmatch(database):
        raise ValueError("Catalog database name is invalid")
    # Read and validate every table before the first write: a missing table
    # fails cleanly without leaving the catalog half-repointed.
    prepared: list[tuple[str, str, dict[str, object]]] = []
    for product in products:
        location = gold_location(bucket, prefix, mode, run_id, product)
        try:
            table = client.get_table(DatabaseName=database, Name=product)["Table"]
        except Exception as error:
            raise CatalogError(
                f"Glue table {product!r} is unavailable in database {database!r}"
            ) from error
        storage = dict(table.get("StorageDescriptor") or {})
        if not storage:
            raise CatalogError(f"Glue table {product!r} has no storage descriptor")
        storage["Location"] = location
        table_input: dict[str, object] = {
            "Name": table["Name"],
            "TableType": table.get("TableType", "EXTERNAL_TABLE"),
            "Parameters": table.get("Parameters", {}),
            "StorageDescriptor": storage,
            "PartitionKeys": table.get("PartitionKeys", []),
        }
        if table.get("Description"):
            table_input["Description"] = table["Description"]
        prepared.append((product, location, table_input))
    updated: dict[str, str] = {}
    for product, location, table_input in prepared:
        try:
            client.update_table(DatabaseName=database, TableInput=table_input)
        except Exception as error:
            raise CatalogError(
                f"Glue table {product!r} could not be updated"
            ) from error
        updated[product] = location
    return updated
