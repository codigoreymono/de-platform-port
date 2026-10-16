import gzip
import io
import json
from collections.abc import Callable
from pathlib import Path

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from botocore.stub import Stubber

from nyc311.manifests import BronzeManifest
from nyc311.s3_store import S3Store, temporary_workspace
from nyc311.silver import transform_bronze
from nyc311.source import SourcePage
from nyc311.storage import (
    manifest_key,
    output_root,
    read_bytes,
    write_bytes,
    write_json,
)


def unsigned_client():
    # UNSIGNED bypasses credential resolution; Stubber intercepts every request.
    return boto3.client(
        "s3", region_name="us-east-1", config=Config(signature_version=UNSIGNED)
    )


def test_object_write_read_and_local_transfer_use_explicit_keys(tmp_path: Path) -> None:
    client = unsigned_client()
    store = S3Store(client, "fixture-bucket", "nyc311/sample")
    content = b'{"original": "bytes"}\n'
    key = "bronze/run=fixture/page-0001.json.gz"
    stream = io.BytesIO(content)
    with Stubber(client) as stub:
        stub.add_response(
            "put_object",
            {"ETag": "fixture"},
            {
                "Bucket": "fixture-bucket",
                "Key": f"nyc311/sample/{key}",
                "Body": content,
            },
        )
        stub.add_response(
            "get_object",
            {"Body": StreamingBody(stream, len(content))},
            {"Bucket": "fixture-bucket", "Key": f"nyc311/sample/{key}"},
        )
        write_bytes(tmp_path / "local", key, content)
        store.upload(tmp_path / "local", key)
        store.download(key, tmp_path / "downloaded")
        stub.assert_no_pending_responses()
    assert stream.closed
    assert read_bytes(tmp_path / "downloaded", key) == content


@pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
def test_missing_object_is_not_a_directory(code: str) -> None:
    client = unsigned_client()
    with Stubber(client) as stub:
        stub.add_client_error(
            "head_object",
            service_error_code=code,
            http_status_code=404,
            expected_params={
                "Bucket": "fixture-bucket",
                "Key": "control/silver/2026-09-01.json",
            },
        )
        assert not S3Store(client, "fixture-bucket").exists(
            "control/silver/2026-09-01.json"
        )
        stub.assert_no_pending_responses()


def test_access_denied_is_not_treated_as_absence() -> None:
    client = unsigned_client()
    with Stubber(client) as stub:
        stub.add_client_error(
            "head_object",
            service_error_code="AccessDenied",
            http_status_code=403,
            expected_params={"Bucket": "fixture-bucket", "Key": "control/run.json"},
        )
        with pytest.raises(ClientError):
            S3Store(client, "fixture-bucket").exists("control/run.json")


def test_failed_download_does_not_create_local_file(tmp_path: Path) -> None:
    client = unsigned_client()
    with Stubber(client) as stub:
        stub.add_client_error(
            "get_object",
            service_error_code="NoSuchKey",
            http_status_code=404,
            expected_params={"Bucket": "fixture-bucket", "Key": "missing.json"},
        )
        with pytest.raises(ClientError):
            S3Store(client, "fixture-bucket").download("missing.json", tmp_path)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "key", ["", ".", "../outside", "/absolute", "a/../b", "a//b", "a\\b"]
)
def test_invalid_keys_fail_before_any_sdk_call(key: str) -> None:
    class NoCalls:
        def __getattr__(self, name: str):
            raise AssertionError("Invalid keys must not access the SDK")

    with pytest.raises(ValueError):
        S3Store(NoCalls(), "fixture-bucket").read(key)


def test_json_object_contract_and_write_errors(tmp_path: Path) -> None:
    client = unsigned_client()
    value = {"status": "incomplete", "pages": []}
    expected = b'{\n  "status": "incomplete",\n  "pages": []\n}\n'
    with Stubber(client) as stub:
        stub.add_response(
            "put_object",
            {},
            {
                "Bucket": "fixture-bucket",
                "Key": "control/bronze.json",
                "Body": expected,
            },
        )
        stub.add_client_error(
            "put_object",
            service_error_code="InternalError",
            http_status_code=500,
            expected_params={
                "Bucket": "fixture-bucket",
                "Key": "failed.json",
                "Body": b"failed",
            },
        )
        store = S3Store(client, "fixture-bucket")
        store.write_json("control/bronze.json", value)
        with pytest.raises(ClientError):
            store.write("failed.json", b"failed")
        stub.assert_no_pending_responses()


def test_local_and_simulated_s3_saved_bronze_transformations_are_equivalent(
    tmp_path: Path,
    silver_input: Callable[..., tuple[BronzeManifest, list[SourcePage]]],
) -> None:
    samples = Path(__file__).resolve().parents[1] / "data/samples"
    groups = [
        json.loads((samples / name).read_bytes())
        for name in ("pagination-001.json", "pagination-002.json")
    ]
    manifest, pages = silver_input(*groups)
    manifest["page_size"] = 2
    root = output_root(tmp_path / "local", "sample")
    for page, reference in zip(pages, manifest["pages"]):
        write_bytes(root, reference["key"], gzip.compress(page["content"], mtime=0))
    key = manifest_key(manifest)
    write_json(root, key, manifest)
    expected = transform_bronze(tmp_path / "local", key)
    keys = [key, *[reference["key"] for reference in manifest["pages"]]]
    client = unsigned_client()
    store = S3Store(client, "fixture-bucket", "nyc311/sample")
    with Stubber(client) as stub:
        for reference in keys:
            content = read_bytes(root, reference)
            stub.add_response(
                "put_object",
                {},
                {
                    "Bucket": "fixture-bucket",
                    "Key": store.key(reference),
                    "Body": content,
                },
            )
        for reference in keys:
            content = read_bytes(root, reference)
            stub.add_response(
                "get_object",
                {"Body": StreamingBody(io.BytesIO(content), len(content))},
                {"Bucket": "fixture-bucket", "Key": store.key(reference)},
            )
        for reference in keys:
            store.upload(root, reference)
        with temporary_workspace(tmp_path) as workspace:
            for reference in keys:
                store.download(reference, output_root(workspace, "sample"))
            actual = transform_bronze(workspace, key)
            assert actual["table"].equals(expected["table"], check_metadata=True)
            assert actual["quarantine"] == expected["quarantine"]
            assert actual["quality_report"] == expected["quality_report"]
        assert not workspace.exists()
        stub.assert_no_pending_responses()


def test_invocation_workspace_cleans_files_on_error(tmp_path: Path) -> None:
    with pytest.raises(OSError):
        with temporary_workspace(tmp_path) as workspace:
            (workspace / "partial.tmp").write_bytes(b"partial")
            raise OSError("interrupted")
    assert not workspace.exists()
