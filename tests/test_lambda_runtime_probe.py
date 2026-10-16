"""Stdlib-only helper tests; run directly to avoid project conftest/runtime imports."""

import json
import runpy
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

PROBE = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "scripts/check_lambda_runtime.py")
)


class FakeClientError(Exception):
    def __init__(self, response, operation):
        self.response = response
        self.operation = operation


class RuntimeProbeHelpers(unittest.TestCase):
    def test_disk_objects_persist_and_missing_calls_are_sdk_shaped(self):
        with TemporaryDirectory() as directory:
            root = Path(directory).resolve() / "objects"
            client = PROBE["DiskS3"](root, FakeClientError)
            arguments = {"Bucket": "fixture-bucket", "Key": "nyc311/sample/page.json"}
            with self.assertRaises(FakeClientError) as missing:
                client.head_object(**arguments)
            self.assertEqual(missing.exception.response["Error"]["Code"], "404")
            with self.assertRaises(FakeClientError) as missing:
                client.get_object(**arguments)
            self.assertEqual(missing.exception.operation, "GetObject")
            self.assertEqual(missing.exception.response["Error"]["Code"], "NoSuchKey")
            client.put_object(**arguments, Body=b"fixture")
            fresh = PROBE["DiskS3"](root, FakeClientError)
            self.assertEqual(fresh.head_object(**arguments)["ContentLength"], 7)
            with fresh.get_object(**arguments)["Body"] as body:
                self.assertEqual(body.read(), b"fixture")

    def test_noncanonical_keys_and_symlink_escapes_are_rejected(self):
        with TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            client = PROBE["DiskS3"](root, FakeClientError)
            for key in (
                "",
                "/absolute",
                "../escape",
                "a/../b",
                "a//b",
                "a/./b",
                "a/",
                "a\\b",
            ):
                with self.subTest(key=key), self.assertRaises(ValueError):
                    client.path("fixture-bucket", key)
            with self.assertRaises(ValueError):
                client.path("another-bucket", "key")
            (root / "linked").symlink_to(root.parent, target_is_directory=True)
            with self.assertRaises(ValueError):
                client.path("fixture-bucket", "linked/escape")

    def test_tmp_allocation_uses_free_not_available_blocks(self):
        stats = SimpleNamespace(f_blocks=100, f_bfree=97, f_bavail=50, f_frsize=4096)
        with patch("os.statvfs", return_value=stats), patch("os.walk", return_value=[]):
            sampler = PROBE["TmpSampler"]()
            self.assertEqual(sampler.baseline["allocated_bytes"], 3 * 4096)
            stats.f_bfree = 90
            sampler.sample()
            self.assertEqual(sampler.allocated_peak, 10 * 4096)
            self.assertEqual(sampler.logical_peak, 0)

    def test_fixture_count_selected_fields_and_cursor_pages(self):
        httpx = ModuleType("httpx")
        httpx.MockTransport = lambda respond: respond
        httpx.Response = lambda status, *, json: SimpleNamespace(
            status=status, json=json
        )
        package = ModuleType("nyc311")
        package.__path__ = []
        contract = ModuleType("nyc311.source_contract")
        contract.SELECTED_FIELDS = ("unique_key", "created_date")
        modules = {
            "httpx": httpx,
            "nyc311": package,
            "nyc311.source_contract": contract,
        }
        with TemporaryDirectory() as directory, patch.dict(sys.modules, modules):
            fixture = Path(directory) / "fixture.json"
            fixture.write_text(
                json.dumps(
                    [
                        {
                            "unique_key": key,
                            "created_date": "2026-09-01T00:00:00",
                            "extra": "omit",
                        }
                        for key in ("c", "a'b", "a")
                    ]
                )
            )
            respond = PROBE["fixture_transport"](fixture)
            params = {
                "$where": "created_date >= '2026-09-01T00:00:00' AND created_date < '2026-09-02T00:00:00'",
                "$select": "count(*) as n",
                "$order": "unique_key ASC",
                "$limit": "2",
            }
            request = SimpleNamespace(url=SimpleNamespace(params=params))
            self.assertEqual(respond(request).json, [{"n": "3"}])
            params["$select"] = "unique_key,created_date"
            page = respond(request).json
            self.assertEqual([row["unique_key"] for row in page], ["a", "a'b"])
            self.assertNotIn("extra", page[0])
            params["$where"] += " AND unique_key > 'a''b'"
            self.assertEqual(respond(request).json[0]["unique_key"], "c")
            params["$select"] = "*"
            with self.assertRaises(AssertionError):
                respond(request)

    def test_origin_audit_rejects_image_packages_and_external_namespaces(self):
        audit = PROBE["module_origins"]
        modules = {
            "native": SimpleNamespace(__file__="/var/task/native.so"),
            "json": SimpleNamespace(
                __file__="/var/lang/lib/python3.12/json/__init__.py"
            ),
            "_sysconfigdata_test": SimpleNamespace(
                __file__="/var/lang/lib/python3.12/_sysconfigdata_test.py"
            ),
            "boto3": SimpleNamespace(
                __file__="/var/lang/lib/python3.12/site-packages/boto3/__init__.py"
            ),
            "namespace": SimpleNamespace(__path__=["/outside/namespace"]),
        }
        fake_sys = SimpleNamespace(modules=modules, stdlib_module_names={"json"})
        with (
            patch.dict(audit.__globals__, {"sys": fake_sys}),
            patch("sysconfig.get_path", return_value="/var/lang/lib/python3.12"),
        ):
            origins = audit()
        self.assertEqual(set(origins["payload_modules"]), {"native"})
        self.assertEqual(set(origins["violations"]), {"boto3", "namespace"})


if __name__ == "__main__":
    unittest.main()
