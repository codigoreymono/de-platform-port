"""Offline stdlib-only tests: never invoke a handler or import its dependencies."""

import gzip
import io
import json
import runpy
import socket
import sys
import unittest
from datetime import UTC
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

PROBE = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "scripts/benchmark_lambda.py")
)
HELPER = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "scripts/check_lambda_runtime.py")
)


class FakeClientError(Exception):
    def __init__(self, response, operation):
        self.response, self.operation = response, operation


class Params(dict):
    def multi_items(self):
        return list(self.items())


class URL:
    def __init__(self, params):
        self.params = Params(params)

    def __str__(self):
        return "https://fixture.invalid/source?query"


class Column(list):
    def to_pylist(self):
        return list(self)

    def equals(self, other):
        return self == other


class Schema(list):
    def equals(self, other, *, check_metadata):
        return self == other


class Table(dict):
    schema = Schema(
        [
            SimpleNamespace(name="float", type="floating"),
            SimpleNamespace(name="exact", type="integer"),
        ]
    )

    @property
    def num_rows(self):
        return len(self["float"])


class LambdaBenchmarkTests(unittest.TestCase):
    def setUp(self):
        httpx = ModuleType("httpx")
        httpx.MockTransport = lambda respond: respond
        httpx.Response = lambda status, *, content: SimpleNamespace(
            status=status, content=content
        )
        package = ModuleType("nyc311")
        package.__path__ = []
        contract = ModuleType("nyc311.source_contract")
        contract.SELECTED_FIELDS = ("unique_key", "created_date")
        contract.ENDPOINT = "https://fixture.invalid/source"
        self.modules = patch.dict(
            sys.modules,
            {"httpx": httpx, "nyc311": package, "nyc311.source_contract": contract},
        )
        self.modules.start()
        self.addCleanup(self.modules.stop)
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.payloads = [
            b'[ {"unique_key":"a"}, {"unique_key":"a\'b"} ]',
            b'[{"unique_key":"z"}]',
        ]
        pages = []
        for index, content in enumerate(self.payloads, start=1):
            key = f"bronze/run=september-2026-001/created_date=2026-09-29/page-{index:04d}.json.gz"
            path = self.root / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(gzip.compress(content, mtime=0))
            pages.append(
                {
                    "key": key,
                    "sha256": sha256(content).hexdigest(),
                    "row_count": 3 - index,
                    "last_cursor": "a'b" if index == 1 else "z",
                    "observed_at_utc": f"2026-10-13T12:00:0{index}+00:00",
                }
            )
        self.manifest = {
            "start_date": "2026-09-29",
            "end_date": "2026-09-30",
            "pages": pages,
            "page_size": 2,
            "source_count_before": 3,
            "source_count_after": 3,
        }
        self.where = "created_date >= '2026-09-29T00:00:00' AND created_date < '2026-09-30T00:00:00'"

    def request(self, params):
        return SimpleNamespace(method="GET", url=URL(params))

    def count(self):
        return {"$select": "count(*) as n", "$where": self.where}

    def page(self, cursor=False):
        return {
            "$select": "unique_key,created_date",
            "$where": self.where + (" AND unique_key > 'a''b'" if cursor else ""),
            "$order": "unique_key ASC",
            "$limit": "2",
        }

    def test_fresh_original_pagination_exact_bytes_counts_clock_and_io(self):
        replay = PROBE["OriginalPages"](self.root, self.manifest)
        respond, clock = replay.transport(), replay.clock()
        self.assertEqual(
            json.loads(respond(self.request(self.count())).content), [{"n": "3"}]
        )
        for index in range(2):
            response = respond(self.request(self.page(bool(index))))
            self.assertEqual(response.content, self.payloads[index])
            self.assertEqual(
                clock.now(UTC).isoformat(),
                self.manifest["pages"][index]["observed_at_utc"],
            )
        self.assertEqual(
            json.loads(respond(self.request(self.count())).content), [{"n": "3"}]
        )
        replay.complete()
        self.assertEqual(
            replay.stats["page_response_bytes"], sum(map(len, self.payloads))
        )
        self.assertEqual(replay.stats["count_requests"], 2)
        self.assertEqual(replay.stats["page_requests"], 2)
        self.assertEqual(
            replay.stats["source_counts"],
            [
                {"position": "source_count_before", "count": 3},
                {"position": "source_count_after", "count": 3},
            ],
        )
        self.assertEqual(
            replay.stats["gzip_file_read_bytes"],
            sum((self.root / p["key"]).stat().st_size for p in self.manifest["pages"]),
        )
        with self.assertRaises(AssertionError):
            respond(self.request(self.page()))
        with self.assertRaises(AssertionError):
            clock.now(UTC)

    def test_bad_day_cursor_selection_limit_order_and_retries_are_rejected(self):
        for field, value in (
            ("$where", "wrong day"),
            ("$select", "*"),
            ("$limit", "5000"),
            ("$order", "unique_key DESC"),
            ("$offset", "0"),
        ):
            with self.subTest(field=field):
                replay = PROBE["OriginalPages"](self.root, self.manifest)
                respond = replay.transport()
                respond(self.request(self.count()))
                with self.assertRaises(AssertionError):
                    respond(self.request(self.page() | {field: value}))
                self.assertEqual(replay.stats["page_requests"], 0)
        replay = PROBE["OriginalPages"](self.root, self.manifest)
        respond = replay.transport()
        with self.assertRaises(AssertionError):
            respond(self.request(self.page()))
        respond(self.request(self.count()))
        with self.assertRaises(AssertionError):
            respond(self.request(self.count()))
        respond(self.request(self.page()))
        replay.clock().now(UTC)
        with self.assertRaises(AssertionError):
            respond(self.request(self.page()))
        with self.assertRaises(AssertionError):
            replay.complete()

    def test_digest_and_original_keys_prevent_substitution(self):
        for field, value in (
            ("sha256", "0" * 64),
            ("key", "../record.json.gz"),
            ("key", self.manifest["pages"][1]["key"]),
        ):
            with self.subTest(field=field, value=value):
                manifest = json.loads(json.dumps(self.manifest))
                manifest["pages"][0][field] = value
                replay = PROBE["OriginalPages"](self.root, manifest)
                respond = replay.transport()
                respond(self.request(self.count()))
                with self.assertRaises(AssertionError):
                    respond(self.request(self.page()))

    def test_count_queries_serve_distinct_original_counts_not_computed_rows(self):
        self.manifest["source_count_before"] = 4
        replay = PROBE["OriginalPages"](self.root, self.manifest)
        respond, clock = replay.transport(), replay.clock()
        self.assertEqual(
            json.loads(respond(self.request(self.count())).content), [{"n": "4"}]
        )
        for index in range(2):
            respond(self.request(self.page(bool(index))))
            clock.now(UTC)
        self.assertEqual(
            json.loads(respond(self.request(self.count())).content), [{"n": "3"}]
        )
        replay.complete()
        # The real source extractor, not the fake service, reconciles these counts.
        self.assertEqual([p["count"] for p in replay.stats["source_counts"]], [4, 3])

    def test_safe_paths_reject_aliases_and_symlinks(self):
        for key in ("", "../escape", "/absolute", "a//b", "a/./b", "a\\b", "a/"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                PROBE["safe_path"](self.root, key)
        (self.root / "link").symlink_to(self.root.parent, target_is_directory=True)
        with self.assertRaises(ValueError):
            PROBE["safe_path"](self.root, "link/escape")

    def test_s3_meter_does_not_retain_bodies_and_counts_failed_calls(self):
        client = PROBE["metered_s3"](HELPER, self.root / "objects", FakeClientError)
        kwargs = {"Bucket": "fixture-bucket", "Key": "nyc311/published/object"}
        with self.assertRaises(FakeClientError):
            client.head_object(**kwargs)
        with self.assertRaises(FakeClientError):
            client.get_object(**kwargs)
        client.put_object(**kwargs, Body=b"original")
        with client.get_object(**kwargs)["Body"] as body:
            self.assertEqual(body.read(), b"original")
        snapshot = client.snapshot()
        self.assertEqual(snapshot["read_bytes"], 8)
        self.assertEqual(snapshot["write_bytes"], 8)
        self.assertEqual(snapshot["calls"], {"head": 1, "get": 2, "put": 1})
        self.assertNotIn("original", json.dumps(snapshot))
        client.put_object(**kwargs, Body=b"changed")
        self.assertEqual(snapshot["write_bytes"], 8)

    def test_network_guards_never_reach_real_socket(self):
        names = (
            "getaddrinfo",
            "gethostbyname",
            "gethostbyname_ex",
            "gethostbyaddr",
            "getnameinfo",
            "create_connection",
        )
        methods = ("connect", "connect_ex", "sendto", "sendmsg")
        from contextlib import ExitStack

        with ExitStack() as stack:
            for name in names:
                stack.enter_context(patch.object(socket, name))
            for name in methods:
                stack.enter_context(patch.object(socket.socket, name))
            PROBE["guard_network"]()
            for name in names:
                with self.assertRaises(RuntimeError):
                    getattr(socket, name)("never.resolve.invalid")
            for name in methods:
                with self.assertRaises(RuntimeError):
                    getattr(socket.socket, name)(None, ("127.0.0.1", 443))

    def test_sdk_guards_cover_captured_classes_clients_and_credentials(self):
        class Session:
            pass

        class CoreSession:
            pass

        boto3, botocore = ModuleType("boto3"), ModuleType("botocore")
        boto3.session = ModuleType("boto3.session")
        botocore.session = ModuleType("botocore.session")
        boto3.Session = boto3.session.Session = Session
        botocore.session.Session = CoreSession
        with patch.dict(
            sys.modules,
            {
                "boto3": boto3,
                "boto3.session": boto3.session,
                "botocore": botocore,
                "botocore.session": botocore.session,
            },
        ):
            PROBE["guard_sdk"]()
            for target in (
                boto3.client,
                boto3.resource,
                boto3.Session,
                boto3.session.Session,
                Session,
                Session.client,
                Session.resource,
                Session.get_credentials,
                CoreSession,
                CoreSession.create_client,
                CoreSession.get_credentials,
                botocore.session.get_session,
            ):
                with self.assertRaises(RuntimeError):
                    target()

    def test_origin_audit_allows_only_own_main_payload_and_stdlib(self):
        main = SimpleNamespace(__file__="/checks/benchmark_lambda.py")
        modules = {
            "__main__": main,
            "json": SimpleNamespace(
                __file__="/var/lang/lib/python3.12/json/__init__.py"
            ),
            "payload": SimpleNamespace(__file__="/var/task/native.so"),
            "boto3": SimpleNamespace(
                __file__="/var/lang/lib/python3.12/site-packages/boto3/__init__.py"
            ),
            "helper": SimpleNamespace(__file__="/checks/check_lambda_runtime.py"),
            "foreign_namespace": SimpleNamespace(__path__=["/outside"]),
        }
        audit = PROBE["module_origins"]
        fake_sys = SimpleNamespace(modules=modules, stdlib_module_names={"json"})
        with (
            patch.dict(audit.__globals__, {"sys": fake_sys}),
            patch("sysconfig.get_path", return_value="/var/lang/lib/python3.12"),
        ):
            origins = audit()
        self.assertEqual(set(origins["payload_modules"]), {"payload"})
        self.assertEqual(
            set(origins["violations"]), {"boto3", "helper", "foreign_namespace"}
        )

    def test_gold_compare_float_tolerance_nan_null_and_exact_columns(self):
        arrow = ModuleType("pyarrow")
        arrow.types = SimpleNamespace(is_floating=lambda kind: kind == "floating")
        compare = PROBE["compare_gold"]
        left = Table(float=Column([1.0, float("nan"), None]), exact=Column([1, 2, 3]))
        right = Table(
            float=Column([1.0 + 1e-11, float("nan"), None]), exact=Column([1, 2, 3])
        )
        with patch.dict(sys.modules, {"pyarrow": arrow}):
            compare(left, right)
            for field, value in (
                ("float", Column([1.01, float("nan"), None])),
                ("float", Column([1.0, None, None])),
                ("exact", Column([1, 2, 4])),
            ):
                with self.subTest(field=field), self.assertRaises(AssertionError):
                    compare(left, Table(right | {field: value}))

    def test_cgroup_counters_are_raw_and_unavailable_is_not_zero(self):
        with patch.object(Path, "read_text", return_value="200000 100000\n"):
            snapshot = PROBE["cgroup_snapshot"]()
        self.assertEqual(snapshot["cpu.max"], "200000 100000")
        self.assertIn("memory.peak", snapshot)
        self.assertIn("memory.events", snapshot)
        with patch.object(Path, "read_text", side_effect=FileNotFoundError):
            snapshot = PROBE["cgroup_snapshot"]()
        self.assertEqual(snapshot["memory.peak"], {"unavailable": "FileNotFoundError"})

    def test_failure_traceback_never_serializes_exception_records(self):
        try:
            raise ValueError('sensitive {"unique_key":"actual record"}')
        except ValueError as error:
            result = PROBE["failure"](error)
        encoded = json.dumps(result)
        self.assertNotIn("actual record", encoded)
        self.assertNotIn("unique_key", encoded)
        self.assertEqual(result["type"], "ValueError")
        self.assertTrue(result["traceback"])

    def test_cli_day_run_and_full_month_contract(self):
        parse = PROBE["parse_args"]
        args = ["--stage", "ingest", "--run-id", "trial-01", "--output", "/evidence"]
        with patch("sys.stderr", new_callable=io.StringIO):
            for extra in (
                [],
                ["--day", "20260929"],
                ["--day", "2026-09-31"],
                ["--day", "2026-09-29", "--run-id", "../escape"],
                ["--day", "2026-09-29", "--run-id", "september-2026-001"],
            ):
                with self.assertRaises(SystemExit):
                    parse(args + extra)
        self.assertEqual(parse(args + ["--day", "2026-09-29"]).day, "2026-09-29")
        args[1] = "gold"
        self.assertIsNone(parse(args).day)
        with (
            patch("sys.stderr", new_callable=io.StringIO),
            self.assertRaises(SystemExit),
        ):
            parse(args + ["--day", "2026-09-29"])


if __name__ == "__main__":
    unittest.main()
