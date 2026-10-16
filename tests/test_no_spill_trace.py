"""Synthetic strace contracts only: no containers, native imports or recapture."""

import hashlib
import json
import runpy
from pathlib import Path

import pytest

MODULE = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "scripts/audit_no_spill_trace.py")
)
TraceAudit = MODULE["TraceAudit"]


def report(case="negative"):
    return {"case": case, "run_id": f"nospill-{case}-trial1"}


def trace(*calls):
    return [
        f"{tid} 1792069647.{i:06d} {text}\n"
        for i, (tid, text) in enumerate(
            (call if isinstance(call, tuple) else (10, call) for call in calls)
        )
    ]


def audit(*calls, case="negative", metadata=None):
    return TraceAudit(metadata or report(case)).consume(trace(*calls))


def marker(label, boundary):
    path = f"/evidence/phases/{label}.{boundary}"
    return f'openat(AT_FDCWD</tmp/work>, "{path}", O_WRONLY|O_CREAT|O_EXCL, 0666) = 9<{path}>'


def test_raw_hex_shared_thread_fd_and_positive_proof():
    result = audit(
        marker("query", "begin"),
        'openat(AT_FDCWD</tmp/work>, "/tmp/positive-spill/a", O_RDWR|O_CREAT, 0666) = 3</tmp/positive-spill/a>',
        (11, "pwrite64(0x3, 0xabcdef, 0x20000, 0) = 0x20000"),
        marker("query", "end"),
        case="positive",
    )
    assert result.verdict() == "pass"
    event = next(e for e in result.events if e["op"] == "pwrite64")
    assert event["path"] == "/tmp/positive-spill/a"
    assert event["bytes"] == 131072
    assert event["tid"] == 11 and event["phases"] == ["query"]
    assert "abcdef" not in json.dumps(result.events)


@pytest.mark.parametrize(
    "path",
    [
        "/tmp/spill",
        "/dev/shm/hidden",
        ".hidden",
        "/var/task/failed",
        "/tmp/positive-spill-shadow/file",
        "/evidence/objects/nyc311/published/evil",
    ],
)
def test_all_failed_writable_open_attempts_and_transient_mutations_count(path):
    result = audit(
        f'openat(AT_FDCWD</tmp/work>, "{path}", O_WRONLY|O_CREAT, 0666) = -1 EROFS (Read-only file system)',
        f'unlink("{path}") = -1 ENOENT (No such file or directory)',
    )
    assert result.verdict() == "fail"
    assert len(result.events) == 2
    assert all(e["category"] == "FORBIDDEN" for e in result.events)


@pytest.mark.parametrize(
    "flags", ["O_RDWR", "O_WRONLY", "O_RDONLY|O_CREAT", "O_RDONLY|O_TRUNC"]
)
def test_access_modes_are_attempts_even_without_success(flags):
    assert audit(f'open("/missing", {flags}) = -1 ENOENT (Missing)').verdict() == "fail"


def test_successful_transient_file_create_write_unlink_is_retained():
    result = audit(
        'open("/dev/shm/.hidden", O_RDWR|O_CREAT, 0600) = 3</dev/shm/.hidden>',
        'unlink("/dev/shm/.hidden") = 0',
        "write(0x3, 0xaaaa, 0x10) = 0x10",
        "close(3</dev/shm/.hidden (deleted)>) = 0",
    )
    assert result.verdict() == "fail"
    assert [e["op"] for e in result.events] == ["open", "unlink", "write"]
    assert result.events[-1]["bytes"] == 16


def test_openat2_struct_flags_and_numeric_unknown_flags():
    result = audit(
        'openat2(AT_FDCWD</tmp/work>, "fallback", {flags=O_RDWR|O_CREAT, mode=0600, resolve=0}, 24) = -1 EROFS (Read-only)'
    )
    assert result.verdict() == "fail"
    assert result.events[0]["path"] == "/tmp/work/fallback"
    for call in (
        'open("/tmp/unknown", O_RDONLY|0x40000000) = 3',
        'open("/tmp/unknown", O_RDONLY|O_UNKNOWN) = 3',
        'openat2(AT_FDCWD</tmp/work>, "unknown", 0xaaaa, 24) = 3',
        "mmap(NULL, 4096, PROT_READ, MAP_PRIVATE|0x40000000, 3, 0) = 0x1000",
        "mmap(NULL, 4096, PROT_READ|0x2, MAP_SHARED, 3, 0) = 0x1000",
    ):
        assert audit(call).verdict() == "inconclusive"


def test_setup_relative_and_dirfd_paths_and_phase_scope():
    result = audit(
        marker("setup", "begin"),
        'openat(AT_FDCWD</tmp/work>, "./.writable", O_WRONLY|O_CREAT, 0666) = 3</tmp/work/.writable>',
        "write(0x3, 0xa, 0x1) = 0x1",
        "close(3</tmp/work/.writable>) = 0",
        'openat(AT_FDCWD</tmp/work>, "/tmp/work", O_RDONLY|O_DIRECTORY) = 4</tmp/work>',
        'unlinkat(4</tmp/work>, ".writable", 0) = 0',
        marker("setup", "end"),
    )
    assert result.verdict() == "pass"
    assert audit('unlink(".writable") = 0').verdict() == "fail"
    assert (
        audit(
            'openat(99, "relative", O_CREAT|O_WRONLY, 0600) = -1 EBADF (Bad fd)'
        ).verdict()
        == "inconclusive"
    )


def test_fd_reuse_close_dup_and_fcntl_are_global():
    result = audit(
        marker("setup", "begin"),
        'open("/tmp/writable", O_WRONLY|O_CREAT, 0666) = 3</tmp/writable>',
        (11, "dup(3</tmp/writable>) = 4</tmp/writable>"),
        "dup2(4</tmp/writable>, 5) = 5</tmp/writable>",
        "dup3(5</tmp/writable>, 6, O_CLOEXEC) = 6</tmp/writable>",
        "fcntl(6</tmp/writable>, F_DUPFD_CLOEXEC, 7) = 7</tmp/writable>",
        "close(3</tmp/writable>) = 0",
        'open("/tmp/new-spill", O_RDWR|O_CREAT, 0666) = 3</tmp/new-spill>',
        "write(0x7, 0xa, 0x1) = 0x1",
        "write(0x3, 0xa, 0x1) = 0x1",
    )
    writes = [e for e in result.events if e["op"] == "write"]
    assert [e["path"] for e in writes] == ["/tmp/writable", "/tmp/new-spill"]
    assert result.verdict() == "fail"


@pytest.mark.parametrize(
    "call",
    [
        "write(0x8, 0xa, 0x1) = 0x1",
        "pwritev2(0x8, 0xa, 0x1, 0, 0) = -1 EBADF (Bad fd)",
        "ftruncate(8, 4096) = 0",
        "dup(9) = 8",
    ],
)
def test_unmapped_source_fd_never_passes(call):
    calls = [call]
    if call.startswith("dup"):
        calls.append("write(0x8, 0xa, 0x1) = 0x1")
    result = audit(*calls)
    assert result.verdict() == "inconclusive"
    assert any(a["kind"] == "UNRESOLVED" for a in result.anomalies)


def test_stdio_allowed_only_until_closed_or_replaced():
    assert (
        audit("write(0x1, 0xa, 0x1) = 0x1", "writev(0x2, 0xa, 0x1) = 0x1").verdict()
        == "pass"
    )
    assert (
        audit("close(1) = 0", "write(0x1, 0xa, 0x1) = 0x1").verdict() == "inconclusive"
    )
    result = audit(
        'open("/spill", O_RDWR|O_CREAT, 0666) = 1</spill>', "write(0x1, 0xa, 0x1) = 0x1"
    )
    assert result.events[-1]["category"] == "FORBIDDEN"


def test_unfinished_resumed_per_tid_preserves_entry_fd_and_fd_reuse():
    result = audit(
        marker("setup", "begin"),
        'open("/tmp/writable", O_WRONLY|O_CREAT, 0666) = 3</tmp/writable>',
        (11, "write(0x3, 0xa, 0x1 <unfinished ...>"),
        "close(3</tmp/writable>) = 0",
        'open("/evidence/writable", O_WRONLY|O_CREAT, 0666) = 3</evidence/writable>',
        (11, "<... write resumed>) = 0x1"),
        (11, "close(3</evidence/writable> <unfinished ...>"),
        'open("/tmp/work/.writable", O_WRONLY|O_CREAT, 0666) = 3</tmp/work/.writable>',
        (11, "<... close resumed>) = 0"),
        "write(0x3, 0xa, 0x1) = 0x1",
    )
    writes = [e["path"] for e in result.events if e["op"] == "write"]
    assert writes == ["/tmp/writable", "/tmp/work/.writable"]
    assert not [a for a in result.anomalies if a["reason"] != "Unclosed phases"]


def test_interleaved_unfinished_open_and_other_tid_close():
    result = audit(
        'openat(AT_FDCWD</tmp/work>, "/tmp/positive-spill/a", O_RDWR|O_CREAT, 0666 <unfinished ...>',
        (11, "close(6</tmp>) = 0"),
        "<... openat resumed>) = 3</tmp/positive-spill/a>",
        "pwrite64(0x3, 0xa, 0x10, 0 <unfinished ...>",
        (11, 'stat("/tmp", {st_mode=S_IFDIR}) = 0'),
        "<... pwrite64 resumed>) = 0x10",
        case="positive",
    )
    assert result.verdict() == "pass"
    assert result.events[-1]["bytes"] == 16


@pytest.mark.parametrize(
    "calls",
    [
        ["<... write resumed>) = 0x1"],
        ["write(0x1, 0xa, 0x1 <unfinished ...>"],
        ["write(0x1, 0xa, 0x1 <unfinished ...>", "<... close resumed>) = 0"],
        ["strace: detached unexpectedly"],
        ['open("/truncated"..., O_WRONLY) = 3'],
        ['mystery_file_mutation("/tmp/foo") = 0'],
        ['execve("/bin/sh", [], []) = 0'],
        ['chdir("/tmp") = 0'],
        ["fcntl(3, F_UNKNOWN, 0) = 0"],
        ['write(0x1, "secret-data", 0xb) = 0xb'],
    ],
)
def test_unknown_truncated_or_nonraw_is_inconclusive(calls):
    result = audit(*calls)
    assert result.verdict() == "inconclusive"
    assert "secret-data" not in json.dumps(result.events + result.anomalies)


def test_readonly_libraries_private_writable_ram_and_shared_file_mmap():
    prefix = [
        'openat(AT_FDCWD</tmp/work>, "/var/task/native.so", O_RDONLY|O_CLOEXEC) = 3</var/task/native.so>'
    ]
    assert (
        audit(
            *prefix,
            "mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_PRIVATE, 3</var/task/native.so>, 0) = 0x1000",
            "mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0) = 0x2000",
            "msync(0x1000, 4096, MS_SYNC) = 0",
        ).verdict()
        == "pass"
    )
    result = audit(
        *prefix,
        "mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_SHARED, 3</var/task/native.so>, 0) = 0x1000",
        "msync(0x1000, 4096, MS_SYNC) = 0",
    )
    assert result.verdict() == "fail"
    assert [e["op"] for e in result.events] == ["mmap", "msync"]
    assert audit("msync(0x1000, 4096, MS_SYNC) = 0").verdict() == "inconclusive"
    assert (
        audit("mmap(NULL, 4096, PROT_WRITE, MAP_SHARED, 99, 0) = 0x1000").verdict()
        == "inconclusive"
    )


@pytest.mark.parametrize(
    "call",
    [
        'openat(AT_FDCWD</tmp/work>, "/tmp", O_RDWR|O_TMPFILE, 0600) = -1 EOPNOTSUPP (Unsupported)',
        'memfd_create("hidden", MFD_CLOEXEC) = 3</memfd:hidden>',
    ],
)
def test_anonymous_files_never_silently_pass(call):
    assert audit(call).verdict() == "fail"


def test_positive_exception_is_component_safe_and_does_not_allow_tmpfile():
    for path, flags in (
        ("/tmp/positive-spill-shadow/file", "O_RDWR|O_CREAT"),
        ("/tmp/positive-spill/../fallback", "O_RDWR|O_CREAT"),
        ("/tmp/positive-spill", "O_RDWR|O_TMPFILE"),
    ):
        assert (
            audit(f'open("{path}", {flags}, 0600) = 3', case="positive").verdict()
            == "fail"
        )


def gold_report():
    return report("gold") | {
        "workspaces": ["/tmp/nyc311-abc"],
        "seed_sha256": {
            "silver/created_date=2026-09-01/part.parquet": "unused",
            "control/gold/old.json": "unchanged",
        },
    }


def test_exact_gold_paths_atomic_siblings_and_directory_ancestors():
    metadata = gold_report()
    base = "/tmp/nyc311-abc/published"
    target = "nospill-gold-trial1"
    allowed = [
        f"{base}/silver/created_date=2026-09-01/part.parquet",
        f"{base}/control/gold/{target}.json",
        f"{base}/control/gold/.{target}.json.AbC123_.tmp",
        f"{base}/gold/run={target}/channel_usage/part.parquet",
        f"/evidence/objects/nyc311/published/control/gold/{target}.json",
    ]
    for path in allowed:
        assert (
            audit(
                f'open("{path}", O_WRONLY|O_CREAT, 0666) = 3<{path}>', metadata=metadata
            ).verdict()
            == "pass"
        )
    for path in [
        "/tmp/nyc311-abc",
        f"{base}/gold",
        f"{base}/silver/created_date=2026-09-01",
    ]:
        assert (
            audit(f'mkdir("{path}", 0700) = 0', metadata=metadata).verdict() == "pass"
        )
    for path in [
        "/tmp/nyc311-abc/spill.tmp",
        f"{base}/unexpected.tmp",
        f"{base}/control/gold/old.json",
        f"{base}/gold/run=other/channel_usage/part.parquet",
        f"{base}/control/gold/.wrong.json.X.tmp",
        "/tmp/nyc311-abc-shadow/published/spill",
        "/evidence/objects/nyc311/published/control/gold/old.json",
        f"{base}/gold/run={target}/unknown_product/part.parquet",
    ]:
        assert (
            audit(
                f'open("{path}", O_WRONLY|O_CREAT, 0666) = 3<{path}>', metadata=metadata
            ).verdict()
            == "fail"
        )
    assert (
        audit(
            marker("connection-1", "begin"),
            f'open("{allowed[1]}", O_RDWR) = 3',
            metadata=metadata,
        ).verdict()
        == "fail"
    )


def test_silver_pwrite_business_only_allowed_but_strict_native_forbidden():
    source = "/tmp/nyc311-abc/published/silver/created_date=2026-09-01/part.parquet"
    prefix = [marker("handler", "begin"), f'open("{source}", O_RDONLY) = 3<{source}>']
    write = "pwrite64(0x3, 0xaaaa, 0x10, 0) = 0x10"
    legacy = audit(
        *prefix,
        marker("business-1", "begin"),
        write,
        marker("business-1", "end"),
        marker("handler", "end"),
        metadata=gold_report(),
    )
    strict = audit(
        *prefix,
        marker("native-product-1", "begin"),
        marker("connection-1", "begin"),
        marker("connection-1", "end"),
        marker("business-1", "begin"),
        (11, write),
        marker("native-product-1", "end"),
        write,
        marker("business-1", "end"),
        marker("handler", "end"),
        metadata=gold_report(),
    )
    assert legacy.verdict() == "pass" and strict.verdict() == "fail"
    writes = [e for e in strict.events if e["op"] == "pwrite64"]
    assert [e["category"] for e in writes] == ["FORBIDDEN", "workspace_data"]
    assert writes[0]["phases"] == ["handler", "native-product-1", "business-1"]
    assert writes[1]["phases"] == ["handler", "business-1"]
    assert strict.strict_context_valid and not strict.active


@pytest.mark.parametrize(
    "path",
    [
        "/tmp/nyc311-abc/published/silver/created_date=2026-09-01/part.parquet",
        "/tmp/nyc311-abc/published/control/gold/nospill-gold-trial1.json",
        "/tmp/nyc311-abc/published/gold/run=nospill-gold-trial1/demand_daily/part.parquet",
        "/tmp/nyc311-abc/published/control/gold/.nospill-gold-trial1.json.AbC123.tmp",
        "/evidence/objects/nyc311/published/control/gold/nospill-gold-trial1.json",
        "/evidence/objects/nyc311/published/gold/run=nospill-gold-trial1/demand_daily/part.parquet",
        "/tmp/nyc311-abc/fallback.tmp",
    ],
)
def test_strict_native_even_allowlisted_writable_open_attempts_are_forbidden(path):
    result = audit(
        marker("handler", "begin"),
        marker("native-product-1", "begin"),
        f'openat(AT_FDCWD</tmp/work>, "{path}", O_RDWR|O_CREAT, 0600) = -1 EROFS (Read-only)',
        marker("native-product-1", "end"),
        marker("handler", "end"),
        metadata=gold_report(),
    )
    assert result.verdict() == "fail"
    event = next(e for e in result.events if e["op"] == "openat" and e["path"] == path)
    assert event["category"] == "FORBIDDEN" and not event["success"]


@pytest.mark.parametrize(
    "call",
    [
        "write(0x3, 0xaaaa, 0x10) = 0x10",
        "ftruncate(3, 4096) = 0",
        "fallocate(3, 0, 0, 4096) = 0",
        "mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_SHARED, 3, 0) = 0x1000",
        "copy_file_range(8, NULL, 3, NULL, 16, 0) = 16",
        'unlink("/tmp/nyc311-abc/published/control/gold/nospill-gold-trial1.json") = 0',
        'truncate("/tmp/nyc311-abc/published/control/gold/nospill-gold-trial1.json", 4096) = 0',
        'chmod("/tmp/nyc311-abc/published/control/gold/nospill-gold-trial1.json", 0600) = 0',
        'mkdir("/tmp/nyc311-abc/published/control/gold", 0700) = -1 EEXIST (Exists)',
    ],
)
def test_strict_native_rejects_all_supported_mutations_not_only_open(call):
    path = "/tmp/nyc311-abc/published/control/gold/nospill-gold-trial1.json"
    result = audit(
        marker("handler", "begin"),
        f'open("{path}", O_RDONLY) = 3<{path}>',
        marker("native-product-1", "begin"),
        call,
        marker("native-product-1", "end"),
        marker("handler", "end"),
        metadata=gold_report(),
    )
    assert result.verdict() == "fail"
    assert any(
        e["category"] == "FORBIDDEN" and e["op"] != "open" for e in result.events
    )


def test_strict_native_allows_readonly_private_ram_stdio_and_empty_markers():
    path = "/tmp/nyc311-abc/published/control/gold/nospill-gold-trial1.json"
    result = audit(
        marker("handler", "begin"),
        marker("native-product-1", "begin"),
        "close(9</evidence/phases/native-product-1.begin>) = 0",
        'unlink("/evidence/phases/native-product-1.begin") = 0',
        f'open("{path}", O_RDONLY) = 3<{path}>',
        "mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_PRIVATE, 3, 0) = 0x1000",
        "mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0) = 0x2000",
        "write(0x1, 0xaaaa, 0x10) = 0x10",
        "write(0x2, 0xaaaa, 0x10) = 0x10",
        marker("native-product-1", "end"),
        marker("handler", "end"),
        metadata=gold_report(),
    )
    assert result.verdict() == "pass"
    bad = audit(
        marker("handler", "begin"),
        marker("native-product-1", "begin"),
        "write(0x9, 0xaaaa, 0x1) = 0x1",
        metadata=gold_report(),
    )
    assert bad.verdict() == "fail" and bad.events[-1]["category"] == "FORBIDDEN"


@pytest.mark.parametrize("tail", ["connection-1", "business-2"])
def test_native_end_only_permits_crossing_its_matching_business_window(tail):
    result = audit(
        marker("handler", "begin"),
        marker("native-product-1", "begin"),
        marker(tail, "begin"),
        marker("native-product-1", "end"),
        metadata=gold_report(),
    )
    assert result.verdict() == "inconclusive" and not result.strict_context_valid


def test_unfinished_write_overlapping_strict_window_cannot_escape_phase_guard():
    source = "/tmp/nyc311-abc/published/silver/created_date=2026-09-01/part.parquet"
    result = audit(
        marker("handler", "begin"),
        f'open("{source}", O_RDONLY) = 3<{source}>',
        (11, "pwrite64(0x3, 0xaaaa, 0x10, 0 <unfinished ...>"),
        marker("native-product-1", "begin"),
        marker("native-product-1", "end"),
        (11, "<... pwrite64 resumed>) = 0x10"),
        marker("handler", "end"),
        metadata=gold_report(),
    )
    assert result.verdict() == "fail"
    event = next(e for e in result.events if e["op"] == "pwrite64")
    assert event["category"] == "FORBIDDEN" and "native-product-1" in event["phases"]


@pytest.mark.parametrize("op", ["rename", "link", "symlink"])
def test_both_mutation_paths_are_checked(op):
    result = audit(f'{op}("/tmp/writable", "/dev/shm/transient") = 0')
    assert result.verdict() == "fail"
    assert [e["path"] for e in result.events] == ["/tmp/writable", "/dev/shm/transient"]


@pytest.mark.parametrize("op", ["renameat", "renameat2", "linkat"])
def test_both_at_relative_paths_are_checked(op):
    result = audit(f'{op}(3</tmp>, "writable", 4</dev/shm>, "transient", 0) = 0')
    assert [e["path"] for e in result.events] == ["/tmp/writable", "/dev/shm/transient"]
    assert result.verdict() == "fail"


@pytest.mark.parametrize(
    "op, args",
    [
        ("chmod", '"/tmp/hidden", 0600'),
        ("chown", '"/tmp/hidden", 1, 1'),
        ("utimes", '"/tmp/hidden", NULL'),
        ("truncate", '"/tmp/hidden", 123'),
        ("mknod", '"/tmp/hidden", S_IFREG|0600, 0'),
        ("utimensat", 'AT_FDCWD</tmp/work>, "hidden", NULL, 0'),
    ],
)
def test_non_open_mutations_are_not_ignored(op, args):
    assert audit(f"{op}({args}) = -1 EROFS (Read-only)").verdict() == "fail"


def test_report_write_only_after_cleanup_and_outside_native_phases():
    call = 'open("/evidence/report.json", O_WRONLY|O_CREAT, 0666) = 3</evidence/report.json>'
    assert audit(call).verdict() == "fail"
    result = audit(
        marker("cleanup", "begin"),
        marker("cleanup", "end"),
        call,
        "write(0x3, 0xa, 0x10) = 0x10",
    )
    assert result.verdict() == "pass"
    assert result.events[-1]["category"] == "final_report"
    assert audit(marker("query", "end")).verdict() == "inconclusive"


@pytest.fixture
def trial(tmp_path):
    metadata = report() | {
        "status": "passed",
        "protected_inputs_and_payload_unchanged": True,
        "seeds_and_old_gold_unchanged": True,
        "seed_sha256": {},
        "expected_error": {"type": "OutOfMemoryException", "message": "native OOM"},
        "outcome": "expected_native_resource_rejection",
        "physical_spill_peak_bytes_lower_bound": 0,
        "effective": {"temp_directory": ""},
    }
    observer = {
        "host_pid": 10,
        "tracer_confirmed_before_go": True,
        "status": "captured",
        "container_exitcode": 0,
        "tracer_exitcode": 0,
        "container_state": {
            "ExitCode": 0,
            "OOMKilled": False,
            "Running": False,
            "Error": "",
        },
        "trace_command": [
            "strace",
            "-f",
            "-ttt",
            "-yy",
            "-p",
            "10",
            "-e",
            "trace=%file,write,writev,pwrite64,pwritev,pwritev2,ftruncate,fsync,fdatasync,close,dup,dup2,dup3,fcntl,mmap,msync,memfd_create",
            "-e",
            "raw=write,writev,pwrite64,pwritev,pwritev2",
        ],
    } | dict.fromkeys(
        (
            "originals_preserved",
            "protected_code_preserved",
            "payload_preserved",
            "inputs_preserved",
        ),
        True,
    )
    ready = {
        "case": "negative",
        "run_id": metadata["run_id"],
        "cwd": "/tmp/work",
        "native_imports_started": False,
    }
    for name, payload in (
        ("report.json", metadata),
        ("observer.json", observer),
        ("ready.json", ready),
    ):
        (tmp_path / name).write_text(json.dumps(payload))
    calls = []
    for phase in (
        "setup",
        "imports",
        "connection",
        "query",
        "connection-close",
        "cleanup",
    ):
        calls.extend([marker(phase, "begin"), marker(phase, "end")])
    calls.extend(["write(0x1, 0xa, 0x10) = 0x10", "+++ exited with 0 +++"])
    (tmp_path / "trace.log").write_text("".join(trace(*calls)))
    (tmp_path / "strace-stderr.txt").write_text("strace: Process 10 attached\n")
    (tmp_path / "stderr.txt").write_text("")
    return tmp_path


def change_json(trial, name, **values):
    path = trial / name
    path.write_text(json.dumps(json.loads(path.read_text()) | values))


def test_trial_cli_writes_detailed_json_and_compact_summary(trial, capsys):
    assert MODULE["main"]([str(trial)]) == 0
    summary = json.loads(capsys.readouterr().out)
    saved = json.loads((trial / "trace-audit.json").read_text())
    assert saved["verdict"] == summary["verdict"] == "pass"
    assert "events" not in summary and saved["events"]
    assert saved["limitations"] and saved["positive_control_required_separately"]


@pytest.mark.parametrize(
    "name, values",
    [
        ("observer.json", {"tracer_confirmed_before_go": False}),
        ("observer.json", {"tracer_exitcode": 1}),
        ("observer.json", {"container_exitcode": 1}),
        ("observer.json", {"trace_command": ["strace", "-f"]}),
        ("report.json", {"status": "failed"}),
        ("report.json", {"expected_error": {"type": "PermissionError"}}),
        (
            "report.json",
            {
                "expected_error": {
                    "type": "OutOfMemoryException",
                    "message": "permission denied",
                }
            },
        ),
        ("report.json", {"effective": {"temp_directory": "/tmp"}}),
        ("ready.json", {"native_imports_started": True}),
    ],
)
def test_functional_pass_does_not_override_missing_evidence(trial, name, values):
    change_json(trial, name, **values)
    assert MODULE["audit_trial"](trial)["verdict"] == "inconclusive"


@pytest.mark.parametrize(
    "name, text",
    [
        ("strace-stderr.txt", "strace: Process 10 attached\nstrace: dropped events\n"),
        (
            "strace-stderr.txt",
            "strace: Process 10 attached\nstrace: Process 11 attached\n",
        ),
        ("stderr.txt", "native failure"),
        ("trace.log", "10 1792069647.1 +++ killed by SIGKILL +++\n"),
        ("trace.log", "10 1792069647.1 +++ exited with 1 +++\n"),
    ],
)
def test_capture_errors_missing_threads_and_nonzero_exits(trial, name, text):
    (trial / name).write_text(text)
    assert MODULE["audit_trial"](trial)["verdict"] == "inconclusive"


def test_missing_evidence_never_passes(tmp_path):
    assert MODULE["main"]([str(tmp_path)]) == 1
    assert (
        json.loads((tmp_path / "trace-audit.json").read_text())["verdict"]
        == "inconclusive"
    )


def test_independent_seed_hash_check(trial):
    root = trial / "objects/nyc311/published/control/gold"
    root.mkdir(parents=True)
    path = root / "old.json"
    path.write_bytes(b"old complete manifest")
    change_json(
        trial,
        "report.json",
        seed_sha256={
            "control/gold/old.json": hashlib.sha256(path.read_bytes()).hexdigest(),
        },
    )
    assert MODULE["audit_trial"](trial)["verdict"] == "pass"
    path.write_bytes(b"changed")
    assert MODULE["audit_trial"](trial)["verdict"] == "inconclusive"


def test_failed_prequery_positive_is_excluded_as_mechanism_proof(trial):
    change_json(
        trial,
        "report.json",
        case="positive",
        run_id="nospill-positive-trial1",
        status="failed",
        rows=0,
        physical_spill_peak_bytes_lower_bound=0,
    )
    change_json(trial, "ready.json", case="positive", run_id="nospill-positive-trial1")
    result = MODULE["audit_trial"](trial)
    assert result["verdict"] == "inconclusive"
    assert any("Positive lacks" in a["reason"] for a in result["anomalies"])


@pytest.mark.parametrize("missing", [None, "write", "physical", "rows"])
def test_positive_requires_actual_mapped_bytes_physical_blocks_and_rows(trial, missing):
    target = "nospill-positive-trial2"
    change_json(
        trial,
        "report.json",
        case="positive",
        run_id=target,
        rows=0 if missing == "rows" else 350000,
        physical_spill_peak_bytes_lower_bound=0 if missing == "physical" else 4096,
        outcome="completed_with_physical_spill",
    )
    change_json(trial, "ready.json", case="positive", run_id=target)
    path = trial / "trace.log"
    lines = path.read_text().splitlines(keepends=True)
    insertion = next(
        i + 1
        for i, line in enumerate(lines)
        if '"/evidence/phases/query.begin"' in line
    )
    extra = [
        'open("/tmp/positive-spill/native.tmp", O_RDWR|O_CREAT, 0600) = 3</tmp/positive-spill/native.tmp>'
    ]
    if missing != "write":
        extra.append("pwrite64(0x3, 0xaaaa, 0x1000, 0) = 0x1000")
    lines[insertion:insertion] = trace(*extra)
    path.write_text("".join(lines))
    result = MODULE["audit_trial"](trial)
    assert result["verdict"] == ("pass" if missing is None else "inconclusive")
    assert result["positive_spill_writable_open_attempts"] == 1


def test_forbidden_event_has_cli_exit_one_even_when_function_passed(trial, capsys):
    path = trial / "trace.log"
    path.write_text(
        "".join(
            trace('open("/tmp/fallback", O_RDWR|O_CREAT, 0600) = -1 EROFS (Read-only)')
        )
        + path.read_text()
    )
    assert MODULE["main"]([str(trial)]) == 1
    assert json.loads(capsys.readouterr().out)["verdict"] == "fail"


def configure_gold_trial(
    trial, case, count, expanded=False, missing_effective=False, strict=False
):
    if expanded:
        expanded_observer(trial)
    target = f"nospill-{case}-trial1"
    failed = case != "gold"
    values = {
        "case": case,
        "run_id": target,
        "workspaces": ["/tmp/nyc311-abc"],
        "connections": [
            {
                "requested_config": MODULE["GOLD_LIMITS"]
                | {"temp_directory": "/tmp/nyc311-abc/duckdb-spill"},
                "native_config": MODULE["GOLD_LIMITS"]
                | {
                    "temp_directory": "",
                    "memory_limit": "1B" if failed and index == count else "512MiB",
                },
                "injected": failed and index == count,
                "effective": {
                    "temp_directory": "",
                    "threads": 1,
                    "memory_limit": "1 byte"
                    if failed and index == count
                    else "512.0 MiB",
                    "max_temp_directory_size": "256.0 MiB",
                },
            }
            for index in range(1, count + 1)
        ],
        "outcome": "expected_native_failure_closed"
        if failed
        else "complete_four_products_323627_rows",
        "validation": {"original_reference_compared": True},
        "failed_target_load_rejected": True,
        "progress": [
            {"status": "failed" if failed else "complete", "products": count - 1}
        ],
        "put_attempts": [
            {
                "key": f"nyc311/published/control/gold/{target}.json",
                "status": "failed" if failed else "complete",
                "persisted": True,
                "products": count - 1,
            }
        ],
    }
    if missing_effective:
        del values["connections"][-1]["effective"]
        error = {
            "type": "OutOfMemoryException",
            "traceback": [
                {
                    "file": "/checks/spike_duckdb_no_spill.py",
                    "function": "live_settings",
                }
            ],
        }
        values["connections"][-1]["error"] = error
        values["expected_error"] = error
    if strict:
        values["strict_compute_windows"] = [
            {"phase": f"native-product-{i}", "product": product}
            for i, product in enumerate(MODULE["PRODUCTS"][:count], 1)
        ]
    change_json(trial, "report.json", **values)
    change_json(trial, "ready.json", case=case, run_id=target)
    calls = []
    for phase in ("setup", "imports"):
        calls.extend([marker(phase, "begin"), marker(phase, "end")])
    calls.append(marker("handler", "begin"))
    for index in range(1, count + 1):
        if strict:
            calls.append(marker(f"native-product-{index}", "begin"))
        calls.extend(
            [
                marker(f"connection-{index}", "begin"),
                marker(f"connection-{index}", "end"),
            ]
        )
        if not failed or index < count:
            calls.append(marker(f"business-{index}", "begin"))
            if strict:
                calls.append(marker(f"native-product-{index}", "end"))
            calls.extend(
                [
                    f'open("/tmp/nyc311-abc/published/control/gold/{target}.json", O_WRONLY|O_CREAT, 0600) = 3',
                    "write(0x3, 0xaaaa, 0x10) = 0x10",
                    marker(f"business-{index}", "end"),
                ]
            )
        elif strict:
            calls.append(marker(f"native-product-{index}", "end"))
    calls.append(marker("handler", "end"))
    for phase in ("validation", "cleanup"):
        calls.extend([marker(phase, "begin"), marker(phase, "end")])
    calls.append("+++ exited with 0 +++")
    (trial / "trace.log").write_text("".join(trace(*calls)))
    return values


@pytest.mark.parametrize(
    "case, count", [("gold", 4), ("fail-first", 1), ("fail-late", 2)]
)
@pytest.mark.parametrize("expanded", [False, True])
@pytest.mark.parametrize("missing_effective", [False, True])
@pytest.mark.parametrize("strict", [False, True])
def test_gold_and_failure_phase_contracts_and_authorization(
    trial, case, count, expanded, missing_effective, strict
):
    configure_gold_trial(trial, case, count, expanded, missing_effective, strict)
    result = MODULE["audit_trial"](trial)
    assert result["strict_compute_coverage"] is strict
    if not strict:
        assert any("legacy Gold/business" in s for s in result["limitations"])
    failed = case != "gold"
    assert result["verdict"] == (
        "inconclusive" if not failed and missing_effective else "pass"
    )
    if failed and missing_effective:
        assert result["live_settings_unavailable_expected_native_oom"] == [count]
        assert any(
            "live_settings_unavailable_expected_native_oom" in s
            for s in result["limitations"]
        )
        assert (
            "effective"
            not in json.loads((trial / "report.json").read_text())["connections"][-1]
        )
    else:
        assert "live_settings_unavailable_expected_native_oom" not in result
    if failed:
        change_json(
            trial,
            "report.json",
            progress=[{"status": "complete"}, {"status": "failed"}],
        )
        assert MODULE["audit_trial"](trial)["verdict"] == "inconclusive"


@pytest.fixture(params=[("gold", 4), ("fail-first", 1), ("fail-late", 2)])
def strict_gold_trial(trial, request):
    case, count = request.param
    configure_gold_trial(
        trial, case, count, expanded=True, missing_effective=case != "gold", strict=True
    )
    return trial


@pytest.mark.parametrize(
    "damage",
    [
        "missing_report_list",
        "empty_list",
        "null_list",
        "wrong_product",
        "wrong_phase",
        "missing_entry",
        "duplicate_entry",
        "missing_trace_window",
        "missing_end",
        "connection_outside_window",
    ],
)
def test_strict_compute_contract_cannot_silently_drop_windows(
    strict_gold_trial, damage
):
    report_path = strict_gold_trial / "report.json"
    payload = json.loads(report_path.read_text())
    windows = payload["strict_compute_windows"]
    trace_path = strict_gold_trial / "trace.log"
    lines = trace_path.read_text().splitlines(keepends=True)
    if damage == "missing_report_list":
        del payload["strict_compute_windows"]
    elif damage == "empty_list":
        payload["strict_compute_windows"] = []
    elif damage == "null_list":
        payload["strict_compute_windows"] = None
    elif damage == "wrong_product":
        windows[0]["product"] = "channel_usage"
    elif damage == "wrong_phase":
        windows[0]["phase"] = "business-1"
    elif damage == "missing_entry":
        windows.pop()
    elif damage == "duplicate_entry":
        windows.append(windows[-1])
    elif damage == "missing_trace_window":
        lines = [line for line in lines if "/native-product-1." not in line]
    elif damage == "missing_end":
        lines = [line for line in lines if "/native-product-1.end" not in line]
    else:
        begin = next(line for line in lines if "/native-product-1.begin" in line)
        lines.remove(begin)
        connection_end = next(
            i for i, line in enumerate(lines) if "/connection-1.end" in line
        )
        lines.insert(connection_end + 1, begin)
    report_path.write_text(json.dumps(payload))
    trace_path.write_text("".join(lines))
    result = MODULE["audit_trial"](strict_gold_trial)
    assert result["verdict"] in {"fail", "inconclusive"}, result["anomalies"]
    assert result["strict_compute_coverage"] is False


def test_strict_compute_product_and_trace_order_are_both_required(trial):
    configure_gold_trial(trial, "gold", 4, strict=True)
    path = trial / "report.json"
    payload = json.loads(path.read_text())
    payload["strict_compute_windows"].reverse()
    path.write_text(json.dumps(payload))
    assert MODULE["audit_trial"](trial)["strict_compute_coverage"] is False
    payload["strict_compute_windows"].reverse()
    path.write_text(json.dumps(payload))
    trace_path = trial / "trace.log"
    lines = trace_path.read_text().splitlines(keepends=True)
    starts = [
        i
        for i, line in enumerate(lines)
        if "/native-product-" in line and '.begin"' in line
    ]
    ends = [
        i for i, line in enumerate(lines) if "/business-" in line and '.end"' in line
    ]
    first, second = lines[starts[0] : ends[0] + 1], lines[starts[1] : ends[1] + 1]
    lines[starts[0] : ends[1] + 1] = second + first
    trace_path.write_text("".join(lines))
    result = MODULE["audit_trial"](trial)
    assert result["verdict"] == "inconclusive" and not result["strict_compute_coverage"]


def test_allowlisted_native_write_fails_even_with_valid_window_contract(
    strict_gold_trial,
):
    payload = json.loads((strict_gold_trial / "report.json").read_text())
    target = f"/tmp/nyc311-abc/published/control/gold/{payload['run_id']}.json"
    path = strict_gold_trial / "trace.log"
    lines = path.read_text().splitlines(keepends=True)
    insertion = next(
        i + 1 for i, line in enumerate(lines) if "/native-product-1.begin" in line
    )
    lines[insertion:insertion] = trace(f'open("{target}", O_RDWR) = 3<{target}>')
    path.write_text("".join(lines))
    result = MODULE["audit_trial"](strict_gold_trial)
    assert result["verdict"] == "fail"
    assert result["strict_compute_coverage"] is True
    assert result["unexpected_mutation_events"] == 1


@pytest.fixture(params=[("fail-first", 1), ("fail-late", 2)])
def oom_failure_trial(trial, request):
    case, count = request.param
    configure_gold_trial(trial, case, count, expanded=True, missing_effective=True)
    return trial


@pytest.mark.parametrize(
    "keys, value",
    [
        (("connections", -1, "injected"), False),
        (("connections", -1, "native_config", "memory_limit"), "2B"),
        (("connections", -1, "native_config", "temp_directory"), "/tmp"),
        (("connections", -1, "native_config", "threads"), 2),
        (("connections", -1, "native_config", "threads"), True),
        (("connections", -1, "native_config", "threads"), "<DELETE>"),
        (("connections", -1, "native_config", "max_temp_directory_size"), "512MiB"),
        (("connections", -1, "native_config", "extra"), 1),
        (("connections", -1, "requested_config", "memory_limit"), "1B"),
        (
            ("connections", -1, "requested_config", "temp_directory"),
            "/tmp/nyc311-other/duckdb-spill",
        ),
        (
            ("connections", -1, "requested_config", "temp_directory"),
            "/tmp/nyc311-abc/../duckdb-spill",
        ),
        (
            ("connections", -1, "requested_config", "max_temp_directory_size"),
            "<DELETE>",
        ),
        (("connections", -1, "error"), "<DELETE>"),
        (("connections", -1, "error", "type"), "PermissionError"),
        (("connections", -1, "error", "message"), "permission denied"),
        (("connections", -1, "error", "traceback", -1, "function"), "connect"),
        (("connections", -1, "effective"), None),
        (("connections", -1, "effective"), {}),
        (("expected_error", "type"), "PermissionError"),
        (("expected_error", "traceback"), []),
        (("outcome",), "complete_four_products_323627_rows"),
        (("failed_target_load_rejected",), False),
        (("put_attempts", -1, "status"), "complete"),
        (("put_attempts", -1, "persisted"), False),
        (("put_attempts", -1, "products"), 9),
        (("progress", -1, "status"), "incomplete"),
        (("progress", -1, "products"), 9),
    ],
)
def test_unavailable_live_settings_exception_is_narrow(oom_failure_trial, keys, value):
    path = oom_failure_trial / "report.json"
    payload = json.loads(path.read_text())
    destination = payload
    for key in keys[:-1]:
        destination = destination[key]
    if value == "<DELETE>":
        del destination[keys[-1]]
    else:
        destination[keys[-1]] = value
    path.write_text(json.dumps(payload))
    result = MODULE["audit_trial"](oom_failure_trial)
    assert result["verdict"] == "inconclusive", result["anomalies"]


@pytest.mark.parametrize(
    "damage",
    [
        "count",
        "earlier_missing",
        "earlier_injected",
        "healthy_memory",
        "complete_put",
        "complete_progress",
    ],
)
def test_oom_exception_does_not_cover_other_connections_or_earlier_complete(
    trial, damage
):
    configure_gold_trial(trial, "fail-late", 2, missing_effective=True)
    path = trial / "report.json"
    payload = json.loads(path.read_text())
    if damage == "count":
        payload["connections"].pop(0)
    elif damage == "earlier_missing":
        del payload["connections"][0]["effective"]
    elif damage == "earlier_injected":
        payload["connections"][0]["injected"] = True
    elif damage == "healthy_memory":
        payload["connections"][0]["effective"]["memory_limit"] = "1 byte"
    elif damage == "complete_put":
        payload["put_attempts"].insert(
            0,
            {
                "key": f"nyc311/published/gold/run={payload['run_id']}/demand_daily/part.parquet",
                "status": "complete",
            },
        )
    else:
        payload["progress"].insert(0, {"status": "complete"})
    path.write_text(json.dumps(payload))
    assert MODULE["audit_trial"](trial)["verdict"] == "inconclusive"


@pytest.mark.parametrize(
    "memory, verdict",
    [
        ("0 bytes", "pass"),
        ("1 byte", "pass"),
        ("1 bytes", "pass"),
        ("1B", "pass"),
        ("1.0 bytes", "pass"),
        ("2 bytes", "inconclusive"),
        (1, "inconclusive"),
        ("0.5 B", "inconclusive"),
        ("nan", "inconclusive"),
    ],
)
def test_available_injected_live_settings_parse_byte_counts(trial, memory, verdict):
    payload = configure_gold_trial(trial, "fail-first", 1)
    payload["connections"][-1]["effective"]["memory_limit"] = memory
    change_json(trial, "report.json", connections=payload["connections"])
    result = MODULE["audit_trial"](trial)
    assert result["verdict"] == verdict
    assert "live_settings_unavailable_expected_native_oom" not in result


@pytest.mark.parametrize(
    "call, verdict",
    [
        ('open("/tmp/fallback", O_RDWR|O_CREAT, 0600) = -1 EROFS (Read-only)', "fail"),
        ("fork() = 11", "inconclusive"),
    ],
)
def test_oom_exception_does_not_bypass_trace_guards(oom_failure_trial, call, verdict):
    path = oom_failure_trial / "trace.log"
    path.write_text("".join(trace(call)) + path.read_text())
    result = MODULE["audit_trial"](oom_failure_trial)
    assert result["verdict"] == verdict
    assert result["live_settings_unavailable_expected_native_oom"]


def expanded_observer(trial, census=None):
    observer = json.loads((trial / "observer.json").read_text())
    command = observer["trace_command"]
    index = next(i for i, arg in enumerate(command) if arg.startswith("trace="))
    command[index] += ",%process," + ",".join(sorted(MODULE["EXPANDED_IO"]))
    change_json(
        trial,
        "observer.json",
        trace_command=command,
        tracer_pid=99,
        before_go_threads={"10": 99} if census is None else census,
    )


@pytest.fixture(params=[("gold", 4), ("fail-first", 1), ("fail-late", 2)])
def production_trial(trial, request):
    case, count = request.param
    work = trial / "production-work"
    evidence = work / "trials" / f"nospill-{case}-production-trial1"
    evidence.mkdir(parents=True)
    for name in (
        "report.json",
        "observer.json",
        "ready.json",
        "trace.log",
        "strace-stderr.txt",
        "stderr.txt",
    ):
        (evidence / name).write_bytes((trial / name).read_bytes())
    values = configure_gold_trial(evidence, case, count, expanded=True, strict=True)
    values["production_policy_verification"] = True
    values["policy_provenance"] = {
        "declared_config": MODULE["PRODUCTION_LIMITS"],
        "config_rewritten_at_native_connect": False,
    }
    payload = work / "payload"
    (payload / "nyc311").mkdir(parents=True)
    (payload / "sql").mkdir()
    source = b"def _compute_query(connection, query):\n    return connection.execute(query)\n"
    (payload / "nyc311/gold.py").write_bytes(source)
    source_hash = hashlib.sha256(source).hexdigest()
    values["production_source"] = {
        "file": "/var/task/nyc311/gold.py",
        "sha256": source_hash,
        "business_execute_lines": [2],
    }
    payload_hashes = {"nyc311/gold.py": source_hash}
    values["business_query_sha256"] = {}
    for product in MODULE["PRODUCTS"][:count]:
        key = f"sql/{product}.sql"
        content = f"SELECT '{product}'".encode()
        (payload / key).write_bytes(content)
        payload_hashes[key] = hashlib.sha256(content).hexdigest()
        values["business_query_sha256"][product] = payload_hashes[key]
    for row in values["connections"]:
        row["requested_config"] = dict(row["native_config"])
        for key in ("requested_config", "native_config", "effective"):
            row[key].pop("max_temp_directory_size")
        row.update(config_rewritten=False, native_kwargs_unchanged=True)
    if case != "gold":
        fault = values["connections"][-1]
        del fault["effective"]
        fault["effective_unavailable"] = (
            "fault connection: readback deliberately skipped; declared config only"
        )
        values["expected_error"] = {
            "type": "OutOfMemoryException",
            "traceback": [
                {
                    "file": "/var/task/nyc311/gold.py",
                    "line": 2,
                    "function": "_compute_query",
                }
            ],
        }
        values["native_oom_boundary"] = "business_execute_query"
        values["fault_injection"] = {
            "product_index": count,
            "product": MODULE["PRODUCTS"][count - 1],
            "boundary": "before original gold_store._compute_query validation",
            "original_config": MODULE["PRODUCTION_LIMITS"],
            "declared_config": MODULE["PRODUCTION_LIMITS"] | {"memory_limit": "1B"},
        }
        values["put_attempts"] += [
            {
                "key": f"nyc311/published/gold/run={values['run_id']}/{product}/part.parquet",
                "persisted": True,
            }
            for product in MODULE["PRODUCTS"][: count - 1]
        ]
        seeds = {"control/gold/old.json": b"old complete"} | {
            f"gold/run=old/{product}/part.parquet": product.encode()
            for product in MODULE["PRODUCTS"]
        }
        values["seed_sha256"] = {}
        for key, content in seeds.items():
            path = evidence / "objects/nyc311/published" / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            values["seed_sha256"][key] = hashlib.sha256(content).hexdigest()
        path = evidence / "trace.log"
        lines = path.read_text().splitlines(keepends=True)
        index = next(
            i for i, line in enumerate(lines) if f"/native-product-{count}.end" in line
        )
        lines[index : index + 1] = trace(
            marker(f"business-{count}", "begin"),
            marker(f"native-product-{count}", "end"),
            marker(f"business-{count}", "end"),
        )
        path.write_text("".join(lines))
    scripts = {
        name: hashlib.sha256(name.encode()).hexdigest()
        for name in (
            "spike_duckdb_no_spill.py",
            "run_no_spill_spike.py",
            "audit_no_spill_trace.py",
            "benchmark_lambda.py",
            "check_lambda_runtime.py",
        )
    }
    (evidence / "scripts-sha256.json").write_text(json.dumps(scripts))
    (work / "baseline.json").write_text(
        json.dumps(
            {
                "production_policy_verification": True,
                "script_sha256": scripts,
                "native_sha256": MODULE["NATIVE_SHA256"],
                "payload": payload_hashes,
            }
        )
    )
    values["native"] = {"actual_binary_sha256": MODULE["NATIVE_SHA256"]}
    change_json(evidence, "report.json", **values)
    change_json(evidence, "ready.json", production_policy_verification=True)
    observer = json.loads((evidence / "observer.json").read_text())
    change_json(
        evidence,
        "observer.json",
        production_policy_verification=True,
        scripts_preserved=True,
        original_zips_preserved=True,
        script_sha256=scripts,
        native_sha256=MODULE["NATIVE_SHA256"],
        trace_command=observer["trace_command"] + ["--decode-pids=pidns"],
    )
    return evidence


def test_production_policy_and_business_oom_contract(production_trial, capsys):
    result = MODULE["audit_trial"](production_trial, production=True)
    assert result["verdict"] == "pass", result["anomalies"]
    assert (
        result["production_policy_verification"] and result["strict_compute_coverage"]
    )
    assert MODULE["audit_trial"](production_trial)["verdict"] == "inconclusive"
    assert MODULE["main"]([str(production_trial), "--production"]) == 0
    assert json.loads(capsys.readouterr().out)["production_policy_verification"]


@pytest.mark.parametrize("positive", [False, True])
def test_production_synthetic_control_shapes_and_query_hash(production_trial, positive):
    path = production_trial / "report.json"
    values = json.loads(path.read_text())
    del values["strict_compute_windows"]
    case = "positive" if positive else "negative"
    config = MODULE["PRODUCTION_LIMITS"] | {"memory_limit": "32MiB"}
    if positive:
        config |= {
            "temp_directory": "/tmp/positive-spill",
            "max_temp_directory_size": "256MiB",
        }
    effective = config | {"memory_limit": "32.0 MiB"}
    if positive:
        effective["max_temp_directory_size"] = "256.0 MiB"
    values.update(
        case=case,
        run_id=f"nospill-{case}-production-trial1",
        config=config,
        effective=effective,
        rows=350000 if positive else 0,
        query_sha256=MODULE["QUERY_SHA256"],
        physical_spill_peak_bytes_lower_bound=4096 if positive else 0,
        outcome="completed_with_physical_spill"
        if positive
        else "expected_native_resource_rejection",
        expected_error={"type": "OutOfMemoryException"},
    )
    values["policy_provenance"]["control_overrides"] = {
        key: value
        for key, value in config.items()
        if MODULE["PRODUCTION_LIMITS"].get(key) != value
    }
    path.write_text(json.dumps(values))
    change_json(production_trial, "ready.json", case=case, run_id=values["run_id"])
    calls = []
    for phase in (
        "setup",
        "imports",
        "connection",
        "query",
        "connection-close",
        "cleanup",
    ):
        calls.append(marker(phase, "begin"))
        if positive and phase == "query":
            calls += [
                'open("/tmp/positive-spill/native.tmp", O_RDWR|O_CREAT, 0600) = 3</tmp/positive-spill/native.tmp>',
                "pwrite64(0x3, 0xaaaa, 0x1000, 0) = 0x1000",
            ]
        calls.append(marker(phase, "end"))
    calls.append("+++ exited with 0 +++")
    (production_trial / "trace.log").write_text("".join(trace(*calls)))
    result = MODULE["audit_trial"](production_trial, production=True)
    assert result["verdict"] == "pass", result["anomalies"]
    values["query_sha256"] = "0" * 64
    path.write_text(json.dumps(values))
    assert (
        MODULE["audit_trial"](production_trial, production=True)["verdict"]
        == "inconclusive"
    )


@pytest.mark.parametrize(
    "damage",
    [
        "extra_key",
        "empty_config",
        "rewritten",
        "bad_live_temp",
        "missing_live",
        "source_hash",
        "sql_bytes",
        "native_hash",
        "script_hash",
        "observer_flag",
        "ready_flag",
        "legacy_capture",
        "strict_missing",
        "fault_boundary",
        "complete_attempt",
        "effective_fabricated",
        "fault_settings_frame",
        "fault_register_frame",
        "wrong_execute_line",
    ],
)
def test_production_evidence_cannot_fall_back_to_legacy(production_trial, damage):
    path = production_trial / "report.json"
    values = json.loads(path.read_text())
    failed = values["case"] != "gold"
    if (
        damage
        in {
            "fault_boundary",
            "complete_attempt",
            "effective_fabricated",
            "fault_settings_frame",
            "fault_register_frame",
            "wrong_execute_line",
        }
        and not failed
    ):
        pytest.skip("Fault-only evidence")
    if damage == "extra_key":
        values["connections"][0]["native_config"]["max_temp_directory_size"] = "256MiB"
    elif damage == "empty_config":
        values["connections"][0]["requested_config"] = {}
    elif damage == "rewritten":
        values["connections"][0]["config_rewritten"] = True
    elif damage == "bad_live_temp":
        values["connections"][0]["effective"] = MODULE["PRODUCTION_LIMITS"] | {
            "temp_directory": "/tmp/spill"
        }
    elif damage == "missing_live":
        values["connections"][0].pop("effective", None)
        if values["case"] == "fail-first":
            values["connections"][0].pop("effective_unavailable")
    elif damage == "source_hash":
        values["production_source"]["sha256"] = "0" * 64
    elif damage == "sql_bytes":
        (production_trial.parent.parent / "payload/sql/demand_daily.sql").write_text(
            "SELECT 0"
        )
    elif damage == "native_hash":
        values["native"]["actual_binary_sha256"] = "0" * 64
    elif damage == "script_hash":
        (production_trial / "scripts-sha256.json").write_text("{}")
    elif damage in {"observer_flag", "ready_flag"}:
        change_json(
            production_trial,
            damage.removesuffix("_flag") + ".json",
            production_policy_verification=False,
        )
    elif damage == "legacy_capture":
        change_json(production_trial, "observer.json", trace_command=["strace", "-f"])
    elif damage == "strict_missing":
        del values["strict_compute_windows"]
    elif damage == "fault_boundary":
        values["native_oom_boundary"] = "live_settings"
    elif damage == "complete_attempt":
        values["put_attempts"].insert(0, {"key": "irrelevant", "status": "complete"})
    elif damage == "effective_fabricated":
        values["connections"][-1]["effective"] = values["connections"][-1][
            "native_config"
        ]
    elif damage in {"fault_settings_frame", "fault_register_frame"}:
        values["expected_error"]["traceback"][-1]["function"] = (
            "live_settings" if damage == "fault_settings_frame" else "register"
        )
    elif damage == "wrong_execute_line":
        values["expected_error"]["traceback"][-1]["line"] = 1
    path.write_text(json.dumps(values))
    result = MODULE["audit_trial"](production_trial, production=True)
    assert result["verdict"] == "inconclusive", result["anomalies"]


def test_legacy_prequery_oom_never_counts_as_production(trial):
    configure_gold_trial(
        trial, "fail-first", 1, expanded=True, missing_effective=True, strict=True
    )
    assert MODULE["audit_trial"](trial)["verdict"] == "pass"
    assert MODULE["audit_trial"](trial, production=True)["verdict"] == "inconclusive"


def test_legacy_scope_is_explicit(trial):
    result = MODULE["audit_trial"](trial)
    assert result["verdict"] == "pass" and result["capture_scope"] == "legacy_limited"
    assert not any(result["coverage"].values())
    assert any("No valid pre-GO" in text for text in result["limitations"])


def test_expanded_scope_census_and_resumed_child_lifecycle(trial):
    expanded_observer(trial)
    path = trial / "trace.log"
    calls = [
        "clone3({flags=CLONE_THREAD|CLONE_FILES, exit_signal=0}, 88) = -1 ENOSYS (Function not implemented)",
        "clone(child_stack=0xaaaa, flags=CLONE_VM|CLONE_THREAD|CLONE_FILES, tls=0xbbbb <unfinished ...>",
        (11, "write(0x1, 0xaaaa, 0x10) = 0x10"),
        (11, "exit(0) = ?"),
        (11, "+++ exited with 0 +++"),
        "<... clone resumed>) = 11",
        "wait4(-1, 0xaaaa, WNOHANG, NULL) = -1 ECHILD (No child processes)",
    ]
    lines = path.read_text().splitlines(keepends=True)
    lines[-1:-1] = trace("exit_group(0) = ?")
    path.write_text("".join(trace(*calls) + lines))
    (trial / "strace-stderr.txt").write_text(
        "strace: Process 10 attached\nstrace: Process 11 attached\n"
    )
    result = MODULE["audit_trial"](trial)
    assert result["verdict"] == "pass", result["anomalies"]
    assert result["capture_scope"] == "expanded_process_kernel_io"
    assert all(result["coverage"].values())
    assert result["thread_parents"] == {11: 10}
    assert result["exit_calls"] == {10: 0, 11: 0}
    assert result["mechanism_events"][0]["outcome"] == "denied"
    assert "aaaa" not in json.dumps(result)


@pytest.mark.parametrize("op", ["clone", "clone3"])
@pytest.mark.parametrize("resumed", [False, True])
def test_pidns_clone_uses_return_annotation_not_first_argument_annotation(
    trial, op, resumed
):
    expanded_observer(trial)
    observer = json.loads((trial / "observer.json").read_text())
    change_json(
        trial,
        "observer.json",
        trace_command=observer["trace_command"] + ["--decode-pids=pidns"],
    )
    annotation = "/* 11 in strace's PID NS */"
    # An earlier decoded argument is not the authoritative returned child PID.
    args = "flags=CLONE_THREAD|CLONE_FILES, parent_tid=[6 /* 999 in strace's PID NS */]"
    args = "{" + args + "}, 88" if op == "clone3" else args
    if resumed:
        calls = [
            f"{op}({args} <unfinished ...>",
            (11, "write(0x1, 0xaaaa, 0x10) = 0x10"),
            (11, "exit(0) = ?"),
            (11, "+++ exited with 0 +++"),
            f"<... {op} resumed>) = 7 {annotation}",
        ]
    else:
        calls = [
            f"{op}({args}) = 7 {annotation}",
            (11, "exit(0) = ?"),
            (11, "+++ exited with 0 +++"),
        ]
    path = trial / "trace.log"
    path.write_text("".join(trace(*calls)) + path.read_text())
    (trial / "strace-stderr.txt").write_text(
        "strace: Process 10 attached\nstrace: Process 11 attached\n"
    )
    result = MODULE["audit_trial"](trial)
    assert result["verdict"] == "pass", result["anomalies"]
    assert result["capture_scope"] == "expanded_process_kernel_io"
    assert all(result["coverage"].values())
    assert result["thread_parents"] == {11: 10}
    clone = next(e for e in result["mechanism_events"] if e["op"] == op)
    assert clone["result"] == 7 and clone["child"] == 11
    assert "999" not in json.dumps(result)


@pytest.mark.parametrize(
    "returned",
    [
        "7",  # Parent argument annotation must not repair a missing return mapping.
        "7 /* 12 in strace's PID NS */",  # Disagrees with actual attached/observed TID.
        "7 /* 11 in strace's PID NS */ /* 12 in strace's PID NS */",
        "7 /* 11 in strace's PID NS */ /* 11 in strace's PID NS */",
        "7 /* 0 in strace's PID NS */",
        "7 /* unknown in strace's PID NS */",
        "7 /* 11 in strace's PID NS",
    ],
)
@pytest.mark.parametrize("resumed", [False, True])
def test_missing_conflicting_or_malformed_pidns_return_never_passes(
    trial, returned, resumed
):
    expanded_observer(trial)
    args = "flags=CLONE_THREAD|CLONE_FILES, parent_tid=[7 /* 11 in strace's PID NS */]"
    calls = (
        [f"clone({args} <unfinished ...>", f"<... clone resumed>) = {returned}"]
        if resumed
        else [f"clone({args}) = {returned}"]
    )
    calls += [(11, "exit(0) = ?"), (11, "+++ exited with 0 +++")]
    path = trial / "trace.log"
    path.write_text("".join(trace(*calls)) + path.read_text())
    (trial / "strace-stderr.txt").write_text(
        "strace: Process 10 attached\nstrace: Process 11 attached\n"
    )
    result = MODULE["audit_trial"](trial)
    assert result["verdict"] == "inconclusive" and result["anomalies"]


@pytest.mark.parametrize(
    "call",
    [
        "fork() = 7 /* 11 in strace's PID NS */",
        "vfork() = 7 /* 11 in strace's PID NS */",
        "clone(flags=CLONE_FILES, parent_tid=[7 /* 11 in strace's PID NS */]) = 7 /* 11 in strace's PID NS */",
        "clone3({flags=CLONE_THREAD}, 88) = 7 /* 11 in strace's PID NS */",
    ],
)
def test_pidns_annotation_does_not_authorize_nonshared_processes(call):
    result = audit(call)
    assert result.verdict() == "inconclusive" and not result.children


@pytest.mark.parametrize(
    "census",
    [{}, {"10": 98}, {"11": 99}, {"10": "99"}, {"x": 99}, {"-10": 99}, {"10": True}],
)
def test_invalid_census_is_not_silently_legacy(trial, census):
    expanded_observer(trial, census)
    result = MODULE["audit_trial"](trial)
    assert result["verdict"] == "inconclusive"
    assert any("Invalid pre-GO" in a["reason"] for a in result["anomalies"])


def test_census_threads_need_attachment_and_exit_and_creation_proof(trial):
    expanded_observer(trial, {"10": 99, "11": 99})
    assert MODULE["audit_trial"](trial)["verdict"] == "inconclusive"
    path = trial / "trace.log"
    (trial / "strace-stderr.txt").write_text(
        "strace: Process 10 attached\nstrace: Process 11 attached\n"
    )
    path.write_text(
        "".join(trace((11, "exit(0) = ?"), (11, "+++ exited with 0 +++")))
        + path.read_text()
    )
    assert MODULE["audit_trial"](trial)["verdict"] == "pass"
    change_json(trial, "observer.json", before_go_threads={"10": 99})
    assert MODULE["audit_trial"](trial)["verdict"] == "inconclusive"


def test_process_capture_without_census_is_inconclusive(trial):
    expanded_observer(trial)
    path = trial / "observer.json"
    observer = json.loads(path.read_text())
    del observer["before_go_threads"]
    path.write_text(json.dumps(observer))
    assert MODULE["audit_trial"](trial)["verdict"] == "inconclusive"


@pytest.mark.parametrize(
    "call",
    [
        "clone(child_stack=0xaaaa, flags=CLONE_THREAD|CLONE_FILES) = 11",
        "clone3({flags=CLONE_FILES|CLONE_THREAD, exit_signal=0}, 88) = 0xb",
        "clone3(0xaaaa, 88) = -1 ENOSYS (Function not implemented)",
        "clone3(0xaaaa, 88) = -1 EPERM (Operation not permitted)",
    ],
)
def test_shared_clone_or_explicitly_denied_probe_is_supported(call):
    result = audit(call)
    assert result.verdict() == "pass"
    assert result.mechanisms and result.mechanisms[0]["result"] != 0


@pytest.mark.parametrize(
    "call",
    [
        "fork() = 11",
        "vfork() = 11",
        "clone(child_stack=NULL, flags=CLONE_FILES|SIGCHLD) = 11",
        "clone3({flags=CLONE_THREAD, exit_signal=0}, 88) = 11",
        "clone3(0xaaaa, 88) = 11",
        "clone3({flags=0xaaaa}, 88) = 11",
        "clone3({flags=CLONE_THREAD|CLONE_FILES|CLONE_UNKNOWN}, 88) = 11",
        "clone3(0xaaaa, 88) = -1 EINVAL (Invalid argument)",
        'execve("/bin/private-record", ["private-record"], 0xaaaa) = 0',
        'execveat(AT_FDCWD, "/bin/private-record", [], [], 0) = -1 ENOENT (Missing)',
        "wait4(-1, 0xaaaa, 0, NULL) = 11",
    ],
)
def test_process_or_unresolved_clone_invalidates_shared_fd_model(call):
    result = audit(call)
    assert result.verdict() == "inconclusive"
    assert "private-record" not in json.dumps(result.mechanisms + result.anomalies)


def test_thread_exit_syscalls_and_pending_exit_record():
    result = audit(
        (11, "exit(0) = ?"),
        (11, "+++ exited with 0 +++"),
        "exit_group(0 <unfinished ...>",
        "+++ exited with 0 +++",
    )
    assert result.verdict() == "pass" and result.exit_calls == {11: 0, 10: 0}
    assert audit("exit(1) = ?", "+++ exited with 0 +++").verdict() == "inconclusive"
    assert audit("+++ exited with 0 +++", "exit(0) = ?").verdict() == "inconclusive"


@pytest.mark.parametrize("op", sorted(MODULE["ASYNC_IO"]))
@pytest.mark.parametrize(
    "returned, verdict",
    [
        ("-1 ENOSYS (Function not implemented)", "pass"),
        ("-1 EPERM (Operation not permitted)", "pass"),
        ("-1 EINVAL (Invalid argument)", "inconclusive"),
        ("0", "inconclusive"),
        ("3", "inconclusive"),
    ],
)
def test_async_io_only_recognized_denials_are_supported(op, returned, verdict):
    result = audit(f"{op}(0xaaaa, 0xbbbb, 0xcccc) = {returned}")
    assert result.verdict() == verdict
    assert result.mechanisms[0]["outcome"] == (
        "denied" if verdict == "pass" else "unsupported"
    )
    assert "aaaa" not in json.dumps(result.mechanisms + result.anomalies)


@pytest.mark.parametrize(
    "call",
    [
        "sendfile(0x3, 0x8, NULL, 0x10) = 0x10",
        "copy_file_range(0x8, NULL, 0x3, NULL, 0x10, 0) = 0x10",
        "splice(0x8, NULL, 0x3, NULL, 0x10, 0) = 0x10",
        "tee(0x8, 0x3, 0x10, 0) = 0x10",
        "vmsplice(0x3, 0xaaaa, 0x1, 0) = 0x10",
    ],
)
def test_transfer_output_fd_is_classified_and_input_fd_is_not_written(call):
    good = audit(
        marker("setup", "begin"),
        'open("/tmp/writable", O_RDWR|O_CREAT, 0600) = 3</tmp/writable>',
        call,
        marker("setup", "end"),
    )
    assert good.verdict() == "pass"
    assert good.events[-2]["path"] == "/tmp/writable" and good.events[-2]["bytes"] == 16
    assert audit(call).verdict() == "inconclusive"
    bad = audit('open("/fallback", O_RDWR|O_CREAT, 0600) = 3</fallback>', call)
    assert bad.verdict() == "fail" and bad.events[-1]["category"] == "FORBIDDEN"
    assert (
        audit(call.rsplit(" = ", 1)[0] + " = -1 ENOSYS (Not implemented)").verdict()
        == "pass"
    )


def test_fallocate_and_truncate_classify_existing_paths_and_failed_attempts():
    result = audit(
        marker("setup", "begin"),
        'open("/tmp/writable", O_RDWR) = 3</tmp/writable>',
        "fallocate(0x3, 0, 0, 4096) = 0",
        'truncate("/tmp/writable", 4096) = 0',
        marker("setup", "end"),
    )
    assert result.verdict() == "pass"
    assert audit("fallocate(0x3, 0, 0, 4096) = 0").verdict() == "inconclusive"
    assert (
        audit("fallocate(3</tmp/hidden>, 0, 0, 4096) = -1 EPERM (Denied)").verdict()
        == "fail"
    )
    assert (
        audit('truncate("/tmp/hidden", 4096) = -1 EROFS (Read-only)').verdict()
        == "fail"
    )
