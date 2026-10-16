"""Assembly and lock checks only; native runtime checks belong to task 8.4."""

import ast
import gzip
import hashlib
import importlib.util
import io
import json
import stat
import subprocess
import tarfile
import tomllib
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "lambda_packaging",
    ROOT / "scripts/build_lambda_zip.py",
)
packaging = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(packaging)
LOCK = tomllib.loads((ROOT / "uv.lock").read_text())


def test_real_lock_stage_closures():
    locked = {item["name"]: item for item in LOCK["package"]}
    for config in packaging.STAGES.values():
        closure = set()
        pending = list(config["roots"])
        while pending:
            name = pending.pop()
            if name in closure:
                continue
            closure.add(name)
            for dependency in locked[name].get("dependencies", []):
                marker = dependency.get("marker")
                if marker:
                    # The current runtime graph has only this non-Linux branch.
                    assert (
                        marker
                        == "sys_platform == 'emscripten' or sys_platform == 'win32'"
                    )
                    continue
                pending.append(dependency["name"])
        assert closure == config["packages"]
        for name in closure:
            assert locked[name]["source"] == {"registry": "https://pypi.org/simple"}
            assert any(
                item["url"].endswith("-none-any.whl")
                or "cp312-cp312-manylinux" in item["url"]
                and "x86_64" in item["url"]
                for item in locked[name]["wheels"]
            )
            assert all(
                item["hash"].startswith("sha256:") for item in locked[name]["wheels"]
            )


@pytest.mark.parametrize("stage", packaging.STAGES)
def test_stage_top_level_imports_are_present(stage):
    modules = set(packaging.STAGES[stage]["modules"])
    for module in modules:
        tree = ast.parse((ROOT / f"src/nyc311/{module}.py").read_text())
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith("nyc311."):
                    assert node.module.split(".")[1] in modules
                elif node.module == "nyc311":
                    assert all(alias.name in modules for alias in node.names)
    assert "pipeline" not in modules and "cli" not in modules
    assert ("source" in modules) == (stage == "ingest")
    assert ("silver" in modules) == (stage == "curate")
    assert ("gold" in modules) == (stage == "gold")


@pytest.fixture
def mocked_uv(monkeypatch):
    calls = []
    locked = {item["name"]: item for item in LOCK["package"]}

    def run(command, **kwargs):
        calls.append(command)
        if command == ["uv", "--version"]:
            return SimpleNamespace(stdout="uv test\n")
        if command[1] == "export":
            path = Path(command[command.index("--output-file") + 1])
            path.write_text("# mocked locked export\n")
        else:
            target = Path(command[command.index("--target") + 1])
            stage = target.parent.parent.name.split("-")[0]
            for name in packaging.STAGES[stage]["packages"]:
                package = locked[name]
                wheel = next(
                    item
                    for item in package["wheels"]
                    if (
                        item["url"].endswith("-none-any.whl")
                        or "cp312-cp312-manylinux" in item["url"]
                        and "x86_64" in item["url"]
                    )
                )
                directory = target / f"{name}-{package['version']}.dist-info"
                directory.mkdir(parents=True)
                (directory / "METADATA").write_text(
                    f"Name: {name}\nVersion: {package['version']}\n",
                )
                tags = packaging.wheel_tags(wheel["url"].rsplit("/", 1)[-1])
                (directory / "WHEEL").write_text(
                    "\n".join(f"Tag: {tag}" for tag in sorted(tags)),
                )
                (directory / "RECORD").write_text("host dependent")
            for relative in (
                "bin/httpx",
                "pandas/tests/test_fake.py",
                "numpy/__pycache__/fake.pyc",
                "boto3/examples/s3.rst",
                "botocore/docs/service.py",
                "botocore/data/s3/2006-03-01/examples-1.json",
            ):
                path = target / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("excluded")
            if stage in {"curate", "gold"}:
                for relative in (
                    "pyarrow/include/arrow/api.h",
                    "pyarrow/include/arrow/testing/helper.h",
                    "pyarrow/src/arrow/python/arrow_to_pandas.cc",
                    "pyarrow/includes/libarrow.pxd",
                    "pyarrow/lib.pyx",
                    "pyarrow/conftest.py",
                    "pyarrow/lib.cpython-312-x86_64-linux-gnu.so",
                    "pyarrow/libarrow.so.2600",
                    "pyarrow/src/CMakeLists.txt",
                    "pyarrow/include/LICENSE.h",
                    "pyarrow/src/licenses/legal.h",
                    "pyarrow-26.0.0.dist-info/licenses/NOTICE.txt",
                ):
                    path = target / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(b"fixture payload")
            path = target / "dateutil/zoneinfo/dateutil-zoneinfo.tar.gz"
            path.parent.mkdir(parents=True, exist_ok=True)
            with (
                gzip.GzipFile(path, "wb", mtime=0) as compressed,
                tarfile.open(fileobj=compressed, mode="w") as archive,
            ):
                for name, content in (
                    ("America/New_York", b"TZif-test-resource"),
                    ("METADATA", b'{"tzversion":"test"}'),
                ):
                    info = tarfile.TarInfo(name)
                    info.size = len(content)
                    archive.addfile(info, io.BytesIO(content))
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(packaging.subprocess, "run", run)
    return calls


@pytest.mark.parametrize("stage", packaging.STAGES)
def test_mocked_build_inventory_and_repeatable_zip(stage, tmp_path, mocked_uv):
    # Mocked wheel bytes are stable, including the timezone archive timestamp.
    first = packaging.build(stage, tmp_path / f"{stage}-first")
    report = json.loads((first.parent / "inventory.json").read_text())
    with zipfile.ZipFile(first) as archive:
        assert archive.namelist() == sorted(archive.namelist())
        assert set(archive.namelist()) == {item["path"] for item in report["entries"]}
        for entry in report["entries"]:
            assert (
                hashlib.sha256(archive.read(entry["path"])).hexdigest()
                == entry["sha256"]
            )
            info = archive.getinfo(entry["path"])
            assert info.date_time == packaging.ZIP_TIME
            assert info.external_attr >> 16 == stat.S_IFREG | 0o644
            assert packaging.include(Path(entry["path"]))
        assert ("sql/demand_daily.sql" in archive.namelist()) == (stage == "gold")
        assert ("timezone/America/New_York" in archive.namelist()) == (
            stage == "curate"
        )
    assert {item["name"] for item in report["packages"]} == packaging.STAGES[stage][
        "packages"
    ]
    for item in report["packages"]:
        locked = next(p for p in LOCK["package"] if p["name"] == item["name"])
        assert item["version"] == locked["version"]
        assert item["wheel"] in locked["wheels"]
    assert packaging.digest(first) == report["zip_sha256"]
    assert report["omitted_nonruntime"] == sorted(
        report["omitted_nonruntime"], key=lambda item: item["path"]
    )
    with zipfile.ZipFile(first) as archive:
        assert "botocore/docs/service.py" in archive.namelist()
        assert "botocore/data/s3/2006-03-01/examples-1.json" in archive.namelist()
        assert "boto3/examples/s3.rst" not in archive.namelist()
        if stage in {"curate", "gold"}:
            assert "pyarrow/libarrow.so.2600" in archive.namelist()
            assert "pyarrow/include/LICENSE.h" in archive.namelist()
            assert "pyarrow/src/licenses/legal.h" in archive.namelist()
            assert "pyarrow-26.0.0.dist-info/licenses/NOTICE.txt" in archive.namelist()
            assert "pyarrow/src/CMakeLists.txt" in archive.namelist()
            assert "pyarrow/include/arrow/api.h" not in archive.namelist()
        for item in report["omitted_nonruntime"]:
            assert item["path"] not in archive.namelist()
            assert item["category"] == packaging.nonruntime_category(Path(item["path"]))
            content = (
                b"excluded"
                if item["category"] == "prose_examples"
                else b"fixture payload"
            )
            assert item["bytes"] == len(content)
            assert item["sha256"] == hashlib.sha256(content).hexdigest()
    assert not list(first.parent.glob("target-*"))
    second = packaging.build(stage, tmp_path / f"{stage}-second")
    assert packaging.digest(first) == packaging.digest(second)
    assert json.loads((second.parent / "inventory.json").read_text()) == report
    for command in mocked_uv:
        if command[1] == "--version":
            continue
        assert "--offline" in command and "--no-python-downloads" in command
        if command[1] == "export":
            assert "--frozen" in command and "--no-default-groups" in command
        else:
            assert "--require-hashes" in command and ":all:" in command
            assert packaging.PLATFORM in command and "3.12" in command


def test_inventory_rejects_unlocked_version(tmp_path):
    directory = tmp_path / "boto3.dist-info"
    directory.mkdir()
    (directory / "METADATA").write_text("Name: boto3\nVersion: 0\n")
    with pytest.raises(ValueError, match="does not match lock"):
        packaging.inventory(tmp_path, LOCK, "ingest")


@pytest.mark.parametrize("stage", packaging.STAGES)
def test_inventory_rejects_missing_transitive_packages(stage, tmp_path):
    with pytest.raises(ValueError, match="transitive inventory"):
        packaging.inventory(tmp_path, LOCK, stage)


@pytest.mark.parametrize("invalid_payload", ("nix-reference", "symlink"))
def test_assembly_rejects_host_dependencies(
    invalid_payload, tmp_path, mocked_uv, monkeypatch
):
    original = packaging.subprocess.run

    def inject(command, **kwargs):
        result = original(command, **kwargs)
        if command[1:3] == ["pip", "install"]:
            target = Path(command[command.index("--target") + 1])
            if invalid_payload == "symlink":
                (target / "host.py").symlink_to(ROOT / "pyproject.toml")
            else:
                (target / "host.py").write_text("/nix/store/host-library")
        return result

    monkeypatch.setattr(packaging.subprocess, "run", inject)
    output = tmp_path / "ingest-invalid"
    with pytest.raises(ValueError, match="Symlink|Nix store"):
        packaging.build("ingest", output)
    assert not list(output.glob("*.zip"))
    assert not list(output.glob("target-*"))


def test_failed_offline_install_has_no_retry(tmp_path, mocked_uv, monkeypatch):
    original = packaging.subprocess.run

    def fail(command, **kwargs):
        if command[1:3] == ["pip", "install"]:
            raise subprocess.CalledProcessError(
                1, command, stderr="missing cached wheel"
            )
        return original(command, **kwargs)

    monkeypatch.setattr(packaging.subprocess, "run", fail)
    output = tmp_path / "ingest-failed"
    with pytest.raises(subprocess.CalledProcessError):
        packaging.build("ingest", output)
    assert not list(output.glob("*.zip"))
    assert not list(output.glob("target-*"))


def test_existing_output_is_not_overwritten(tmp_path):
    with pytest.raises(FileExistsError):
        packaging.build("ingest", tmp_path)


@pytest.mark.parametrize(
    "path,category",
    [
        ("numpy/conftest.py", "pytest_configuration"),
        ("pandas/conftest.py", "pytest_configuration"),
        ("pyarrow/conftest.py", "pytest_configuration"),
        ("boto3/examples/s3.rst", "prose_examples"),
        ("boto3/examples/cloudfront.rst", "prose_examples"),
        ("pyarrow/lib.pyx", "arrow_cython_sources"),
        ("pyarrow/lib.pxd", "arrow_cython_sources"),
        ("pyarrow/_compute.pxi", "arrow_cython_sources"),
        ("pyarrow/includes/libarrow.pxd", "arrow_cython_sources"),
        ("pyarrow/src/arrow/python/io.cc", "arrow_cpp_sources"),
        ("pyarrow/src/arrow/python/io.cpp", "arrow_cpp_sources"),
        ("pyarrow/src/arrow/python/io.c", "arrow_cpp_sources"),
        ("pyarrow/src/arrow/python/io.h", "arrow_cpp_sources"),
        ("pyarrow/src/arrow/python/io.hpp", "arrow_cpp_sources"),
        ("pyarrow/include/arrow/api.h", "arrow_headers"),
        ("pyarrow/include/arrow/util/foo.hpp", "arrow_headers"),
    ],
)
def test_narrow_nonruntime_exclusions(path, category):
    assert packaging.nonruntime_category(Path(path)) == category
    assert not packaging.include(Path(path))


@pytest.mark.parametrize(
    "path",
    [
        "pyarrow/lib.cpython-312-x86_64-linux-gnu.so",
        "pyarrow/libarrow.so.2600",
        "pyarrow/libarrow_flight.so.2600",
        "pyarrow/include/runtime.so",
        "pyarrow/src/runtime.a",
        "numpy/random/lib/libnpyrandom.a",
        "numpy/_core/lib/libnpymath.a",
        "numpy.libs/libscipy_openblas64_.so",
        "_duckdb.cpython-312-x86_64-linux-gnu.so",
        "pandas/_libs/groupby.cpython-312-x86_64-linux-gnu.so",
        "pyarrow/__init__.py",
        "pyarrow/_generated_version.py",
        "pyarrow/src/CMakeLists.txt",
        "pyarrow/include/LICENSE.h",
        "pyarrow/src/NOTICE.cpp",
        "pyarrow/src/licenses/legal.h",
        "numpy/testing/__init__.py",
        "numpy/_pytesttester.py",
        "pandas/_testing/__init__.py",
        "pandas/testing.py",
        "anyio/_core/_testing.py",
        "boto3/docs/action.py",
        "botocore/docs/service.py",
        "botocore/data/endpoints.json",
        "botocore/data/s3/2006-03-01/examples-1.json",
        "botocore/data/glue/2017-03-31/service-2.json.gz",
        "dateutil/zoneinfo/dateutil-zoneinfo.tar.gz",
        "certifi/cacert.pem",
        "pyarrow-26.0.0.dist-info/licenses/LICENSE.txt",
        "pyarrow-26.0.0.dist-info/METADATA",
        "pyarrow-26.0.0.dist-info/WHEEL",
        "other/include/api.h",
        "other/src/module.pyx",
        "other/conftest.py",
    ],
)
def test_runtime_native_metadata_and_legal_payloads_are_preserved(path):
    assert packaging.nonruntime_category(Path(path)) is None
    assert packaging.include(Path(path))
