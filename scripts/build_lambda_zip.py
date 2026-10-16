"""Offline, locked-wheel Lambda ZIP assembly; not a runtime compatibility gate."""

import argparse
import hashlib
import itertools
import json
import re
import stat
import subprocess
import sys
import tarfile
import tomllib
import zipfile
import zlib
from email.parser import Parser
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parents[1]
PLATFORM = "x86_64-manylinux_2_34"
ZIP_TIME = (1980, 1, 1, 0, 0, 0)
SDK = {
    "boto3",
    "botocore",
    "jmespath",
    "python-dateutil",
    "s3transfer",
    "six",
    "urllib3",
}
COMMON_MODULES = (
    "__init__",
    "config",
    "manifests",
    "storage",
    "s3_store",
    "lambda_common",
    "lambda_runtime",
    "source_contract",
    "bronze",
)
STAGES = {
    "ingest": {
        "roots": ("boto3", "httpx"),
        "prune": ("duckdb", "pandas", "pyarrow"),
        "packages": SDK
        | {
            "anyio",
            "certifi",
            "h11",
            "httpcore",
            "httpx",
            "idna",
            "typing-extensions",
        },
        "modules": COMMON_MODULES + ("source", "lambda_ingest"),
    },
    "curate": {
        "roots": ("boto3", "pandas", "pyarrow"),
        "prune": ("httpx", "duckdb"),
        "packages": SDK | {"numpy", "pandas", "pyarrow"},
        "modules": COMMON_MODULES
        + (
            "silver_contract",
            "silver",
            "silver_store",
            "lambda_curate",
        ),
    },
    "gold": {
        "roots": ("boto3", "duckdb", "pyarrow"),
        "prune": ("httpx", "pandas"),
        "packages": SDK | {"duckdb", "pyarrow"},
        "modules": COMMON_MODULES
        + (
            "silver_contract",
            "silver_store",
            "gold",
            "gold_store",
            "catalog",
            "lambda_gold",
        ),
    },
}
SQL_PRODUCTS = (
    "channel_usage",
    "closure_duration",
    "demand_daily",
    "outstanding_requests",
)


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def canonical_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def wheel_tags(filename: str) -> set[str]:
    fields = filename.removesuffix(".whl").rsplit("-", 3)[1:]
    return {
        "-".join(parts)
        for parts in itertools.product(*(field.split(".") for field in fields))
    }


def inventory(target: Path, lock: dict, stage: str) -> list[dict]:
    locked = {package["name"]: package for package in lock["package"]}
    result = []
    for directory in sorted(target.glob("*.dist-info")):
        metadata = Parser().parsestr((directory / "METADATA").read_text())
        name = canonical_name(metadata["Name"])
        package = locked.get(name)
        if package is None or metadata["Version"] != package["version"]:
            raise ValueError(f"Installed distribution does not match lock: {name}")
        wheel = Parser().parsestr((directory / "WHEEL").read_text())
        tags = set(wheel.get_all("Tag", []))
        candidates = [
            item
            for item in package["wheels"]
            if wheel_tags(unquote(urlparse(item["url"]).path.rsplit("/", 1)[-1]))
            == tags
        ]
        if len(candidates) != 1:
            raise ValueError(f"Cannot identify unique locked wheel: {name}, {tags}")
        result.append(
            {
                "name": name,
                "version": metadata["Version"],
                "wheel": candidates[0],
                "tags": sorted(tags),
                "dependencies": package.get("dependencies", []),
            }
        )
    names = [item["name"] for item in result]
    if len(names) != len(set(names)) or set(names) != STAGES[stage]["packages"]:
        raise ValueError(f"Unexpected {stage} transitive inventory: {sorted(names)}")
    return sorted(result, key=lambda item: item["name"])


def nonruntime_category(path: Path) -> str | None:
    """Narrow exclusions for prebuilt stages that never compile extensions."""
    parts = path.parts
    if any(
        part.lower() in {"license", "licenses"} for part in parts
    ) or path.name.lower().startswith(("license", "notice", "copying")):
        return None
    if path.as_posix() in {
        "numpy/conftest.py",
        "pandas/conftest.py",
        "pyarrow/conftest.py",
    }:
        return "pytest_configuration"
    if path.as_posix() in {
        "boto3/examples/cloudfront.rst",
        "boto3/examples/s3.rst",
    }:
        return "prose_examples"
    if parts[:1] != ("pyarrow",):
        return None
    # Compiled Arrow extensions stay intact; only downstream build inputs are omitted.
    if (len(parts) == 2 and path.suffix in {".pyx", ".pxd", ".pxi"}) or (
        len(parts) == 3 and parts[1] == "includes" and path.suffix == ".pxd"
    ):
        return "arrow_cython_sources"
    if parts[:2] == ("pyarrow", "src") and path.suffix in {
        ".c",
        ".cc",
        ".cpp",
        ".h",
        ".hpp",
    }:
        return "arrow_cpp_sources"
    if parts[:2] == ("pyarrow", "include") and path.suffix in {".h", ".hpp"}:
        return "arrow_headers"
    return None


def include(path: Path) -> bool:
    excluded = {
        "__pycache__",
        ".pytest_cache",
        "tests",
        "test",
        "notebooks",
        "bin",
    }
    if excluded.intersection(path.parts) or path.suffix in {".pyc", ".pyo"}:
        return False
    # Installed RECORD includes generated console-script hashes and is host-dependent.
    if any(part.endswith(".dist-info") for part in path.parts):
        return path.name not in {"RECORD", "INSTALLER", "REQUESTED", "direct_url.json"}
    return nonruntime_category(path) is None


def build(stage: str, output: Path, *, root: Path = ROOT) -> Path:
    config = STAGES[stage]
    output = output.resolve()
    if not output.parent.is_dir():
        raise ValueError("Output parent must already exist")
    # Refuse existing outputs rather than replacing another build's evidence.
    output.mkdir()
    lock = tomllib.loads((root / "uv.lock").read_text())
    uv_version = subprocess.run(
        ["uv", "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    commands = []
    with TemporaryDirectory(prefix="target-", dir=output) as work:
        target = Path(work) / "packages"
        requirements = output / "requirements.txt"
        export = [
            "uv",
            "export",
            "--offline",
            "--frozen",
            "--no-python-downloads",
            "--format",
            "requirements.txt",
            "--no-default-groups",
            "--no-group",
            "dev",
            "--no-group",
            "notebooks",
            "--no-emit-project",
            "--no-header",
            "--no-annotate",
        ]
        for package in config["prune"]:
            export.extend(["--prune", package])
        export.extend(["--output-file", str(requirements)])
        install = [
            "uv",
            "pip",
            "install",
            "--offline",
            "--no-python-downloads",
            "--python-version",
            "3.12",
            "--python-platform",
            PLATFORM,
            "--only-binary",
            ":all:",
            "--require-hashes",
            "--link-mode",
            "copy",
            "--no-compile-bytecode",
            "--target",
            str(target),
            "--requirements",
            str(requirements),
        ]
        for command in (export, install):
            subprocess.run(
                command, cwd=root, check=True, capture_output=True, text=True
            )
            commands.append(
                [
                    argument.replace(str(target), "<target>").replace(
                        str(requirements), "<output>/requirements.txt"
                    )
                    for argument in command
                ]
            )
        packages = inventory(target, lock, stage)
        files = {}
        omitted = []
        for path in target.rglob("*"):
            if path.is_symlink():
                raise ValueError(f"Symlink in isolated install: {path}")
            relative = path.relative_to(target)
            if path.is_file():
                category = nonruntime_category(relative)
                if category:
                    omitted.append(
                        {
                            "path": relative.as_posix(),
                            "category": category,
                            "bytes": path.stat().st_size,
                            "sha256": digest(path),
                        }
                    )
                elif include(relative):
                    files[relative.as_posix()] = (path, "wheel")
        for module in config["modules"]:
            relative = f"nyc311/{module}.py"
            if relative in files:
                raise ValueError(f"Project/wheel collision: {relative}")
            files[relative] = (root / "src" / relative, "source")
        if stage == "gold":
            for product in SQL_PRODUCTS:
                relative = f"sql/{product}.sql"
                files[relative] = (root / relative, "source")
        timezone = None
        if stage == "curate":
            archive = target / "dateutil/zoneinfo/dateutil-zoneinfo.tar.gz"
            with tarfile.open(archive) as zones:
                zone = zones.extractfile("America/New_York").read()
                metadata = json.loads(zones.extractfile("METADATA").read())
            if not zone.startswith(b"TZif"):
                raise ValueError("Locked timezone resource is not TZif")
            zone_path = Path(work) / "New_York"
            zone_path.write_bytes(zone)
            files["timezone/America/New_York"] = (zone_path, "timezone-resource")
            timezone = {
                "source": "dateutil/zoneinfo/dateutil-zoneinfo.tar.gz",
                "source_sha256": digest(archive),
                "metadata": metadata,
                "environment": {"PYTHONTZPATH": "/var/task/timezone"},
            }
        entries = []
        artifact = Path(work) / f"nyc311-{stage}.zip"
        with zipfile.ZipFile(
            artifact,
            "x",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            for name, (path, origin) in sorted(files.items()):
                info = zipfile.ZipInfo(name, date_time=ZIP_TIME)
                info.create_system = 3
                info.external_attr = (stat.S_IFREG | 0o644) << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                checksum = hashlib.sha256()
                previous = b""
                with path.open("rb") as source, archive.open(info, "w") as destination:
                    while chunk := source.read(1024 * 1024):
                        if b"/nix/store/" in previous + chunk:
                            raise ValueError(f"Nix store reference in payload: {name}")
                        previous = chunk[-16:]
                        checksum.update(chunk)
                        destination.write(chunk)
                entries.append(
                    {
                        "path": name,
                        "sha256": checksum.hexdigest(),
                        "origin": origin,
                        "mode": "0644",
                        "mtime": list(ZIP_TIME),
                    }
                )
        report = {
            "schema_version": 1,
            "stage": stage,
            "handler": f"nyc311.lambda_{stage}.handler",
            "python": "3.12",
            "architecture": "x86_64",
            "platform": PLATFORM,
            "direct_dependencies": list(config["roots"]),
            "packages": packages,
            "modules": list(config["modules"]),
            "timezone": timezone,
            "inputs": {
                name: digest(root / name)
                for name in (
                    "pyproject.toml",
                    "uv.lock",
                    "scripts/build_lambda_zip.py",
                )
            },
            "requirements_sha256": digest(requirements),
            "tools": {
                "uv": uv_version,
                "python": sys.version,
                "zlib": zlib.ZLIB_RUNTIME_VERSION,
            },
            "commands": commands,
            "entries": entries,
            "omitted_nonruntime": sorted(omitted, key=lambda item: item["path"]),
            "zip_sha256": digest(artifact),
            "verification": "assembly only; task 8.4 runtime and size gates pending",
        }
        (output / "inventory.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        artifact.replace(output / artifact.name)
    return output / artifact.name


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=STAGES)
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New directory below an existing parent",
    )
    args = parser.parse_args()
    try:
        print(build(args.stage, args.output))
    except subprocess.CalledProcessError as error:
        parser.exit(
            1, f"Offline uv command failed (no network fallback):\n{error.stderr}"
        )


if __name__ == "__main__":
    main()
