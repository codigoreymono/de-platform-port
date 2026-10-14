"""Small local object I/O and layout helpers, without pipeline publication logic."""

import json
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import date, timedelta
from math import isfinite
from pathlib import Path, PurePosixPath
from tempfile import NamedTemporaryFile

from nyc311.config import Mode, validate_window
from nyc311.manifests import Manifest


def output_root(base: Path, mode: Mode) -> Path:
    """Sample and published outputs are disjoint siblings beneath the base."""
    if mode not in ("sample", "published"):
        raise ValueError("Mode must be sample or published")
    root = base.resolve() / mode
    if root.is_symlink():
        raise ValueError("Output root must not be a symbolic link")
    return root


def object_path(root: Path, key: str) -> Path:
    relative = PurePosixPath(key)
    if (
        not key
        or relative.is_absolute()
        or ".." in relative.parts
        or not relative.parts
        or "\\" in key
    ):
        raise ValueError("Storage key must be a nonempty relative POSIX path")
    root = root.resolve()
    path = root.joinpath(*relative.parts).resolve()
    if not path.is_relative_to(root):
        raise ValueError("Storage key must remain inside its root")
    return path


def manifest_key(manifest: Manifest) -> str:
    run_id = manifest["run_id"]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", run_id):
        raise ValueError("Run identifier must be a single safe path component")
    start = date.fromisoformat(manifest["start_date"])
    end = date.fromisoformat(manifest["end_date"])
    validate_window(start, end)
    kind = manifest["kind"]
    if kind == "run":
        return f"control/runs/{run_id}.json"
    if kind == "gold":
        return f"control/gold/{run_id}.json"
    if end - start != timedelta(days=1):
        raise ValueError("Bronze, Silver, and quality manifests cover one day")
    if kind == "bronze":
        return f"control/bronze/{run_id}/{start.isoformat()}.json"
    if kind == "silver":
        return f"control/silver/{start.isoformat()}.json"
    if kind == "quality":
        return f"quality/run={run_id}/{start.isoformat()}.json"
    raise ValueError("Unknown manifest kind")


def write_bytes(root: Path, key: str, content: bytes) -> None:
    path = object_path(root, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def read_bytes(root: Path, key: str) -> bytes:
    return object_path(root, key).read_bytes()


def write_json(root: Path, key: str, value: Mapping[str, object]) -> None:
    content = json.dumps(dict(value), indent=2, allow_nan=False) + "\n"
    write_bytes(root, key, content.encode("utf-8"))


@contextmanager
def temporary_object(root: Path, key: str) -> Iterator[Path]:
    """Stage beside the destination for same-filesystem replacement, not append."""
    path = object_path(root, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with NamedTemporaryFile(
            dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
        yield temporary_path
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass


def write_json_atomic(root: Path, key: str, value: Mapping[str, object]) -> None:
    with temporary_object(root, key) as temporary:
        write_json(root, temporary.relative_to(root).as_posix(), value)
        temporary.replace(object_path(root, key))


def decode_json(content: bytes) -> dict[str, object]:
    def finite_number(value: str) -> float:
        number = float(value)
        if not isfinite(number):
            raise ValueError("JSON numbers must be finite")
        return number

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result = {}
        for name, value in pairs:
            if name in result:
                raise ValueError(f"Duplicate JSON key: {name}")
            result[name] = value
        return result

    value = json.loads(
        content,
        parse_float=finite_number,
        parse_constant=finite_number,
        object_pairs_hook=unique_object,
    )
    if not isinstance(value, dict):
        raise ValueError("Manifest JSON must contain an object")
    return value


def read_json(root: Path, key: str) -> dict[str, object]:
    return decode_json(read_bytes(root, key))
