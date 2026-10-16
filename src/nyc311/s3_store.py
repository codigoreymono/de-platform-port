"""Explicit-client S3 object I/O. No POSIX rename or multi-object transaction claims."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

from nyc311.storage import decode_json, read_bytes, write_bytes

if TYPE_CHECKING:
    from botocore.client import BaseClient


def _relative_key(key: str) -> str:
    path = PurePosixPath(key)
    if (
        not key
        or path.is_absolute()
        or not path.parts
        or ".." in path.parts
        or "\\" in key
        or path.as_posix() != key
    ):
        raise ValueError("S3 keys must be canonical relative POSIX paths")
    return key


@contextmanager
def temporary_workspace(parent: Path | None = None) -> Iterator[Path]:
    """An invocation-private workspace; no correctness dependency on warm /tmp state."""
    with TemporaryDirectory(prefix="nyc311-", dir=parent or Path("/tmp")) as directory:
        yield Path(directory)


class S3Store:
    """A bucket/prefix namespace with an already explicitly constructed SDK client."""

    def __init__(
        self,
        client: BaseClient,
        bucket: str,
        prefix: str = "",
        *,
        check: Callable[[], None] | None = None,
    ) -> None:
        if not isinstance(bucket, str) or not bucket or "/" in bucket:
            raise ValueError("Bucket must be a bucket name, not a URI")
        self.client = client
        self.bucket = bucket
        self.prefix = _relative_key(prefix) if prefix else ""
        self.check = check or (lambda: None)
        self.bounded_reads = check is not None

    def key(self, key: str) -> str:
        relative = _relative_key(key)
        return f"{self.prefix}/{relative}" if self.prefix else relative

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        resolved = self.key(key)
        self.check()
        try:
            self.client.head_object(Bucket=self.bucket, Key=resolved)
        except ClientError as error:
            self.check()
            if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise
        self.check()
        return True

    def read(self, key: str) -> bytes:
        resolved = self.key(key)
        self.check()
        response = self.client.get_object(Bucket=self.bucket, Key=resolved)
        body = response["Body"]
        try:
            self.check()
            if not self.bounded_reads:
                return body.read()
            content = bytearray()
            while True:
                self.check()
                chunk = body.read(1024 * 1024)
                self.check()
                if not chunk:
                    return bytes(content)
                content.extend(chunk)
        finally:
            body.close()

    def write(self, key: str, content: bytes) -> None:
        # PutObject publishes one complete object; a prefix is not a directory.
        resolved = self.key(key)
        self.check()
        self.client.put_object(Bucket=self.bucket, Key=resolved, Body=content)
        self.check()

    def read_json(self, key: str) -> dict[str, object]:
        return decode_json(self.read(key))

    def write_json(self, key: str, value: Mapping[str, object]) -> None:
        content = (json.dumps(dict(value), indent=2, allow_nan=False) + "\n").encode()
        self.write(key, content)

    def download(self, key: str, root: Path) -> None:
        write_bytes(root, key, self.read(key))

    def upload(self, root: Path, key: str) -> None:
        self.write(key, read_bytes(root, key))
