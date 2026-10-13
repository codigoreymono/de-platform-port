import os
import socket
from pathlib import Path
from typing import NoReturn

import pytest


@pytest.fixture(autouse=True)
def offline_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Fail closed on real network use and avoid host AWS credential discovery."""

    def deny_network(*args: object, **kwargs: object) -> NoReturn:
        raise RuntimeError("Real network access is disabled in offline tests")

    for name in (
        "create_connection",
        "getaddrinfo",
        "gethostbyname",
        "gethostbyname_ex",
        "gethostbyaddr",
        "getnameinfo",
    ):
        monkeypatch.setattr(socket, name, deny_network)
    for name in ("connect", "connect_ex", "sendto"):
        monkeypatch.setattr(socket.socket, name, deny_network)
    if hasattr(socket.socket, "sendmsg"):
        monkeypatch.setattr(socket.socket, "sendmsg", deny_network)

    for name in list(os.environ):
        if name.startswith("AWS_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-aws-config"))
