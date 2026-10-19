"""Repository-wide pytest guards (applies to both test suites).

1. Tests must exercise this checkout: the repo root goes first on sys.path and
   on PYTHONPATH (inherited by subprocess-based tests), and a stale editable
   install of ``mailbot_v26`` elsewhere fails the session immediately.
2. Tests must not reach the network: connections to non-loopback addresses
   raise, so an unmocked Telegram/IMAP/LLM call fails loudly instead of
   silently depending on connectivity.
"""

from __future__ import annotations

import os
import socket
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
_existing_pythonpath = os.environ.get("PYTHONPATH", "")
if str(REPO_ROOT) not in _existing_pythonpath.split(os.pathsep):
    os.environ["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(REPO_ROOT), _existing_pythonpath) if part
    )


def pytest_sessionstart(session: pytest.Session) -> None:
    import mailbot_v26

    package_file = Path(mailbot_v26.__file__).resolve()
    if REPO_ROOT not in package_file.parents:
        raise pytest.UsageError(
            "mailbot_v26 is imported from "
            f"{package_file}, not from this checkout ({REPO_ROOT}). "
            "Uninstall the stale editable install (python -m pip uninstall "
            "mailbot_v26) or run tests from a dedicated virtual environment."
        )


_LOOPBACK_HOSTS = {"localhost", "::1", "0.0.0.0", ""}


def _is_loopback(address: object) -> bool:
    if not isinstance(address, tuple) or not address:
        return True  # non-IP families (e.g. AF_UNIX) are local by definition
    host = str(address[0]).strip().lower()
    return host in _LOOPBACK_HOSTS or host.startswith("127.")


@pytest.fixture(autouse=True)
def _block_external_network(monkeypatch: pytest.MonkeyPatch) -> None:
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex

    def guarded_connect(self: socket.socket, address: object) -> None:
        if not _is_loopback(address):
            raise RuntimeError(f"tests must not open network connections: {address!r}")
        return original_connect(self, address)

    def guarded_connect_ex(self: socket.socket, address: object) -> int:
        if not _is_loopback(address):
            raise RuntimeError(f"tests must not open network connections: {address!r}")
        return original_connect_ex(self, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)
