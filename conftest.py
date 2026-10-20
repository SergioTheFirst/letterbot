"""Repository-wide pytest guards (applies to both test suites).

1. Tests must exercise this checkout: the repo root goes first on sys.path and
   on PYTHONPATH (inherited by subprocess-based tests), and a stale editable
   install of ``mailbot_v26`` elsewhere fails the session immediately.
2. Tests must not reach the network: connections to non-loopback addresses
   raise, so an unmocked Telegram/IMAP/LLM call fails loudly instead of
   silently depending on connectivity.
3. Tests must never open the live database (repo-root database.sqlite): the
   pipeline's import-time singletons get a throwaway database via
   LETTERBOT_DEFAULT_DB_PATH, and any sqlite3.connect to the live file raises.
"""

from __future__ import annotations

import atexit
import os
import shutil
import socket
import sqlite3
import sys
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest

REPO_ROOT = Path(__file__).resolve().parent
LIVE_DB = (REPO_ROOT / "database.sqlite").resolve()

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
_existing_pythonpath = os.environ.get("PYTHONPATH", "")
if str(REPO_ROOT) not in _existing_pythonpath.split(os.pathsep):
    os.environ["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(REPO_ROOT), _existing_pythonpath) if part
    )

# Set before any test module imports mailbot_v26 (pipeline singletons open a DB
# at import time); inherited by subprocess-based tests too.
if not os.environ.get("LETTERBOT_DEFAULT_DB_PATH"):
    _import_db_dir = tempfile.mkdtemp(prefix="letterbot-tests-")
    atexit.register(shutil.rmtree, _import_db_dir, ignore_errors=True)
    os.environ["LETTERBOT_DEFAULT_DB_PATH"] = str(
        Path(_import_db_dir) / "import-default.sqlite"
    )


def _sqlite_target(database: object) -> Path | None:
    text = os.fspath(database) if isinstance(database, (str, os.PathLike)) else None
    if not text or text == ":memory:":
        return None
    if text.startswith("file:"):
        path = unquote(urlsplit(text).path)
        if len(path) > 2 and path[0] == "/" and path[2] == ":":
            path = path[1:]  # file:///C:/x -> C:/x
        text = path or ""
        if not text:
            return None
    try:
        return Path(text).resolve()
    except OSError:
        return None


_original_sqlite_connect = sqlite3.connect


def _guarded_sqlite_connect(database, *args, **kwargs):
    if _sqlite_target(database) == LIVE_DB:
        raise RuntimeError(f"tests must not open the live database: {database!r}")
    return _original_sqlite_connect(database, *args, **kwargs)


sqlite3.connect = _guarded_sqlite_connect


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
