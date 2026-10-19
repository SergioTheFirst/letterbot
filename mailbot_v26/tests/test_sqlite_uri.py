from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

from mailbot_v26.storage.sqlite_uri import sqlite_readonly_uri


def test_readonly_uri_opens_paths_with_special_characters(tmp_path: Path) -> None:
    db_path = tmp_path / "letter#bot 100% &x" / "mail.sqlite"
    db_path.parent.mkdir()
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE marker (value TEXT)")
        conn.execute("INSERT INTO marker VALUES ('here')")
    conn.close()

    ro = sqlite3.connect(sqlite_readonly_uri(db_path), uri=True)
    try:
        assert ro.execute("SELECT value FROM marker").fetchone() == ("here",)
        with pytest.raises(sqlite3.OperationalError):
            ro.execute("INSERT INTO marker VALUES ('nope')")
    finally:
        ro.close()


def test_readonly_uri_never_creates_a_missing_database(tmp_path: Path) -> None:
    missing = tmp_path / "absent.sqlite"

    with pytest.raises(sqlite3.OperationalError):
        sqlite3.connect(sqlite_readonly_uri(missing), uri=True).execute("SELECT 1")

    assert not missing.exists()


@pytest.mark.skipif(sys.platform != "win32", reason="UNC paths are Windows-only")
def test_readonly_uri_keeps_unc_host_inside_the_path() -> None:
    uri = sqlite_readonly_uri("\\\\fileserver\\share\\mail #1.sqlite")

    # SQLite rejects a URI authority, so the UNC host must stay in the path part.
    assert uri == "file:////fileserver/share/mail%20%231.sqlite?mode=ro"
