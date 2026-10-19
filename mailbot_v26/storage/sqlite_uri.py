"""SQLite URI helpers shared by every read-only connection."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote


def sqlite_readonly_uri(path: str | Path) -> str:
    """Return a ``file:`` URI that opens *path* read-only (use with ``uri=True``).

    Unlike ``f"file:{path}?mode=ro"`` it survives '#', '?', '%' and spaces in
    the path, and unlike ``Path.as_uri()`` it keeps UNC paths
    (\\\\server\\share\\db.sqlite) openable: SQLite rejects a URI authority, so
    the UNC host stays inside the path part (``file:////server/share/...``).
    ``mode=ro`` never creates a missing database file. The path is made
    absolute without touching the filesystem (resolve() can stall on an
    unreachable network share).
    """
    text = os.path.abspath(os.fspath(path)).replace("\\", "/")
    prefix = "//" if text.startswith("//") else "///"
    return f"file:{prefix}{quote(text, safe='/:$')}?mode=ro"


__all__ = ["sqlite_readonly_uri"]
