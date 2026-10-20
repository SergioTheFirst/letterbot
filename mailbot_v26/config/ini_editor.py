"""Line-preserving editor for the user INI files (Settings page, CONSTITUTION IV).

Only the edited lines change: comments, key order, unknown keys, other
sections ([cloudflare], [gigachat] ...), the UTF-8 BOM and each line's own
line ending stay byte for byte. Values are written as typed: Letterbot reads
these files without %-interpolation (ini_utils.new_user_ini_parser), so "%" is
never escaped. Every edit is verified by parsing the result: when anything
other than the requested values would read differently, nothing is written.
Trade-off: comment lines inside a removed section go with it.

Writes are atomic (temporary file in the same folder, fsync, os.replace) and
first copy the current file to "<name>.bak-YYYYMMDD-HHMMSS" (local time; the
newest BACKUP_KEEP are kept). Backups hold secrets: they stay next to the
file, match no "*.ini" glob, and are only ever addressed by a name that
matches the backup pattern. SECRET_COPY_PATTERNS names them (and the temp
file of an interrupted write) for .gitignore, the source bundle and the
installer, which must never pick them up.
"""

from __future__ import annotations

import codecs
import configparser
import fnmatch
import hashlib
import hmac
import logging
import os
import re
import secrets
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence, Union

from mailbot_v26.config.ini_utils import new_user_ini_parser

logger = logging.getLogger(__name__)

BACKUP_KEEP = 10
_BACKUP_STAMP = "%Y%m%d-%H%M%S"
_BOM = codecs.BOM_UTF8
_EOL_RE = re.compile(r"\r\n|\r|\n")
_SECTCRE = configparser.ConfigParser.SECTCRE
_OPTCRE = configparser.ConfigParser.OPTCRE
# Names Letterbot writes: lowercase like the keys configparser reports.
_NAME_RE = re.compile(r"[a-z0-9_]{1,64}")
# Control characters (tab allowed) cannot round-trip through one INI line.
_CONTROL_RE = re.compile(r"[\x00-\x08\x0a-\x1f\x7f]")
_REPLACE_ATTEMPTS = 3
_REPLACE_RETRY_SECONDS = 0.05
# Copies of a user INI file, holding the same passwords and tokens: backups
# made here ("<name>.bak-<stamp>[-n]") and by migrate-config ("<name>.bak"),
# and what an interrupted atomic_write leaves (".<name>.<random>.tmp").
# .gitignore lists the same patterns.
SECRET_COPY_PATTERNS = ("*.ini.bak", "*.ini.bak-*", ".*.ini.*.tmp")
_ETAG_KEY = secrets.token_bytes(32)


def is_secret_copy_name(name: str) -> bool:
    """True for a backup or temp copy of an INI file: never commit or ship it."""
    lowered = name.lower()
    return any(fnmatch.fnmatchcase(lowered, pattern) for pattern in SECRET_COPY_PATTERNS)


class IniEditError(ValueError):
    """An edit was refused; ``code`` says why. Never carries a value."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class SetValue:
    section: str
    key: str
    value: str


@dataclass(frozen=True)
class RemoveKey:
    section: str
    key: str


@dataclass(frozen=True)
class AddSection:
    section: str
    items: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class RemoveSection:
    section: str


Edit = Union[SetValue, RemoveKey, AddSection, RemoveSection]


@dataclass(frozen=True)
class _Line:
    text: str
    eol: str


@dataclass(frozen=True)
class _Doc:
    bom: bool
    lines: tuple[_Line, ...]
    newline: str


# ---------------------------------------------------------------- parsing


def _parse_doc(raw: bytes) -> _Doc:
    """Split UTF-8 bytes into lines with their own endings; UnicodeDecodeError if not UTF-8."""
    bom = raw.startswith(_BOM)
    text = (raw[len(_BOM) :] if bom else raw).decode("utf-8")
    lines: list[_Line] = []
    position = 0
    counts = {"\r\n": 0, "\n": 0, "\r": 0}
    for match in _EOL_RE.finditer(text):
        lines.append(_Line(text[position : match.start()], match.group()))
        counts[match.group()] += 1
        position = match.end()
    if position < len(text):
        lines.append(_Line(text[position:], ""))
    newline = max(("\r\n", "\n", "\r"), key=lambda eol: counts[eol]) if any(counts.values()) else "\n"
    return _Doc(bom=bom, lines=tuple(lines), newline=newline)


def _serialize(doc: _Doc) -> bytes:
    body = "".join(line.text + line.eol for line in doc.lines).encode("utf-8")
    return _BOM + body if doc.bom else body


def _read_ini(doc: _Doc) -> configparser.ConfigParser:
    parser = new_user_ini_parser()
    parser.read_string("\n".join(line.text for line in doc.lines))
    return parser


def _snapshot(parser: configparser.ConfigParser) -> dict[str, dict[str, str]]:
    return {
        section: dict(parser.items(section, raw=True)) for section in parser.sections()
    }


def _layout(lines: Sequence[_Line]) -> list[tuple[str, str | None, str | None]]:
    """(kind, section, key) per line, following configparser's reading rules."""
    result: list[tuple[str, str | None, str | None]] = []
    section: str | None = None
    key: str | None = None
    indent_level = 0
    for line in lines:
        stripped = line.text.strip()
        if not stripped:
            result.append(("blank", section, key))
            continue
        if stripped.startswith(("#", ";")):
            result.append(("comment", section, key))
            continue
        indent = len(line.text) - len(line.text.lstrip())
        if section is not None and key is not None and indent > indent_level:
            result.append(("continuation", section, key))
            continue
        indent_level = indent
        header = _SECTCRE.match(stripped)
        if header:
            section, key = header.group("header"), None
            result.append(("section", section, None))
            continue
        option = _OPTCRE.match(stripped)
        if section is not None and option and option.group("option").strip():
            key = option.group("option").rstrip().lower()
            result.append(("option", section, key))
        else:
            key = None
            result.append(("other", section, None))
    return result


def _option_span(layout: list, section: str, key: str) -> tuple[int, int] | None:
    for index, (kind, owner, name) in enumerate(layout):
        if kind != "option" or owner != section or name != key:
            continue
        end = index + 1
        for later in range(index + 1, len(layout)):
            later_kind = layout[later][0]
            if later_kind == "continuation":
                end = later + 1
            elif later_kind not in ("blank", "comment"):
                break
        return index, end
    return None


def _section_bounds(layout: list, section: str) -> tuple[int, int] | None:
    """(header index, index after the section's last key line)."""
    header = next(
        (i for i, (kind, owner, _) in enumerate(layout) if kind == "section" and owner == section),
        None,
    )
    if header is None:
        return None
    content_end = header + 1
    for index in range(header + 1, len(layout)):
        kind = layout[index][0]
        if kind == "section":
            break
        if kind in ("option", "continuation", "other"):
            content_end = index + 1
    return header, content_end


# ---------------------------------------------------------------- editing


def _check_name(name: str) -> None:
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
        raise IniEditError("bad_name")


def _check_value(value: str) -> None:
    if not isinstance(value, str) or _CONTROL_RE.search(value) or value != value.strip():
        raise IniEditError("bad_value")


def _with_newline(lines: list[_Line], newline: str) -> None:
    if lines and lines[-1].eol == "":
        lines[-1] = _Line(lines[-1].text, newline)


def _replace_value(text: str, value: str) -> str:
    indent = len(text) - len(text.lstrip())
    body = text[indent:]
    match = _OPTCRE.match(body)
    if match is None:
        raise IniEditError("layout")
    prefix = body[: match.start("value")]
    if value and not prefix[-1:].isspace() and not match.group("value").strip():
        prefix += " "
    return text[:indent] + prefix + value


def _set_value(doc: _Doc, edit: SetValue) -> _Doc:
    layout = _layout(doc.lines)
    span = _option_span(layout, edit.section, edit.key)
    if span is not None:
        start, end = span
        eol = doc.lines[start].eol
        if end == len(doc.lines) and doc.lines[-1].eol == "":
            eol = ""
        new_line = _Line(_replace_value(doc.lines[start].text, edit.value), eol)
        return _Doc(doc.bom, doc.lines[:start] + (new_line,) + doc.lines[end:], doc.newline)
    bounds = _section_bounds(layout, edit.section)
    if bounds is None:
        return _add_section(doc, AddSection(edit.section, ((edit.key, edit.value),)))
    _, content_end = bounds
    before = list(doc.lines[:content_end])
    _with_newline(before, doc.newline)
    new_line = _Line(f"{edit.key} = {edit.value}", doc.newline)
    return _Doc(doc.bom, (*before, new_line, *doc.lines[content_end:]), doc.newline)


def _remove_key(doc: _Doc, edit: RemoveKey) -> _Doc:
    span = _option_span(_layout(doc.lines), edit.section, edit.key)
    if span is None:
        return doc
    start, end = span
    return _Doc(doc.bom, doc.lines[:start] + doc.lines[end:], doc.newline)


def _add_section(doc: _Doc, edit: AddSection) -> _Doc:
    if any(
        kind == "section" and owner == edit.section
        for kind, owner, _ in _layout(doc.lines)
    ):
        raise IniEditError("section_exists")
    lines = list(doc.lines)
    _with_newline(lines, doc.newline)
    if lines and lines[-1].text.strip():
        lines.append(_Line("", doc.newline))
    lines.append(_Line(f"[{edit.section}]", doc.newline))
    lines.extend(_Line(f"{key} = {value}", doc.newline) for key, value in edit.items)
    return _Doc(doc.bom, tuple(lines), doc.newline)


def _remove_section(doc: _Doc, edit: RemoveSection) -> _Doc:
    bounds = _section_bounds(_layout(doc.lines), edit.section)
    if bounds is None:
        return doc
    start, end = bounds
    lines = list(doc.lines[:start] + doc.lines[end:])
    # One blank line was the separator on each side: keep only one.
    if start > 0 and not lines[start - 1].text.strip() and (
        start == len(lines) or not lines[start].text.strip()
    ):
        del lines[start - 1]
    return _Doc(doc.bom, tuple(lines), doc.newline)


def _check_edit(edit: Edit) -> None:
    _check_name(edit.section)
    if isinstance(edit, SetValue):
        _check_name(edit.key)
        _check_value(edit.value)
    elif isinstance(edit, RemoveKey):
        _check_name(edit.key)
    elif isinstance(edit, AddSection):
        for key, value in edit.items:
            _check_name(key)
            _check_value(value)
    elif not isinstance(edit, RemoveSection):
        raise IniEditError("bad_edit")


def _expected(snapshot: dict[str, dict[str, str]], edits: Iterable[Edit]) -> dict:
    result = {section: dict(values) for section, values in snapshot.items()}
    for edit in edits:
        if isinstance(edit, SetValue):
            result.setdefault(edit.section, {})[edit.key] = edit.value
        elif isinstance(edit, RemoveKey):
            result.get(edit.section, {}).pop(edit.key, None)
        elif isinstance(edit, AddSection):
            result[edit.section] = dict(edit.items)
        else:
            result.pop(edit.section, None)
    return result


_APPLY = {
    SetValue: _set_value,
    RemoveKey: _remove_key,
    AddSection: _add_section,
    RemoveSection: _remove_section,
}


def apply_edits(raw: bytes, edits: Sequence[Edit]) -> bytes:
    """The file bytes with ``edits`` applied; everything else byte for byte.

    Raises UnicodeDecodeError (not UTF-8) or configparser.Error (unreadable)
    for the input, IniEditError for a refused edit or when the result would
    not read back as exactly the requested change.
    """
    doc = _parse_doc(raw)
    before = _snapshot(_read_ini(doc))
    for edit in edits:
        _check_edit(edit)
        doc = _APPLY[type(edit)](doc, edit)
    result = _serialize(doc)
    try:
        after = _snapshot(_read_ini(_parse_doc(result)))
    except configparser.Error as exc:
        raise IniEditError("layout") from exc
    if after != _expected(before, edits):
        raise IniEditError("layout")
    return result


def check_readable(raw: bytes) -> None:
    """IniEditError("unreadable") unless ``raw`` is UTF-8 INI text Letterbot can read."""
    try:
        _read_ini(_parse_doc(raw))
    except (UnicodeDecodeError, configparser.Error) as exc:
        raise IniEditError("unreadable") from exc


def file_etag(raw: bytes | None, stat: os.stat_result | None = None) -> str:
    """Opaque version tag for optimistic locking ("missing" without a file).

    Keyed with a per-process random key, so it is no hash a client could
    test guessed file contents (a secret in the file) against. With ``stat``
    it is also bound to this write of the file (file id, modification time,
    size): rewriting identical bytes still gives a new tag, so a save never
    shows whether a typed secret equals the stored one. Tags do not survive
    a restart (an open page then gets a conflict and reloads).
    """
    if raw is None:
        return "missing"
    mac = hmac.new(_ETAG_KEY, digestmod=hashlib.sha256)
    if stat is not None:
        mac.update(f"{stat.st_ino}:{stat.st_mtime_ns}:{stat.st_size}:".encode("ascii"))
    mac.update(raw)
    return mac.hexdigest()[:16]


def read_versioned(path: Path) -> tuple[bytes | None, str]:
    """The file's bytes and etag, both from one open handle; (None, "missing") without a file."""
    try:
        with path.open("rb") as stream:
            raw = stream.read()
            stat = os.fstat(stream.fileno())
    except FileNotFoundError:
        return None, "missing"
    return raw, file_etag(raw, stat)


# ---------------------------------------------------------------- files


def _replace(source: Path, target: Path) -> None:
    for attempt in range(_REPLACE_ATTEMPTS):
        try:
            os.replace(source, target)
            return
        except PermissionError:
            # Windows: a reader (antivirus, the worker) may hold the file briefly.
            if attempt == _REPLACE_ATTEMPTS - 1:
                raise
            time.sleep(_REPLACE_RETRY_SECONDS)


def atomic_write(path: Path, data: bytes) -> None:
    """Replace ``path`` with ``data`` in one step; the old file stays on failure."""
    handle, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        _replace(temp_path, path)
    except BaseException:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
        except OSError as cleanup_exc:
            logger.warning(
                "settings_temp_cleanup_failed path=%s error_class=%s",
                temp_path,
                type(cleanup_exc).__name__,
            )
        raise


def _backup_pattern(path: Path) -> re.Pattern[str]:
    return re.compile(rf"{re.escape(path.name)}\.bak-(\d{{8}}-\d{{6}})(?:-(\d{{1,3}}))?")


def list_backups(path: Path) -> list[str]:
    """Backup file names of ``path``, newest first."""
    pattern = _backup_pattern(path)
    try:
        entries = list(path.parent.iterdir())
    except FileNotFoundError:
        return []
    found: list[tuple[str, int, str]] = []
    for entry in entries:
        match = pattern.fullmatch(entry.name)
        if match and entry.is_file():
            found.append((match.group(1), int(match.group(2) or 0), entry.name))
    return [name for _, _, name in sorted(found, reverse=True)]


def _new_backup(path: Path, data: bytes, now: datetime) -> str:
    stamp = now.strftime(_BACKUP_STAMP)
    for counter in range(1000):
        name = f"{path.name}.bak-{stamp}" + (f"-{counter}" if counter else "")
        target = path.with_name(name)
        if not target.exists():
            atomic_write(target, data)
            return name
    raise IniEditError("backup_failed")


def _prune(path: Path, keep: int) -> None:
    for name in list_backups(path)[keep:]:
        try:
            path.with_name(name).unlink()
        except OSError as exc:
            logger.warning(
                "settings_backup_prune_failed name=%s error_class=%s",
                name,
                type(exc).__name__,
            )


def write_with_backup(
    path: Path,
    data: bytes,
    *,
    now: datetime | None = None,
    keep: int = BACKUP_KEEP,
) -> str | None:
    """Back up the current file (if any), then replace it atomically.

    Returns the backup's file name (None when there was no file).
    """
    backup = None
    if path.exists():
        backup = _new_backup(path, path.read_bytes(), now or datetime.now())
    atomic_write(path, data)
    _prune(path, keep)
    return backup


def read_backup(path: Path, name: str) -> bytes:
    """Bytes of one listed backup of ``path``; IniEditError for any other name."""
    if name not in list_backups(path):
        raise IniEditError("backup_not_found")
    return path.with_name(name).read_bytes()


def restore_backup(
    path: Path,
    name: str,
    *,
    now: datetime | None = None,
    keep: int = BACKUP_KEEP,
) -> str | None:
    """Put backup ``name`` back; the current file is backed up first.

    Returns the name of that new backup of the replaced file.
    """
    data = read_backup(path, name)
    try:
        check_readable(data)
    except IniEditError as exc:
        raise IniEditError("backup_unreadable") from exc
    return write_with_backup(path, data, now=now, keep=keep)


__all__ = [
    "AddSection",
    "BACKUP_KEEP",
    "Edit",
    "IniEditError",
    "RemoveKey",
    "RemoveSection",
    "SECRET_COPY_PATTERNS",
    "SetValue",
    "apply_edits",
    "atomic_write",
    "check_readable",
    "file_etag",
    "is_secret_copy_name",
    "list_backups",
    "read_backup",
    "read_versioned",
    "restore_backup",
    "write_with_backup",
]
