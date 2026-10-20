from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import zipfile
from pathlib import Path, PurePosixPath

from mailbot_v26.config.ini_editor import is_secret_copy_name
from mailbot_v26.tools.verify_dist import verify_dist_contract

DEFAULT_DIST_DIR = Path("dist") / "Letterbot"
DEFAULT_OUTPUT = Path("dist") / "Letterbot-Setup.exe"
DEFAULT_WORK_DIR = Path("build") / "installer"
DEFAULT_INSTALL_DIR = r"%LOCALAPPDATA%\Letterbot"
MANIFEST_NAME = "manifest.sha256.json"
PAYLOAD_NAME = "payload.zip"
INSTALL_SCRIPT_NAME = "install.cmd"
IEXPRESS_TIMEOUT_SECONDS = 900
# Characters that would break the generated cmd script (quoting, pipes,
# redirection, delayed expansion). `%` stays allowed for %LOCALAPPDATA%.
_FORBIDDEN_INSTALL_DIR_CHARS = frozenset('"&|<>^!\r\n')
_ABSOLUTE_INSTALL_DIR_RE = re.compile(r"^(?:[A-Za-z]:[\\/]|%[A-Za-z_][A-Za-z0-9_]*%[\\/])")
# Defence in depth on top of the manifest allowlist: these must never ship.
_NEVER_SHIP_NAMES = frozenset({"accounts.ini", "settings.ini", "keys.ini", "config.ini", "state.json"})
_NEVER_SHIP_SUFFIXES = (".sqlite", ".sqlite-wal", ".sqlite-shm", ".db", ".log")


class InstallerBuildError(RuntimeError):
    """Raised when the Windows installer bundle cannot be produced."""


def _windows_path(path: Path) -> str:
    return str(Path(path).resolve()).replace("/", "\\")


def normalize_install_dir(install_dir: str) -> str:
    cleaned = str(install_dir or "").strip()
    if not cleaned:
        raise InstallerBuildError("install directory must not be empty")
    if not cleaned.isascii():
        # install.cmd must stay ASCII (cmd code pages); %VAR% references such as
        # %LOCALAPPDATA% still expand to non-ASCII user folders at install time.
        raise InstallerBuildError(
            "install directory must be ASCII; use environment variables such as "
            "%LOCALAPPDATA% for user folders"
        )
    bad = sorted(set(cleaned) & _FORBIDDEN_INSTALL_DIR_CHARS)
    if bad:
        raise InstallerBuildError(
            f"install directory contains unsupported characters: {''.join(bad)!r}"
        )
    if not _ABSOLUTE_INSTALL_DIR_RE.match(cleaned):
        raise InstallerBuildError(
            "install directory must be an absolute drive path (C:\\...) or start "
            "with an environment variable such as %LOCALAPPDATA%\\"
        )
    if ".." in re.split(r"[\\/]", cleaned):
        raise InstallerBuildError("install directory must not contain '..' segments")
    if "%" in re.sub(r"%[A-Za-z_][A-Za-z0-9_]*%", "", cleaned):
        # cmd would expand a stray % pair into an unrelated variable.
        raise InstallerBuildError(
            "install directory may use % only for %VARIABLE% references"
        )
    # A trailing backslash would turn into an escaped quote on cmd lines.
    return cleaned.rstrip("\\/")


def _install_cmd_text(*, install_dir: str = DEFAULT_INSTALL_DIR) -> str:
    install_dir = normalize_install_dir(install_dir)
    # ASCII only and no parenthesized blocks: values such as a profile path
    # containing ")" must never be parsed as part of a block.
    lines = [
        "@echo off",
        "setlocal EnableExtensions",
        f'set "PAYLOAD=%~dp0{PAYLOAD_NAME}"',
        f'set "INSTALL_ROOT={install_dir}"',
        'set "QUIET=0"',
        'if /I "%~1"=="--quiet" set "QUIET=1"',
        'set "LOG=%TEMP%\\Letterbot-install.log"',
        'echo Letterbot install started %DATE% %TIME%> "%LOG%"',
        "if not defined LOCALAPPDATA goto fail_env",
        'if not exist "%PAYLOAD%" goto fail_payload',
        # System tools by full path: Git for Windows puts GNU tar/find first
        # on PATH, and GNU tar reads "C:\..." as a remote host.
        'set "SYS32=%SystemRoot%\\System32"',
        '"%SYS32%\\tasklist.exe" /FI "IMAGENAME eq Letterbot.exe" 2>nul'
        ' | "%SYS32%\\find.exe" /I "Letterbot.exe" >nul',
        "if not errorlevel 1 goto fail_running",
        'if "%QUIET%"=="0" echo Installing Letterbot to "%INSTALL_ROOT%" ...',
        'if not exist "%INSTALL_ROOT%\\" mkdir "%INSTALL_ROOT%" 2>>"%LOG%"',
        'if not exist "%INSTALL_ROOT%\\" goto fail_mkdir',
        'if not exist "%SYS32%\\tar.exe" goto extract_powershell',
        '"%SYS32%\\tar.exe" -xf "%PAYLOAD%" -C "%INSTALL_ROOT%" >>"%LOG%" 2>&1',
        "if errorlevel 1 goto fail_extract",
        "goto extracted",
        ":extract_powershell",
        '"%SYS32%\\WindowsPowerShell\\v1.0\\powershell.exe" -NoProfile'
        " -ExecutionPolicy Bypass -Command "
        '"Expand-Archive -LiteralPath $env:PAYLOAD -DestinationPath $env:INSTALL_ROOT -Force"'
        ' >>"%LOG%" 2>&1',
        "if errorlevel 1 goto fail_extract",
        ":extracted",
        'if not exist "%INSTALL_ROOT%\\run.bat" goto fail_extract',
        'echo Letterbot install finished>> "%LOG%"',
        'if "%QUIET%"=="1" exit /b 0',
        "echo Launching Letterbot ...",
        'start "" /D "%INSTALL_ROOT%" "%INSTALL_ROOT%\\run.bat"',
        "exit /b 0",
        ":fail_env",
        'set "REASON=LOCALAPPDATA is not set."',
        "goto fail",
        ":fail_payload",
        'set "REASON=installer payload is missing."',
        "goto fail",
        ":fail_running",
        'set "REASON=Letterbot is running. Close it and run the installer again."',
        "goto fail",
        ":fail_mkdir",
        'set "REASON=could not create the install folder."',
        "goto fail",
        ":fail_extract",
        'set "REASON=could not unpack Letterbot into the install folder."',
        "goto fail",
        ":fail",
        "echo ERROR: %REASON%",
        'echo Target: "%INSTALL_ROOT%"',
        'echo Log: "%LOG%"',
        'echo ERROR: %REASON%>> "%LOG%"',
        'if "%QUIET%"=="0" pause',
        "exit /b 1",
    ]
    return "\r\n".join(lines) + "\r\n"


def payload_members(dist_dir: Path) -> tuple[list[str], list[str]]:
    """Return (files shipped in the payload, files left out) for a dist.

    Only files listed in the integrity manifest (plus the manifest itself) are
    shipped, so runtime-mutable files such as accounts.ini, settings.ini,
    database.sqlite or logs can never leak into the installer.
    """
    dist_dir = Path(dist_dir)
    manifest_raw = json.loads((dist_dir / MANIFEST_NAME).read_text(encoding="utf-8"))
    if not isinstance(manifest_raw, dict):
        raise InstallerBuildError(f"{MANIFEST_NAME} must contain a JSON object")
    members: list[str] = []
    dist_root = dist_dir.resolve()
    for rel in sorted({*manifest_raw.keys(), MANIFEST_NAME}):
        rel_text = str(rel)
        rel_path = PurePosixPath(rel_text)
        if (
            rel_path.is_absolute()
            or ".." in rel_path.parts
            or "\\" in rel_text
            or ":" in rel_text
        ):
            raise InstallerBuildError(f"unsafe path in {MANIFEST_NAME}: {rel!r}")
        source = dist_dir / rel_path
        if not source.is_file():
            raise InstallerBuildError(f"manifest lists a missing file: {rel}")
        if dist_root not in source.resolve().parents:
            raise InstallerBuildError(f"manifest entry escapes the dist folder: {rel}")
        name = rel_path.name.lower()
        if (
            name in _NEVER_SHIP_NAMES
            or name.endswith(_NEVER_SHIP_SUFFIXES)
            or is_secret_copy_name(name)
            or (rel_path.parts and rel_path.parts[0] == "data")
        ):
            raise InstallerBuildError(f"runtime file must not be shipped: {rel}")
        members.append(rel_path.as_posix())
    shipped = set(members)
    excluded = sorted(
        path.relative_to(dist_dir).as_posix()
        for path in dist_dir.rglob("*")
        if path.is_file() and path.relative_to(dist_dir).as_posix() not in shipped
    )
    return members, excluded


def stage_installer_layout(
    dist_dir: Path,
    work_dir: Path = DEFAULT_WORK_DIR,
    *,
    install_dir: str = DEFAULT_INSTALL_DIR,
) -> Path:
    ok, details = verify_dist_contract(dist_dir)
    if not ok:
        raise InstallerBuildError(f"dist contract invalid: {details}")

    dist_dir = Path(dist_dir)
    members, _excluded = payload_members(dist_dir)
    script_text = _install_cmd_text(install_dir=install_dir)

    stage_dir = Path(work_dir) / "stage"
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(
        stage_dir / PAYLOAD_NAME, "w", compression=zipfile.ZIP_DEFLATED
    ) as archive:
        for rel in members:
            archive.write(dist_dir / rel, arcname=rel)
    (stage_dir / INSTALL_SCRIPT_NAME).write_bytes(script_text.encode("ascii"))
    return stage_dir


def _sed_encoding() -> str:
    # iexpress.exe reads the SED through ANSI INI APIs.
    return "mbcs" if os.name == "nt" else "utf-8"


def write_iexpress_sed(
    stage_dir: Path,
    output_path: Path = DEFAULT_OUTPUT,
    *,
    friendly_name: str = "Letterbot Setup",
) -> Path:
    stage_dir = Path(stage_dir)
    output_path = Path(output_path)
    sed_path = stage_dir.parent / "letterbot_installer.sed"
    # IExpress stores bare file names only, so the payload must be flat.
    file_entries = [INSTALL_SCRIPT_NAME, PAYLOAD_NAME]
    for name in file_entries:
        if not (stage_dir / name).is_file():
            raise InstallerBuildError(f"staged file missing: {name}")

    strings_lines = [
        "InstallPrompt=",
        "DisplayLicense=",
        "FinishMessage=",
        f"TargetName={_windows_path(output_path)}",
        f"FriendlyName={friendly_name}",
        f"AppLaunched=cmd.exe /d /c {INSTALL_SCRIPT_NAME}",
        "PostInstallCmd=<None>",
        f"AdminQuietInstCmd=cmd.exe /d /c {INSTALL_SCRIPT_NAME} --quiet",
        f"UserQuietInstCmd=cmd.exe /d /c {INSTALL_SCRIPT_NAME} --quiet",
    ]
    for index, name in enumerate(file_entries):
        strings_lines.append(f'FILE{index}="{name}"')

    source_lines = [f"%FILE{index}%=" for index, _ in enumerate(file_entries)]

    sed_text = "\r\n".join(
        [
            "[Version]",
            "Class=IEXPRESS",
            "SEDVersion=3",
            "[Options]",
            "PackagePurpose=InstallApp",
            # 0 = default (visible) window, so install.cmd errors are seen.
            "ShowInstallProgramWindow=0",
            "HideExtractAnimation=0",
            "UseLongFileName=1",
            "InsideCompressed=1",
            "CAB_FixedSize=0",
            "CAB_ResvCodeSigning=0",
            "RebootMode=N",
            "InstallPrompt=%InstallPrompt%",
            "DisplayLicense=%DisplayLicense%",
            "FinishMessage=%FinishMessage%",
            "TargetName=%TargetName%",
            "FriendlyName=%FriendlyName%",
            "AppLaunched=%AppLaunched%",
            "PostInstallCmd=%PostInstallCmd%",
            "AdminQuietInstCmd=%AdminQuietInstCmd%",
            "UserQuietInstCmd=%UserQuietInstCmd%",
            "SourceFiles=SourceFiles",
            "SelfDelete=0",
            "[Strings]",
            *strings_lines,
            "[SourceFiles]",
            f"SourceFiles0={_windows_path(stage_dir)}\\",
            "[SourceFiles0]",
            *source_lines,
        ]
    ) + "\r\n"

    try:
        sed_path.write_bytes(sed_text.encode(_sed_encoding()))
    except UnicodeEncodeError as exc:
        raise InstallerBuildError(
            "installer paths must be representable in the Windows ANSI code page "
            f"(IExpress limitation); move the checkout to a simpler path: {exc}"
        ) from exc
    return sed_path


def build_windows_installer(
    dist_dir: Path = DEFAULT_DIST_DIR,
    output_path: Path = DEFAULT_OUTPUT,
    *,
    work_dir: Path = DEFAULT_WORK_DIR,
    install_dir: str = DEFAULT_INSTALL_DIR,
    timeout_seconds: int = IEXPRESS_TIMEOUT_SECONDS,
) -> Path:
    iexpress_exe = shutil.which("iexpress.exe")
    if not iexpress_exe:
        raise InstallerBuildError(
            "IExpress is not available. Install or enable Windows iexpress.exe."
        )

    stage_dir = stage_installer_layout(
        dist_dir=Path(dist_dir),
        work_dir=Path(work_dir),
        install_dir=install_dir,
    )
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()
    sed_path = write_iexpress_sed(stage_dir=stage_dir, output_path=output_path)

    try:
        completed = subprocess.run(
            [iexpress_exe, "/N", "/Q", str(sed_path)],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise InstallerBuildError(
            f"IExpress did not finish within {timeout_seconds} s"
        ) from exc
    if completed.returncode != 0:
        details = "\n".join(
            part.strip()
            for part in (completed.stdout or "", completed.stderr or "")
            if part.strip()
        )
        raise InstallerBuildError(
            "IExpress failed to build the Letterbot installer."
            + (f"\n{details}" if details else "")
        )
    if not output_path.exists():
        raise InstallerBuildError(
            f"IExpress finished without creating installer: {output_path}"
        )
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build single-file Windows installer for Letterbot."
    )
    parser.add_argument("dist_dir", nargs="?", default=str(DEFAULT_DIST_DIR))
    parser.add_argument("output", nargs="?", default=str(DEFAULT_OUTPUT))
    parser.add_argument(
        "--work-dir",
        default=str(DEFAULT_WORK_DIR),
        help="Working directory for staged installer files.",
    )
    parser.add_argument(
        "--install-dir",
        default=DEFAULT_INSTALL_DIR,
        help="Target install directory used by install.cmd.",
    )
    args = parser.parse_args()

    try:
        _members, excluded = payload_members(Path(args.dist_dir))
        for rel in excluded:
            print(f"INSTALLER_BUILD SKIP (not in manifest): {rel}")
        built = build_windows_installer(
            dist_dir=Path(args.dist_dir),
            output_path=Path(args.output),
            work_dir=Path(args.work_dir),
            install_dir=str(args.install_dir),
        )
    except (InstallerBuildError, OSError, ValueError) as exc:
        print(f"INSTALLER_BUILD FAIL: {exc}")
        return 1

    print(f"INSTALLER_BUILD PASS: {built}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
