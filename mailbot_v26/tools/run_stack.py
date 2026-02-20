from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from mailbot_v26.config_loader import CONFIG_DIR, load_storage_config
from mailbot_v26.config_yaml import (
    ConfigError as YamlConfigError,
    load_config as load_yaml_config,
    validate_config as validate_yaml_config,
)


@dataclass(frozen=True)
class StackCommand:
    name: str
    args: list[str]


def _build_worker_command(python_exe: str, config_dir: Path | None) -> StackCommand:
    if config_dir is None:
        return StackCommand("worker", [python_exe, "-m", "mailbot_v26"])
    config_value = str(config_dir)
    script = (
        "from pathlib import Path; "
        "from mailbot_v26.start import main; "
        f"main(config_dir=Path({config_value!r}))"
    )
    return StackCommand("worker", [python_exe, "-c", script])


def _build_web_command(
    python_exe: str,
    *,
    config_dir: Path,
    db_path: Path,
    bind: str | None,
    port: int | None,
) -> StackCommand:
    args = [
        python_exe,
        "-m",
        "mailbot_v26.web_observability.app",
        "--config",
        str(config_dir),
        "--db",
        str(db_path),
    ]
    if bind:
        args.extend(["--bind", bind])
    if port is not None:
        args.extend(["--port", str(port)])
    return StackCommand("web", args)


def _build_doctor_command(python_exe: str, config_dir: Path | None) -> StackCommand:
    if config_dir is None:
        return StackCommand("doctor", [python_exe, "-m", "mailbot_v26", "doctor"])
    config_value = str(config_dir)
    script = (
        "from pathlib import Path; "
        "from mailbot_v26.doctor import report_exit_code, run_doctor; "
        f"report = run_doctor(Path({config_value!r})); "
        "raise SystemExit(report_exit_code(report))"
    )
    return StackCommand("doctor", [python_exe, "-c", script])


def _format_command(args: Iterable[str]) -> str:
    return subprocess.list2cmdline(list(args))


def _timestamp_label() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _prepare_log_path(name: str) -> Path:
    log_dir = Path("runtime") / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"{name}_{_timestamp_label()}.log"


def _open_browser_when_ready(url: str, timeout_seconds: float) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    import webbrowser

                    webbrowser.open(url)
                    return
        except urllib.error.HTTPError as exc:
            if exc.code == 200:
                import webbrowser

                webbrowser.open(url)
                return
        except Exception:
            pass
        time.sleep(0.5)
    print(f"[WARN] Web UI not ready at {url}. Open it manually when available.")


def _terminate_processes(processes: list[subprocess.Popen[str]]) -> None:
    for proc in processes:
        if proc.poll() is None:
            proc.terminate()
    deadline = time.time() + 5
    for proc in processes:
        if proc.poll() is None:
            remaining = max(0.1, deadline - time.time())
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                proc.kill()


def _run_processes(
    commands: list[StackCommand],
    *,
    open_browser: bool,
    web_url: str,
    web_timeout: float,
) -> int:
    processes: list[subprocess.Popen[str]] = []
    log_files: list[tuple[str, object]] = []
    try:
        for command in commands:
            log_path = _prepare_log_path(command.name)
            log_file = log_path.open("a", encoding="utf-8")
            log_files.append((command.name, log_file))
            print(f"[INFO] Starting {command.name} -> {_format_command(command.args)}")
            print(f"[INFO] Logging {command.name} output to {log_path}")
            proc = subprocess.Popen(
                command.args,
                stdout=log_file,
                stderr=log_file,
                text=True,
                env=os.environ.copy(),
            )
            processes.append(proc)

        if open_browser:
            _open_browser_when_ready(web_url, web_timeout)

        exit_code = 0
        while processes:
            for proc in list(processes):
                code = proc.poll()
                if code is None:
                    continue
                processes.remove(proc)
                if code != 0:
                    exit_code = code
                    print(f"[WARN] {proc.args[0]} exited with code {code}")
            if processes:
                time.sleep(0.5)
        return exit_code
    except KeyboardInterrupt:
        print("[INFO] Shutdown requested. Stopping child processes...")
        _terminate_processes(processes)
        return 0
    finally:
        for _, handle in log_files:
            try:
                handle.close()
            except Exception:
                pass


def _resolve_config_dir(config_dir: Path | None) -> Path:
    return config_dir or CONFIG_DIR


def build_commands(
    *,
    python_exe: str,
    mode: str,
    config_dir: Path | None,
    db_path: Path | None,
    bind: str,
    port: int,
) -> list[StackCommand]:
    if mode == "worker":
        return [_build_worker_command(python_exe, config_dir)]
    if mode == "web":
        resolved_config = _resolve_config_dir(config_dir)
        resolved_db = db_path or load_storage_config(resolved_config).db_path
        return [
            _build_web_command(
                python_exe,
                config_dir=resolved_config,
                db_path=resolved_db,
                bind=bind,
                port=port,
            )
        ]
    if mode == "doctor":
        return [_build_doctor_command(python_exe, config_dir)]
    if mode == "all":
        resolved_config = _resolve_config_dir(config_dir)
        resolved_db = db_path or load_storage_config(resolved_config).db_path
        return [
            _build_worker_command(python_exe, config_dir),
            _build_web_command(
                python_exe,
                config_dir=resolved_config,
                db_path=resolved_db,
                bind=bind,
                port=port,
            ),
        ]
    raise ValueError(f"Unsupported mode: {mode}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Letterbot stack launcher")
    parser.add_argument(
        "mode",
        nargs="?",
        default="all",
        choices=("all", "worker", "web", "doctor"),
        help="Run mode (default: all)",
    )
    parser.add_argument("--config-dir", type=Path, help="Config directory override")
    parser.add_argument("--db-path", type=Path, help="SQLite DB path override")
    parser.add_argument("--bind", help="Web bind address")
    parser.add_argument("--port", type=int, help="Web port")
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Disable auto-open browser for web UI",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them",
    )
    return parser


def _resolve_yaml_config_path() -> Path | None:
    current_dir = Path(__file__).resolve().parent.parent
    candidates = [
        current_dir / "config.yaml",
        current_dir.parent / "config.yaml",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _load_web_ui_defaults() -> tuple[str | None, int | None]:
    config_path = _resolve_yaml_config_path()
    if not config_path:
        return None, None
    try:
        raw = load_yaml_config(config_path)
    except (FileNotFoundError, YamlConfigError):
        return None, None
    ok, _error = validate_yaml_config(raw)
    if not ok:
        return None, None
    web_ui = raw.get("web_ui")
    if not isinstance(web_ui, dict):
        return None, None
    bind = web_ui.get("bind")
    port = web_ui.get("port")
    return (str(bind).strip() if bind else None), (int(port) if port is not None else None)


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    bind = args.bind
    port = args.port
    if bind is None or port is None:
        default_bind, default_port = _load_web_ui_defaults()
        bind = bind or default_bind
        port = port if port is not None else default_port

    python_exe = sys.executable
    commands = build_commands(
        python_exe=python_exe,
        mode=args.mode,
        config_dir=args.config_dir,
        db_path=args.db_path,
        bind=bind or "127.0.0.1",
        port=port or 8111,
    )

    if args.dry_run:
        for command in commands:
            print(f"{command.name}: {_format_command(command.args)}")
        return 0

    web_url = f"http://{bind or '127.0.0.1'}:{port or 8111}/login"
    open_browser = (args.mode in {"all", "web"}) and not args.no_browser
    return _run_processes(
        commands,
        open_browser=open_browser,
        web_url=web_url,
        web_timeout=15.0,
    )


if __name__ == "__main__":
    raise SystemExit(main())
