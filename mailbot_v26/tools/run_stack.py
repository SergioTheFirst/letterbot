from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from mailbot_v26.config_loader import CONFIG_DIR, load_storage_config, load_web_config


@dataclass(frozen=True)
class StackCommand:
    name: str
    args: list[str]


def _config_dir_args(config_dir: Path | None) -> list[str]:
    if config_dir is None:
        return []
    return ["--config-dir", str(Path(config_dir).resolve())]


def _build_worker_command(python_exe: str, config_dir: Path | None) -> StackCommand:
    return StackCommand(
        "worker",
        [python_exe, "-m", "mailbot_v26", *_config_dir_args(config_dir)],
    )


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
        str(Path(config_dir).resolve()),
        "--db",
        str(db_path),
    ]
    if bind:
        args.extend(["--bind", bind])
    if port is not None:
        args.extend(["--port", str(port)])
    return StackCommand("web", args)


def _build_doctor_command(python_exe: str, config_dir: Path | None) -> StackCommand:
    return StackCommand(
        "doctor",
        [python_exe, "-m", "mailbot_v26", "doctor", *_config_dir_args(config_dir)],
    )


def _format_command(args: Iterable[str]) -> str:
    return subprocess.list2cmdline(list(args))


def _timestamp_label() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _prepare_log_path(name: str) -> Path:
    log_dir = Path("runtime") / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"{name}_{_timestamp_label()}.log"


def _tail_log(path: Path, *, lines: int = 30) -> list[str]:
    if lines <= 0:
        return []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            chunks = handle.readlines()
    except OSError:
        return []
    return [line.rstrip("\n") for line in chunks[-lines:]]


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
    Path("runtime").mkdir(parents=True, exist_ok=True)
    (Path("runtime") / "logs").mkdir(parents=True, exist_ok=True)
    processes: list[subprocess.Popen[str]] = []
    process_meta: dict[int, tuple[str, Path]] = {}
    log_files: list[object] = []
    try:
        for command in commands:
            log_path = _prepare_log_path(command.name)
            log_file = log_path.open("a", encoding="utf-8")
            log_files.append(log_file)
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
            process_meta[proc.pid] = (command.name, log_path)

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
                    name, log_path = process_meta.get(
                        proc.pid, ("process", Path("unknown"))
                    )
                    print(f"[ERROR] {name} exited with code {code}")
                    print(f"[ERROR] {name} log: {log_path}")
                    tail_lines = _tail_log(log_path, lines=30)
                    if tail_lines:
                        print(
                            f"[ERROR] {name} log tail (last {len(tail_lines)} lines):"
                        )
                        for line in tail_lines:
                            print(f"[ERROR][{name}] {line}")
                    else:
                        print(f"[ERROR] {name} log tail unavailable")
            if processes:
                time.sleep(0.5)
        return exit_code
    except KeyboardInterrupt:
        print("[INFO] Shutdown requested. Stopping child processes...")
        _terminate_processes(processes)
        return 0
    finally:
        for handle in log_files:
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


def _load_web_ui_defaults(config_dir: Path | None) -> tuple[str, int]:
    resolved = _resolve_config_dir(config_dir)
    web = load_web_config(resolved)
    return web.host, web.port


def _is_port_busy(host: str, port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, int(port)))
        return False
    except OSError:
        return True


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    bind = args.bind
    port = args.port
    if bind is None or port is None:
        default_bind, default_port = _load_web_ui_defaults(args.config_dir)
        bind = bind or default_bind
        port = port if port is not None else default_port

    python_exe = sys.executable
    if args.dry_run:
        dry_config_dir = args.config_dir or CONFIG_DIR
        dry_db_path = args.db_path or Path("mailbot_v26/data/knowledge.db")
        if args.mode == "worker":
            commands = [_build_worker_command(python_exe, args.config_dir)]
        elif args.mode == "web":
            commands = [
                _build_web_command(
                    python_exe,
                    config_dir=dry_config_dir,
                    db_path=dry_db_path,
                    bind=bind or "127.0.0.1",
                    port=port or 8787,
                )
            ]
        elif args.mode == "doctor":
            commands = [_build_doctor_command(python_exe, args.config_dir)]
        else:
            commands = [
                _build_worker_command(python_exe, args.config_dir),
                _build_web_command(
                    python_exe,
                    config_dir=dry_config_dir,
                    db_path=dry_db_path,
                    bind=bind or "127.0.0.1",
                    port=port or 8787,
                ),
            ]
        for command in commands:
            print(f"{command.name}: {_format_command(command.args)}")
        return 0

    effective_bind = bind or "127.0.0.1"
    effective_port = port or 8787

    commands = build_commands(
        python_exe=python_exe,
        mode=args.mode,
        config_dir=args.config_dir,
        db_path=args.db_path,
        bind=effective_bind,
        port=effective_port,
    )

    if args.mode == "all" and _is_port_busy(effective_bind, effective_port):
        print(
            f"[WARN] DEGRADED_NO_WEB: Порт {effective_port} занят. "
            "Откройте mailbot_v26/config/settings.ini и измените [web] port = ..."
        )
        commands = [command for command in commands if command.name != "web"]

    web_url = f"http://{effective_bind}:{effective_port}/login"
    open_browser = (args.mode in {"all", "web"}) and not args.no_browser
    return _run_processes(
        commands,
        open_browser=open_browser,
        web_url=web_url,
        web_timeout=15.0,
    )


if __name__ == "__main__":
    raise SystemExit(main())
