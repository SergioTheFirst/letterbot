from __future__ import annotations

import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
WINDOWS_ONLY = pytest.mark.skipif(os.name != "nt", reason="Windows launcher smoke tests")


def _write_fake_mailbot_package(repo_root: Path) -> None:
    package_root = repo_root / "mailbot_v26"
    tools_root = package_root / "tools"
    tools_root.mkdir(parents=True, exist_ok=True)

    (package_root / "__init__.py").write_text("", encoding="utf-8")
    (tools_root / "__init__.py").write_text("", encoding="utf-8")
    (package_root / "__main__.py").write_text(
        textwrap.dedent(
            """
            from __future__ import annotations

            import sys
            from pathlib import Path


            def _arg(name: str, default: str = ".") -> str:
                if name not in sys.argv:
                    return default
                index = sys.argv.index(name)
                if index + 1 >= len(sys.argv):
                    return default
                return sys.argv[index + 1]


            def _write_if_missing(path: Path, content: str) -> None:
                if not path.exists():
                    path.write_text(content, encoding="utf-8")


            def main() -> None:
                command = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else "start"
                config_dir = Path(_arg("--config-dir", "."))

                if command == "init-config":
                    config_dir.mkdir(parents=True, exist_ok=True)
                    _write_if_missing(
                        config_dir / "settings.ini",
                        "[general]\\ncheck_interval = 120\\n[storage]\\ndb_path = data/mailbot.sqlite\\n",
                    )
                    _write_if_missing(
                        config_dir / "accounts.ini",
                        "\\n".join(
                            [
                                "[work]",
                                "login = user@example.com",
                                "password = CHANGE_ME",
                                "host = CHANGE_ME",
                                "port = 993",
                                "use_ssl = true",
                                "",
                                "[telegram]",
                                "bot_token = CHANGE_ME",
                                "chat_id = CHANGE_ME",
                            ]
                        ),
                    )
                    print("init-config: configuration templates ready.")
                    raise SystemExit(0)

                if command == "config-ready":
                    accounts_path = config_dir / "accounts.ini"
                    accounts_text = accounts_path.read_text(encoding="utf-8") if accounts_path.exists() else ""
                    print("config-ready: readiness report")
                    if "CHANGE_ME" in accounts_text:
                        print("STATUS: NOT_READY")
                        print("CRITICAL: password")
                        print("CRITICAL: host")
                        print("WARN: bot_token")
                        raise SystemExit(2)
                    print("STATUS: OK")
                    raise SystemExit(0)

                if command == "doctor":
                    print("doctor ok")
                    raise SystemExit(0)

                if command == "validate-config":
                    print("validate-config: configuration report")
                    print("STATUS: OK")
                    raise SystemExit(0)

                raise SystemExit(0)


            if __name__ == "__main__":
                main()
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (tools_root / "run_stack.py").write_text(
        textwrap.dedent(
            """
            from __future__ import annotations

            import os
            import sys
            from pathlib import Path


            def _arg(name: str, default: str = ".") -> str:
                if name not in sys.argv:
                    return default
                index = sys.argv.index(name)
                if index + 1 >= len(sys.argv):
                    return default
                return sys.argv[index + 1]


            def main() -> None:
                config_dir = Path(_arg("--config-dir", "."))
                config_dir.mkdir(parents=True, exist_ok=True)
                (config_dir / "run_stack_called.txt").write_text("ok", encoding="utf-8")
                print("run_stack called")
                raise SystemExit(int(os.environ.get("LETTERBOT_FAKE_RUN_STACK_EXIT", "0")))


            if __name__ == "__main__":
                main()
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )


def _write_filled_config(repo_root: Path) -> None:
    (repo_root / "settings.ini").write_text(
        "[general]\ncheck_interval = 120\n[storage]\ndb_path = data/mailbot.sqlite\n",
        encoding="utf-8",
    )
    (repo_root / "accounts.ini").write_text(
        "\n".join(
            [
                "[work]",
                "login = user@example.com",
                "password = secret-pass",
                "host = imap.example.com",
                "port = 993",
                "use_ssl = true",
                "",
                "[telegram]",
                "bot_token = tg-token",
                "chat_id = 123",
            ]
        ),
        encoding="utf-8",
    )


def _build_fake_repo(
    tmp_path: Path,
    *,
    repo_name: str,
    filled_config: bool = False,
    requirements_text: str = "",
) -> Path:
    repo_root = tmp_path / repo_name
    repo_root.mkdir(parents=True, exist_ok=True)
    shutil.copy2(REPO_ROOT / "letterbot.bat", repo_root / "letterbot.bat")
    (repo_root / "requirements.txt").write_text(requirements_text, encoding="utf-8")
    _write_fake_mailbot_package(repo_root)
    if filled_config:
        _write_filled_config(repo_root)
    return repo_root


def _launcher_env(*, missing_python: bool = False) -> dict[str, str]:
    env = os.environ.copy()
    env["LETTERBOT_SKIP_NOTEPAD"] = "1"
    env["LETTERBOT_SKIP_PAUSE"] = "1"
    env["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    env["PIP_NO_INPUT"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    if missing_python:
        system_root = Path(os.environ.get("SystemRoot", r"C:\Windows"))
        env["PATH"] = str(system_root / "System32")
    else:
        python_dir = str(Path(sys.executable).resolve().parent)
        env["PATH"] = os.pathsep.join([python_dir, env.get("PATH", "")])
    return env


def _run_launcher(repo_root: Path, *, cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    comspec = os.environ.get("ComSpec", r"C:\Windows\System32\cmd.exe")
    return subprocess.run(
        [comspec, "/d", "/c", str(repo_root / "letterbot.bat")],
        cwd=cwd,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=240,
        check=False,
    )


@WINDOWS_ONLY
def test_letterbot_bat_smoke_bootstrap_in_fresh_folder(tmp_path: Path) -> None:
    repo_root = _build_fake_repo(
        tmp_path,
        repo_name="Новая папка\\Letterbot smoke",
    )

    outside_cwd = tmp_path / "outside"
    outside_cwd.mkdir()
    result = _run_launcher(repo_root, cwd=outside_cwd, env=_launcher_env())

    assert result.returncode == 2
    assert (repo_root / ".venv" / "Scripts" / "python.exe").exists()
    assert (repo_root / "settings.ini").exists()
    assert (repo_root / "accounts.ini").exists()
    assert "First run setup" in result.stdout
    assert "accounts.ini will open in Notepad now." in result.stdout
    assert "Skipping Notepad because LETTERBOT_SKIP_NOTEPAD=1." in result.stdout
    assert not (repo_root / "run_stack_called.txt").exists()


@WINDOWS_ONLY
def test_letterbot_bat_smoke_runs_from_other_cwd_and_reuses_existing_venv(tmp_path: Path) -> None:
    repo_root = _build_fake_repo(
        tmp_path,
        repo_name="Another Folder\\Letterbot ready",
        filled_config=True,
    )

    first_cwd = tmp_path / "shell one"
    first_cwd.mkdir()
    first_run = _run_launcher(repo_root, cwd=first_cwd, env=_launcher_env())

    assert first_run.returncode == 0
    assert "LetterBot.ru is running." in first_run.stdout
    assert (repo_root / "run_stack_called.txt").exists()

    second_cwd = tmp_path / "shell two"
    second_cwd.mkdir()
    second_run = _run_launcher(repo_root, cwd=second_cwd, env=_launcher_env())

    assert second_run.returncode == 0
    assert "Dependencies are up to date." in second_run.stdout
    assert "First run setup" not in second_run.stdout


@WINDOWS_ONLY
def test_letterbot_bat_smoke_missing_python_is_actionable(tmp_path: Path) -> None:
    repo_root = _build_fake_repo(
        tmp_path,
        repo_name="Missing Python\\Letterbot",
    )

    result = _run_launcher(repo_root, cwd=repo_root, env=_launcher_env(missing_python=True))

    assert result.returncode == 1
    assert "[ERROR] Python was not found" in result.stdout
    assert "https://www.python.org/downloads/" in result.stdout
    assert "Add Python to PATH" in result.stdout
    assert "is not recognized" not in result.stdout


@WINDOWS_ONLY
def test_letterbot_bat_smoke_dependency_failure_is_actionable(tmp_path: Path) -> None:
    repo_root = _build_fake_repo(
        tmp_path,
        repo_name="Broken requirements\\Letterbot",
        requirements_text="not a valid requirement ===\n",
    )

    result = _run_launcher(repo_root, cwd=repo_root, env=_launcher_env())

    assert result.returncode == 1
    assert "[ERROR] Could not install dependencies" in result.stdout
    assert "Possible causes:" in result.stdout
    assert "Last log lines:" in result.stdout
