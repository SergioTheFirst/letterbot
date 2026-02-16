from __future__ import annotations

import importlib.util
from dataclasses import dataclass


class DependencyError(RuntimeError):
    """Raised when required runtime dependencies are missing."""


@dataclass(frozen=True)
class Requirement:
    module: str
    pip_name: str
    help_line: str


RUNTIME_REQUIREMENTS = {
    "yaml": Requirement("yaml", "PyYAML", "Нужен для загрузки config.yaml"),
    "imapclient": Requirement("imapclient", "imapclient", "Нужен для IMAP-подключения"),
}

MODE_REQUIREMENTS: dict[str, tuple[Requirement, ...]] = {
    "runtime": (RUNTIME_REQUIREMENTS["yaml"], RUNTIME_REQUIREMENTS["imapclient"]),
    "web_ui": (RUNTIME_REQUIREMENTS["yaml"],),
    "doctor": (RUNTIME_REQUIREMENTS["yaml"], RUNTIME_REQUIREMENTS["imapclient"]),
    "validate_config": (RUNTIME_REQUIREMENTS["yaml"],),
    "tests": (RUNTIME_REQUIREMENTS["yaml"], RUNTIME_REQUIREMENTS["imapclient"]),
}


def has(module: str) -> bool:
    return importlib.util.find_spec(module) is not None


def _windows_next_step() -> str:
    return (
        "Windows: активируйте .venv и ставьте пакеты через '\\.venv\\Scripts\\python -m pip install -r requirements.txt' "
        "или запустите install_and_run.bat."
    )


def require(module: str, pip_name: str, help_line: str) -> None:
    if has(module):
        return
    install_cmd = f"python -m pip install {pip_name}"
    raise DependencyError(
        "Missing dependency: "
        f"{module}. {help_line}. Install: {install_cmd}. {_windows_next_step()}"
    )


def require_runtime_for(mode: str = "runtime") -> None:
    requirements = MODE_REQUIREMENTS.get(mode)
    if requirements is None:
        raise ValueError(f"Unknown dependency mode: {mode}")
    for requirement in requirements:
        require(requirement.module, requirement.pip_name, requirement.help_line)

