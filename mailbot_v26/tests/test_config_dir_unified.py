from __future__ import annotations

from pathlib import Path

from mailbot_v26 import __main__ as cli_main
from mailbot_v26.config.paths import resolve_config_paths
from mailbot_v26.config_loader import CONFIG_DIR
from mailbot_v26.tools import run_stack


def test_resolve_config_paths_default_points_to_package_config_dir() -> None:
    paths = resolve_config_paths()

    assert paths.config_dir == CONFIG_DIR
    assert paths.config_dir.name == "config"
    assert paths.config_dir.parent.name == "mailbot_v26"


def test_cli_parser_subcommands_use_single_source_of_truth_config_dir() -> None:
    parser = cli_main._build_parser()
    args = parser.parse_args(["doctor"])

    assert Path(args.config_dir) == CONFIG_DIR


def test_run_stack_default_config_dir_uses_single_source_of_truth() -> None:
    assert run_stack._resolve_config_dir(None) == CONFIG_DIR
