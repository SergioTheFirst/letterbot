from __future__ import annotations

from pathlib import Path

from mailbot_v26 import __main__ as cli_main
from mailbot_v26.config.paths import resolve_config_paths
from mailbot_v26.config_loader import CONFIG_DIR
from mailbot_v26.tools import run_stack
from mailbot_v26.pipeline import digest_scheduler, processor


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


def test_processor_config_dir_configures_module_loaders(tmp_path: Path) -> None:
    config_dir = tmp_path / "cfg"
    config_dir.mkdir()
    (config_dir / "config.ini").write_text(
        "[budgets]\ndefault_llm_budget_tokens_per_day = 123\n", encoding="utf-8"
    )

    processor.configure_processor_config_dir(config_dir)

    assert processor.get_budget_gate_config().default_llm_budget_tokens_per_day == 123


def test_digest_config_dir_updates_loader_path(tmp_path: Path) -> None:
    config_dir = tmp_path / "cfg"
    config_dir.mkdir()

    digest_scheduler.configure_digest_config_dir(config_dir)

    assert digest_scheduler._CONFIG_PATH == config_dir / "config.ini"
