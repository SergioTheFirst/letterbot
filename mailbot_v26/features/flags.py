"""Feature flag definitions for MailBot.

Flags are loaded from ``config/config.ini`` when available. Missing files,
sections, or invalid values silently fall back to ``False`` to guarantee that
introducing new flags never alters runtime behavior unexpectedly.
"""

from __future__ import annotations

import configparser
from pathlib import Path
from typing import Optional


class FeatureFlags:
    """Container for feature toggles.

    Parameters
    ----------
    base_dir:
        Optional configuration directory override. Defaults to
        ``mailbot_v26/config``.
    """

    def __init__(self, base_dir: Optional[Path] = None) -> None:
        self.ENABLE_AUTO_PRIORITY = False
        self.ENABLE_TASK_SUGGESTIONS = False
        self.ENABLE_TG_EDITING = False
        self.ENABLE_SHADOW_PERSISTENCE = False
        self.ENABLE_CRM_DIAGNOSTICS = False

        config_dir = base_dir or Path(__file__).resolve().parents[1] / "config"
        config_path = config_dir / "config.ini"

        parser = configparser.ConfigParser()
        if not config_path.exists():
            return

        parser.read(config_path, encoding="utf-8")
        self.ENABLE_AUTO_PRIORITY = self._get_flag(parser, "enable_auto_priority")
        self.ENABLE_TASK_SUGGESTIONS = self._get_flag(parser, "enable_task_suggestions")
        self.ENABLE_TG_EDITING = self._get_flag(parser, "enable_tg_editing")
        self.ENABLE_SHADOW_PERSISTENCE = self._get_flag(parser, "enable_shadow_persistence")
        self.ENABLE_CRM_DIAGNOSTICS = self._get_flag(parser, "enable_crm_diagnostics")

    @staticmethod
    def _get_flag(parser: configparser.ConfigParser, option: str) -> bool:
        try:
            return parser.getboolean("features", option, fallback=False)
        except ValueError:
            return False


__all__ = ["FeatureFlags"]
