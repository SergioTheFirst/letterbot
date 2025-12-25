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
        self.ENABLE_AUTO_ACTIONS = False
        self.ENABLE_TASK_SUGGESTIONS = False
        self.ENABLE_TG_EDITING = False
        self.ENABLE_SHADOW_PERSISTENCE = False
        self.ENABLE_CRM_DIAGNOSTICS = False
        self.ENABLE_PREVIEW_ACTIONS = False
        self.ENABLE_COMMITMENT_TRACKER = False
        self.ENABLE_DAILY_DIGEST = False
        self.ENABLE_WEEKLY_DIGEST = False
        self.AUTO_PRIORITY_CONFIDENCE_THRESHOLD = 0.6
        self.AUTO_ACTION_CONFIDENCE_THRESHOLD = 0.75

        config_dir = base_dir or Path(__file__).resolve().parents[1] / "config"
        config_path = config_dir / "config.ini"

        parser = configparser.ConfigParser()
        if not config_path.exists():
            return

        parser.read(config_path, encoding="utf-8")
        self.ENABLE_AUTO_PRIORITY = self._get_flag(parser, "enable_auto_priority")
        self.ENABLE_AUTO_ACTIONS = self._get_flag(parser, "enable_auto_actions")
        self.ENABLE_TASK_SUGGESTIONS = self._get_flag(parser, "enable_task_suggestions")
        self.ENABLE_TG_EDITING = self._get_flag(parser, "enable_tg_editing")
        self.ENABLE_SHADOW_PERSISTENCE = self._get_flag(parser, "enable_shadow_persistence")
        self.ENABLE_CRM_DIAGNOSTICS = self._get_flag(parser, "enable_crm_diagnostics")
        self.ENABLE_PREVIEW_ACTIONS = self._get_flag(parser, "enable_preview_actions")
        self.ENABLE_COMMITMENT_TRACKER = self._get_flag(parser, "enable_commitment_tracker")
        self.ENABLE_DAILY_DIGEST = self._get_flag(parser, "enable_daily_digest")
        self.ENABLE_WEEKLY_DIGEST = self._get_flag(parser, "enable_weekly_digest")
        self.AUTO_PRIORITY_CONFIDENCE_THRESHOLD = self._get_float(
            parser, "auto_priority_confidence_threshold", default=0.6
        )
        self.AUTO_ACTION_CONFIDENCE_THRESHOLD = self._get_float(
            parser, "auto_action_confidence_threshold", default=0.75
        )

    @staticmethod
    def _get_flag(parser: configparser.ConfigParser, option: str) -> bool:
        try:
            return parser.getboolean("features", option, fallback=False)
        except ValueError:
            return False

    @staticmethod
    def _get_float(
        parser: configparser.ConfigParser, option: str, *, default: float
    ) -> float:
        try:
            return parser.getfloat("features", option, fallback=default)
        except ValueError:
            return default


__all__ = ["FeatureFlags"]
