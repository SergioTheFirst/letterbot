"""Feature flag definitions for MailBot.

Flags are loaded from ``settings.ini`` in 2-file mode and may fall back to
legacy ``config.ini`` when running legacy config layouts. Missing files,
sections, or invalid values silently fall back to deterministic defaults.
"""

from __future__ import annotations

import configparser
import logging
from pathlib import Path
from typing import Optional

from mailbot_v26.config.ini_utils import read_user_ini_with_defaults
from mailbot_v26.config.paths import resolve_config_paths

_LOGGER = logging.getLogger(__name__)


class FeatureFlags:
    """Container for feature toggles.

    Parameters
    ----------
    base_dir:
        Optional configuration directory override. Defaults to
        ``mailbot_v26/config``.
    """

    def __init__(self, base_dir: Optional[Path] = None) -> None:
        self.ENABLE_AUTO_PRIORITY = True
        self.ENABLE_AUTO_ACTIONS = False
        self.ENABLE_TASK_SUGGESTIONS = False
        self.ENABLE_TG_EDITING = False
        self.ENABLE_SHADOW_PERSISTENCE = False
        self.ENABLE_CRM_DIAGNOSTICS = False
        self.ENABLE_PREVIEW_ACTIONS = False
        self.ENABLE_COMMITMENT_TRACKER = False
        self.ENABLE_DAILY_DIGEST = True
        self.ENABLE_WEEKLY_DIGEST = True
        self.ENABLE_WEEKLY_ACCURACY_REPORT = False
        self.ENABLE_WEEKLY_CALIBRATION_REPORT = False
        self.ENABLE_DIGEST_INSIGHTS = False
        self.ENABLE_ANOMALY_ALERTS = False
        self.ENABLE_ATTENTION_ECONOMICS = False
        self.ENABLE_QUALITY_METRICS = False
        self.ENABLE_HIERARCHICAL_MAIL_TYPES = False
        self.ENABLE_PRIORITY_V2 = True
        self.ENABLE_NARRATIVE_BINDING = True
        self.ENABLE_NARRATIVE_PATTERNS = True
        self.ENABLE_NOTIFICATION_SLA = True
        self.ENABLE_CIRCADIAN_DELIVERY = False
        self.ENABLE_FLOW_PROTECTION = False
        self.ENABLE_ATTENTION_DEBT = False
        self.ENABLE_SURPRISE_BUDGET = "shadow"
        self.ENABLE_SILENCE_AS_SIGNAL = "shadow"
        self.ENABLE_DEADLOCK_DETECTION = "shadow"
        self.ENABLE_PREMIUM_PROCESSOR = True
        self.ENABLE_PREMIUM_CLARITY_V1 = True
        self.ENABLE_BEHAVIOR_METRICS_DIGEST = False
        self.ENABLE_DIGEST_ACTION_TEMPLATES = False
        self.ENABLE_TRUST_BOOTSTRAP = False
        self.ENABLE_REGRET_MINIMIZATION = False
        self.ENABLE_UNCERTAINTY_QUEUE = False
        self.ENABLE_COMMITMENT_CHAIN_DIGEST = False
        self.DONATE_ENABLED = False
        self.AUTO_PRIORITY_CONFIDENCE_THRESHOLD = 0.6
        self.AUTO_ACTION_CONFIDENCE_THRESHOLD = 0.75

        config_dir = base_dir or Path(__file__).resolve().parents[1] / "config"
        resolved = resolve_config_paths(config_dir)
        config_path = resolved.settings_path
        if not config_path.exists():
            config_path = resolved.legacy_ini_path

        parser = read_user_ini_with_defaults(
            config_path,
            logger=_LOGGER,
            scope_label="feature flags",
        )
        self.ENABLE_AUTO_PRIORITY = self._get_flag(
            parser, "enable_auto_priority", fallback_default=True
        )
        self.ENABLE_AUTO_ACTIONS = self._get_flag(parser, "enable_auto_actions")
        self.ENABLE_TASK_SUGGESTIONS = self._get_flag(parser, "enable_task_suggestions")
        self.ENABLE_TG_EDITING = self._get_flag(parser, "enable_tg_editing")
        self.ENABLE_SHADOW_PERSISTENCE = self._get_flag(
            parser, "enable_shadow_persistence"
        )
        self.ENABLE_CRM_DIAGNOSTICS = self._get_flag(parser, "enable_crm_diagnostics")
        self.ENABLE_PREVIEW_ACTIONS = self._get_flag(parser, "enable_preview_actions")
        self.ENABLE_COMMITMENT_TRACKER = self._get_flag(
            parser, "enable_commitment_tracker"
        )
        self.ENABLE_DAILY_DIGEST = self._get_flag_alias(
            parser, "enable_daily_digest", aliases=("daily_digest_enabled",)
        )
        self.ENABLE_WEEKLY_DIGEST = self._get_flag_alias(
            parser, "enable_weekly_digest", aliases=("weekly_digest_enabled",)
        )
        self.ENABLE_WEEKLY_ACCURACY_REPORT = self._get_flag(
            parser, "enable_weekly_accuracy_report"
        )
        self.ENABLE_WEEKLY_CALIBRATION_REPORT = self._get_flag(
            parser, "enable_weekly_calibration_report"
        )
        self.ENABLE_DIGEST_INSIGHTS = self._get_flag(parser, "enable_digest_insights")
        self.ENABLE_ANOMALY_ALERTS = self._get_flag(parser, "enable_anomaly_alerts")
        self.ENABLE_ATTENTION_ECONOMICS = self._get_flag(
            parser, "enable_attention_economics"
        )
        self.ENABLE_QUALITY_METRICS = self._get_flag(parser, "enable_quality_metrics")
        self.ENABLE_HIERARCHICAL_MAIL_TYPES = self._get_flag(
            parser, "enable_hierarchical_mail_types"
        )
        if parser.has_option("features", "enable_narrative_binding"):
            self.ENABLE_NARRATIVE_BINDING = self._get_flag(
                parser, "enable_narrative_binding"
            )
        if parser.has_option("features", "enable_narrative_patterns"):
            self.ENABLE_NARRATIVE_PATTERNS = self._get_flag(
                parser, "enable_narrative_patterns"
            )
        if parser.has_option("features", "enable_priority_v2"):
            self.ENABLE_PRIORITY_V2 = self._get_flag(parser, "enable_priority_v2")
        if parser.has_option("features", "enable_notification_sla"):
            self.ENABLE_NOTIFICATION_SLA = self._get_flag(
                parser, "enable_notification_sla"
            )
        self.ENABLE_CIRCADIAN_DELIVERY = self._get_flag(
            parser, "enable_circadian_delivery"
        )
        self.ENABLE_FLOW_PROTECTION = self._get_flag(parser, "enable_flow_protection")
        self.ENABLE_ATTENTION_DEBT = self._get_flag(parser, "enable_attention_debt")
        self.ENABLE_SURPRISE_BUDGET = self._get_flag_mode(
            parser, "enable_surprise_budget", default="shadow"
        )
        self.ENABLE_SILENCE_AS_SIGNAL = self._get_flag_mode(
            parser, "enable_silence_as_signal", default="shadow"
        )
        self.ENABLE_DEADLOCK_DETECTION = self._get_flag_mode(
            parser, "enable_deadlock_detection", default="shadow"
        )
        self.ENABLE_PREMIUM_PROCESSOR = self._get_flag(
            parser,
            "enable_premium_processor",
            fallback_default=True,
        )
        self.ENABLE_PREMIUM_CLARITY_V1 = self._get_flag(
            parser, "enable_premium_clarity_v1", fallback_default=True
        )
        self.ENABLE_BEHAVIOR_METRICS_DIGEST = self._get_flag(
            parser, "enable_behavior_metrics_digest"
        )
        self.ENABLE_DIGEST_ACTION_TEMPLATES = self._get_flag(
            parser, "enable_digest_action_templates"
        )
        self.ENABLE_TRUST_BOOTSTRAP = self._get_flag(parser, "enable_trust_bootstrap")
        self.ENABLE_REGRET_MINIMIZATION = self._get_flag(
            parser, "enable_regret_minimization"
        )
        self.ENABLE_UNCERTAINTY_QUEUE = self._get_flag(
            parser, "enable_uncertainty_queue"
        )
        self.ENABLE_COMMITMENT_CHAIN_DIGEST = self._get_flag(
            parser, "enable_commitment_chain_digest"
        )
        self.DONATE_ENABLED = self._get_support_flag(parser)
        self.AUTO_PRIORITY_CONFIDENCE_THRESHOLD = self._get_float(
            parser, "auto_priority_confidence_threshold", default=0.6
        )
        self.AUTO_ACTION_CONFIDENCE_THRESHOLD = self._get_float(
            parser, "auto_action_confidence_threshold", default=0.75
        )

    @staticmethod
    def _get_flag_alias(
        parser: configparser.ConfigParser,
        option: str,
        *,
        aliases: tuple[str, ...] = (),
    ) -> bool:
        if parser.has_option("features", option):
            return FeatureFlags._get_flag(parser, option)
        for alias in aliases:
            if parser.has_option("features", alias):
                return FeatureFlags._get_flag(parser, alias)
        return True

    @staticmethod
    def _get_flag(
        parser: configparser.ConfigParser,
        option: str,
        *,
        fallback_default: bool = False,
    ) -> bool:
        try:
            return parser.getboolean("features", option, fallback=fallback_default)
        except ValueError:
            return fallback_default

    @staticmethod
    def _get_float(
        parser: configparser.ConfigParser, option: str, *, default: float
    ) -> float:
        try:
            return parser.getfloat("features", option, fallback=default)
        except ValueError:
            return default

    @staticmethod
    def _get_flag_mode(
        parser: configparser.ConfigParser, option: str, *, default: str
    ) -> str:
        raw = parser.get("features", option, fallback="").strip().lower()
        if not raw:
            return default
        if raw in {"shadow", "enabled", "disabled"}:
            return raw
        if raw in {"true", "yes", "1", "on"}:
            return "enabled"
        if raw in {"false", "no", "0", "off"}:
            return "disabled"
        return default

    @staticmethod
    def _get_support_flag(parser: configparser.ConfigParser) -> bool:
        try:
            if parser.has_option("support", "enabled"):
                return parser.getboolean("support", "enabled", fallback=False)
            if parser.has_option("features", "support"):
                return parser.getboolean("features", "support", fallback=False)
            return parser.getboolean("features", "donate_enabled", fallback=False)
        except ValueError:
            return False


__all__ = ["FeatureFlags"]
