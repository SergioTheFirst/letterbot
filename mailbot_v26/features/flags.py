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
        self.ENABLE_PREMIUM_PROCESSOR = False
        self.ENABLE_BEHAVIOR_METRICS_DIGEST = False
        self.ENABLE_DIGEST_ACTION_TEMPLATES = False
        self.ENABLE_TRUST_BOOTSTRAP = False
        self.ENABLE_REGRET_MINIMIZATION = False
        self.ENABLE_UNCERTAINTY_QUEUE = False
        self.ENABLE_COMMITMENT_CHAIN_DIGEST = False
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
        self.ENABLE_WEEKLY_ACCURACY_REPORT = self._get_flag(
            parser, "enable_weekly_accuracy_report"
        )
        self.ENABLE_WEEKLY_CALIBRATION_REPORT = self._get_flag(
            parser, "enable_weekly_calibration_report"
        )
        self.ENABLE_DIGEST_INSIGHTS = self._get_flag(
            parser, "enable_digest_insights"
        )
        self.ENABLE_ANOMALY_ALERTS = self._get_flag(parser, "enable_anomaly_alerts")
        self.ENABLE_ATTENTION_ECONOMICS = self._get_flag(
            parser, "enable_attention_economics"
        )
        self.ENABLE_QUALITY_METRICS = self._get_flag(
            parser, "enable_quality_metrics"
        )
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
        self.ENABLE_FLOW_PROTECTION = self._get_flag(
            parser, "enable_flow_protection"
        )
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
            parser, "enable_premium_processor"
        )
        self.ENABLE_BEHAVIOR_METRICS_DIGEST = self._get_flag(
            parser, "enable_behavior_metrics_digest"
        )
        self.ENABLE_DIGEST_ACTION_TEMPLATES = self._get_flag(
            parser, "enable_digest_action_templates"
        )
        self.ENABLE_TRUST_BOOTSTRAP = self._get_flag(
            parser, "enable_trust_bootstrap"
        )
        self.ENABLE_REGRET_MINIMIZATION = self._get_flag(
            parser, "enable_regret_minimization"
        )
        self.ENABLE_UNCERTAINTY_QUEUE = self._get_flag(
            parser, "enable_uncertainty_queue"
        )
        self.ENABLE_COMMITMENT_CHAIN_DIGEST = self._get_flag(
            parser, "enable_commitment_chain_digest"
        )
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


__all__ = ["FeatureFlags"]
