from __future__ import annotations

import configparser
import logging
import re
from dataclasses import asdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from mailbot_v26.domain.mail_type_classifier import MailTypeClassifier
from mailbot_v26.facts.fact_extractor import FactExtractor
from mailbot_v26.insights.commitment_lifecycle import parse_sqlite_datetime
from mailbot_v26.insights.commitment_tracker import Commitment, extract_deadline_ru
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.observability.decision_trace_v1 import compute_model_fingerprint


@dataclass(frozen=True, slots=True)
class PriorityBreakdownItem:
    signal: str
    points: int
    reason_code: str
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class PriorityResultV2:
    priority: str
    score: int
    breakdown: tuple[PriorityBreakdownItem, ...]
    reason_codes: tuple[str, ...]
    model_version: str = "v2"


@dataclass(frozen=True, slots=True)
class PriorityV2Config:
    urgency_weight_default: int = 12
    urgency_weight_by_type: int = 30
    amount_base_points: int = 5
    amount_10k_points: int = 10
    amount_50k_points: int = 20
    amount_100k_points: int = 30
    deadline_1d_points: int = 30
    deadline_3d_points: int = 20
    deadline_7d_points: int = 10
    type_invoice_final_points: int = 25
    type_reminder_escalation_points: int = 22
    type_contract_termination_points: int = 20
    type_claim_points: int = 20
    vip_base_score: int = 20
    vip_multiplier_fyi: float = 0.3
    vip_multiplier_freq: float = 0.5
    vip_multiplier_commitment: float = 1.5
    vip_multiplier_min: float = 0.2
    vip_multiplier_max: float = 2.0
    freq_spike_threshold: float = 3.0
    freq_spike_points: int = 15
    chain_two_points: int = 10
    chain_three_points: int = 25
    priority_red_threshold: int = 70
    priority_yellow_threshold: int = 35


@dataclass(frozen=True, slots=True)
class VipSenderMatcher:
    patterns: tuple[str, ...] = ()

    def matches(self, from_email: str) -> bool:
        normalized = (from_email or "").strip().lower()
        if not normalized:
            return False
        for raw_pattern in self.patterns:
            pattern = raw_pattern.strip().lower()
            if not pattern:
                continue
            if pattern.startswith("@"):
                if normalized.endswith(pattern):
                    return True
            elif pattern.endswith("@"):  # prefix match (e.g. legal@)
                if normalized.startswith(pattern):
                    return True
            elif normalized == pattern:
                return True
        return False


_PRIORITY_CONFIG_SECTION = "priority_v2"
_VIP_CONFIG_SECTION = "vip_senders"
_DEFAULT_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
_LOGGER = logging.getLogger(__name__)


class PriorityConfigError(RuntimeError):
    """Raised when priority config.ini is malformed and cannot be parsed as INI."""


def _invalid_ini_message(config_path: Path, config_example_path: Path) -> str:
    return (
        f"Invalid INI format in {config_path}. "
        f"Use {config_example_path} as template. "
        "INI must include [sections]. "
        "Windows regenerate command: "
        f"copy {config_example_path} {config_path}"
    )


def _load_ini_with_fallback(config_dir: Path) -> configparser.ConfigParser:
    config_path = config_dir / "config.ini"
    config_example_path = config_dir / "config.ini.example"
    parser = configparser.ConfigParser()
    if not config_path.exists():
        _LOGGER.warning(
            "priority config.ini missing at %s; using deterministic defaults for priority/vip settings.",
            config_path,
        )
        return parser
    try:
        parser.read(config_path, encoding="utf-8")
    except (configparser.MissingSectionHeaderError, configparser.ParsingError) as exc:
        err = PriorityConfigError(_invalid_ini_message(config_path, config_example_path))
        _LOGGER.warning(
            "%s Falling back to deterministic defaults for priority/vip settings.",
            err,
        )
        _LOGGER.debug("priority config parse error details", exc_info=exc)
        return configparser.ConfigParser()
    return parser


def load_priority_v2_config(base_dir: Path | None = None) -> PriorityV2Config:
    config_dir = base_dir or _DEFAULT_CONFIG_DIR
    parser = _load_ini_with_fallback(config_dir)
    section = parser[_PRIORITY_CONFIG_SECTION] if _PRIORITY_CONFIG_SECTION in parser else {}

    def _get_int(key: str, default: int) -> int:
        try:
            return int(section.get(key, default))
        except (TypeError, ValueError):
            return default

    def _get_float(key: str, default: float) -> float:
        try:
            return float(section.get(key, default))
        except (TypeError, ValueError):
            return default

    return PriorityV2Config(
        urgency_weight_default=_get_int("urgency_weight_default", 12),
        urgency_weight_by_type=_get_int("urgency_weight_by_type", 30),
        amount_base_points=_get_int("amount_base_points", 5),
        amount_10k_points=_get_int("amount_10k_points", 10),
        amount_50k_points=_get_int("amount_50k_points", 20),
        amount_100k_points=_get_int("amount_100k_points", 30),
        deadline_1d_points=_get_int("deadline_1d_points", 30),
        deadline_3d_points=_get_int("deadline_3d_points", 20),
        deadline_7d_points=_get_int("deadline_7d_points", 10),
        type_invoice_final_points=_get_int("type_invoice_final_points", 25),
        type_reminder_escalation_points=_get_int("type_reminder_escalation_points", 22),
        type_contract_termination_points=_get_int("type_contract_termination_points", 20),
        type_claim_points=_get_int("type_claim_points", 20),
        vip_base_score=_get_int("vip_base_score", 20),
        vip_multiplier_fyi=_get_float("vip_multiplier_fyi", 0.3),
        vip_multiplier_freq=_get_float("vip_multiplier_freq", 0.5),
        vip_multiplier_commitment=_get_float("vip_multiplier_commitment", 1.5),
        vip_multiplier_min=_get_float("vip_multiplier_min", 0.2),
        vip_multiplier_max=_get_float("vip_multiplier_max", 2.0),
        freq_spike_threshold=_get_float("freq_spike_threshold", 3.0),
        freq_spike_points=_get_int("freq_spike_points", 15),
        chain_two_points=_get_int("chain_two_points", 10),
        chain_three_points=_get_int("chain_three_points", 25),
        priority_red_threshold=_get_int("priority_red_threshold", 70),
        priority_yellow_threshold=_get_int("priority_yellow_threshold", 35),
    )


def load_vip_senders(base_dir: Path | None = None) -> VipSenderMatcher:
    config_dir = base_dir or _DEFAULT_CONFIG_DIR
    parser = _load_ini_with_fallback(config_dir)
    if _VIP_CONFIG_SECTION not in parser:
        return VipSenderMatcher()
    patterns: list[str] = []
    for raw_pattern, raw_value in parser[_VIP_CONFIG_SECTION].items():
        value = str(raw_value or "").strip().lower()
        if value in {"0", "false", "no", "off"}:
            continue
        pattern = raw_pattern.strip().lower()
        if pattern:
            patterns.append(pattern)
    return VipSenderMatcher(tuple(patterns))


class PriorityEngineV2:
    _URGENCY_KEYWORDS = {
        *MailTypeClassifier.URGENCY_KEYWORDS,
        "немедленно",
        "срочн",
    }
    _FYI_MARKERS = {"fyi", "к сведению", "для сведения"}
    _COMMITMENT_TYPES = {
        "INVOICE",
        "INVOICE_FINAL",
        "INVOICE_OVERDUE",
        "PAYMENT_REMINDER",
        "REMINDER_ESCALATION",
        "REMINDER_FIRST",
        "DEADLINE_REMINDER",
    }
    _REMINDER_TYPES = {
        "PAYMENT_REMINDER",
        "REMINDER_ESCALATION",
        "REMINDER_FIRST",
        "DEADLINE_REMINDER",
    }

    def __init__(
        self,
        analytics: KnowledgeAnalytics,
        *,
        config: PriorityV2Config | None = None,
        vip_senders: VipSenderMatcher | None = None,
    ) -> None:
        self._analytics = analytics
        self._config = config or load_priority_v2_config()
        self._vip_senders = vip_senders or load_vip_senders()
        self._fact_extractor = FactExtractor()

    def compute(
        self,
        *,
        subject: str,
        body_text: str,
        from_email: str,
        mail_type: str,
        received_at: datetime,
        commitments: Iterable[Commitment] | None = None,
    ) -> PriorityResultV2:
        score = 0
        breakdown: list[PriorityBreakdownItem] = []
        reason_codes: list[str] = []

        combined_text = f"{subject or ''}\n{body_text or ''}".strip()
        normalized_mail_type = (mail_type or "").strip().upper()
        normalized_subject = subject or ""
        reference_time = received_at or datetime.now(timezone.utc)

        def add_points(points: int, reason_code: str, signal: str, detail: str | None = None) -> None:
            nonlocal score
            if points <= 0:
                return
            score += points
            breakdown.append(
                PriorityBreakdownItem(
                    signal=signal,
                    points=points,
                    reason_code=reason_code,
                    detail=detail,
                )
            )
            reason_codes.append(reason_code)

        urgency_match = self._find_keyword(combined_text, self._URGENCY_KEYWORDS)
        if urgency_match:
            urgency_weight = self._config.urgency_weight_default
            add_points(
                urgency_weight,
                "PRIO_URGENT_KEYWORD",
                "urgency",
                detail=urgency_match,
            )
            if self._is_weighted_urgency_type(normalized_mail_type):
                extra = max(
                    0,
                    self._config.urgency_weight_by_type - urgency_weight,
                )
                add_points(
                    extra,
                    "PRIO_URGENT_WEIGHTED_BY_TYPE",
                    "urgency_weighted",
                    detail=normalized_mail_type,
                )

        amount_value = self._max_amount_value(combined_text)
        if amount_value is not None:
            if amount_value > 100_000:
                add_points(
                    self._config.amount_100k_points,
                    "PRIO_AMOUNT_100K",
                    "amount",
                    detail=str(int(amount_value)),
                )
            elif amount_value > 50_000:
                add_points(
                    self._config.amount_50k_points,
                    "PRIO_AMOUNT_50K",
                    "amount",
                    detail=str(int(amount_value)),
                )
            elif amount_value > 10_000:
                add_points(
                    self._config.amount_10k_points,
                    "PRIO_AMOUNT_10K",
                    "amount",
                    detail=str(int(amount_value)),
                )
            else:
                add_points(
                    self._config.amount_base_points,
                    "PRIO_AMOUNT_BASE",
                    "amount",
                    detail=str(int(amount_value)),
                )

        deadline_days = self._deadline_days_out(
            combined_text,
            reference_time,
            commitments=commitments,
        )
        if deadline_days is not None:
            if deadline_days <= 1:
                add_points(
                    self._config.deadline_1d_points,
                    "PRIO_DEADLINE_1D",
                    "deadline",
                    detail=str(deadline_days),
                )
            elif deadline_days <= 3:
                add_points(
                    self._config.deadline_3d_points,
                    "PRIO_DEADLINE_3D",
                    "deadline",
                    detail=str(deadline_days),
                )
            elif deadline_days <= 7:
                add_points(
                    self._config.deadline_7d_points,
                    "PRIO_DEADLINE_7D",
                    "deadline",
                    detail=str(deadline_days),
                )

        type_points, type_reason = self._mail_type_boost(normalized_mail_type)
        if type_points > 0 and type_reason:
            add_points(type_points, type_reason, "mail_type", detail=normalized_mail_type)

        events_window = self._recent_email_events(reference_time)
        frequency_ratio = self._frequency_ratio(
            events_window,
            reference_time=reference_time,
            from_email=from_email,
        )
        if frequency_ratio is not None and frequency_ratio > self._config.freq_spike_threshold:
            add_points(
                self._config.freq_spike_points,
                "PRIO_FREQ_SPIKE_3X",
                "frequency_spike",
                detail=f"{frequency_ratio:.2f}",
            )

        chain_count = self._reminder_chain_length(
            events_window,
            reference_time=reference_time,
            from_email=from_email,
            subject=normalized_subject,
        )
        if chain_count >= 3:
            add_points(
                self._config.chain_three_points,
                "PRIO_CHAIN_3PLUS",
                "reminder_chain",
                detail=str(chain_count),
            )
        elif chain_count >= 2:
            add_points(
                self._config.chain_two_points,
                "PRIO_CHAIN_2PLUS",
                "reminder_chain",
                detail=str(chain_count),
            )

        if self._vip_senders.matches(from_email):
            multiplier = 1.0
            vip_reasons: list[str] = []
            if self._contains_any(normalized_subject, self._FYI_MARKERS):
                multiplier *= self._config.vip_multiplier_fyi
                vip_reasons.append("PRIO_VIP_FYI_DAMPEN")
            if frequency_ratio is not None and frequency_ratio > self._config.freq_spike_threshold:
                multiplier *= self._config.vip_multiplier_freq
                vip_reasons.append("PRIO_VIP_FREQ_DAMPEN")
            if normalized_mail_type in self._COMMITMENT_TYPES:
                multiplier *= self._config.vip_multiplier_commitment
                vip_reasons.append("PRIO_VIP_COMMITMENT_BOOST")
            multiplier = max(
                self._config.vip_multiplier_min,
                min(self._config.vip_multiplier_max, multiplier),
            )
            vip_points = int(round(self._config.vip_base_score * multiplier))
            add_points(
                vip_points,
                "PRIO_VIP_BASE",
                "vip",
                detail=f"{multiplier:.2f}",
            )
            for reason in vip_reasons:
                reason_codes.append(reason)
                breakdown.append(
                    PriorityBreakdownItem(
                        signal="vip",
                        points=0,
                        reason_code=reason,
                        detail=f"{multiplier:.2f}",
                    )
                )

        score = max(0, min(100, score))
        priority = self._score_to_priority(score)

        return PriorityResultV2(
            priority=priority,
            score=score,
            breakdown=tuple(breakdown),
            reason_codes=tuple(reason_codes),
        )

    def evaluate_signals(
        self,
        *,
        subject: str,
        body_text: str,
        from_email: str,
        mail_type: str,
        received_at: datetime,
        commitments: Iterable[Commitment] | None = None,
    ) -> dict[str, bool]:
        combined_text = f"{subject or ''}\n{body_text or ''}".strip()
        normalized_mail_type = (mail_type or "").strip().upper()
        normalized_subject = subject or ""
        reference_time = received_at or datetime.now(timezone.utc)

        urgency_match = self._find_keyword(combined_text, self._URGENCY_KEYWORDS)
        amount_value = self._max_amount_value(combined_text)
        deadline_days = self._deadline_days_out(
            combined_text,
            reference_time,
            commitments=commitments,
        )
        type_points, _ = self._mail_type_boost(normalized_mail_type)
        events_window = self._recent_email_events(reference_time)
        frequency_ratio = self._frequency_ratio(
            events_window,
            reference_time=reference_time,
            from_email=from_email,
        )
        chain_count = self._reminder_chain_length(
            events_window,
            reference_time=reference_time,
            from_email=from_email,
            subject=normalized_subject,
        )
        vip_sender = self._vip_senders.matches(from_email)

        return {
            "URGENCY_KEYWORD": bool(urgency_match),
            "URGENCY_WEIGHTED_BY_TYPE": bool(
                urgency_match and self._is_weighted_urgency_type(normalized_mail_type)
            ),
            "AMOUNT_PRESENT": amount_value is not None,
            "AMOUNT_10K": bool(amount_value is not None and amount_value > 10_000),
            "AMOUNT_50K": bool(amount_value is not None and amount_value > 50_000),
            "AMOUNT_100K": bool(amount_value is not None and amount_value > 100_000),
            "DEADLINE_WITHIN_1D": bool(deadline_days is not None and deadline_days <= 1),
            "DEADLINE_WITHIN_3D": bool(deadline_days is not None and deadline_days <= 3),
            "DEADLINE_WITHIN_7D": bool(deadline_days is not None and deadline_days <= 7),
            "MAIL_TYPE_BOOST": type_points > 0,
            "FREQUENCY_SPIKE": bool(
                frequency_ratio is not None
                and frequency_ratio > self._config.freq_spike_threshold
            ),
            "REMINDER_CHAIN_2PLUS": chain_count >= 2,
            "REMINDER_CHAIN_3PLUS": chain_count >= 3,
            "VIP_SENDER": vip_sender,
            "VIP_FYI_DAMPEN": bool(
                vip_sender and self._contains_any(normalized_subject, self._FYI_MARKERS)
            ),
            "VIP_FREQ_DAMPEN": bool(
                vip_sender
                and frequency_ratio is not None
                and frequency_ratio > self._config.freq_spike_threshold
            ),
            "VIP_COMMITMENT_BOOST": bool(
                vip_sender and normalized_mail_type in self._COMMITMENT_TYPES
            ),
        }

    def explain_codes(self, result: PriorityResultV2) -> list[str]:
        return sorted(set(result.reason_codes))

    def model_fingerprint(self) -> str:
        snapshot = {
            "config": asdict(self._config),
            "vip_senders": asdict(self._vip_senders),
        }
        return compute_model_fingerprint(snapshot)

    def _recent_email_events(self, reference_time: datetime) -> list[dict[str, object]]:
        try:
            return self._analytics.recent_email_events(days=30, now_dt=reference_time)
        except Exception:
            return []

    def _frequency_ratio(
        self,
        events: Iterable[dict[str, object]],
        *,
        reference_time: datetime,
        from_email: str,
    ) -> float | None:
        normalized_sender = (from_email or "").strip().lower()
        if not normalized_sender:
            return None
        threshold_short = reference_time - timedelta(days=7)
        threshold_long = reference_time - timedelta(days=30)
        count_short = 0
        count_long = 0
        for row in events:
            payload = self._analytics.event_payload(row)
            sender = str(payload.get("from_email") or "").strip().lower()
            if sender != normalized_sender:
                continue
            event_time = parse_sqlite_datetime(str(row.get("timestamp") or ""))
            if event_time is None:
                continue
            if event_time >= threshold_long:
                count_long += 1
            if event_time >= threshold_short:
                count_short += 1
        if count_long == 0 and count_short == 0:
            return None
        week_rate = count_short / 7.0
        baseline_rate = max(count_long / 30.0, 0.1)
        return week_rate / baseline_rate

    def _reminder_chain_length(
        self,
        events: Iterable[dict[str, object]],
        *,
        reference_time: datetime,
        from_email: str,
        subject: str,
    ) -> int:
        normalized_sender = (from_email or "").strip().lower()
        normalized_subject = self._normalize_subject(subject)
        threshold = reference_time - timedelta(days=14)
        count = 0
        for row in events:
            payload = self._analytics.event_payload(row)
            sender = str(payload.get("from_email") or "").strip().lower()
            event_subject = self._normalize_subject(str(payload.get("subject") or ""))
            mail_type = str(payload.get("mail_type") or "").strip().upper()
            if mail_type not in self._REMINDER_TYPES:
                continue
            if normalized_sender and sender != normalized_sender:
                if normalized_subject and event_subject != normalized_subject:
                    continue
            event_time = parse_sqlite_datetime(str(row.get("timestamp") or ""))
            if event_time is None:
                continue
            if event_time >= threshold:
                count += 1
        return count

    def _mail_type_boost(self, mail_type: str) -> tuple[int, str | None]:
        if mail_type == "INVOICE_FINAL":
            return self._config.type_invoice_final_points, "PRIO_TYPE_INVOICE_FINAL"
        if mail_type == "REMINDER_ESCALATION":
            return (
                self._config.type_reminder_escalation_points,
                "PRIO_TYPE_REMINDER_ESCALATION",
            )
        if mail_type == "CONTRACT_TERMINATION":
            return (
                self._config.type_contract_termination_points,
                "PRIO_TYPE_CONTRACT_TERMINATION",
            )
        if mail_type.startswith("CLAIM"):
            return self._config.type_claim_points, "PRIO_TYPE_CLAIM"
        return 0, None

    def _is_weighted_urgency_type(self, mail_type: str) -> bool:
        if not mail_type:
            return False
        return (
            mail_type.startswith("INVOICE")
            or mail_type.startswith("REMINDER")
            or mail_type.startswith("PAYMENT_REMINDER")
            or mail_type.startswith("CLAIM")
        )

    def _max_amount_value(self, text: str) -> float | None:
        if not text:
            return None
        facts = self._fact_extractor.extract_facts(text)
        values: list[float] = []
        for raw in facts.amounts:
            parsed = self._parse_amount(raw)
            if parsed is not None:
                values.append(parsed)
        if not values:
            return None
        return max(values)

    def _deadline_days_out(
        self,
        text: str,
        reference_time: datetime,
        *,
        commitments: Iterable[Commitment] | None,
    ) -> int | None:
        deadlines: list[date] = []
        for commitment in commitments or []:
            if not commitment.deadline_iso:
                continue
            parsed = self._parse_iso_date(commitment.deadline_iso)
            if parsed:
                deadlines.append(parsed)
        if not deadlines and text:
            deadline_iso = extract_deadline_ru(text)
            parsed = self._parse_iso_date(deadline_iso) if deadline_iso else None
            if parsed:
                deadlines.append(parsed)
        if not deadlines:
            for raw in self._fact_extractor.extract_facts(text).dates:
                parsed = self._parse_date_token(raw, reference_time)
                if parsed:
                    deadlines.append(parsed)
        if not deadlines:
            return None
        soonest = min(deadlines)
        delta_days = (soonest - reference_time.date()).days
        if delta_days < 0:
            return 0
        return delta_days

    def _parse_iso_date(self, value: str | None) -> date | None:
        if not value:
            return None
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            parsed_dt = parse_sqlite_datetime(str(value))
            return parsed_dt.date() if parsed_dt else None

    def _parse_date_token(self, token: str, reference_time: datetime) -> date | None:
        token = token.strip()
        match = re.search(r"(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?", token)
        if not match:
            return None
        day = int(match.group(1))
        month = int(match.group(2))
        year_raw = match.group(3)
        year = reference_time.year
        if year_raw:
            year = int(year_raw)
            if year < 100:
                year += 2000
        try:
            return date(year, month, day)
        except ValueError:
            return None

    @staticmethod
    def _parse_amount(raw: str) -> float | None:
        cleaned = re.sub(r"[^0-9,\.\s]", "", str(raw))
        cleaned = cleaned.replace("\u00A0", " ")
        cleaned = cleaned.strip()
        if not cleaned:
            return None
        cleaned = cleaned.replace(" ", "")
        if "," in cleaned and "." in cleaned:
            cleaned = cleaned.replace(",", "")
        elif "," in cleaned:
            cleaned = cleaned.replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None

    def _score_to_priority(self, score: int) -> str:
        if score >= self._config.priority_red_threshold:
            return "🔴"
        if score >= self._config.priority_yellow_threshold:
            return "🟡"
        return "🔵"

    @staticmethod
    def _find_keyword(text: str, keywords: Iterable[str]) -> str | None:
        lowered = text.lower()
        for marker in sorted(keywords, key=str):
            if marker in lowered:
                return marker
        return None

    @staticmethod
    def _contains_any(text: str, markers: Iterable[str]) -> bool:
        lowered = text.lower()
        return any(marker in lowered for marker in markers)

    @staticmethod
    def _normalize_subject(subject: str) -> str:
        cleaned = re.sub(r"^(re:|fw:|fwd:)\s*", "", subject.strip(), flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.lower()


__all__ = [
    "PriorityBreakdownItem",
    "PriorityResultV2",
    "PriorityV2Config",
    "PriorityEngineV2",
    "VipSenderMatcher",
    "load_priority_v2_config",
    "load_vip_senders",
]
