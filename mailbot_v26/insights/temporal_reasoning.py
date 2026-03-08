from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from mailbot_v26.config.temporal_rules import DEFAULT_TEMPORAL_RULES, TemporalRuleConfig
from mailbot_v26.storage.analytics import KnowledgeAnalytics


@dataclass(frozen=True)
class TemporalState:
    entity_id: str
    state_type: str
    severity: str
    detected_at: str
    due_at: str | None
    evidence: dict[str, float | int | str | None]


class TemporalRule(Protocol):
    def evaluate(
        self,
        *,
        entity_id: str,
        from_email: str | None,
        now: datetime,
    ) -> list[TemporalState]: ...


class CommitmentDeadlineRule:
    def __init__(
        self, analytics: KnowledgeAnalytics, config: TemporalRuleConfig
    ) -> None:
        self.analytics = analytics
        self.config = config

    def evaluate(
        self,
        *,
        entity_id: str,
        from_email: str | None,
        now: datetime,
    ) -> list[TemporalState]:
        if not from_email:
            return []
        rows = self.analytics.pending_commitments_with_deadline(
            from_email=from_email,
            days_ahead=self.config.commitment_max_window_days,
        )
        results: list[TemporalState] = []
        for row in rows:
            deadline_raw = row.get("deadline_iso")
            if not deadline_raw:
                continue
            try:
                deadline_value = str(deadline_raw)
                if len(deadline_value) == 10:
                    deadline_at = datetime.fromisoformat(deadline_value + "T00:00:00")
                else:
                    deadline_at = datetime.fromisoformat(deadline_value)
                deadline_at = deadline_at.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            hours_to_deadline = (deadline_at - now).total_seconds() / 3600.0
            if hours_to_deadline < -self.config.commitment_grace_hours:
                continue
            if hours_to_deadline <= 0:
                state_type = "commitment_overdue"
                severity = "HIGH"
            elif hours_to_deadline <= self.config.commitment_warning_hours:
                state_type = "commitment_deadline_risk"
                severity = "MEDIUM"
            else:
                continue
            results.append(
                TemporalState(
                    entity_id=entity_id,
                    state_type=state_type,
                    severity=severity,
                    detected_at=now.isoformat(),
                    due_at=deadline_at.isoformat(),
                    evidence={
                        "commitment_id": row.get("commitment_id"),
                        "commitment_text": row.get("commitment_text"),
                        "deadline_iso": str(deadline_raw),
                        "hours_to_deadline": round(hours_to_deadline, 2),
                    },
                )
            )
        return results


class ResponseOverdueRule:
    def __init__(
        self, analytics: KnowledgeAnalytics, config: TemporalRuleConfig
    ) -> None:
        self.analytics = analytics
        self.config = config

    def evaluate(
        self,
        *,
        entity_id: str,
        from_email: str | None,
        now: datetime,
    ) -> list[TemporalState]:
        last_received = self.analytics.latest_interaction_event_time(
            entity_id=entity_id,
            event_type="email_received",
        )
        if last_received is None:
            return []
        response_times = self.analytics.interaction_event_response_times(
            entity_id=entity_id,
            event_type="response_time",
            days=self.config.response_window_days,
        )
        if not response_times:
            return []
        avg_response = sum(response_times) / len(response_times)
        expected_hours = max(
            self.config.response_min_hours,
            avg_response * self.config.response_multiplier,
        )
        elapsed_hours = (now - last_received).total_seconds() / 3600.0
        if elapsed_hours <= expected_hours:
            return []
        ratio = elapsed_hours / expected_hours if expected_hours > 0 else None
        severity = "LOW"
        if ratio is not None and ratio >= self.config.response_severity_high:
            severity = "HIGH"
        elif ratio is not None and ratio >= self.config.response_severity_medium:
            severity = "MEDIUM"
        return [
            TemporalState(
                entity_id=entity_id,
                state_type="response_overdue",
                severity=severity,
                detected_at=now.isoformat(),
                due_at=None,
                evidence={
                    "last_received_at": last_received.isoformat(),
                    "elapsed_hours": round(elapsed_hours, 2),
                    "avg_response_hours": round(avg_response, 2),
                    "expected_hours": round(expected_hours, 2),
                    "ratio": round(ratio, 2) if ratio is not None else None,
                },
            )
        ]


class SilenceBreakRule:
    def __init__(
        self, analytics: KnowledgeAnalytics, config: TemporalRuleConfig
    ) -> None:
        self.analytics = analytics
        self.config = config

    def evaluate(
        self,
        *,
        entity_id: str,
        from_email: str | None,
        now: datetime,
    ) -> list[TemporalState]:
        baseline = self.analytics.entity_baseline(
            entity_id=entity_id, metric="email_frequency"
        )
        baseline_value = baseline.get("baseline_value")
        if baseline_value is None:
            return []
        try:
            baseline_value = float(baseline_value)
        except (TypeError, ValueError):
            return []
        baseline_weekly = baseline_value * 7.0
        if baseline_weekly < self.config.silence_baseline_weekly:
            return []
        last_received = self.analytics.latest_interaction_event_time(
            entity_id=entity_id,
            event_type="email_received",
        )
        if last_received is None:
            return []
        silence_days = (now - last_received).days
        if silence_days < self.config.silence_window_days:
            return []
        return [
            TemporalState(
                entity_id=entity_id,
                state_type="silence_break",
                severity="MEDIUM",
                detected_at=now.isoformat(),
                due_at=None,
                evidence={
                    "baseline_weekly": round(baseline_weekly, 2),
                    "last_received_at": last_received.isoformat(),
                    "silence_days": silence_days,
                },
            )
        ]


class TemporalReasoningEngine:
    def __init__(
        self,
        analytics: KnowledgeAnalytics,
        config: TemporalRuleConfig | None = None,
    ) -> None:
        self.analytics = analytics
        self.config = config or DEFAULT_TEMPORAL_RULES
        self.rules: tuple[TemporalRule, ...] = (
            CommitmentDeadlineRule(analytics, self.config),
            ResponseOverdueRule(analytics, self.config),
            SilenceBreakRule(analytics, self.config),
        )

    def evaluate(
        self,
        *,
        entity_id: str,
        from_email: str | None,
        now: datetime | None = None,
    ) -> list[TemporalState]:
        current_time = now or datetime.now(timezone.utc)
        results: list[TemporalState] = []
        for rule in self.rules:
            results.extend(
                rule.evaluate(
                    entity_id=entity_id,
                    from_email=from_email,
                    now=current_time,
                )
            )
        return results


__all__ = ["TemporalReasoningEngine", "TemporalState"]
