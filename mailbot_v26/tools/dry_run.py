"""Offline dry-run harness for fixture emails."""

from __future__ import annotations

import argparse
import json
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from email import message_from_bytes
from email.utils import parseaddr, parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterator

from mailbot_v26.bot_core.pipeline import parse_raw_email
from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    GeneralConfig,
    KeysConfig,
    StorageConfig,
)
from mailbot_v26.pipeline import processor as pipeline_processor
from mailbot_v26.telegram.render_contract import (
    TelegramRenderRequest,
    TelegramRenderResult,
    render_email_notification,
)
from mailbot_v26.tools.eval_golden_corpus import (
    OfflinePipelineArtifacts,
    build_offline_artifacts,
)


@dataclass(frozen=True, slots=True)
class DryRunResult:
    fixture_path: Path
    sender_email: str
    sender_name: str
    subject: str
    received_at: datetime
    inbound_body: str
    attachments: tuple[dict[str, Any], ...]
    stage_order: tuple[str, ...]
    artifacts: OfflinePipelineArtifacts
    render: TelegramRenderResult
    suppressed_events: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        template_fields = {
            key: self.artifacts.final_facts.get(key)
            for key in (
                "template_id",
                "template_strong_match",
                "template_sender_match",
                "template_confidence_boost",
                "template_promotion_applied",
                "template_runtime_promotion_candidate",
            )
            if key in self.artifacts.final_facts
        }
        return {
            "fixture_path": str(self.fixture_path),
            "stage_order": list(self.stage_order),
            "parse": {
                "sender_email": self.sender_email,
                "sender_name": self.sender_name,
                "subject": self.subject,
                "received_at": self.received_at.isoformat(),
                "body_text": self.inbound_body,
                "attachments": [dict(item) for item in self.attachments],
            },
            "classification": {
                "mail_type": self.artifacts.mail_type,
                "mail_type_reasons": list(self.artifacts.mail_type_reasons),
            },
            "facts": self.artifacts.collected_facts,
            "validation": self.artifacts.validated_facts,
            "scoring": self.artifacts.scored_facts,
            "consistency": self.artifacts.consistent_facts,
            "template": template_fields,
            "decision": {
                "priority": self.artifacts.decision.priority,
                "action": self.artifacts.decision.action,
                "summary": self.artifacts.decision.summary,
                "doc_kind": self.artifacts.decision.doc_kind,
                "amount": self.artifacts.decision.amount,
                "due_date": self.artifacts.decision.due_date,
                "confidence": self.artifacts.decision.confidence,
                "context": self.artifacts.decision.context,
            },
            "interpretation": {
                "email_id": self.artifacts.interpretation.email_id,
                "sender_email": self.artifacts.interpretation.sender_email,
                "doc_kind": self.artifacts.interpretation.doc_kind,
                "amount": self.artifacts.interpretation.amount,
                "due_date": self.artifacts.interpretation.due_date,
                "action": self.artifacts.interpretation.action,
                "priority": self.artifacts.interpretation.priority,
                "confidence": self.artifacts.interpretation.confidence,
                "context": self.artifacts.interpretation.context,
                "document_id": self.artifacts.interpretation.document_id,
                "template_id": self.artifacts.interpretation.template_id,
                "issuer_key": self.artifacts.interpretation.issuer_key,
                "issuer_label": self.artifacts.interpretation.issuer_label,
                "issuer_domain": self.artifacts.interpretation.issuer_domain,
                "issuer_tax_id": self.artifacts.interpretation.issuer_tax_id,
            },
            "render": {
                "text": self.render.text,
                "parse_mode": self.render.parse_mode,
                "render_mode": self.render.render_mode,
                "payload_invalid": self.render.payload_invalid,
                "message_ref": self.render.message_ref,
                "timestamp_iso": self.render.timestamp_iso,
                "sender_identity_key": self.render.sender_identity_key,
                "sender_identity_label": self.render.sender_identity_label,
                "reply_markup": self.render.reply_markup,
            },
            "suppressed_events": list(self.suppressed_events),
        }


def _build_offline_config(base_dir: Path) -> BotConfig:
    general = GeneralConfig(
        check_interval=1,
        max_email_mb=15,
        max_attachment_mb=2,
        max_zip_uncompressed_mb=80,
        max_extracted_chars=50_000,
        max_extracted_total_chars=120_000,
        admin_chat_id="dry-run-admin",
    )
    account = AccountConfig(
        account_id="dry-run",
        login="dry-run@example.com",
        password="",
        host="imap.invalid",
        port=993,
        use_ssl=True,
        telegram_chat_id="dry-run-chat",
    )
    return BotConfig(
        general=general,
        accounts=[account],
        keys=KeysConfig(telegram_bot_token="dry-run-token"),
        storage=StorageConfig(db_path=base_dir / "dry_run.sqlite"),
    )


def _normalize_received_at(value: str) -> datetime:
    if value:
        try:
            parsed = parsedate_to_datetime(value)
        except Exception:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
    return datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)


@contextmanager
def _suppress_render_side_effects() -> Iterator[list[dict[str, Any]]]:
    emitted: list[dict[str, Any]] = []
    original_emitter = pipeline_processor.event_emitter

    class _DryRunEventSink:
        def emit(self, **kwargs: Any) -> None:
            emitted.append(dict(kwargs))

    pipeline_processor.event_emitter = _DryRunEventSink()
    try:
        yield emitted
    finally:
        pipeline_processor.event_emitter = original_emitter


def run_dry_run_fixture(
    fixture_path: str | Path,
    *,
    storage_dir: str | Path | None = None,
) -> DryRunResult:
    path = Path(fixture_path)
    raw_email = path.read_bytes()
    config = _build_offline_config(Path(storage_dir) if storage_dir else path.parent)
    inbound = parse_raw_email(raw_email, config)
    message = message_from_bytes(raw_email)
    sender_name, sender_email = parseaddr(inbound.sender or "")
    attachments = tuple(
        {
            "filename": str(item.filename or ""),
            "text": str(item.text or ""),
            "content_type": str(item.content_type or ""),
            "size_bytes": int(item.size_bytes or 0),
            "metadata": dict(item.metadata or {}),
        }
        for item in (inbound.attachments or [])
    )
    received_at = _normalize_received_at(str(message.get("Date") or ""))
    artifacts = build_offline_artifacts(
        sender_email=sender_email or str(inbound.sender or ""),
        subject=str(inbound.subject or ""),
        body_text=str(inbound.body or ""),
        attachments=attachments,
        mail_type=str(inbound.mail_type or ""),
        email_id=1,
        document_id=f"dry-run:{path.stem}",
    )
    body_summary = (
        str(artifacts.decision.summary or "").strip()
        or pipeline_processor._build_heuristic_summary(
            subject=str(inbound.subject or ""),
            body_text=str(inbound.body or ""),
            attachments=list(attachments),
            message_facts=artifacts.final_facts,
        )
    )
    attachment_summaries = pipeline_processor._build_heuristic_attachment_summaries(
        list(attachments)
    )
    request = TelegramRenderRequest(
        email_id=1,
        received_at=received_at,
        sender_email=sender_email or str(inbound.sender or ""),
        sender_name=sender_name or None,
        subject=str(inbound.subject or ""),
        interpretation=artifacts.interpretation,
        action_line=str(artifacts.decision.action or ""),
        mail_type=artifacts.mail_type,
        body_summary=body_summary,
        body_text=str(inbound.body or ""),
        attachments=list(attachments),
        attachment_summaries=list(attachment_summaries),
        account_email="dry-run@example.com",
        telegram_chat_id="dry-run-chat",
        telegram_bot_token="dry-run-token",
    )
    with _suppress_render_side_effects() as emitted:
        render = render_email_notification(request)
    return DryRunResult(
        fixture_path=path,
        sender_email=sender_email or str(inbound.sender or ""),
        sender_name=sender_name,
        subject=str(inbound.subject or ""),
        received_at=received_at,
        inbound_body=str(inbound.body or ""),
        attachments=attachments,
        stage_order=("parse_raw_email", *artifacts.stage_order, "render_email_notification"),
        artifacts=artifacts,
        render=render,
        suppressed_events=tuple(emitted),
    )


def _write_stdout(text: str) -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        sys.stdout.write(text)
        if not text.endswith("\n"):
            sys.stdout.write("\n")
    except UnicodeEncodeError:
        sys.stdout.buffer.write(text.encode("utf-8", errors="replace"))
        if not text.endswith("\n"):
            sys.stdout.buffer.write(b"\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run an offline dry-run for a fixture email.")
    parser.add_argument("--fixture", required=True, help="Path to the .eml fixture.")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON with canonical pipeline stages.",
    )
    args = parser.parse_args(argv)
    result = run_dry_run_fixture(args.fixture)
    if args.json:
        _write_stdout(
            json.dumps(
                result.to_dict(),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                default=str,
            )
        )
    else:
        _write_stdout(result.render.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["DryRunResult", "run_dry_run_fixture", "main"]
