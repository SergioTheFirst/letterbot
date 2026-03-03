from __future__ import annotations

import argparse
import base64
import csv
import configparser
import hmac
import html
import io
import mimetypes
import ipaddress
import json
import logging
import math
import os
import re
import secrets
import sqlite3
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Optional

try:
    from flask import (
        Flask,
        Response,
        jsonify,
        redirect,
        render_template,
        request,
        session,
        url_for,
    )
    USING_FLASK_STUB = False
except ModuleNotFoundError:
    from mailbot_v26.web_observability.flask_stub import (
        Flask,
        Response,
        jsonify,
        redirect,
        render_template,
        request,
        session,
        url_for,
    )

    USING_FLASK_STUB = True

from mailbot_v26.config_loader import (
    CONFIG_DIR,
    load_storage_config,
    load_web_config,
    load_web_ui_password_from_ini,
)
from mailbot_v26.config.paths import resolve_config_paths
from mailbot_v26.deps import DependencyError, require_runtime_for
from mailbot_v26.config_yaml import (
    ConfigError as YamlConfigError,
    load_config as load_yaml_config,
    validate_config as validate_yaml_config,
    resolve_support_enabled,
)
from mailbot_v26.observability.calibration_report import compute_priority_calibration_report
from mailbot_v26.observability.decision_trace_store import load_latest_decision_traces
from mailbot_v26.observability.decision_trace_v1 import (
    from_canonical_json,
    get_default_decision_trace_emitter,
)
from mailbot_v26.observability.decision_trace_view import summaries_as_payload
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.version import get_version
from mailbot_v26.web_observability.doctor_export import build_diagnostics_zip

logger = logging.getLogger(__name__)

ALLOWED_WINDOWS = {7, 30, 90}
ALLOWED_ARCHIVE_WINDOWS = {1, 7, 30, 90}
ARCHIVE_PAGE_SIZE = 50
COMMITMENTS_PAGE_SIZE = 50
ARCHIVE_STATUSES = {"any", "ok", "warn", "fail"}
COMMITMENT_STATUSES = {"open", "closed", "all"}
EVENTS_GROUP_PAGE_SIZE = 20
EVENT_FILTERS = {"all", "processing", "delivery", "health", "learning"}
WEB_EMAIL_REDACTED_PREVIEW = "Summary hidden"
WEB_EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
LANE_KEYS = ("all", "critical", "commitments", "deferred", "failures", "learning")
LANE_LABELS = {
    "all": "все",
    "critical": "критично",
    "commitments": "обязательства",
    "deferred": "отложено",
    "failures": "сбои",
    "learning": "обучение",
}
ALLOWED_ATTENTION_SORTS = {"time", "cost", "count"}


@dataclass(frozen=True)
class DashboardVars:
    account_emails: list[str]
    window_days: int
    limit: int
    pii: bool


@dataclass(frozen=True)
class WebUISettings:
    enabled: bool
    bind: str
    port: int
    password: str
    api_token: str
    allow_lan: bool
    allow_cidrs: list[str]
    prod_server: bool
    require_strong_password_on_lan: bool


@dataclass(frozen=True)
class SupportMethod:
    type: str
    label: str
    details: str
    phone: str
    number: str
    url: str
    qr_image: str
    qr_image_data_uri: str


@dataclass(frozen=True)
class SupportSettings:
    enabled: bool
    show_in_nav: bool
    methods: list[SupportMethod]
    text: str = ""


class _TTLCache:
    def __init__(self, ttl_seconds: float) -> None:
        self._ttl_seconds = max(0.0, ttl_seconds)
        self._items: dict[tuple[object, ...], tuple[float, object]] = {}

    def get(self, key: tuple[object, ...]) -> object | None:
        if not self._ttl_seconds:
            return None
        item = self._items.get(key)
        if not item:
            return None
        stored_at, value = item
        if time.monotonic() - stored_at > self._ttl_seconds:
            self._items.pop(key, None)
            return None
        return value

    def set(self, key: tuple[object, ...], value: object) -> None:
        if not self._ttl_seconds:
            return
        self._items[key] = (time.monotonic(), value)


STATUS_STRIP_REFRESH_MS = 10_000
HEALTH_REFRESH_MS = 30_000

_COCKPIT_CACHE = _TTLCache(10.0)
_HEALTH_SUMMARY_CACHE = _TTLCache(15.0)
_HEALTH_COMPONENT_CACHE = _TTLCache(30.0)
_HEALTH_INCIDENT_CACHE = _TTLCache(30.0)
_EVENTS_NARRATIVE_CACHE = _TTLCache(20.0)
_COMMITMENTS_CACHE = _TTLCache(20.0)
_COMMITMENTS_COUNT_CACHE = _TTLCache(15.0)
_ATTENTION_TOTALS_CACHE = _TTLCache(20.0)
_ATTENTION_TABLE_CACHE = _TTLCache(20.0)
_BUDGET_CACHE = _TTLCache(20.0)
_TRIAGE_LANES_CACHE = _TTLCache(20.0)
_DECISION_TRACE_CACHE = _TTLCache(20.0)
_DECISION_TRACE_HIST_CACHE = _TTLCache(30.0)


def _open_readonly_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        conn.execute("PRAGMA busy_timeout = 750")
        conn.execute("PRAGMA query_only = ON")
    except sqlite3.Error:
        conn.close()
        raise
    return conn


def _render_template(app: Flask, template_name: str, **context: object) -> str:
    context.setdefault("request", request)
    context.setdefault("session", session)
    support_settings = app.config.get("SUPPORT_SETTINGS")
    support_nav_enabled = bool(
        isinstance(support_settings, SupportSettings)
        and support_settings.enabled
        and support_settings.show_in_nav
    )
    donate_enabled = bool(app.config.get("DONATE_ENABLED", False))
    context.setdefault("support_nav_enabled", support_nav_enabled)
    context.setdefault("cfg", {"features": {"donate_enabled": donate_enabled}})
    context.setdefault("app_version", get_version())
    if USING_FLASK_STUB:
        template_path = Path(app.template_folder or "") / template_name
        return _render_stub_html(template_name, context, template_path)
    return render_template(template_name, **context)


def _render_stub_html(
    template_name: str, context: Mapping[str, object], template_path: Path
) -> str:
    dashboard_vars = context.get("dashboard_vars") if isinstance(context, Mapping) else None
    window_val = getattr(dashboard_vars, "window_days", 7) if dashboard_vars else 7
    limit_val = getattr(dashboard_vars, "limit", 25) if dashboard_vars else 25
    account_scope = ""
    if dashboard_vars and getattr(dashboard_vars, "account_emails", None):
        account_scope = ",".join(getattr(dashboard_vars, "account_emails"))
    account_email = html.escape(str(context.get("account_email") or ""))
    lane_value = html.escape(str(context.get("lane") or "")) if "lane" in context else ""
    share_url = html.escape(str(context.get("share_url") or "")) if "share_url" in context else ""

    def _options(values: list[int], selected: int) -> str:
        opts: list[str] = []
        for value in values:
            sel = " selected" if value == selected else ""
            opts.append(f'<option value="{value}"{sel}>{value}</option>')
        if selected not in values:
            opts.append(f'<option value="{selected}" selected>{selected}</option>')
        return "".join(opts)

    account_hidden = (
        f'<input type="hidden" name="account_email" value="{account_email}">' if account_email else ""
    )
    lane_hidden = f'<input type="hidden" name="lane" value="{lane_value}">' if lane_value else ""
    header = (
        "<div class=\"dashboard-vars\">"
        f'<input id="account_emails" name="account_emails" value="{html.escape(account_scope)}">'
        f'<select name="window_days">{_options([7, 30, 90], int(window_val))}</select>'
        f'<select name="limit">{_options([10, 25, 50], int(limit_val))}</select>'
        f"{account_hidden}"
        f"{lane_hidden}"
        f'<button id="copy-share-link" data-share-url="{share_url}">Copy share link</button>'
        "</div>"
    )

    if template_name in {"bridge.html", "cockpit.html"}:
        activity_rows = context.get("activity_rows") if isinstance(context, Mapping) else []
        digest_today = context.get("digest_today") if isinstance(context, Mapping) else []
        digest_week = context.get("digest_week") if isinstance(context, Mapping) else []
        engineer_mode = bool(context.get("engineer_mode")) if isinstance(context, Mapping) else False
        lane_pills = context.get("lane_pills") if isinstance(context, Mapping) else []
        lane_html = ""
        if isinstance(lane_pills, list) and lane_pills:
            lane_html = "<div class=\"lane-pills\">" + "".join(
                (
                    "<a class=\"lane-pill {active}\" href=\"{url}\">"
                    "{label} {count}</a>"
                ).format(
                    active="active" if pill.get("active") else "",
                    url=html.escape(str(pill.get("url") or "")),
                    label=html.escape(str(pill.get("label") or "")),
                    count=html.escape(str(pill.get("count") or "")),
                )
                for pill in lane_pills
            ) + "</div>"
        activity_body = "".join(
            """
            <tr>
              <td>{delivered}</td><td>{e2e}</td><td>{from_label}</td><td>{to_label}</td>
              <td>{preview}</td><td>{status}</td><td>{mode}</td>
            </tr>
            """.format(
                delivered=html.escape(row.get("delivered", "")),
                e2e=html.escape(str(row.get("e2e") or "")),
                from_label=html.escape(str(row.get("from_label") or "")),
                to_label=html.escape(str(row.get("to_label") or "")),
                preview=html.escape(str(row.get("telegram_preview") or "")),
                status=html.escape(str(row.get("status") or "")),
                mode=html.escape(str(row.get("mode") or "")),
            )
            for row in (activity_rows or [])
        )
        digest_today_block = "".join(
            f"<li>{html.escape(str(item.get('title') or ''))} {html.escape(str(item.get('time') or ''))}</li>"
            for item in (digest_today or [])
        ) or "<div class=\"hint\">Adjust filters to view today's highlights.</div>"
        digest_week_block = "".join(
            f"<li>{html.escape(str(item.get('title') or ''))} {html.escape(str(item.get('time') or ''))}</li>"
            for item in (digest_week or [])
        ) or "<div class=\"hint\">Expand window to see weekly digest.</div>"
        engineer_block = (
            "<div data-testid=\"engineer-blocks\"><details><summary>Engineer</summary></details></div>"
            if engineer_mode
            else ""
        )
        support_card = ""
        support_enabled = bool(context.get("support_nav_enabled")) if isinstance(context, Mapping) else False
        if support_enabled:
            support_preview = ""
            methods = context.get("support_methods") if isinstance(context, Mapping) else []
            if isinstance(methods, list) and methods:
                qr_uri = str(getattr(methods[0], "qr_image_data_uri", "") or "")
                if qr_uri:
                    support_preview = '<img alt="Support QR" src="%s">' % html.escape(qr_uri)
            support_card = f"<h2>Support Letterbot</h2><a href=\"/support\">/support</a>{support_preview}"
        return (
            f"<html><body>{header}<h2>Today Digest</h2>{digest_today_block}<h2>Week Digest</h2>{digest_week_block}"
            f"<h2>Recent Activity</h2>{lane_html}{engineer_block}{support_card}<table>{activity_body}</table></body></html>"
        )

    if template_name in {"health.html", "partials/health_overview.html"}:
        component_matrix = context.get("component_matrix") if isinstance(context, Mapping) else []
        incidents = context.get("incidents") if isinstance(context, Mapping) else []
        engineer_mode = bool(context.get("engineer_mode")) if isinstance(context, Mapping) else False
        health_trend = context.get("health_trend") if isinstance(context, Mapping) else []
        component_rows = "".join(
            """
            <tr data-testid="component-row">
              <td>{component}</td><td>{status}</td><td>{last_check}</td><td>{last_issue}</td>
            </tr>
            """.format(
                component=html.escape(str(row.get("component") or "")),
                status=html.escape(str(row.get("status") or "")),
                last_check=html.escape(str(row.get("last_check") or "")),
                last_issue=html.escape(str(row.get("last_issue") or "")),
            )
            for row in (component_matrix or [])
        )
        incident_rows = "".join(
            """
            <tr data-testid="incident-row">
              <td>{ts}</td><td>{component}</td><td>{symptom}</td><td>{outcome}</td>
            </tr>
            """.format(
                ts=html.escape(str(row.get("ts") or "")),
                component=html.escape(str(row.get("component") or "")),
                symptom=html.escape(str(row.get("symptom") or "")),
                outcome=html.escape(str(row.get("outcome") or "")),
            )
            for row in (incidents or [])
        )
        trend_rows = "".join(
            """
            <tr data-testid="health-trend-row" data-snapshot="{snapshot}">
              <td>{ts}</td><td>{mode}</td><td>{gates}</td><td>{metrics}</td><td>{snapshot_short}</td>
            </tr>
            """.format(
                ts=html.escape(str(row.get("ts") or "")),
                mode=html.escape(str(row.get("mode") or "")),
                gates=html.escape(str(row.get("gates") or "")),
                metrics=html.escape(str(row.get("metrics") or "")),
                snapshot=html.escape(str(row.get("snapshot") or "")),
                snapshot_short=html.escape(str(row.get("snapshot_short") or "")),
            )
            for row in (health_trend or [])
        )
        engineer_block = (
            '<div data-testid="health-engineer-block"></div>' if engineer_mode else ""
        )
        html_body = (
            f"{header}<div data-testid=\"health-component-matrix\">Component matrix</div>"
            f"<table>{component_rows}</table><table>{incident_rows}</table>"
            f"<table>{trend_rows}</table>{engineer_block}"
        )
        if template_name == "partials/health_overview.html":
            return html_body
        return f"<html><body>{html_body}</body></html>"

    if template_name == "archive.html":
        archive_rows = context.get("archive_rows") if isinstance(context, Mapping) else []
        engineer_mode = bool(context.get("engineer_mode")) if isinstance(context, Mapping) else False
        rows = []
        for row in archive_rows or []:
            extra_cols = ""
            if engineer_mode:
                extra_cols = (
                    f"<td>{html.escape(str(row.get('delivery_mode') or ''))}</td>"
                    f"<td>{html.escape(str(row.get('failure_reason') or ''))}</td>"
                    f"<td>{html.escape(str(row.get('stage_hint') or ''))}</td>"
                )
            rows.append(
                """
                <tr data-email-id="{email_id}">
                  <td>{received}</td><td>{from_label}</td><td>{account_label}</td>
                  <td>{preview}</td><td>{status}</td><td>{latency}</td>{extra_cols}
                </tr>
                """.format(
                    email_id=html.escape(str(row.get("email_id") or "")),
                    received=html.escape(str(row.get("received") or "")),
                    from_label=html.escape(str(row.get("from_label") or "")),
                    account_label=html.escape(str(row.get("account_label") or "")),
                    preview=html.escape(str(row.get("preview") or "")),
                    status=html.escape(str(row.get("status") or "")),
                    latency=html.escape(str(row.get("e2e_ms") or "")),
                    extra_cols=extra_cols,
                )
            )
        header_row = (
            "<tr><th>Time (UTC)</th><th>From</th><th>Account</th>"
            "<th>Preview</th><th>TG status</th><th>E2E latency</th></tr>"
        )
        return (
            f"<html><body>{header}<table>{header_row}{''.join(rows)}</table></body></html>"
        )

    if template_name == "commitments.html":
        commitments_rows = context.get("commitments_rows") if isinstance(context, Mapping) else []
        rows = []
        for row in commitments_rows or []:
            rows.append(
                """
                <tr data-commitment-id="{commitment_id}">
                  <td>{last_activity}</td><td>{counterparty}</td><td>{account}</td>
                  <td>{kind}</td><td>{due}</td><td>{evidence}</td><td>{forensics}</td>
                </tr>
                """.format(
                    commitment_id=html.escape(str(row.get("commitment_id") or "")),
                    last_activity=html.escape(str(row.get("last_activity") or "")),
                    counterparty=html.escape(str(row.get("counterparty_label") or "")),
                    account=html.escape(str(row.get("account_label") or "")),
                    kind=html.escape(str(row.get("kind") or "")),
                    due=html.escape(str(row.get("due_signal") or "")),
                    evidence=html.escape(str(row.get("evidence_count") or "")),
                    forensics=html.escape(str(row.get("forensics_url") or "")),
                )
            )
        header_row = (
            "<tr><th>Last activity (UTC)</th><th>Counterparty</th><th>Account</th>"
            "<th>Kind / Status</th><th>Age / Due</th><th>Evidence</th><th>Forensics</th></tr>"
        )
        return (
            f"<html><body>{header}<table>{header_row}{''.join(rows)}</table></body></html>"
        )

    if template_name == "email_detail.html":
        timeline_rows = context.get("timeline_rows") if isinstance(context, Mapping) else []
        evidence_rows = context.get("evidence_rows") if isinstance(context, Mapping) else []
        engineer_mode = bool(context.get("engineer_mode")) if isinstance(context, Mapping) else False
        rows = []
        for row in timeline_rows or []:
            extra_cols = ""
            if engineer_mode:
                extra_cols = f"<td>{html.escape(str(row.get('error_code') or ''))}</td>"
            rows.append(
                """
                <tr data-stage="{stage}" data-span-id="{span_id}">
                  <td>{ts}</td><td>{stage}</td><td>{duration}</td><td>{outcome}</td>{extra_cols}
                </tr>
                """.format(
                    stage=html.escape(str(row.get("stage") or "")),
                    span_id=html.escape(str(row.get("span_id") or "")),
                    ts=html.escape(str(row.get("ts") or "")),
                    duration=html.escape(str(row.get("duration_ms") or "")),
                    outcome=html.escape(str(row.get("outcome") or "")),
                    extra_cols=extra_cols,
                )
        )
        evidence = []
        for row in evidence_rows or []:
            evidence.append(
                """
                <tr data-evidence-id="{evidence_id}">
                  <td>{ts}</td><td>{kind}</td><td>{event_type}</td><td>{stage}</td><td>{duration}</td>
                </tr>
                """.format(
                    evidence_id=html.escape(str(row.get("id") or "")),
                    ts=html.escape(str(row.get("ts") or "")),
                    kind=html.escape(str(row.get("kind") or "")),
                    event_type=html.escape(str(row.get("event_type") or "")),
                    stage=html.escape(str(row.get("stage") or "")),
                    duration=html.escape(str(row.get("duration") or "")),
                )
            )
        return (
            f"<html><body>{header}<table>{''.join(evidence)}</table><table>{''.join(rows)}</table></body></html>"
        )

    if template_name == "support.html":
        methods = context.get("support_methods") if isinstance(context, Mapping) else []
        rows: list[str] = []
        for method in methods or []:
            label = html.escape(str(getattr(method, "label", "") or ""))
            phone = html.escape(str(getattr(method, "phone", "") or ""))
            number = html.escape(str(getattr(method, "number", "") or ""))
            url = html.escape(str(getattr(method, "url", "") or ""))
            details = html.escape(str(getattr(method, "details", "") or ""))
            rows.append(
                f'<div class="support-method"><h3>{label}</h3><p>{details}</p><p>{phone}</p><p>{number}</p><p>{url}</p><button>Скопировать</button></div>'
            )
        return f"<html><body>{header}{''.join(rows)}</body></html>"

    if template_name == "events.html":
        groups = context.get("groups") if isinstance(context, Mapping) else []
        blocks = []
        for group in groups or []:
            if not isinstance(group, Mapping):
                continue
            group_kind = group.get("group_kind")
            headline = group.get("headline") if isinstance(group.get("headline"), Mapping) else {}
            if group_kind == "email":
                label = f"Email {group.get('group_id')}"
            else:
                label = str(headline.get("label") or group.get("group_id") or "")
            blocks.append(f"<div class=\"event-group\">{html.escape(label)}</div>")
        body = "".join(blocks) if blocks else "<div class=\"hint\">Adjust window_days or account scope.</div>"
        return f"<html><body>{header}{body}</body></html>"

    if template_name == "attention.html":
        summary = context.get("summary") if isinstance(context, Mapping) else {}
        entities = summary.get("entities") if isinstance(summary, Mapping) else []
        rows = "".join(
            """
            <tr>
              <td>{label}</td><td>{count}</td><td>{minutes}</td><td>{cost}</td><td>{signals}</td>
            </tr>
            """.format(
                label=html.escape(str(row.get("entity_label") or "")),
                count=html.escape(str(row.get("message_count") or 0)),
                minutes=html.escape(str(row.get("estimated_read_minutes") or 0.0)),
                cost=html.escape(str(row.get("estimated_cost") or "")),
                signals=html.escape(str(row.get("signals") or "")),
            )
            for row in (entities or [])
            if isinstance(row, Mapping)
        )
        table = (
            "<table class=\"table compact fixed attention-table\">"
            f"<tbody>{rows}</tbody></table>"
        )
        return f"<html><body>{header}{table}</body></html>"

    return render_template(str(template_path), **context)


def _static_url() -> str:
    return url_for("static", filename="style.css") if not USING_FLASK_STUB else "/static/style.css"


def _build_select_options(values: list[str], selected: str | None) -> str:
    options = []
    for value in values:
        escaped = html.escape(value)
        is_selected = " selected" if value == selected else ""
        options.append(f"<option value=\"{escaped}\"{is_selected}>{escaped}</option>")
    return "".join(options)


def _build_window_options(selected: int | None) -> str:
    options = []
    for value in [7, 30, 90]:
        is_selected = " selected" if value == selected else ""
        options.append(f"<option value=\"{value}\"{is_selected}>Last {value} days</option>")
    return "".join(options)


def _parse_event_filter(raw: Optional[str]) -> tuple[str, Optional[str]]:
    if raw is None or raw == "":
        return "all", None
    cleaned = str(raw).strip().lower()
    if cleaned in EVENT_FILTERS:
        return cleaned, None
    return "all", f"type must be one of {', '.join(sorted(EVENT_FILTERS))}"


def _parse_lane(raw: Optional[str]) -> str:
    cleaned = str(raw or "all").strip().lower()
    if cleaned in LANE_KEYS:
        return cleaned
    return "all"


def _build_lane_pills(
    *,
    selected_lane: str,
    counts: Mapping[str, int],
    base_params: Mapping[str, str],
    endpoint: str,
) -> list[dict[str, object]]:
    pills: list[dict[str, object]] = []
    for lane_key in LANE_KEYS:
        params = dict(base_params)
        params["lane"] = lane_key
        pills.append(
            {
                "key": lane_key,
                "label": LANE_LABELS.get(lane_key, lane_key),
                "count": int(counts.get(lane_key, 0) or 0),
                "url": url_for(endpoint, **params),
                "active": lane_key == selected_lane,
            }
        )
    return pills


def _parse_page(raw: Optional[str], *, default: int = 1) -> int:
    try:
        value = int(raw or default)
    except (TypeError, ValueError):
        return default
    return max(1, value)


def _build_archive_window_options(selected: int | None) -> str:
    options = []
    for value in [1, 7, 30, 90]:
        is_selected = " selected" if value == selected else ""
        options.append(f"<option value=\"{value}\"{is_selected}>{value} days</option>")
    return "".join(options)


def _format_number(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "–"
    if number.is_integer():
        return str(int(number))
    return f"{number:.2f}"


def _format_duration_ms(value: object) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return "–"
    return f"{_format_number(numeric)} ms"


def _safe_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_percent(value: object) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return "–"
    return _format_number(numeric * 100)


def _format_bytes(value: object) -> str:
    try:
        size = float(value)
    except (TypeError, ValueError):
        return "–"
    if size < 0:
        return "–"
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024.0:
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}{unit}"
        size /= 1024.0
    return f"{size:.1f}TB"


def _format_ts_utc(value: object) -> str:
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return "–"
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return "–"
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_due_signal(*, created_ts: float | None, deadline_iso: str | None) -> str:
    now = datetime.now(timezone.utc)
    if deadline_iso:
        try:
            deadline = datetime.fromisoformat(deadline_iso).replace(tzinfo=timezone.utc)
        except ValueError:
            deadline = None
        if deadline:
            delta = deadline - now
            days = int(delta.total_seconds() // 86400)
            if days < 0:
                return f"Overdue {-days}d"
            if days == 0:
                return "Due today"
            return f"Due in {days}d"
    if created_ts is None:
        return "–"
    try:
        created = datetime.fromtimestamp(float(created_ts), tz=timezone.utc)
    except (OverflowError, OSError, ValueError, TypeError):
        return "–"
    delta = now - created
    days = max(0, int(delta.total_seconds() // 86400))
    return f"Age {days}d"


def _summarize_mapping(data: Mapping[str, object] | None, *, limit: int = 4) -> str:
    if not data:
        return "–"
    items: list[str] = []
    for key in sorted(data.keys()):
        if len(items) >= max(1, limit):
            break
        value = data.get(key)
        value_str = html.escape(str(value)) if value is not None else "–"
        items.append(f"{html.escape(str(key))}: {value_str}")
    return "; ".join(items) if items else "–"


def _short_id(value: object, length: int = 8) -> str:
    if not value:
        return ""
    text = str(value)
    return text[:length]


def _events_table(items: list[dict[str, object]]) -> str:
    if not items:
        return ""
    rows = []

    def _detail_badges(details: Mapping[str, object] | None) -> str:
        if not details:
            return ""
        badges: list[str] = []
        for key in sorted(details.keys()):
            if len(badges) >= 3:
                break
            value = details.get(key)
            badges.append(
                f"<span class=\"badge\">{html.escape(str(key))}: {html.escape(str(value))}</span>"
            )
        return " ".join(badges)

    for item in items:
        details_badges = _detail_badges(item.get("details") if isinstance(item, Mapping) else {})
        rows.append(
            """
            <tr>
              <td>{}</td>
              <td><span class="badge">{}</span></td>
              <td>{}</td>
              <td>{}</td>
              <td>{}</td>
              <td>{}</td>
            </tr>
            """.format(
                html.escape(_format_ts_utc(item.get("ts_utc"))),
                html.escape(str(item.get("event_type") or "")),
                html.escape(_short_id(item.get("email_id"), 12)),
                html.escape(_short_id(item.get("entity_id"), 12)),
                html.escape(str(item.get("summary") or "")),
                details_badges,
            )
        )
    return (
        "<table class=\"data-table\">"
        "<thead><tr><th>Timestamp (UTC)</th><th>Type</th><th>Email ID</th><th>Entity ID</th><th>Summary</th><th>Details</th></tr></thead>"
        + f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _build_activity_table_rows(activity_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    if not activity_rows:
        return []
    table_rows: list[dict[str, object]] = []
    for row in activity_rows:
        delivered_ts = row.get("delivered_ts_utc") or row.get("received_ts_utc")
        e2e_value = row.get("e2e_seconds")
        status_text = str(row.get("status") or "")
        status_class = "muted"
        if status_text.lower() == "delivered":
            status_class = "success"
        elif status_text.lower() == "failed":
            status_class = "danger"
        table_rows.append(
            {
                "delivered": _format_ts_utc(delivered_ts) if delivered_ts else "",
                "e2e": _format_number(e2e_value) if e2e_value is not None else "",
                "from_label": _sanitize_sender_label(row.get("from_label")),
                "to_label": _sanitize_account_label(row.get("to_label")),
                "telegram_preview": _sanitize_email_preview(row.get("telegram_preview")),
                "status": status_text or "",
                "status_class": status_class,
                "mode": row.get("delivery_mode") or "",
            }
        )
    return table_rows


def _summarize_digest_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    if not rows:
        return []
    digest_items: list[dict[str, object]] = []
    for row in rows:
        delivered_ts = row.get("delivered_ts_utc") or row.get("received_ts_utc")
        digest_items.append(
            {
                "title": _sanitize_email_preview(row.get("telegram_preview")),
                "from_label": _sanitize_sender_label(row.get("from_label")),
                "status": row.get("status") or "",
                "time": _format_ts_utc(delivered_ts) if delivered_ts else "",
            }
        )
    return digest_items


def _mask_email_address(value: object) -> str:
    if not value:
        return ""
    text = str(value).strip()
    if "@" not in text:
        return ""
    local, _, domain = text.partition("@")
    if not domain:
        return ""
    first = local[0] if local else ""
    return f"{first}…@{domain}"


def _mask_account_emails(values: Iterable[str]) -> list[str]:
    return [_mask_email_address(value) for value in values if value]


def _sanitize_email_label(value: object, *, fallback: str) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if "…@" in text:
        return text
    match = WEB_EMAIL_PATTERN.search(text)
    if not match:
        return fallback
    return _mask_email_address(match.group(0))


def _sanitize_sender_label(value: object) -> str:
    return _sanitize_email_label(value, fallback="Sender hidden") if value else ""


def _sanitize_account_label(value: object) -> str:
    return _sanitize_email_label(value, fallback="Account hidden") if value else ""


def _sanitize_email_preview(value: object) -> str:
    if not value:
        return ""
    return WEB_EMAIL_REDACTED_PREVIEW


def _sanitize_archive_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        **row,
        "from_label": _sanitize_sender_label(row.get("from_label")),
        "account_label": _sanitize_account_label(row.get("account_label")),
        "preview": _sanitize_email_preview(row.get("preview")),
    }


def _sanitize_event_headline(headline: Mapping[str, object] | None) -> dict[str, object]:
    if not headline:
        return {}
    sanitized = dict(headline)
    sanitized["from_masked"] = _sanitize_sender_label(headline.get("from_masked"))
    sanitized["to_masked"] = _sanitize_account_label(headline.get("to_masked"))
    sanitized["preview_masked"] = _sanitize_email_preview(headline.get("preview_masked"))
    return sanitized


def _health_current_block(current: dict[str, object] | None) -> str:
    if not current:
        return ""
    system_mode = html.escape(str(current.get("system_mode") or ""))
    gates_state = current.get("gates_state") if isinstance(current, dict) else {}
    metrics_brief = current.get("metrics_brief") if isinstance(current, dict) else {}
    rows = []
    gates_summary = _summarize_mapping(gates_state)
    metrics_summary = _summarize_mapping(metrics_brief)
    rows.append(
        """
        <div class="metric"><div class="label">System mode</div><div class="value"><span class="badge">{}</span></div></div>
        """.format(system_mode or "–")
    )
    rows.append(
        """
        <div class="metric"><div class="label">Last check (UTC)</div><div class="value">{}</div></div>
        """.format(_format_ts_utc(current.get("ts_end_utc")))
    )
    rows.append(
        """
        <div class="metric"><div class="label">Gates</div><div class="value">{}</div></div>
        """.format(gates_summary)
    )
    rows.append(
        """
        <div class="metric"><div class="label">Metrics</div><div class="value">{}</div></div>
        """.format(metrics_summary)
    )
    return f"<div class=\"metrics-grid\">{''.join(rows)}</div>"


def _health_timeline_block(timeline: list[dict[str, object]]) -> str:
    if not timeline:
        return ""
    rows = []
    for item in timeline:
        rows.append(
            """
            <tr>
              <td>{}</td>
              <td><span class="badge">{}</span></td>
              <td>{}</td>
              <td>{}</td>
              <td>{}</td>
            </tr>
            """.format(
                html.escape(_format_ts_utc(item.get("ts_end_utc"))),
                html.escape(str(item.get("system_mode") or "")),
                _summarize_mapping(item.get("gates_state"), limit=3),
                _summarize_mapping(item.get("metrics_brief"), limit=3),
                html.escape(_short_id(item.get("snapshot_id"), 10)),
            )
        )
    return (
        "<table class=\"data-table\">"
        "<thead><tr><th>Timestamp (UTC)</th><th>Mode</th><th>Gates</th><th>Metrics</th><th>Snapshot</th></tr></thead>"
        + f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _load_web_ui_settings(config_path: Path | None) -> WebUISettings:
    def _defaults() -> WebUISettings:
        return WebUISettings(
            enabled=True,
            bind="127.0.0.1",
            port=8787,
            password="",
            api_token="",
            allow_lan=False,
            allow_cidrs=[],
            prod_server=False,
            require_strong_password_on_lan=True,
        )

    if config_path is None or not config_path.exists():
        logger.warning("config.yaml missing for web UI; using deterministic defaults")
        return _defaults()
    try:
        raw = load_yaml_config(config_path)
    except (FileNotFoundError, YamlConfigError) as exc:
        logger.warning("config.yaml load failed for web UI; using defaults: %s", str(exc))
        return _defaults()
    ok, error = validate_yaml_config(raw)
    if not ok:
        logger.warning(
            "config.yaml validation failed for web UI; using defaults: %s",
            error or "Invalid config.yaml",
        )
        return _defaults()
    web_ui = raw.get("web_ui")
    if not isinstance(web_ui, dict):
        logger.warning("config.yaml missing web_ui section; using defaults")
        return _defaults()
    enabled = bool(web_ui.get("enabled", False))
    bind = str(web_ui.get("bind", "127.0.0.1")).strip()
    port = int(web_ui.get("port", 8080))
    password = str(web_ui.get("password", "")).strip()
    api_token = str(web_ui.get("api_token", "") or "").strip()
    allow_lan = bool(web_ui.get("allow_lan", False))
    allow_cidrs = web_ui.get("allow_cidrs") or []
    prod_server = bool(web_ui.get("prod_server", False))
    require_strong_password_on_lan = bool(web_ui.get("require_strong_password_on_lan", True))
    if not isinstance(allow_cidrs, list):
        allow_cidrs = []
    return WebUISettings(
        enabled=enabled,
        bind=bind,
        port=port,
        password=password,
        api_token=api_token,
        allow_lan=allow_lan,
        allow_cidrs=[str(item) for item in allow_cidrs],
        prod_server=prod_server,
        require_strong_password_on_lan=require_strong_password_on_lan,
    )


def _image_data_uri(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    mime_type, _ = mimetypes.guess_type(str(path))
    if not mime_type or not mime_type.startswith("image/"):
        mime_type = "image/png"
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _load_support_settings(config_path: Path | None) -> SupportSettings:
    def _from_ini(settings_path: Path) -> SupportSettings:
        parser = configparser.ConfigParser()
        if not settings_path.exists():
            return SupportSettings(enabled=False, show_in_nav=False, methods=[])
        try:
            parser.read(settings_path, encoding="utf-8")
        except (OSError, configparser.Error) as exc:
            logger.warning("support_settings_ini_load_failed", path=str(settings_path), error=str(exc))
            return SupportSettings(enabled=False, show_in_nav=False, methods=[])
        if not parser.has_section("support"):
            return SupportSettings(enabled=False, show_in_nav=False, methods=[])

        enabled = parser.getboolean("support", "enabled", fallback=False)
        show_in_nav = parser.getboolean("support", "show_in_nav", fallback=True)
        label = parser.get("support", "label", fallback="Support Letterbot").strip()
        text = parser.get("support", "text", fallback="").strip()
        details = parser.get("support", "details", fallback="").strip()
        url = parser.get("support", "url", fallback="").strip()
        qr_rel = parser.get("support", "qr_image", fallback="").strip()
        qr_uri = ""
        if qr_rel:
            qr_path = (settings_path.parent / qr_rel).resolve()
            try:
                qr_uri = _image_data_uri(qr_path)
            except Exception as exc:
                logger.warning("support_qr_load_failed", path=str(qr_path), error=str(exc))
                qr_uri = ""
        method = SupportMethod(
            type="support",
            label=label,
            details=details,
            phone="",
            number="",
            url=url,
            qr_image=qr_rel,
            qr_image_data_uri=qr_uri,
        )
        methods = [method] if enabled else []
        return SupportSettings(enabled=enabled, show_in_nav=show_in_nav, methods=methods, text=text)

    if config_path is None:
        return SupportSettings(enabled=False, show_in_nav=False, methods=[])
    settings_path = config_path.parent / "settings.ini"
    if not config_path.exists():
        return _from_ini(settings_path)
    try:
        raw = load_yaml_config(config_path)
    except Exception as exc:
        logger.warning("support_config_load_failed", error=str(exc))
        return _from_ini(settings_path)
    if not isinstance(raw, dict):
        return _from_ini(settings_path)
    if not resolve_support_enabled(raw):
        return _from_ini(settings_path)
    support = raw.get("support")
    if not isinstance(support, dict):
        return _from_ini(settings_path)
    enabled = bool(support.get("enabled", False))
    ui = support.get("ui") if isinstance(support.get("ui"), dict) else {}
    show_in_nav = bool(ui.get("show_in_nav", False))
    methods_raw = support.get("methods") if isinstance(support.get("methods"), list) else []
    if not enabled or not methods_raw:
        return _from_ini(settings_path)
    methods: list[SupportMethod] = []
    base_dir = config_path.parent
    for item in methods_raw:
        if not isinstance(item, dict):
            continue
        qr_rel = str(item.get("qr_image", "") or "").strip()
        qr_uri = ""
        if qr_rel:
            qr_path = (base_dir / qr_rel).resolve()
            try:
                qr_uri = _image_data_uri(qr_path)
            except Exception as exc:
                logger.warning("support_qr_load_failed", path=str(qr_path), error=str(exc))
                qr_uri = ""
        methods.append(
            SupportMethod(
                type=str(item.get("type", "") or "").strip(),
                label=str(item.get("label", "") or "").strip(),
                details=str(item.get("details", "") or "").strip(),
                phone=str(item.get("phone", "") or "").strip(),
                number=str(item.get("number", "") or "").strip(),
                url=str(item.get("url", "") or "").strip(),
                qr_image=qr_rel,
                qr_image_data_uri=qr_uri,
            )
        )
    return SupportSettings(enabled=enabled, show_in_nav=show_in_nav, methods=methods)


def _load_web_ui_secrets(config_dir: Path) -> tuple[str, float]:
    parser = configparser.ConfigParser()
    config_path = config_dir / "config.ini"
    if config_path.exists():
        try:
            parser.read(config_path, encoding="utf-8")
        except (OSError, configparser.Error):
            logger.warning("Failed to read config.ini from %s", config_path)
    secret_key = os.environ.get("WEB_SECRET_KEY") or parser.get(
        "general", "web_secret_key", fallback=""
    )
    if not secret_key:
        logger.warning(
            "WEB_SECRET_KEY missing; using deterministic local fallback key"
        )
        secret_key = "letterbot-local-web-secret"
    try:
        attention_cost = float(
            parser.get("general", "attention_cost_per_hour", fallback="0")
        )
    except (TypeError, ValueError, configparser.Error):
        attention_cost = 0.0
    attention_cost = max(0.0, attention_cost)
    return secret_key, attention_cost


def _parse_account_emails(raw: str | None) -> list[str]:
    if not raw:
        return []
    seen: set[str] = set()
    emails: list[str] = []
    for item in raw.split(","):
        trimmed = item.strip()
        if not trimmed:
            continue
        if trimmed in seen:
            continue
        seen.add(trimmed)
        emails.append(trimmed)
    return emails


def _parse_archive_status(raw: Optional[str]) -> tuple[str, Optional[str]]:
    if raw is None or raw == "":
        return "any", None
    cleaned = str(raw).strip().lower()
    if cleaned in ARCHIVE_STATUSES:
        return cleaned, None
    return "any", "status must be one of any, ok, warn, fail"


def _parse_commitment_status(raw: Optional[str]) -> tuple[str, Optional[str]]:
    if raw is None or raw == "":
        return "open", None
    cleaned = str(raw).strip().lower()
    if cleaned in COMMITMENT_STATUSES:
        return cleaned, None
    return "open", "status must be one of open, closed, all"


def _parse_window_days(
    raw: Optional[str], default: int = 7, allowed: set[int] | None = None
) -> tuple[Optional[int], Optional[str]]:
    if raw is None or raw == "":
        if allowed is not None and default not in allowed:
            return None, f"window_days must be one of {', '.join(map(str, sorted(allowed)))}"
        if default < 1 or default > 365:
            return None, "window_days must be between 1 and 365"
        return default, None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None, "window_days must be an integer"
    if allowed is not None and value not in allowed:
        return None, f"window_days must be one of {', '.join(map(str, sorted(allowed)))}"
    if value < 1 or value > 365:
        return None, "window_days must be between 1 and 365"
    return value, None


def _parse_limit(
    raw: Optional[str],
    default: int = 200,
    max_limit: int = 500,
    min_value: int = 1,
) -> tuple[int | None, Optional[str]]:
    if raw is None or raw == "":
        return default, None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None, "limit must be an integer"
    if value < min_value:
        return None, f"limit must be >= {min_value}"
    if value > max_limit:
        return max_limit, None
    return value, None


def resolve_dashboard_vars(request, session, allow_pii: bool | None = None) -> DashboardVars:
    try:
        session_vars = session.get("dashboard_vars") or {}
    except Exception:
        session_vars = {}

    def _session_value(key: str) -> object:
        if isinstance(session_vars, Mapping):
            return session_vars.get(key)
        return None

    def _parse_accounts(raw: object) -> list[str]:
        return _parse_account_emails(str(raw)) if raw not in (None, "") else []

    query_accounts_raw = request.args.get("account_emails")
    accounts = _parse_accounts(query_accounts_raw)
    if not accounts:
        accounts = _parse_accounts(_session_value("account_emails"))

    def _int_in_range(raw_value: object, minimum: int, maximum: int, default: int) -> int:
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            return default
        return max(minimum, min(maximum, value))

    window_raw = request.args.get("window_days")
    window_days = None
    if window_raw is not None:
        window_days = _int_in_range(window_raw, 1, 365, 7)
    elif _session_value("window_days") is not None:
        window_days = _int_in_range(_session_value("window_days"), 1, 365, 7)
    else:
        window_days = 7

    limit_raw = request.args.get("limit")
    limit_value = None
    if limit_raw is not None:
        limit_value = _int_in_range(limit_raw, 1, 200, 25)
    elif _session_value("limit") is not None:
        limit_value = _int_in_range(_session_value("limit"), 1, 200, 25)
    else:
        limit_value = 25

    if allow_pii is None:
        allow_pii_flag = str(os.getenv("WEB_OBSERVABILITY_ALLOW_PII", "0")).lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
    else:
        allow_pii_flag = bool(allow_pii)

    pii_raw = request.args.get("pii") if allow_pii_flag else None
    pii_value = False
    if allow_pii_flag and pii_raw is not None:
        pii_value = str(pii_raw).lower() in {"1", "true", "yes", "on"}
    elif allow_pii_flag and _session_value("pii") is not None:
        pii_value = bool(_session_value("pii"))

    resolved = DashboardVars(
        account_emails=accounts,
        window_days=window_days,
        limit=limit_value,
        pii=pii_value if allow_pii_flag else False,
    )
    try:
        session["dashboard_vars"] = {
            "account_emails": resolved.account_emails,
            "window_days": resolved.window_days,
            "limit": resolved.limit,
            "pii": resolved.pii,
        }
    except Exception:
        logger.debug("Dashboard vars session persist skipped", exc_info=True)
    return resolved


def _resolve_cockpit_mode(request, session) -> str:
    try:
        session_vars = session.get("cockpit_mode")
    except Exception:
        session_vars = None
    raw_mode = request.args.get("mode")
    if raw_mode is None and session_vars:
        raw_mode = str(session_vars)
    mode = str(raw_mode or "basic").strip().lower()
    if mode == "owner":
        mode = "basic"
    if mode not in {"basic", "engineer"}:
        mode = "basic"
    try:
        session["cockpit_mode"] = mode
    except Exception:
        logger.debug("Cockpit mode session persist skipped", exc_info=True)
    return mode


def _status_from_value(value: object) -> tuple[str, str]:
    if value is None or value == "":
        return "unknown", "muted"
    if isinstance(value, bool):
        return ("ok", "success") if value else ("down", "danger")
    text = str(value).strip()
    if not text:
        return "unknown", "muted"
    lowered = text.lower()
    if lowered in {"ok", "open", "healthy", "ready", "true", "1", "up", "pass", "green"}:
        return "ok", "success"
    if lowered in {"warn", "warning", "degraded", "yellow"}:
        return "warn", "warn"
    if lowered in {"down", "fail", "failed", "error", "closed", "red", "false", "0"}:
        return "down", "danger"
    return text, "muted"


def _status_from_mapping(values: Mapping[str, object], *, keys: Iterable[str]) -> object | None:
    for key in keys:
        if key in values:
            return values.get(key)
    for candidate_key in sorted(values.keys(), key=lambda item: str(item).lower()):
        candidate_value = values.get(candidate_key)
        lowered = str(candidate_key).lower()
        for key in keys:
            if key in lowered:
                return candidate_value
    return None


def _status_strip_view(
    status_strip: Mapping[str, object] | None, *, now_ts: float
) -> dict[str, object]:
    status_strip = status_strip or {}
    system_mode = str(status_strip.get("system_mode") or "unknown")
    gates_state = status_strip.get("gates_state") if isinstance(status_strip, Mapping) else {}
    metrics_brief = status_strip.get("metrics_brief") if isinstance(status_strip, Mapping) else {}
    gates_state = gates_state if isinstance(gates_state, Mapping) else {}
    metrics_brief = metrics_brief if isinstance(metrics_brief, Mapping) else {}
    metrics_window = {}
    if metrics_brief:
        for window_key in sorted(metrics_brief.keys(), key=lambda item: str(item)):
            window_values = metrics_brief.get(window_key)
            if isinstance(window_values, Mapping):
                metrics_window = window_values
                break

    imap_value = _status_from_mapping(gates_state, keys=["imap", "mail", "inbox"])
    llm_failure_rate = None
    if isinstance(metrics_window, Mapping):
        llm_failure_rate = metrics_window.get("llm_failure_rate")
    tg_success_rate = None
    if isinstance(metrics_window, Mapping):
        tg_success_rate = metrics_window.get("telegram_delivery_success_rate")

    llm_status = "unknown"
    llm_class = "muted"
    llm_rate = _safe_float(llm_failure_rate)
    if llm_rate is not None:
        if llm_rate < 0.05:
            llm_status, llm_class = "ok", "success"
        elif llm_rate < 0.12:
            llm_status, llm_class = "warn", "warn"
        else:
            llm_status, llm_class = "down", "danger"

    tg_status = "unknown"
    tg_class = "muted"
    tg_rate = _safe_float(tg_success_rate)
    if tg_rate is not None:
        if tg_rate >= 0.98:
            tg_status, tg_class = "ok", "success"
        elif tg_rate >= 0.9:
            tg_status, tg_class = "warn", "warn"
        else:
            tg_status, tg_class = "down", "danger"

    db_size_bytes = status_strip.get("db_size_bytes")
    db_status = "unknown"
    db_class = "muted"
    if db_size_bytes is not None:
        db_status, db_class = "ok", "success"

    imap_status_text, imap_class = _status_from_value(imap_value)

    updated_ts = _safe_float(status_strip.get("updated_ts_utc"))
    updated_ago = "–"
    if updated_ts is not None:
        age = max(0, int(now_ts - updated_ts))
        updated_ago = f"{age}s ago"

    return {
        "system_mode": system_mode,
        "imap": {"text": imap_status_text, "class": imap_class},
        "llm": {"text": llm_status, "class": llm_class},
        "telegram": {"text": tg_status, "class": tg_class},
        "db": {"text": db_status, "class": db_class},
        "db_size": _format_bytes(db_size_bytes),
        "updated_ago": updated_ago,
    }


def _golden_signals_view(golden_signals: Mapping[str, object] | None) -> dict[str, str]:
    if not golden_signals:
        return {}
    latency_p50 = _format_number(golden_signals.get("latency_p50_ms"))
    latency_p95 = _format_number(golden_signals.get("latency_p95_ms"))
    error_rate = _format_percent(golden_signals.get("error_rate"))
    fallback_rate = _format_percent(golden_signals.get("fallback_rate"))
    tg_failure_rate = _format_percent(golden_signals.get("tg_failure_rate"))
    traffic_volume = _format_number(golden_signals.get("span_count"))
    db_size = _format_bytes(golden_signals.get("db_size_bytes"))
    saturation_parts = [part for part in [db_size, f"{traffic_volume} spans"] if part != "–"]
    saturation = " • ".join(saturation_parts) if saturation_parts else "–"
    return {
        "latency_p50": f"{latency_p50} ms" if latency_p50 != "–" else "–",
        "latency_p95": f"{latency_p95} ms" if latency_p95 != "–" else "–",
        "error_rate": f"{error_rate}%" if error_rate != "–" else "–",
        "fallback_rate": f"{fallback_rate}%" if fallback_rate != "–" else "–",
        "tg_failure_rate": f"{tg_failure_rate}%" if tg_failure_rate != "–" else "–",
        "traffic_volume": traffic_volume if traffic_volume != "–" else "–",
        "saturation": saturation,
    }


def _metrics_window(metrics_brief: Mapping[str, object] | None) -> Mapping[str, object]:
    if not metrics_brief:
        return {}
    if not isinstance(metrics_brief, Mapping):
        return {}
    for window_key in sorted(metrics_brief.keys(), key=lambda item: str(item)):
        window_values = metrics_brief.get(window_key)
        if isinstance(window_values, Mapping):
            return window_values
    return {}


def _status_class_for_mode(mode: str) -> str:
    normalized = mode.upper()
    if normalized == "FULL":
        return "success"
    if "EMERGENCY" in normalized:
        return "danger"
    if "DEGRADED" in normalized:
        return "warn"
    return "muted"


def _status_class_for_label(label: str) -> str:
    normalized = str(label or "").strip().lower()
    if normalized in {"ok", "delivered", "success", "full", "ready"}:
        return "success"
    if normalized in {"failed", "fail", "error", "down", "emergency"}:
        return "danger"
    if normalized in {"warn", "warning", "degraded", "in-flight", "pending"}:
        return "warn"
    return "muted"


def _health_mode_explanation(mode: str, gates_state: Mapping[str, object] | None) -> str:
    if not mode:
        return "Health snapshots unavailable for this scope."
    normalized = mode.upper()
    if normalized == "FULL":
        return "All core systems are operating within normal thresholds."
    if "EMERGENCY" in normalized:
        return "Emergency read-only mode is active; review incidents and gate failures."
    if "DEGRADED" in normalized:
        return "Operating in degraded mode with active safeguards."
    if gates_state:
        return "Health gate evaluation completed with mixed signals."
    return "Operating status captured; monitor for changes."


def _clamp_text(value: object, limit: int = 96) -> str:
    text = str(value or "").strip()
    if not text:
        return "–"
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"


def _health_golden_signals_view(
    *,
    summary: Mapping[str, object] | None,
    metrics_brief: Mapping[str, object] | None,
) -> dict[str, str]:
    summary = summary if isinstance(summary, Mapping) else {}
    metrics_window = _metrics_window(metrics_brief)
    tg_success = _format_percent(metrics_window.get("telegram_delivery_success_rate"))
    fallback_rate = _format_percent(summary.get("fallback_rate"))
    latency_p95 = _format_number(summary.get("total_duration_ms_p95"))
    return {
        "tg_success_rate": f"{tg_success}%" if tg_success != "–" else "–",
        "fallback_rate": f"{fallback_rate}%" if fallback_rate != "–" else "–",
        "latency_p95": f"{latency_p95} ms" if latency_p95 != "–" else "–",
    }


def _derive_incident_component(item: Mapping[str, object]) -> str:
    error_code = str(item.get("error_code") or "").lower()
    llm_provider = str(item.get("llm_provider") or "").lower()
    if "telegram" in error_code or error_code.startswith("tg_") or "tg_" in error_code:
        return "Telegram"
    if "imap" in error_code or "mail" in error_code:
        return "IMAP"
    if "sqlite" in error_code or "db" in error_code:
        return "DB"
    if llm_provider or "llm" in error_code:
        return "LLM"
    return "Pipeline"


def _health_incidents_view(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    if not rows:
        return []
    sorted_rows = sorted(
        rows,
        key=lambda item: (
            -(_safe_float(item.get("ts_start") or item.get("ts_start_utc")) or 0.0),
            str(item.get("span_id") or ""),
        ),
    )
    results: list[dict[str, object]] = []
    for item in sorted_rows:
        component = _derive_incident_component(item)
        symptom = _clamp_text(item.get("error_code") or "Processing error", 72)
        outcome = _clamp_text(item.get("outcome") or "error", 40)
        results.append(
            {
                "ts": _format_ts_utc(item.get("ts_start") or item.get("ts_start_utc")),
                "component": component,
                "symptom": symptom,
                "outcome": outcome,
                "span_id": item.get("span_id") or "",
            }
        )
    return results


def _health_component_matrix_view(
    *,
    current: Mapping[str, object] | None,
    status_strip: Mapping[str, object] | None,
    incidents: list[dict[str, object]],
) -> list[dict[str, object]]:
    if not current or not status_strip:
        return []
    incident_by_component: dict[str, dict[str, object]] = {}
    for item in incidents:
        component = str(item.get("component") or "")
        if component and component not in incident_by_component:
            incident_by_component[component] = item

    last_check = _format_ts_utc(current.get("ts_end_utc"))
    system_mode = str(current.get("system_mode") or "")
    system_status = system_mode or "unknown"
    system_class = _status_class_for_mode(system_status) if system_status else "muted"

    component_order = [
        ("IMAP", "imap"),
        ("LLM", "llm"),
        ("Telegram", "telegram"),
        ("DB", "db"),
        ("System", "system"),
    ]
    rows: list[dict[str, object]] = []
    for label, key in component_order:
        if key == "system":
            status_text = system_status or "unknown"
            status_class = system_class
        else:
            status_entry = status_strip.get(key) if isinstance(status_strip, Mapping) else None
            status_text = (
                str(status_entry.get("text"))
                if isinstance(status_entry, Mapping) and status_entry.get("text") is not None
                else "unknown"
            )
            status_class = (
                str(status_entry.get("class"))
                if isinstance(status_entry, Mapping) and status_entry.get("class") is not None
                else "muted"
            )
        incident = incident_by_component.get(label)
        if incident:
            last_issue = f"{incident.get('symptom')} · {incident.get('outcome')}"
        elif label == "System" and system_status.upper() == "FULL":
            last_issue = "All checks green."
        else:
            last_issue = "No recent incidents."
        rows.append(
            {
                "component": label,
                "status": status_text,
                "status_class": status_class,
                "last_check": last_check,
                "last_issue": _clamp_text(last_issue, 96),
            }
        )
    return rows


def _health_trend_view(timeline: list[dict[str, object]]) -> list[dict[str, object]]:
    if not timeline:
        return []
    sorted_items = sorted(
        timeline,
        key=lambda item: (
            _safe_float(item.get("ts_end_utc")) or 0.0,
            str(item.get("snapshot_id") or ""),
        ),
        reverse=True,
    )
    results: list[dict[str, object]] = []
    for item in sorted_items:
        snapshot_id = str(item.get("snapshot_id") or "")
        results.append(
            {
                "ts": _format_ts_utc(item.get("ts_end_utc")),
                "mode": item.get("system_mode") or "–",
                "gates": _summarize_mapping(item.get("gates_state"), limit=2),
                "metrics": _summarize_mapping(item.get("metrics_brief"), limit=2),
                "snapshot": snapshot_id,
                "snapshot_short": _short_id(snapshot_id, 10),
            }
        )
    return results


def _engineer_slowest_view(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    if not rows:
        return []
    sorted_rows = sorted(
        rows,
        key=lambda item: (
            -(_safe_float(item.get("total_ms")) or 0.0),
            -(_safe_float(item.get("started_at") or item.get("ts_start_utc")) or 0.0),
            str(item.get("span_id") or ""),
        ),
    )
    results: list[dict[str, object]] = []
    for item in sorted_rows:
        results.append(
            {
                "started": _format_ts_utc(item.get("started_at") or item.get("ts_start_utc")),
                "total_ms": _format_number(item.get("total_ms") or item.get("total_duration_ms")),
                "outcome": item.get("outcome") or "–",
                "llm": " ".join(
                    part for part in [item.get("llm_provider"), item.get("llm_model")] if part
                )
                or "–",
                "snapshot": item.get("health_snapshot_id") or item.get("snapshot_id") or "–",
            }
        )
    return results


def _engineer_errors_view(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    if not rows:
        return []
    sorted_rows = sorted(
        rows,
        key=lambda item: (
            -(_safe_float(item.get("ts_start") or item.get("ts_start_utc")) or 0.0),
            str(item.get("span_id") or ""),
        ),
    )
    results: list[dict[str, object]] = []
    for item in sorted_rows:
        results.append(
            {
                "ts": _format_ts_utc(item.get("ts_start") or item.get("ts_start_utc")),
                "outcome": item.get("outcome") or "–",
                "error_code": item.get("error_code") or "–",
                "llm": " ".join(
                    part for part in [item.get("llm_provider"), item.get("llm_model")] if part
                )
                or "–",
                "total_ms": _format_number(item.get("total_duration_ms")),
            }
        )
    return results


def _latency_distribution_view(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    if not rows:
        return []
    max_count = max(int(row.get("count") or 0) for row in rows) or 1
    results: list[dict[str, object]] = []
    for row in rows:
        count = int(row.get("count") or 0)
        bar_len = max(1, round((count / max_count) * 20)) if count else 0
        bar = "#" * bar_len
        results.append({"label": row.get("label") or "", "count": count, "bar": bar})
    return results


def _validate_latency_params(
    *,
    args,
    require_account: bool = True,
    default_account: str | None = None,
    window_default: int = 7,
    allowed_windows: set[int] | None = ALLOWED_WINDOWS,
) -> tuple[Optional[str], list[str], Optional[int], Optional[str]]:
    account_email = (args.get("account_email") or "").strip()
    account_emails = _parse_account_emails(args.get("account_emails"))
    window_days, error = _parse_window_days(
        args.get("window_days"), window_default, allowed_windows
    )
    if error:
        return None, [], None, error
    if account_emails and account_email and account_email not in account_emails:
        return None, [], None, "account_email must match one of account_emails"
    if not account_email and account_emails:
        account_email = account_emails[0]
    if not account_email and default_account:
        account_email = default_account
    if not account_emails and account_email:
        account_emails = [account_email]
    if require_account and not account_email:
        return None, [], None, "account_email is required"
    return account_email, account_emails, window_days, None


def _parse_attention_sort(raw: Optional[str]) -> tuple[str, Optional[str]]:
    if raw is None or raw == "":
        return "time", None
    cleaned = str(raw).strip().lower()
    if cleaned in ALLOWED_ATTENTION_SORTS:
        return cleaned, None
    return "time", "sort must be one of: time, cost, count"


def _validate_attention_params(
    *,
    args,
    default_account: str | None = None,
) -> tuple[list[str], int, str, Optional[str]]:
    account_email = (args.get("account_email") or "").strip()
    account_emails = _parse_account_emails(args.get("account_emails"))
    window_days, window_error = _parse_window_days(
        args.get("window_days"), 30, allowed=ALLOWED_WINDOWS
    )
    if window_error:
        return [], 0, "time", window_error
    sort_mode, sort_error = _parse_attention_sort(args.get("sort"))
    if sort_error:
        return [], 0, "time", sort_error
    if account_emails and account_email and account_email not in account_emails:
        return [], 0, "time", "account_email must match one of account_emails"
    if not account_emails and account_email:
        account_emails = [account_email]
    if not account_emails and default_account:
        account_emails = [default_account]
    if not account_emails:
        return [], 0, "time", "account_emails is required"
    return account_emails, window_days or 30, sort_mode, None


def _validate_learning_params(
    *,
    args,
    default_account: str | None = None,
) -> tuple[Optional[str], list[str], Optional[int], Optional[int], Optional[str]]:
    account_email = (args.get("account_email") or "").strip()
    account_emails = _parse_account_emails(args.get("account_emails"))
    window_days, window_error = _parse_window_days(args.get("window"), 30)
    if window_error:
        return None, [], None, None, window_error
    limit, limit_error = _parse_limit(args.get("limit"), default=50, max_limit=200, min_value=1)
    if limit_error:
        return None, [], None, None, limit_error
    if account_emails and account_email and account_email not in account_emails:
        return None, [], None, None, "account_email must match one of account_emails"
    if not account_email and account_emails:
        account_email = account_emails[0]
    if not account_email and default_account:
        account_email = default_account
    if not account_emails and account_email:
        account_emails = [account_email]
    if not account_email:
        return None, [], None, None, "account_email is required"
    return account_email, account_emails, window_days, limit, None


def _available_accounts(db_path: Path) -> list[str]:
    query = "SELECT DISTINCT account_id FROM processing_spans ORDER BY account_id ASC"
    try:
        with _open_readonly_connection(db_path) as conn:
            rows = conn.execute(query).fetchall()
    except sqlite3.OperationalError:
        rows = []
    accounts = [str(row[0]) for row in rows if row and row[0]]
    if accounts:
        return accounts
    emails_query = "SELECT DISTINCT account_email FROM emails ORDER BY account_email ASC"
    try:
        with _open_readonly_connection(db_path) as conn:
            rows = conn.execute(emails_query).fetchall()
    except sqlite3.OperationalError:
        rows = []
    accounts = [str(row[0]) for row in rows if row and row[0]]
    if accounts:
        return accounts
    fallback_query = "SELECT DISTINCT account_id FROM events_v1 ORDER BY account_id ASC"
    try:
        with _open_readonly_connection(db_path) as conn:
            rows = conn.execute(fallback_query).fetchall()
    except sqlite3.OperationalError:
        return []
    return [str(row[0]) for row in rows if row and row[0]]


def _db_size_bytes(db_path: Path) -> int | None:
    try:
        return db_path.stat().st_size
    except OSError:
        return None


def _decision_trace_payload(db_path: Path, email_id: int) -> tuple[list[dict[str, object]], str]:
    cache_key = ("decision_trace", str(db_path), int(email_id))
    cached = _DECISION_TRACE_CACHE.get(cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]
    traces = load_latest_decision_traces(
        db_path=db_path, email_id=email_id, limit=10, read_only=True
    )
    payload = summaries_as_payload(traces)
    updated = datetime.now(timezone.utc).isoformat()
    _DECISION_TRACE_CACHE.set(cache_key, (payload, updated))
    return payload, updated


def _decision_trace_histogram(db_path: Path, *, limit: int = 1000) -> tuple[list[dict[str, object]], str]:
    cache_key = ("decision_trace_hist", str(db_path), int(limit))
    cached = _DECISION_TRACE_HIST_CACHE.get(cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]
    counts: Counter[str] = Counter()
    try:
        with _open_readonly_connection(db_path) as conn:
            rows = conn.execute(
                """
                SELECT payload_json
                FROM events_v1
                WHERE event_type = ?
                ORDER BY ts_utc DESC
                LIMIT ?
                """,
                ("DECISION_TRACE_RECORDED", int(limit)),
            ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    for (payload_json,) in rows:
        trace = from_canonical_json(payload_json)
        if not trace:
            continue
        for code in trace.explain_codes:
            counts[code] += 1
    histogram = [
        {"code": code, "count": int(count)}
        for code, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    updated = datetime.now(timezone.utc).isoformat()
    _DECISION_TRACE_HIST_CACHE.set(cache_key, (histogram, updated))
    return histogram, updated


def _get_or_create_csrf_token() -> str:
    token = session.get("csrf_token")
    if isinstance(token, str) and token:
        return token
    token = secrets.token_urlsafe(32)
    session["csrf_token"] = token
    return token


def _validate_csrf_token() -> bool:
    expected = session.get("csrf_token")
    provided = request.form.get("csrf_token", "")
    if not isinstance(expected, str) or not expected:
        return False
    if not isinstance(provided, str) or not provided:
        return False
    return hmac.compare_digest(provided, expected)





def _text_response(body: str, status_code: int) -> Response:
    if USING_FLASK_STUB:
        return Response(body, status_code=status_code)
    return Response(body, status=status_code)


def _request_remote_addr() -> str | None:
    try:
        remote_addr = request.remote_addr
    except Exception:
        return None
    if remote_addr is None:
        return None
    return str(remote_addr)


def _ensure_authenticated() -> bool:
    return bool(session.get("authenticated"))


def _is_loopback(remote_addr: str | None) -> bool:
    if not remote_addr:
        return True
    try:
        return ipaddress.ip_address(remote_addr).is_loopback
    except ValueError:
        return False


def _cockpit_token_ok(token: str | None, expected: str) -> bool:
    if not expected:
        return False
    if not token:
        return False
    return hmac.compare_digest(token, expected)


def _parse_cidrs(values: Iterable[str]) -> list[ipaddress._BaseNetwork]:
    networks: list[ipaddress._BaseNetwork] = []
    for raw in values:
        if not raw:
            continue
        try:
            networks.append(ipaddress.ip_network(str(raw), strict=False))
        except ValueError:
            logger.warning("Invalid CIDR in web_ui.allow_cidrs: %s", raw)
    return networks


def _ip_allowed(remote_addr: str | None, allow_cidrs: Iterable[ipaddress._BaseNetwork]) -> bool:
    if _is_loopback(remote_addr):
        return True
    if not remote_addr:
        return False
    try:
        address = ipaddress.ip_address(remote_addr)
    except ValueError:
        return False
    for network in allow_cidrs:
        if address in network:
            return True
    return False


def _is_loopback_bind(bind: str) -> bool:
    if not bind:
        return False
    if bind.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(bind).is_loopback
    except ValueError:
        return False


def _resolve_yaml_config_path(config_path: Path | None, config_dir: Path) -> Path | None:
    if config_path is not None:
        return config_path
    resolved = resolve_config_paths(config_dir)
    if resolved.yaml_path is not None:
        return resolved.yaml_path
    return None


def _scrub_pii_line(line: str) -> str:
    cleaned = WEB_EMAIL_PATTERN.sub("[redacted]", line)
    return cleaned[:200]


def _tail_lines(path: Path, *, limit: int = 50) -> list[str]:
    if limit <= 0:
        return []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            return list(deque(handle, maxlen=limit))
    except OSError:
        return []


def _decision_trace_health_payload(
    db_path: Path,
    *,
    limit: int = 300,
) -> dict[str, object]:
    emitter = get_default_decision_trace_emitter()
    snapshot = emitter.snapshot()
    delivered_ids: list[int] = []
    traces_found: set[int] = set()
    try:
        with _open_readonly_connection(db_path) as conn:
            rows = conn.execute(
                """
                SELECT email_id
                FROM events_v1
                WHERE event_type = ?
                  AND email_id IS NOT NULL
                ORDER BY ts_utc DESC
                LIMIT ?
                """,
                ("telegram_delivered", int(limit)),
            ).fetchall()
            delivered_ids = [int(row[0]) for row in rows if row and row[0] is not None]
            if delivered_ids:
                placeholders = ", ".join(["?"] * len(delivered_ids))
                trace_rows = conn.execute(
                    f"""
                    SELECT DISTINCT email_id
                    FROM events_v1
                    WHERE event_type = ?
                      AND email_id IN ({placeholders})
                    """,
                    ["DECISION_TRACE_RECORDED", *delivered_ids],
                ).fetchall()
                traces_found = {int(row[0]) for row in trace_rows if row and row[0] is not None}
    except sqlite3.OperationalError:
        delivered_ids = []
        traces_found = set()
    delivered_total = len(set(delivered_ids))
    traced_total = len(traces_found)
    trace_coverage = (
        traced_total / delivered_total if delivered_total else None
    )
    log_path = Path(snapshot.get("drop_log_path") or "logs/decision_trace_failures.ndjson")
    tail_lines = _tail_lines(log_path, limit=50)
    tail_payload = [_scrub_pii_line(line) for line in tail_lines if line]
    return {
        "snapshot": snapshot,
        "trace_coverage": trace_coverage,
        "trace_coverage_sample": {
            "delivered": delivered_total,
            "traced": traced_total,
        },
        "drop_log_tail": tail_payload,
    }


def create_app(
    *,
    db_path: Path,
    password: str,
    secret_key: str,
    title: str = "Observability Console",
    attention_cost_per_hour: float = 0.0,
    allow_pii: bool | None = None,
    api_token: str = "",
    allow_cidrs: Iterable[str] | None = None,
    config_path: Path | None = None,
    log_path: Path | None = None,
    dist_root: Path | None = None,
    web_ui_bind: str = "127.0.0.1",
    web_ui_port: int = 8080,
    support_settings: SupportSettings | None = None,
) -> Flask:
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parent / "templates"),
        static_folder=str(Path(__file__).parent / "static"),
    )
    app.config["DB_PATH"] = Path(db_path)
    app.config["WEB_PASSWORD"] = password
    app.config["APP_TITLE"] = title
    app.config["ATTENTION_COST_PER_HOUR"] = max(0.0, float(attention_cost_per_hour))
    resolved_allow_pii = allow_pii
    if resolved_allow_pii is None:
        resolved_allow_pii = str(os.getenv("WEB_OBSERVABILITY_ALLOW_PII", "0")).lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
    app.config["WEB_OBSERVABILITY_ALLOW_PII"] = bool(resolved_allow_pii)
    env_token = str(os.getenv("WEB_OBSERVABILITY_TOKEN", "")).strip()
    app.config["COCKPIT_API_TOKEN"] = env_token or str(api_token or "").strip()
    app.secret_key = secret_key
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    allowed_networks = _parse_cidrs(allow_cidrs or [])
    app.config["WEB_UI_ALLOWED_CIDRS"] = allowed_networks
    app.config["WEB_UI_STARTED_AT"] = time.monotonic()
    app.config["DOCTOR_EXPORT_LAST_TS"] = {}
    app.config["DOCTOR_EXPORT_COOLDOWN_SECONDS"] = 60
    app.config["DOCTOR_CONFIG_PATH"] = Path(config_path) if config_path else Path("config.yaml")
    app.config["DOCTOR_LOG_PATH"] = Path(log_path) if log_path else Path("mailbot.log")
    app.config["DOCTOR_DIST_ROOT"] = Path(dist_root) if dist_root else Path.cwd()
    app.config["WEB_UI_BIND"] = str(web_ui_bind)
    app.config["WEB_UI_PORT"] = int(web_ui_port)
    app.config["ANALYTICS_FACTORY"] = lambda: KnowledgeAnalytics(
        app.config["DB_PATH"], read_only=True
    )
    resolved_support_settings = support_settings or SupportSettings(
        enabled=False,
        show_in_nav=False,
        methods=[],
    )
    app.config["SUPPORT_SETTINGS"] = resolved_support_settings
    app.config["DONATE_ENABLED"] = bool(resolved_support_settings.enabled)

    @app.before_request
    def _require_login():
        remote_addr = _request_remote_addr()
        if not _ip_allowed(remote_addr, app.config.get("WEB_UI_ALLOWED_CIDRS", [])):
            return Response("Forbidden", status=403)
        open_paths = {"login", "static"}
        cockpit_endpoints = {
            "api_cockpit_calibration",
            "api_cockpit_decision_trace_health",
        }
        if request.endpoint in cockpit_endpoints:
            try:
                headers = request.headers
            except Exception:
                headers = {}
            header_token = headers.get("X-Api-Token") if isinstance(headers, Mapping) else None
            token = header_token or request.args.get("token")
            if not _cockpit_token_ok(
                token, app.config["COCKPIT_API_TOKEN"]
            ):
                return Response("Forbidden", status=403)
            return None
        if request.endpoint in open_paths or request.path.startswith("/static"):
            if request.method == "POST" and not _validate_csrf_token():
                return _text_response("Forbidden: invalid CSRF token", 403)
            return None
        if request.method == "POST" and not _validate_csrf_token():
            return _text_response("Forbidden: invalid CSRF token", 403)
        if not _ensure_authenticated():
            return redirect(url_for("login", next=request.path))
        return None

    @app.route("/login", methods=["GET", "POST"])
    def login():
        error: str | None = None
        if request.method == "POST":
            provided = request.form.get("password", "")
            if hmac.compare_digest(str(provided), str(app.config["WEB_PASSWORD"])):
                session["authenticated"] = True
                session["auth_user"] = _request_remote_addr() or "local"
                session.permanent = False
                next_path = request.args.get("next")
                return redirect(next_path or url_for("index"))
            error = "Incorrect password. Please try again."
        return _render_template(
            app,
            "login.html",
            title=app.config["APP_TITLE"],
            page_title="Login",
            hide_nav=True,
            error=error,
            csrf_token=_get_or_create_csrf_token(),
        )

    @app.route("/doctor", methods=["GET"])
    def doctor() -> str:
        dist_root = Path(app.config["DOCTOR_DIST_ROOT"])
        manifest_path = dist_root / "manifest.sha256.json"
        if not manifest_path.exists():
            manifest_status = "NO_MANIFEST"
        else:
            from mailbot_v26.integrity import verify_manifest

            try:
                ok, _changed_files = verify_manifest(dist_root, manifest_path)
            except Exception:
                ok = False
            manifest_status = "OK" if ok else "MODIFIED"
        return _render_template(
            app,
            "doctor.html",
            title=app.config["APP_TITLE"],
            page_title="One-click Doctor",
            app_version=get_version(),
            manifest_status=manifest_status,
            log_path=str(app.config["DOCTOR_LOG_PATH"]),
            csrf_token=_get_or_create_csrf_token(),
        )

    @app.route("/support", methods=["GET"])
    def support() -> str:
        support_settings = app.config.get("SUPPORT_SETTINGS")
        if not isinstance(support_settings, SupportSettings) or not support_settings.enabled:
            return _text_response("Not Found", 404)
        return _render_template(
            app,
            "support.html",
            title=app.config["APP_TITLE"],
            page_title="Support",
            support_methods=support_settings.methods,
            support_text=support_settings.text,
            hide_limit=True,
            dashboard_vars=None,
            pii_allowed=False,
            share_url="",
        )

    @app.route("/doctor/export", methods=["POST"])
    def doctor_export() -> Response:
        user_key = str(session.get("auth_user") or _request_remote_addr() or "local")
        now_monotonic = time.monotonic()
        last_by_user = app.config["DOCTOR_EXPORT_LAST_TS"]
        assert isinstance(last_by_user, dict)
        cooldown = int(app.config.get("DOCTOR_EXPORT_COOLDOWN_SECONDS", 60))
        previous = float(last_by_user.get(user_key, 0.0) or 0.0)
        if previous and now_monotonic - previous < cooldown:
            wait_seconds = max(1, int(cooldown - (now_monotonic - previous)))
            return Response(
                f"Too many exports. Retry in {wait_seconds}s.",
                status=429,
            )
        last_by_user[user_key] = now_monotonic
        app.config["DOCTOR_EXPORT_LAST_TS"] = last_by_user

        payload = build_diagnostics_zip(
            Path(app.config["DOCTOR_CONFIG_PATH"]),
            Path(app.config["DOCTOR_LOG_PATH"]),
            Path(app.config["DB_PATH"]),
            Path(app.config["DOCTOR_DIST_ROOT"]),
            web_ui_bind=str(app.config["WEB_UI_BIND"]),
            web_ui_port=int(app.config["WEB_UI_PORT"]),
            uptime_seconds=int(now_monotonic - float(app.config["WEB_UI_STARTED_AT"])),
        )
        headers = {
            "Content-Type": "application/zip",
            "Content-Disposition": 'attachment; filename="diagnostics.zip"',
            "Cache-Control": "no-store",
        }
        if USING_FLASK_STUB:
            return Response(payload, status_code=200, headers=headers)
        return Response(payload, status=200, headers=headers)

    @app.route("/")
    def index():
        dashboard_vars = _dashboard_vars()
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None

        account_email = (request.args.get("account_email") or "").strip()
        if not account_email and dashboard_vars.account_emails:
            account_email = dashboard_vars.account_emails[0]
        if not account_email:
            account_email = default_account or ""
        account_emails = dashboard_vars.account_emails or ([] if not account_email else [account_email])
        if account_email and account_email not in account_emails:
            account_emails.append(account_email)
        account_emails = sorted({email for email in account_emails if email})
        window_days = dashboard_vars.window_days or 7
        lane = _parse_lane(request.args.get("lane"))
        reveal_pii = bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")) and bool(
            dashboard_vars.pii
        )
        mode = _resolve_cockpit_mode(request, session)
        include_engineer = mode == "engineer"

        analytics = _analytics()
        cache_key = (
            "cockpit",
            str(app.config["DB_PATH"]),
            tuple(account_emails),
            window_days,
            bool(reveal_pii),
            bool(include_engineer),
        )
        summary = _COCKPIT_CACHE.get(cache_key)
        if summary is None:
            summary = analytics.cockpit_summary(
                account_emails=account_emails,
                window_days=window_days,
                allow_pii=reveal_pii,
                include_engineer=include_engineer,
                activity_limit=15,
            )
            _COCKPIT_CACHE.set(cache_key, summary)

        summary = summary if isinstance(summary, Mapping) else {}
        status_strip = _status_strip_view(
            summary.get("status_strip") if isinstance(summary, Mapping) else None,
            now_ts=time.time(),
        )
        activity_cache_key = (
            "lane_activity",
            str(app.config["DB_PATH"]),
            tuple(account_emails),
            window_days,
            lane,
            dashboard_vars.limit,
            bool(reveal_pii),
        )
        cached_activity = _COCKPIT_CACHE.get(activity_cache_key)
        if cached_activity is None:
            if account_emails and hasattr(analytics, "lane_activity_rows"):
                cached_activity = analytics.lane_activity_rows(
                    account_email=account_emails[0],
                    account_emails=account_emails,
                    window_days=window_days,
                    limit=dashboard_vars.limit,
                    lane=lane,
                    reveal_pii=reveal_pii,
                )
            else:
                cached_activity = (
                    summary.get("recent_activity") if isinstance(summary, Mapping) else []
                )
            _COCKPIT_CACHE.set(activity_cache_key, cached_activity)
        activity_rows = _build_activity_table_rows(
            cached_activity if isinstance(cached_activity, list) else []
        )
        lane_counts: dict[str, int] = {key: 0 for key in LANE_KEYS}
        if account_emails and hasattr(analytics, "lane_counts"):
            lane_cache_key = (
                "lane_counts",
                str(app.config["DB_PATH"]),
                tuple(account_emails),
                window_days,
            )
            cached_counts = _COCKPIT_CACHE.get(lane_cache_key)
            if isinstance(cached_counts, Mapping):
                lane_counts = {key: int(cached_counts.get(key) or 0) for key in LANE_KEYS}
            else:
                lane_counts = analytics.lane_counts(
                    account_email=account_emails[0],
                    account_emails=account_emails,
                    window_days=window_days,
                )
                _COCKPIT_CACHE.set(lane_cache_key, lane_counts)
        digest_today = summary.get("today_digest") if isinstance(summary, Mapping) else {}
        digest_week = summary.get("week_digest") if isinstance(summary, Mapping) else {}
        today_source = digest_today.get("items", []) if lane == "all" else []
        week_source = digest_week.get("items", []) if lane == "all" else []
        today_items = _summarize_digest_rows(today_source)
        week_items = _summarize_digest_rows(week_source)
        golden_signals = _golden_signals_view(
            summary.get("golden_signals") if isinstance(summary, Mapping) else {}
        )
        engineer_payload = summary.get("engineer") if isinstance(summary, Mapping) else {}
        engineer_slowest = _engineer_slowest_view(
            engineer_payload.get("slow_spans", []) if isinstance(engineer_payload, Mapping) else []
        )
        engineer_errors = _engineer_errors_view(
            engineer_payload.get("recent_errors", []) if isinstance(engineer_payload, Mapping) else []
        )
        latency_distribution = _latency_distribution_view(
            engineer_payload.get("latency_distribution", [])
            if isinstance(engineer_payload, Mapping)
            else []
        )
        top_senders: list[dict[str, object]] = []
        silent_contacts: list[dict[str, object]] = []
        stalled_threads: list[dict[str, object]] = []
        if account_emails:
            try:
                top_senders = analytics.cockpit_top_senders(account_emails, days=30, limit=3)
            except Exception:
                top_senders = []
            try:
                silent_contacts = analytics.cockpit_silent_contacts(
                    account_emails,
                    silent_days=14,
                    days=90,
                    min_msgs=3,
                    limit=3,
                )
            except Exception:
                silent_contacts = []
            try:
                stalled_threads = analytics.cockpit_stalled_threads(account_emails, days=30, limit=3)
            except Exception:
                stalled_threads = []

        open_commitments = 0
        commitments_url = None
        if account_emails and hasattr(analytics, "commitment_status_counts"):
            commitments_cache_key = (
                "commitments_open",
                str(app.config["DB_PATH"]),
                tuple(account_emails),
            )
            cached_counts = _COMMITMENTS_COUNT_CACHE.get(commitments_cache_key)
            if cached_counts is None:
                cached_counts = analytics.commitment_status_counts(
                    account_email=account_emails[0],
                    account_emails=account_emails,
                )
                _COMMITMENTS_COUNT_CACHE.set(commitments_cache_key, cached_counts)
            if isinstance(cached_counts, Mapping):
                open_commitments = int(cached_counts.get("pending") or 0)
            commitments_params = {}
            if account_emails:
                commitments_params["account_emails"] = ",".join(account_emails)
            commitments_params["window_days"] = str(window_days)
            commitments_params["status"] = "open"
            if reveal_pii:
                commitments_params["pii"] = "1"
            if mode:
                commitments_params["mode"] = mode
            commitments_url = url_for("commitments", **commitments_params)

        scope_hint = None
        if account_email:
            scope_hint = f"{account_email} • last {window_days} days"

        def _mode_link(target: str) -> str:
            params = {k: v for k, v in request.args.items()}
            if "account_emails" not in params and account_emails:
                params["account_emails"] = ",".join(account_emails)
            if "window_days" not in params:
                params["window_days"] = str(window_days)
            if reveal_pii and "pii" not in params:
                params["pii"] = "1"
            if "lane" not in params:
                params["lane"] = lane
            params["mode"] = target
            return url_for("index", **params)

        lane_params: dict[str, str] = {}
        if account_emails:
            lane_params["account_emails"] = ",".join(account_emails)
        if window_days:
            lane_params["window_days"] = str(window_days)
        if dashboard_vars.limit:
            lane_params["limit"] = str(dashboard_vars.limit)
        if reveal_pii:
            lane_params["pii"] = "1"
        if mode:
            lane_params["mode"] = mode

        share_params = dict(lane_params)
        share_params["lane"] = lane
        share_url = url_for("index", **share_params)
        lane_pills = _build_lane_pills(
            selected_lane=lane,
            counts=lane_counts,
            base_params=lane_params,
            endpoint="index",
        )

        return _render_template(
            app,
            "cockpit.html",
            title=app.config["APP_TITLE"],
            page_title="Bridge Cockpit",
            scope_hint=scope_hint,
            dashboard_vars=dashboard_vars,
            account_email=account_email,
            account_emails_value=",".join(account_emails),
            window_days=window_days,
            status_strip=status_strip,
            golden_signals=golden_signals,
            activity_rows=activity_rows,
            lane=lane,
            lane_pills=lane_pills,
            digest_today=today_items,
            digest_today_counts=digest_today.get("counts", []),
            digest_week=week_items,
            digest_week_counts=digest_week.get("counts", []),
            pii_allowed=bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")),
            pii_enabled=reveal_pii,
            cockpit_mode=mode,
            mode_basic_url=_mode_link("basic"),
            mode_engineer_url=_mode_link("engineer"),
            engineer_mode=include_engineer,
            engineer_slowest=engineer_slowest,
            engineer_errors=engineer_errors,
            latency_distribution=latency_distribution,
            open_commitments=open_commitments,
            commitments_url=commitments_url,
            top_senders=top_senders,
            silent_contacts=silent_contacts,
            stalled_threads=stalled_threads,
            support_methods=app.config["SUPPORT_SETTINGS"].methods[:1],
            hide_limit=True,
            status_refresh_ms=STATUS_STRIP_REFRESH_MS,
            share_url=share_url,
        )

    @app.route("/cockpit")
    def cockpit_redirect():
        return redirect(url_for("index"))

    @app.route("/archive")
    def archive():
        dashboard_vars = _dashboard_vars()
        account_emails = _resolve_account_scope(dashboard_vars)
        window_raw = request.args.get("window_days")
        if window_raw is None and dashboard_vars.window_days:
            window_raw = str(dashboard_vars.window_days)
        window_days, window_error = _parse_window_days(
            window_raw, default=7, allowed=ALLOWED_ARCHIVE_WINDOWS
        )
        lane = _parse_lane(request.args.get("lane"))
        status, status_error = _parse_archive_status(request.args.get("status"))
        try:
            page = int(request.args.get("page") or 1)
        except (TypeError, ValueError):
            page = 1
        if page < 1:
            page = 1

        reveal_pii = bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")) and bool(
            dashboard_vars.pii
        )
        mode = _resolve_cockpit_mode(request, session)
        include_engineer = mode == "engineer"

        error_message = window_error or status_error
        rows: list[dict[str, object]] = []
        total_count = 0
        lane_counts: dict[str, int] = {key: 0 for key in LANE_KEYS}
        if account_emails and window_days:
            analytics = _analytics()
            if hasattr(analytics, "lane_counts"):
                lane_cache_key = (
                    "lane_counts",
                    str(app.config["DB_PATH"]),
                    tuple(account_emails),
                    window_days,
                )
                cached_counts = _COCKPIT_CACHE.get(lane_cache_key)
                if isinstance(cached_counts, Mapping):
                    lane_counts = {key: int(cached_counts.get(key) or 0) for key in LANE_KEYS}
                else:
                    lane_counts = analytics.lane_counts(
                        account_email=account_emails[0],
                        account_emails=account_emails,
                        window_days=window_days,
                    )
                    _COCKPIT_CACHE.set(lane_cache_key, lane_counts)
            archive_cache_key = (
                "archive_lane",
                str(app.config["DB_PATH"]),
                tuple(account_emails),
                window_days,
                status,
                lane,
                page,
                bool(reveal_pii),
                int(time.time()) // 15,
            )
            cached_payload = _COCKPIT_CACHE.get(archive_cache_key)
            if isinstance(cached_payload, Mapping):
                payload = dict(cached_payload)
            else:
                if hasattr(analytics, "lane_archive_rows"):
                    payload = analytics.lane_archive_rows(
                        account_email=account_emails[0],
                        account_emails=account_emails,
                        window_days=window_days,
                        page=page,
                        page_size=ARCHIVE_PAGE_SIZE,
                        status=status,
                        lane=lane,
                        reveal_pii=reveal_pii,
                    )
                else:
                    payload = analytics.email_archive_page(
                        account_email=account_emails[0],
                        account_emails=account_emails,
                        window_days=window_days,
                        page=page,
                        page_size=ARCHIVE_PAGE_SIZE,
                        status=status,
                        reveal_pii=reveal_pii,
                    )
                _COCKPIT_CACHE.set(archive_cache_key, payload)
            payload = payload if isinstance(payload, Mapping) else {}
            rows = payload.get("rows") if isinstance(payload, Mapping) else []
            if not isinstance(rows, list):
                rows = []
            total_count = int(payload.get("total_count") or 0) if isinstance(payload, Mapping) else 0
        else:
            error_message = error_message or "Select an account to view the archive."

        total_pages = max(1, int(math.ceil(total_count / ARCHIVE_PAGE_SIZE))) if total_count else 1
        if page > total_pages:
            page = total_pages

        def _base_params() -> dict[str, str]:
            params: dict[str, str] = {}
            if account_emails:
                params["account_emails"] = ",".join(account_emails)
            if window_days:
                params["window_days"] = str(window_days)
            if lane:
                params["lane"] = lane
            if status and status != "any":
                params["status"] = status
            if reveal_pii:
                params["pii"] = "1"
            if mode:
                params["mode"] = mode
            return params

        base_params = _base_params()
        detail_params = dict(base_params)
        detail_params["page"] = str(page)

        def _mode_link(target: str) -> str:
            params = dict(base_params)
            params["mode"] = target
            return url_for("archive", **params)

        prev_url = None
        if page > 1:
            prev_params = dict(base_params)
            prev_params["page"] = str(page - 1)
            prev_url = url_for("archive", **prev_params)
        next_url = None
        if page < total_pages:
            next_params = dict(base_params)
            next_params["page"] = str(page + 1)
            next_url = url_for("archive", **next_params)

        formatted_rows = []
        for row in rows:
            sanitized_row = _sanitize_archive_row(row if isinstance(row, Mapping) else {})
            e2e_ms = None
            e2e_seconds = sanitized_row.get("e2e_seconds")
            if e2e_seconds is not None:
                try:
                    e2e_ms = float(e2e_seconds) * 1000.0
                except (TypeError, ValueError):
                    e2e_ms = None
            formatted_rows.append(
                {
                    "email_id": sanitized_row.get("email_id"),
                    "received": _format_ts_utc(sanitized_row.get("received_ts_utc")),
                    "from_label": sanitized_row.get("from_label") or "",
                    "account_label": sanitized_row.get("account_label") or "",
                    "preview": sanitized_row.get("preview") or "",
                    "status": sanitized_row.get("status") or "",
                    "e2e_ms": _format_duration_ms(e2e_ms),
                    "delivery_mode": sanitized_row.get("delivery_mode") or "",
                    "failure_reason": sanitized_row.get("failure_reason") or "",
                    "stage_hint": sanitized_row.get("stage_hint") or "",
                }
            )

        return _render_template(
            app,
            "archive.html",
            title=app.config["APP_TITLE"],
            page_title="Email Archive",
            dashboard_vars=dashboard_vars,
            lane=lane,
            lane_pills=_build_lane_pills(
                selected_lane=lane,
                counts=lane_counts,
                base_params=base_params,
                endpoint="archive",
            ),
            account_emails=account_emails,
            window_days=window_days or 7,
            status=status,
            page=page,
            total_pages=total_pages,
            total_count=total_count,
            page_size=ARCHIVE_PAGE_SIZE,
            archive_rows=formatted_rows,
            detail_params=detail_params,
            prev_url=prev_url,
            next_url=next_url,
            window_options=_build_archive_window_options(window_days or 7),
            pii_allowed=bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")),
            pii_enabled=reveal_pii,
            cockpit_mode=mode,
            mode_basic_url=_mode_link("basic"),
            mode_engineer_url=_mode_link("engineer"),
            engineer_mode=include_engineer,
            error=error_message,
            hide_limit=True,
            share_url=url_for("archive", **detail_params),
        )

    @app.route("/commitments")
    def commitments():
        dashboard_vars = _dashboard_vars()
        account_emails = _resolve_account_scope(dashboard_vars)
        window_raw = request.args.get("window_days")
        if window_raw is None and dashboard_vars.window_days:
            window_raw = str(dashboard_vars.window_days)
        window_days, window_error = _parse_window_days(
            window_raw, default=7, allowed=ALLOWED_WINDOWS
        )
        status, status_error = _parse_commitment_status(request.args.get("status"))
        page = _parse_page(request.args.get("page"), default=1)
        reveal_pii = bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")) and bool(
            dashboard_vars.pii
        )
        mode = _resolve_cockpit_mode(request, session)

        error_message = window_error or status_error
        rows: list[dict[str, object]] = []
        total_count = 0
        if account_emails and window_days:
            analytics = _analytics()
            cache_key = (
                "commitments",
                str(app.config["DB_PATH"]),
                tuple(account_emails),
                window_days,
                status,
                page,
                bool(reveal_pii),
            )
            payload = _COMMITMENTS_CACHE.get(cache_key)
            if payload is None:
                payload = analytics.commitments_ledger_page(
                    account_emails=account_emails,
                    window_days=window_days,
                    status=status,
                    page=page,
                    page_size=COMMITMENTS_PAGE_SIZE,
                    reveal_pii=reveal_pii,
                    evidence_limit=8,
                )
                _COMMITMENTS_CACHE.set(cache_key, payload)
            rows = payload.get("rows") if isinstance(payload, Mapping) else []
            if not isinstance(rows, list):
                rows = []
            total_count = int(payload.get("total_count") or 0) if isinstance(payload, Mapping) else 0
        else:
            error_message = error_message or "Select an account to view commitments."

        total_pages = (
            max(1, int(math.ceil(total_count / COMMITMENTS_PAGE_SIZE))) if total_count else 1
        )
        if page > total_pages:
            page = total_pages

        def _base_params() -> dict[str, str]:
            params: dict[str, str] = {}
            if account_emails:
                params["account_emails"] = ",".join(account_emails)
            if window_days:
                params["window_days"] = str(window_days)
            if status and status != "open":
                params["status"] = status
            if reveal_pii:
                params["pii"] = "1"
            if mode:
                params["mode"] = mode
            return params

        base_params = _base_params()
        detail_params = dict(base_params)
        detail_params["page"] = str(page)

        prev_url = None
        if page > 1:
            prev_params = dict(base_params)
            prev_params["page"] = str(page - 1)
            prev_url = url_for("commitments", **prev_params)
        next_url = None
        if page < total_pages:
            next_params = dict(base_params)
            next_params["page"] = str(page + 1)
            next_url = url_for("commitments", **next_params)

        status_links = {
            "open": url_for("commitments", **{**base_params, "status": "open"}),
            "closed": url_for("commitments", **{**base_params, "status": "closed"}),
            "all": url_for("commitments", **{**base_params, "status": "all"}),
        }

        formatted_rows = []
        for row in rows:
            evidence_items = row.get("evidence") if isinstance(row, Mapping) else []
            evidence_rows = []
            if isinstance(evidence_items, list):
                for evidence in evidence_items:
                    if not isinstance(evidence, Mapping):
                        continue
                    duration_ms = evidence.get("duration_ms")
                    evidence_rows.append(
                        {
                            "ts": _format_ts_utc(evidence.get("ts_utc")),
                            "event_type": evidence.get("event_type") or "",
                            "stage": evidence.get("stage") or "",
                            "outcome": evidence.get("outcome") or "",
                            "duration": _format_duration_ms(duration_ms)
                            if duration_ms is not None
                            else "",
                            "event_id": evidence.get("event_id") or "",
                        }
                    )
            status_value = str(row.get("status") or "").strip()
            status_label = status_value.upper() if status_value else "UNKNOWN"
            source_label = str(row.get("source") or "").strip()
            kind_label = status_label
            if source_label:
                kind_label = f"{status_label} · {source_label}"
            email_id = row.get("email_id")
            forensics_url = None
            if email_id:
                try:
                    forensics_url = url_for(
                        "email_details", email_id=int(email_id), **detail_params
                    )
                except (TypeError, ValueError):
                    forensics_url = None
            formatted_rows.append(
                {
                    "commitment_id": row.get("commitment_id"),
                    "last_activity": _format_ts_utc(row.get("last_activity_ts")),
                    "counterparty_label": row.get("counterparty_label") or "",
                    "account_label": row.get("account_label") or "",
                    "kind": kind_label,
                    "due_signal": _format_due_signal(
                        created_ts=row.get("created_ts"),
                        deadline_iso=row.get("deadline_iso"),
                    ),
                    "evidence_count": int(row.get("evidence_count") or 0),
                    "evidence_last_ts": _format_ts_utc(row.get("last_evidence_ts")),
                    "evidence_rows": evidence_rows,
                    "forensics_url": forensics_url,
                }
            )

        return _render_template(
            app,
            "commitments.html",
            title=app.config["APP_TITLE"],
            page_title="Commitments Ledger",
            dashboard_vars=dashboard_vars,
            account_emails=account_emails,
            window_days=window_days or 7,
            status=status,
            page=page,
            total_pages=total_pages,
            total_count=total_count,
            page_size=COMMITMENTS_PAGE_SIZE,
            commitments_rows=formatted_rows,
            detail_params=detail_params,
            prev_url=prev_url,
            next_url=next_url,
            status_links=status_links,
            pii_allowed=bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")),
            pii_enabled=reveal_pii,
            cockpit_mode=mode,
            error=error_message,
            hide_limit=True,
        )

    @app.route("/email/<int:email_id>")
    def email_details(email_id: int):
        dashboard_vars = _dashboard_vars()
        reveal_pii = bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")) and bool(
            dashboard_vars.pii
        )
        mode = _resolve_cockpit_mode(request, session)
        include_engineer = mode == "engineer"
        analytics = _analytics()
        detail = analytics.email_forensics_detail(email_id=email_id, reveal_pii=reveal_pii)
        if not detail:
            return (
                _render_template(
                    app,
                    "email_detail.html",
                    title=app.config["APP_TITLE"],
                    page_title="Email Details",
                    dashboard_vars=dashboard_vars,
                    pii_allowed=bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")),
                    pii_enabled=reveal_pii,
                    cockpit_mode=mode,
                    mode_basic_url=url_for("archive", mode="basic"),
                    mode_engineer_url=url_for("archive", mode="engineer"),
                    engineer_mode=include_engineer,
                    error="Email record not found.",
                    hide_limit=True,
                ),
                404,
            )

        detail = _sanitize_archive_row(detail)
        timeline_raw = analytics.email_processing_timeline(email_id=email_id)
        timeline_rows = []
        for row in timeline_raw:
            timeline_rows.append(
                {
                    "ts": _format_ts_utc(row.get("ts_start_utc")),
                    "stage": row.get("stage") or "",
                    "duration_ms": _format_duration_ms(row.get("duration_ms")),
                    "outcome": row.get("outcome") or "",
                    "error_code": row.get("error_code") or "",
                    "span_id": row.get("span_id") or "",
                }
            )

        evidence_items = []
        event_evidence = analytics.email_forensics_events(email_id=email_id, limit=8)
        for event in event_evidence:
            evidence_items.append(
                {
                    "kind": "event",
                    "id": event.get("event_id") or "",
                    "ts_utc": event.get("ts_utc"),
                    "event_type": event.get("event_type") or "",
                    "stage": event.get("stage") or "",
                    "outcome": event.get("outcome") or "",
                    "duration_ms": event.get("duration_ms"),
                }
            )
        span_seen: set[str] = set()
        for row in timeline_raw:
            span_id = str(row.get("span_id") or "").strip()
            if not span_id or span_id in span_seen:
                continue
            span_seen.add(span_id)
            evidence_items.append(
                {
                    "kind": "span",
                    "id": span_id,
                    "ts_utc": row.get("ts_start_utc"),
                    "event_type": "processing_span",
                    "stage": row.get("stage") or "",
                    "outcome": row.get("outcome") or "",
                    "duration_ms": row.get("duration_ms"),
                }
            )
        evidence_items.sort(
            key=lambda item: (
                -float(item.get("ts_utc") or 0.0),
                str(item.get("kind") or ""),
                str(item.get("id") or ""),
            )
        )
        evidence_rows = []
        for item in evidence_items[:8]:
            duration_ms = item.get("duration_ms")
            evidence_rows.append(
                {
                    "kind": item.get("kind") or "",
                    "id": item.get("id") or "",
                    "ts": _format_ts_utc(item.get("ts_utc")),
                    "event_type": item.get("event_type") or "",
                    "stage": item.get("stage") or "",
                    "outcome": item.get("outcome") or "",
                    "duration": _format_duration_ms(duration_ms)
                    if duration_ms is not None
                    else "",
                }
            )

        status_label = detail.get("status") or ""
        status_class = "muted"
        if status_label.lower() == "delivered":
            status_class = "success"
        elif status_label.lower() == "failed":
            status_class = "danger"
        elif status_label.lower() == "in-flight":
            status_class = "warn"

        e2e_ms = None
        if detail.get("e2e_seconds") is not None:
            try:
                e2e_ms = float(detail.get("e2e_seconds")) * 1000.0
            except (TypeError, ValueError):
                e2e_ms = None

        archive_params = {}
        for key in ("account_emails", "window_days", "status", "page", "mode", "pii"):
            value = request.args.get(key)
            if value:
                archive_params[key] = value
        archive_url = url_for("archive", **archive_params)
        decision_traces, decision_trace_updated = _decision_trace_payload(
            app.config["DB_PATH"], email_id
        )
        histogram, histogram_updated = _decision_trace_histogram(app.config["DB_PATH"])

        return _render_template(
            app,
            "email_detail.html",
            title=app.config["APP_TITLE"],
            page_title=f"Email {email_id}",
            dashboard_vars=dashboard_vars,
            pii_allowed=bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")),
            pii_enabled=reveal_pii,
            cockpit_mode=mode,
            mode_basic_url=url_for("email_details", email_id=email_id, mode="basic"),
            mode_engineer_url=url_for("email_details", email_id=email_id, mode="engineer"),
            engineer_mode=include_engineer,
            hide_limit=True,
            detail={
                "email_id": email_id,
                "received": _format_ts_utc(detail.get("received_ts_utc")),
                "account": detail.get("account_label") or "",
                "from_label": detail.get("from_label") or "",
                "status": status_label,
                "status_class": status_class,
                "delivery_mode": detail.get("delivery_mode") or "",
                "failure_reason": detail.get("failure_reason") or "",
                "e2e_ms": _format_duration_ms(e2e_ms),
                "preview": detail.get("preview") or "",
            },
            timeline_rows=timeline_rows,
            evidence_rows=evidence_rows,
            archive_url=archive_url,
            cockpit_url=url_for("index"),
            decision_traces=decision_traces,
            decision_trace_updated=decision_trace_updated,
            decision_trace_histogram=histogram,
            decision_trace_hist_updated=histogram_updated,
        )

    @app.route("/partial/status_strip")
    def status_strip_partial():
        dashboard_vars = _dashboard_vars()
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_email = (request.args.get("account_email") or "").strip()
        if not account_email and dashboard_vars.account_emails:
            account_email = dashboard_vars.account_emails[0]
        if not account_email:
            account_email = default_account or ""
        account_emails = dashboard_vars.account_emails or ([] if not account_email else [account_email])
        if account_email and account_email not in account_emails:
            account_emails.append(account_email)
        account_emails = sorted({email for email in account_emails if email})
        window_days = dashboard_vars.window_days or 7
        reveal_pii = bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")) and bool(
            dashboard_vars.pii
        )
        include_engineer = False

        analytics = _analytics()
        cache_key = (
            "cockpit",
            str(app.config["DB_PATH"]),
            tuple(account_emails),
            window_days,
            bool(reveal_pii),
            bool(include_engineer),
        )
        summary = _COCKPIT_CACHE.get(cache_key)
        if summary is None:
            summary = analytics.cockpit_summary(
                account_emails=account_emails,
                window_days=window_days,
                allow_pii=reveal_pii,
                include_engineer=include_engineer,
                activity_limit=15,
            )
            _COCKPIT_CACHE.set(cache_key, summary)

        summary = summary if isinstance(summary, Mapping) else {}
        status_strip = _status_strip_view(
            summary.get("status_strip") if isinstance(summary, Mapping) else None,
            now_ts=time.time(),
        )
        return _render_template(
            app,
            "partials/status_strip.html",
            status_strip=status_strip,
        )

    def _analytics() -> KnowledgeAnalytics:
        factory = app.config.get("ANALYTICS_FACTORY")
        if callable(factory):
            return factory()
        return KnowledgeAnalytics(app.config["DB_PATH"], read_only=True)

    def _dashboard_vars() -> DashboardVars:
        return resolve_dashboard_vars(
            request,
            session,
            allow_pii=bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")),
        )

    def _resolve_account_scope(dashboard_vars: DashboardVars) -> list[str]:
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        raw_accounts = request.args.get("account_emails")
        resolved = _parse_account_emails(raw_accounts) if raw_accounts else []
        if not resolved:
            resolved = list(dashboard_vars.account_emails)
        if not resolved and default_account:
            resolved = [default_account]
        return sorted({email for email in resolved if email})

    def _attention_payload(
        *,
        account_emails: list[str],
        window_days: int,
        sort_mode: str,
    ) -> dict[str, object]:
        totals_cache_key = (
            "attention_totals",
            str(app.config["DB_PATH"]),
            tuple(account_emails),
            window_days,
        )
        table_cache_key = (
            "attention_table",
            str(app.config["DB_PATH"]),
            tuple(account_emails),
            window_days,
            sort_mode,
        )
        cached_totals = _ATTENTION_TOTALS_CACHE.get(totals_cache_key)
        cached_entities = _ATTENTION_TABLE_CACHE.get(table_cache_key)
        if isinstance(cached_totals, Mapping) and isinstance(cached_entities, list):
            return {
                **cached_totals,
                "entities": cached_entities,
                "sort": sort_mode,
                "limit": 50,
            }
        analytics = _analytics()
        summary = analytics.attention_economics_summary(
            account_emails=account_emails,
            window_days=window_days,
            limit=50,
            sort=sort_mode,
            attention_cost_per_hour=float(app.config.get("ATTENTION_COST_PER_HOUR", 0.0)),
        )
        totals_payload = {
            "window_days": summary.get("window_days"),
            "account_emails": summary.get("account_emails"),
            "limit": summary.get("limit"),
            "totals": summary.get("totals", {}),
            "lane_breakdown": summary.get("lane_breakdown", []),
            "top_contact_label": summary.get("top_contact_label", ""),
            "generated_at_utc": summary.get("generated_at_utc", ""),
        }
        _ATTENTION_TOTALS_CACHE.set(totals_cache_key, totals_payload)
        entities = summary.get("entities", [])
        if isinstance(entities, list):
            _ATTENTION_TABLE_CACHE.set(table_cache_key, entities)
        return {
            **totals_payload,
            "entities": entities if isinstance(entities, list) else [],
            "sort": sort_mode,
            "limit": summary.get("limit", 50),
        }

    def _budget_cockpit_payload(
        *,
        account_emails: list[str],
        window_days: int,
        trend_days: int,
    ) -> dict[str, object]:
        cache_key = (
            "cockpit_budgets",
            str(app.config["DB_PATH"]),
            tuple(account_emails),
            window_days,
            trend_days,
        )
        cached = _BUDGET_CACHE.get(cache_key)
        if isinstance(cached, Mapping):
            return dict(cached)
        analytics = _analytics()
        status = analytics.budgets_llm_status(
            account_emails=account_emails,
            window_days=window_days,
        )
        trend = analytics.budgets_llm_trend(
            account_emails=account_emails,
            days=trend_days,
        )
        accounts = status.get("accounts") if isinstance(status, Mapping) else None
        masked_accounts: list[dict[str, object]] = []
        if isinstance(accounts, list):
            for entry in accounts:
                if not isinstance(entry, Mapping):
                    continue
                account_label = entry.get("account_label") or entry.get("account_email") or ""
                masked_label = _mask_email_address(account_label)
                sanitized = dict(entry)
                sanitized["account_label"] = masked_label or ""
                sanitized.pop("account_email", None)
                masked_accounts.append(sanitized)
        masked_status = dict(status) if isinstance(status, Mapping) else {}
        if masked_accounts:
            masked_status["accounts"] = masked_accounts
        payload = {"status": masked_status, "trend": trend}
        _BUDGET_CACHE.set(cache_key, payload)
        return payload

    def _triage_lane_payload(
        *,
        account_emails: list[str],
        window_days: int,
    ) -> dict[str, object]:
        cache_key = (
            "cockpit_lanes",
            str(app.config["DB_PATH"]),
            tuple(account_emails),
            window_days,
        )
        cached = _TRIAGE_LANES_CACHE.get(cache_key)
        if isinstance(cached, Mapping):
            return dict(cached)
        analytics = _analytics()
        payload = analytics.triage_lane_distribution(
            account_emails=account_emails,
            window_days=window_days,
        )
        _TRIAGE_LANES_CACHE.set(cache_key, payload)
        return payload

    def _health_summary_payload(
        *,
        account_emails: list[str],
        window_days: int,
        reveal_pii: bool,
        mode: str,
    ) -> dict[str, object]:
        cache_key = (
            "health_summary",
            str(app.config["DB_PATH"]),
            tuple(account_emails),
            window_days,
            mode,
            reveal_pii,
        )
        cached = _HEALTH_SUMMARY_CACHE.get(cache_key)
        if isinstance(cached, Mapping):
            return dict(cached)
        db_size_bytes = _db_size_bytes(app.config["DB_PATH"])
        summary: dict[str, object] = {
            "current": None,
            "status_strip": _status_strip_view(
                {
                    "system_mode": "unknown",
                    "gates_state": {},
                    "metrics_brief": {},
                    "updated_ts_utc": None,
                    "db_size_bytes": db_size_bytes,
                },
                now_ts=time.time(),
            ),
            "metrics_digest": {},
            "trend": [],
            "metrics_brief": {},
            "db_size_bytes": db_size_bytes,
        }
        if not account_emails:
            _HEALTH_SUMMARY_CACHE.set(cache_key, summary)
            return summary

        primary = account_emails[0]
        analytics = _analytics()
        current = analytics.processing_spans_health_current(
            account_email=primary,
            account_emails=account_emails,
            window_days=window_days,
        )
        metrics_digest = analytics.processing_spans_metrics_digest(
            account_email=primary,
            account_emails=account_emails,
            window_days=window_days,
        )
        trend = analytics.processing_spans_health_timeline(
            account_email=primary,
            account_emails=account_emails,
            window_days=window_days,
            limit=5,
        )
        status_payload = {
            "system_mode": current.get("system_mode") if current else "unknown",
            "gates_state": current.get("gates_state") if current else {},
            "metrics_brief": current.get("metrics_brief") if current else {},
            "updated_ts_utc": current.get("ts_end_utc") if current else None,
            "db_size_bytes": db_size_bytes,
        }
        summary = {
            "current": current,
            "status_strip": _status_strip_view(status_payload, now_ts=time.time()),
            "metrics_digest": metrics_digest,
            "trend": trend,
            "metrics_brief": current.get("metrics_brief") if current else {},
            "db_size_bytes": db_size_bytes,
        }
        _HEALTH_SUMMARY_CACHE.set(cache_key, summary)
        return summary

    def _health_incidents_payload(
        *,
        account_emails: list[str],
        window_days: int,
        reveal_pii: bool,
        mode: str,
    ) -> list[dict[str, object]]:
        cache_key = (
            "health_incidents",
            str(app.config["DB_PATH"]),
            tuple(account_emails),
            window_days,
            mode,
            reveal_pii,
        )
        cached = _HEALTH_INCIDENT_CACHE.get(cache_key)
        if isinstance(cached, list):
            return cached
        if not account_emails:
            _HEALTH_INCIDENT_CACHE.set(cache_key, [])
            return []
        primary = account_emails[0]
        analytics = _analytics()
        raw_incidents = analytics.processing_spans_recent_errors(
            account_email=primary,
            account_emails=account_emails,
            window_days=window_days,
            limit=10,
        )
        incidents = _health_incidents_view(raw_incidents)[:5]
        _HEALTH_INCIDENT_CACHE.set(cache_key, incidents)
        return incidents

    def _health_component_payload(
        *,
        current: Mapping[str, object] | None,
        status_strip: Mapping[str, object] | None,
        incidents: list[dict[str, object]],
        account_emails: list[str],
        window_days: int,
        reveal_pii: bool,
        mode: str,
    ) -> list[dict[str, object]]:
        cache_key = (
            "health_components",
            str(app.config["DB_PATH"]),
            tuple(account_emails),
            window_days,
            mode,
            reveal_pii,
        )
        cached = _HEALTH_COMPONENT_CACHE.get(cache_key)
        if isinstance(cached, list):
            return cached
        components = _health_component_matrix_view(
            current=current,
            status_strip=status_strip,
            incidents=incidents,
        )
        _HEALTH_COMPONENT_CACHE.set(cache_key, components)
        return components

    @app.route("/api/v1/cockpit/budgets", methods=["GET"])
    @app.route("/api/cockpit/budgets", methods=["GET"])
    def api_cockpit_budgets():
        account_emails, window_days, _, error = _validate_attention_params(
            args=request.args
        )
        if error:
            resp = jsonify({"error": error})
            resp.status_code = 400
            return resp
        trend_days, trend_error = _parse_window_days(
            request.args.get("days"), 30, allowed=ALLOWED_WINDOWS
        )
        if trend_error:
            resp = jsonify({"error": trend_error})
            resp.status_code = 400
            return resp
        resolved_window = window_days or 30
        resolved_trend = trend_days or 30
        payload = _budget_cockpit_payload(
            account_emails=account_emails,
            window_days=resolved_window,
            trend_days=resolved_trend,
        )
        return jsonify(
            {
                "window_days": resolved_window,
                "account_emails": _mask_account_emails(account_emails),
                "status": payload.get("status", {}),
                "trend": payload.get("trend", {}),
            }
        )

    @app.route("/api/v1/cockpit/lanes", methods=["GET"])
    @app.route("/api/cockpit/lanes", methods=["GET"])
    def api_cockpit_lanes():
        account_emails, window_days, _, error = _validate_attention_params(
            args=request.args
        )
        if error:
            resp = jsonify({"error": error})
            resp.status_code = 400
            return resp
        resolved_window = window_days or 30
        payload = _triage_lane_payload(
            account_emails=account_emails,
            window_days=resolved_window,
        )
        return jsonify(
            {
                "window_days": resolved_window,
                "account_emails": _mask_account_emails(account_emails),
                "distribution": payload,
            }
        )

    @app.route("/api/v1/cockpit/decision-trace", methods=["GET"])
    def api_cockpit_decision_trace():
        raw_email_id = (request.args.get("email_id") or "").strip()
        if not raw_email_id.isdigit():
            return jsonify({"error": "email_id is required"}), 400
        email_id = int(raw_email_id)
        traces, updated = _decision_trace_payload(app.config["DB_PATH"], email_id)
        histogram, histogram_updated = _decision_trace_histogram(app.config["DB_PATH"])
        return jsonify(
            {
                "email_id": email_id,
                "traces": traces,
                "last_updated_ts": updated,
                "histogram": histogram,
                "histogram_last_updated_ts": histogram_updated,
            }
        )

    @app.route("/api/v1/cockpit/calibration", methods=["GET"])
    def api_cockpit_calibration():
        days, days_error = _parse_window_days(
            request.args.get("days"), 30, allowed=ALLOWED_WINDOWS
        )
        if days_error:
            return jsonify({"error": days_error}), 400
        max_rows, limit_error = _parse_limit(
            request.args.get("max_rows"), default=1000, max_limit=2000, min_value=10
        )
        if limit_error:
            return jsonify({"error": limit_error}), 400
        report = compute_priority_calibration_report(
            db_path=app.config["DB_PATH"],
            days=days or 30,
            max_rows=max_rows or 1000,
        )
        return jsonify(report)

    @app.route("/api/v1/cockpit/decision-trace/health", methods=["GET"])
    def api_cockpit_decision_trace_health():
        limit, limit_error = _parse_limit(
            request.args.get("limit"), default=300, max_limit=1000, min_value=50
        )
        if limit_error:
            return jsonify({"error": limit_error}), 400
        payload = _decision_trace_health_payload(
            app.config["DB_PATH"], limit=limit or 300
        )
        return jsonify(payload)

    @app.route("/api/v1/observability/latency_summary", methods=["GET"])
    def api_latency_summary():
        account_email, account_emails, window_days, error = _validate_latency_params(
            args=request.args, require_account=True
        )
        if error:
            return jsonify({"error": error}), 400
        analytics = _analytics()
        summary = analytics.processing_spans_metrics_digest(
            account_email=account_email,
            account_emails=account_emails,
            window_days=window_days or 7,
        )
        recent_errors = analytics.processing_spans_recent_errors(
            account_email=account_email,
            account_emails=account_emails,
            window_days=window_days or 7,
        )
        slowest = analytics.processing_spans_slowest(
            account_email=account_email,
            account_emails=account_emails,
            window_days=window_days or 7,
            limit=5,
        )
        return jsonify(
            {
                "window_days": window_days,
                "account_email": account_email,
                "account_emails": account_emails,
                "summary": summary,
                "recent_errors": recent_errors,
                "slowest": slowest,
            }
        )

    @app.route("/api/v1/observability/health_timeline", methods=["GET"])
    def api_health_timeline():
        account_email, account_emails, window_days, error = _validate_latency_params(
            args=request.args, require_account=True, window_default=7
        )
        if error:
            return jsonify({"error": error}), 400
        analytics = _analytics()
        resolved_window = window_days or 30
        current = analytics.processing_spans_health_current(
            account_email=account_email,
            account_emails=account_emails,
            window_days=resolved_window,
        )
        timeline = analytics.processing_spans_health_timeline(
            account_email=account_email,
            account_emails=account_emails,
            window_days=resolved_window,
        )
        return jsonify(
            {
                "window_days": resolved_window,
                "account_email": account_email,
                "account_emails": account_emails,
                "current": current,
                "timeline": timeline,
            }
        )

    @app.route("/api/v1/events/timeline", methods=["GET"])
    def api_events_timeline():
        account_email, account_emails, window_days, error = _validate_latency_params(
            args=request.args, require_account=True, window_default=30
        )
        if error:
            return jsonify({"error": error}), 400
        limit, limit_error = _parse_limit(request.args.get("limit"), default=200, max_limit=500)
        if limit_error:
            return jsonify({"error": limit_error}), 400
        resolved_window = window_days or 30
        analytics = _analytics()
        items = analytics.events_timeline(
            account_email=account_email,
            account_emails=account_emails,
            window_days=resolved_window,
            limit=limit or 0,
        )
        return jsonify(
            {
                "window_days": resolved_window,
                "account_email": account_email,
                "account_emails": account_emails,
                "items": items,
            }
        )

    @app.route("/api/v1/relationships/graph", methods=["GET"])
    def api_relationships_graph():
        account_email, account_emails, window_days, error = _validate_latency_params(
            args=request.args, require_account=True, window_default=30
        )
        if error:
            resp = jsonify({"error": error})
            resp.status_code = 400
            return resp
        limit, limit_error = _parse_limit(request.args.get("limit"), default=50, max_limit=200)
        if limit_error:
            resp = jsonify({"error": limit_error})
            resp.status_code = 400
            return resp
        analytics = _analytics()
        graph = analytics.relationship_graph(
            account_email=account_email,
            account_emails=account_emails,
            window_days=window_days or 30,
            limit=limit or 50,
        )
        return jsonify(graph)

    @app.route("/api/v1/relationships/contact", methods=["GET"])
    def api_relationship_contact():
        account_email, account_emails, window_days, error = _validate_latency_params(
            args=request.args, require_account=True, window_default=30
        )
        if error:
            resp = jsonify({"error": error})
            resp.status_code = 400
            return resp
        contact_id = request.args.get("contact_id", "")
        if not contact_id:
            resp = jsonify({"error": "contact_id is required"})
            resp.status_code = 400
            return resp
        analytics = _analytics()
        detail = analytics.relationship_contact_detail(
            account_email=account_email,
            account_emails=account_emails,
            contact_id=contact_id,
            window_days=window_days or 30,
        )
        if not detail:
            resp = jsonify({"error": "contact unavailable"})
            resp.status_code = 404
            return resp
        return jsonify(detail)

    @app.route("/api/v1/intelligence/attention_economics", methods=["GET"])
    def api_attention_economics():
        account_emails, window_days, sort_mode, error = _validate_attention_params(
            args=request.args
        )
        if error:
            resp = jsonify({"error": error})
            resp.status_code = 400
            return resp
        summary = _attention_payload(
            account_emails=account_emails,
            window_days=window_days or 30,
            sort_mode=sort_mode,
        )
        return jsonify(summary)

    @app.route("/api/v1/intelligence/learning_summary", methods=["GET"])
    def api_learning_summary():
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_email, account_emails, window_days, limit, error = _validate_learning_params(
            args=request.args, default_account=default_account
        )
        if error:
            resp = jsonify({"error": error})
            resp.status_code = 400
            return resp
        analytics = _analytics()
        summary = analytics.behavioral_metrics_summary(
            account_email=account_email,
            account_emails=account_emails,
            window_days=window_days or 30,
            now_ts=datetime.now(timezone.utc).timestamp(),
        )
        return jsonify(summary)

    @app.route("/api/v1/intelligence/learning_timeline", methods=["GET"])
    def api_learning_timeline():
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_email, account_emails, window_days, limit, error = _validate_learning_params(
            args=request.args, default_account=default_account
        )
        if error:
            resp = jsonify({"error": error})
            resp.status_code = 400
            return resp
        analytics = _analytics()
        timeline = analytics.learning_timeline(
            account_email=account_email,
            account_emails=account_emails,
            window_days=window_days or 30,
            limit=limit or 50,
            now_ts=datetime.now(timezone.utc).timestamp(),
        )
        return jsonify(timeline)

    @app.route("/latency", methods=["GET"])
    def latency():
        dashboard_vars = _dashboard_vars()
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_email_arg = (request.args.get("account_email") or "").strip()
        resolved_account = account_email_arg or (dashboard_vars.account_emails[0] if dashboard_vars.account_emails else "")
        if not resolved_account and default_account:
            resolved_account = default_account
        account_email, account_emails, window_days, error = _validate_latency_params(
            args={
                **{k: v for k, v in request.args.items()},
                "account_email": resolved_account,
                "account_emails": ",".join(dashboard_vars.account_emails),
                "window_days": str(dashboard_vars.window_days),
            },
            require_account=False,
            default_account=default_account,
            allowed_windows=None,
        )
        error_message = error or ("Select an account to view latency." if not account_email else "")
        analytics = _analytics()
        summary: dict[str, object] | None = None
        recent_errors: list[dict[str, object]] = []
        slowest: list[dict[str, object]] = []
        activity_rows: list[dict[str, object]] = []
        reveal_pii = bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")) and bool(
            dashboard_vars.pii
        )
        if not error_message and account_email:
            summary = analytics.processing_spans_metrics_digest(
                account_email=account_email,
                account_emails=account_emails,
                window_days=window_days or 7,
            )
            recent_errors = analytics.processing_spans_recent_errors(
                account_email=account_email,
                account_emails=account_emails,
                window_days=window_days or 7,
                limit=10,
            )
            slowest = analytics.processing_spans_slowest(
                account_email=account_email,
                account_emails=account_emails,
                window_days=window_days or 7,
                limit=5,
            )
            activity_rows = analytics.recent_mail_activity(
                account_email=account_email,
                account_emails=account_emails,
                window_days=window_days or 7,
                limit=dashboard_vars.limit or 25,
                reveal_pii=reveal_pii,
            )

        sample_size = int(summary.get("span_count") or 0) if summary else 0
        metrics_cards: list[dict[str, object]] = []
        stage_breakdown: list[dict[str, object]] = []
        slowest_rows: list[dict[str, object]] = []
        error_rows: list[dict[str, object]] = []
        activity_table_rows: list[dict[str, object]] = _build_activity_table_rows(activity_rows)

        if summary:
            metrics_cards = [
                {"label": "Pipeline p50", "value": _format_number(summary.get("total_duration_ms_p50")), "suffix": "ms"},
                {"label": "Pipeline p90", "value": _format_number(summary.get("total_duration_ms_p90")), "suffix": "ms"},
                {"label": "Pipeline p95", "value": _format_number(summary.get("total_duration_ms_p95")), "suffix": "ms"},
                {"label": "LLM p90", "value": _format_number(summary.get("llm_latency_ms_p90")), "suffix": "ms"},
                {"label": "Error rate", "value": _format_percent(summary.get("error_rate")), "suffix": "%"},
                {"label": "Fallback rate", "value": _format_percent(summary.get("fallback_rate")), "suffix": "%"},
                {"label": "Quality avg", "value": _format_number(summary.get("llm_quality_avg")), "suffix": "score"},
                {"label": "Samples", "value": _format_number(sample_size), "suffix": "spans"},
            ]
            total_avg = _safe_float(summary.get("total_duration_ms_avg")) or 0.0
            stage_stats = summary.get("stage_durations") if isinstance(summary, dict) else {}
            for stage_name in sorted(stage_stats.keys()):
                stats = stage_stats.get(stage_name) or {}
                avg = _safe_float(stats.get("avg")) or 0.0
                share_percent = 0.0
                if total_avg > 0:
                    share_percent = max(0.0, min(100.0, (avg / total_avg) * 100.0))
                stage_breakdown.append(
                    {
                        "name": stage_name,
                        "avg": _format_number(stats.get("avg")),
                        "p50": _format_number(stats.get("p50")),
                        "p90": _format_number(stats.get("p90")),
                        "p95": _format_number(stats.get("p95")),
                        "share": f"{share_percent:.1f}",
                    }
                )

        if slowest:
            sorted_slowest = sorted(
                slowest,
                key=lambda item: (
                    -(_safe_float(item.get("total_ms")) or 0.0),
                    -(_safe_float(item.get("started_at")) or 0.0),
                    str(item.get("span_id") or ""),
                ),
            )
            for item in sorted_slowest:
                slowest_rows.append(
                    {
                        "started": _format_ts_utc(item.get("started_at")),
                        "total_ms": _format_number(item.get("total_ms")),
                        "outcome": item.get("outcome") or "–",
                        "llm": " ".join(
                            part for part in [item.get("llm_provider"), item.get("llm_model")] if part
                        )
                        or "–",
                        "snapshot": item.get("health_snapshot_id") or "–",
                    }
                )

        if recent_errors:
            sorted_errors = sorted(
                recent_errors,
                key=lambda item: (
                    -(_safe_float(item.get("ts_start")) or 0.0),
                    str(item.get("span_id") or ""),
                ),
            )
            for item in sorted_errors:
                error_rows.append(
                    {
                        "ts": _format_ts_utc(item.get("ts_start")),
                        "outcome": item.get("outcome") or "–",
                        "error_code": item.get("error_code") or "–",
                        "llm": " ".join(
                            part for part in [item.get("llm_provider"), item.get("llm_model")] if part
                        )
                        or "–",
                        "total_ms": _format_number(item.get("total_duration_ms")),
                    }
                )

        scope_hint = None
        if account_email:
            scope_hint = f"{account_email} • last {window_days or 7} days"

        return _render_template(
            app,
            "latency.html",
            title=app.config["APP_TITLE"],
            page_title="Latency",
            error=error_message,
            scope_hint=scope_hint,
            dashboard_vars=dashboard_vars,
            account_options=accounts,
            account_email=account_email,
            account_emails_value=",".join(account_emails),
            window_options=[7, 30, 90],
            window_days=window_days or 7,
            metrics_cards=metrics_cards,
            stage_breakdown=stage_breakdown,
            slowest_spans=slowest_rows,
            recent_errors=error_rows,
            sample_size=sample_size,
            activity_rows=activity_table_rows,
            pii_allowed=bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")),
            pii_enabled=reveal_pii,
        )

    @app.route("/attention", methods=["GET"])
    def attention():
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_emails, window_days, sort_mode, error = _validate_attention_params(
            args=request.args, default_account=default_account
        )
        error_message = error or ""
        if error_message:
            summary = {
                "window_days": window_days or 30,
                "account_emails": account_emails,
                "limit": 50,
                "sort": sort_mode,
                "totals": {
                    "estimated_read_minutes": 0.0,
                    "message_count": 0,
                    "estimated_cost": 0.0,
                },
                "entities": [],
                "lane_breakdown": [],
                "top_contact_label": "",
                "generated_at_utc": "",
            }
        else:
            summary = _attention_payload(
                account_emails=account_emails,
                window_days=window_days or 30,
                sort_mode=sort_mode,
            )
        generated_at = summary.get("generated_at_utc") or ""
        scope_hint = ""
        if account_emails:
            if len(account_emails) == 1:
                scope_hint = f"{account_emails[0]} • last {window_days or 30} days"
            else:
                scope_hint = f"{len(account_emails)} accounts • last {window_days or 30} days"
        total_minutes = float(
            summary.get("totals", {}).get("estimated_read_minutes") or 0.0
        )
        total_hours = round(total_minutes / 60.0, 2)
        attention_cost = float(app.config.get("ATTENTION_COST_PER_HOUR", 0.0))
        csv_params: dict[str, str] = {
            "account_emails": ",".join(account_emails),
            "window_days": str(window_days or 30),
            "sort": sort_mode,
        }
        attention_csv_url = url_for("attention_csv", **csv_params)
        sort_options = [
            {"value": "time", "label": "time", "selected": sort_mode == "time"},
            {"value": "cost", "label": "cost", "selected": sort_mode == "cost"},
            {"value": "count", "label": "count", "selected": sort_mode == "count"},
        ]
        dashboard_vars = _dashboard_vars()
        return _render_template(
            app,
            "attention.html",
            title=app.config["APP_TITLE"],
            page_title="Attention economics",
            scope_hint=scope_hint,
            dashboard_vars=dashboard_vars,
            hide_limit=True,
            error=error_message or None,
            attention_url=url_for("attention"),
            attention_csv_url=attention_csv_url,
            account_emails_value=",".join(account_emails),
            window_days=window_days or 30,
            sort_options=sort_options,
            summary=summary,
            generated_at=generated_at,
            total_hours=total_hours,
            attention_cost_per_hour=attention_cost,
            lane_labels=LANE_LABELS,
        )

    @app.route("/attention.csv", methods=["GET"])
    def attention_csv():
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_emails, window_days, sort_mode, error = _validate_attention_params(
            args=request.args, default_account=default_account
        )
        if error:
            resp = jsonify({"error": error})
            resp.status_code = 400
            return resp
        summary = _attention_payload(
            account_emails=account_emails,
            window_days=window_days or 30,
            sort_mode=sort_mode,
        )
        rows = summary.get("entities", [])
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Contact", "Emails", "Attention minutes", "Estimated cost", "Signals"])
        for row in rows if isinstance(rows, list) else []:
            writer.writerow(
                [
                    str(row.get("entity_label") or ""),
                    int(row.get("message_count") or 0),
                    f"{float(row.get('estimated_read_minutes') or 0.0):.2f}",
                    str(row.get("estimated_cost") or ""),
                    str(row.get("signals") or ""),
                ]
            )
        csv_text = output.getvalue()
        response = Response(csv_text.encode("utf-8"), mimetype="text/csv; charset=utf-8")
        response.headers = {"Content-Disposition": "attachment; filename=attention.csv"}
        return response

    @app.route("/learning", methods=["GET"])
    def learning():
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_email, account_emails, window_days, limit, error = _validate_learning_params(
            args=request.args, default_account=default_account
        )
        error_message = error or ("Account selection required" if not account_email else "")
        analytics = _analytics()
        summary: dict[str, object] | None = None
        timeline: dict[str, object] | None = None
        calibration_report: dict[str, object] | None = None
        decision_trace_health: dict[str, object] | None = None
        if not error_message and account_email:
            now_ts = datetime.now(timezone.utc).timestamp()
            resolved_window = window_days or 30
            resolved_limit = limit or 50
            summary = analytics.behavioral_metrics_summary(
                account_email=account_email,
                account_emails=account_emails,
                window_days=resolved_window,
                now_ts=now_ts,
            )
            timeline = analytics.learning_timeline(
                account_email=account_email,
                account_emails=account_emails,
                window_days=resolved_window,
                limit=resolved_limit,
                now_ts=now_ts,
            )
            calibration_report = compute_priority_calibration_report(
                db_path=app.config["DB_PATH"],
                days=resolved_window,
                max_rows=1000,
                now_ts_utc=now_ts,
            )
            decision_trace_health = _decision_trace_health_payload(
                app.config["DB_PATH"], limit=300
            )
        account_options = _build_select_options(accounts, account_email)
        window_options = _build_window_options(window_days or 30)
        limit_value = str(limit or 50)
        error_block = f'<div class="alert">{html.escape(error_message)}</div>' if error_message else ""
        return _render_template(
            app,
            "learning.html",
            title=app.config["APP_TITLE"],
            static_url=_static_url(),
            latency_url=url_for("latency"),
            health_url=url_for("health"),
            attention_url=url_for("attention"),
            events_url=url_for("events"),
            relationships_url=url_for("relationships"),
            learning_url=url_for("learning"),
            error_block=error_block,
            account_options=account_options,
            account_emails_value=",".join(account_emails),
            window_options=window_options,
            limit_value=limit_value,
            summary=summary or {},
            timeline=timeline or {"items": []},
            calibration=calibration_report or {},
            decision_trace_health=decision_trace_health or {},
        )

    @app.route("/health", methods=["GET"])
    def health():
        dashboard_vars = _dashboard_vars()
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_email = (request.args.get("account_email") or "").strip()
        if not account_email and dashboard_vars.account_emails:
            account_email = dashboard_vars.account_emails[0]
        if not account_email:
            account_email = default_account or ""
        account_emails = dashboard_vars.account_emails or ([] if not account_email else [account_email])
        if account_email and account_email not in account_emails:
            account_emails.append(account_email)
        account_emails = sorted({email for email in account_emails if email})
        window_days = dashboard_vars.window_days or 7
        reveal_pii = bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")) and bool(
            dashboard_vars.pii
        )
        mode = _resolve_cockpit_mode(request, session)
        include_engineer = mode == "engineer"

        summary = _health_summary_payload(
            account_emails=account_emails,
            window_days=window_days,
            reveal_pii=reveal_pii,
            mode=mode,
        )
        current = summary.get("current") if isinstance(summary, Mapping) else None
        status_strip = summary.get("status_strip") if isinstance(summary, Mapping) else None
        metrics_digest = summary.get("metrics_digest") if isinstance(summary, Mapping) else {}
        metrics_brief = summary.get("metrics_brief") if isinstance(summary, Mapping) else {}
        trend = summary.get("trend") if isinstance(summary, Mapping) else []

        incidents = _health_incidents_payload(
            account_emails=account_emails,
            window_days=window_days,
            reveal_pii=reveal_pii,
            mode=mode,
        )
        components = _health_component_payload(
            current=current if isinstance(current, Mapping) else None,
            status_strip=status_strip if isinstance(status_strip, Mapping) else None,
            incidents=incidents,
            account_emails=account_emails,
            window_days=window_days,
            reveal_pii=reveal_pii,
            mode=mode,
        )

        if isinstance(current, Mapping):
            system_mode = str(current.get("system_mode") or "")
            gates_state = current.get("gates_state") if isinstance(current.get("gates_state"), Mapping) else {}
            last_snapshot = _format_ts_utc(current.get("ts_end_utc"))
        else:
            system_mode = ""
            gates_state = {}
            last_snapshot = "–"
        health_header = {
            "mode": system_mode or "unknown",
            "mode_class": _status_class_for_mode(system_mode),
            "last_snapshot": last_snapshot,
            "explanation": _health_mode_explanation(system_mode, gates_state),
        }

        health_signals = _health_golden_signals_view(
            summary=metrics_digest if isinstance(metrics_digest, Mapping) else {},
            metrics_brief=metrics_brief if isinstance(metrics_brief, Mapping) else {},
        )
        trend_rows = _health_trend_view(trend if isinstance(trend, list) else [])

        engineer_timeline_rows: list[dict[str, object]] = []
        if include_engineer and account_emails:
            analytics = _analytics()
            engineer_timeline = analytics.processing_spans_health_timeline(
                account_email=account_emails[0],
                account_emails=account_emails,
                window_days=window_days,
                limit=200,
            )
            engineer_timeline_rows = _health_trend_view(engineer_timeline)

        scope_hint = None
        if account_email:
            scope_hint = f"{account_email} • last {window_days} days"

        def _mode_link(target: str) -> str:
            params = {k: v for k, v in request.args.items()}
            if "account_emails" not in params and account_emails:
                params["account_emails"] = ",".join(account_emails)
            if "window_days" not in params:
                params["window_days"] = str(window_days)
            if reveal_pii and "pii" not in params:
                params["pii"] = "1"
            params["mode"] = target
            return url_for("health", **params)

        error_message = ""
        if not account_emails:
            error_message = "Account scope required to display health snapshots."

        return _render_template(
            app,
            "health.html",
            title=app.config["APP_TITLE"],
            page_title="Health Cockpit",
            scope_hint=scope_hint,
            dashboard_vars=dashboard_vars,
            account_email=account_email,
            pii_allowed=bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")),
            pii_enabled=reveal_pii,
            cockpit_mode=mode,
            mode_basic_url=_mode_link("basic"),
            mode_engineer_url=_mode_link("engineer"),
            engineer_mode=include_engineer,
            status_strip=status_strip or {},
            health_header=health_header,
            health_signals=health_signals,
            health_trend=trend_rows,
            engineer_timeline=engineer_timeline_rows,
            component_matrix=components,
            incidents=incidents,
            status_refresh_ms=STATUS_STRIP_REFRESH_MS,
            health_refresh_ms=HEALTH_REFRESH_MS,
            error=error_message,
            hide_limit=True,
        )

    @app.route("/partial/health", methods=["GET"])
    def health_partial():
        dashboard_vars = _dashboard_vars()
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_email = (request.args.get("account_email") or "").strip()
        if not account_email and dashboard_vars.account_emails:
            account_email = dashboard_vars.account_emails[0]
        if not account_email:
            account_email = default_account or ""
        account_emails = dashboard_vars.account_emails or ([] if not account_email else [account_email])
        if account_email and account_email not in account_emails:
            account_emails.append(account_email)
        account_emails = sorted({email for email in account_emails if email})
        window_days = dashboard_vars.window_days or 7
        reveal_pii = bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")) and bool(
            dashboard_vars.pii
        )
        mode = _resolve_cockpit_mode(request, session)

        summary = _health_summary_payload(
            account_emails=account_emails,
            window_days=window_days,
            reveal_pii=reveal_pii,
            mode=mode,
        )
        current = summary.get("current") if isinstance(summary, Mapping) else None
        status_strip = summary.get("status_strip") if isinstance(summary, Mapping) else None
        incidents = _health_incidents_payload(
            account_emails=account_emails,
            window_days=window_days,
            reveal_pii=reveal_pii,
            mode=mode,
        )
        components = _health_component_payload(
            current=current if isinstance(current, Mapping) else None,
            status_strip=status_strip if isinstance(status_strip, Mapping) else None,
            incidents=incidents,
            account_emails=account_emails,
            window_days=window_days,
            reveal_pii=reveal_pii,
            mode=mode,
        )
        return _render_template(
            app,
            "partials/health_overview.html",
            component_matrix=components,
            incidents=incidents,
        )

    @app.route("/events", methods=["GET"])
    def events():
        dashboard_vars = _dashboard_vars()
        account_emails = _resolve_account_scope(dashboard_vars)
        window_days, window_error = _parse_window_days(
            request.args.get("window_days"),
            default=30,
            allowed=ALLOWED_WINDOWS,
        )
        lane = _parse_lane(request.args.get("lane"))
        event_filter, filter_error = _parse_event_filter(request.args.get("type"))
        page = _parse_page(request.args.get("page"), default=1)
        reveal_pii = bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")) and bool(
            dashboard_vars.pii
        )
        error_message = window_error or filter_error
        if not account_emails:
            error_message = error_message or "Account scope required to view events."

        adjusted_dashboard = DashboardVars(
            account_emails=account_emails,
            window_days=window_days or 30,
            limit=dashboard_vars.limit,
            pii=dashboard_vars.pii,
        )
        scope_hint = None
        if account_emails:
            scope_hint = f"{account_emails[0]} • last {window_days or 30} days"

        analytics = _analytics()
        narrative: dict[str, object] = {"groups": [], "total_groups": 0, "page": page}
        lane_counts: dict[str, int] = {key: 0 for key in LANE_KEYS}
        if not error_message and window_days:
            cache_key = (
                "events_narrative_lane",
                str(app.config["DB_PATH"]),
                tuple(account_emails),
                window_days,
                lane,
                event_filter,
                page,
                bool(reveal_pii),
                int(time.time()) // 15,
            )
            cached = _EVENTS_NARRATIVE_CACHE.get(cache_key)
            if isinstance(cached, Mapping):
                narrative = dict(cached)
            else:
                if hasattr(analytics, "lane_event_groups"):
                    narrative = analytics.lane_event_groups(
                        account_email=account_emails[0],
                        account_emails=account_emails,
                        window_days=window_days,
                        lane=lane,
                        event_filter=event_filter,
                        page=page,
                        page_size=EVENTS_GROUP_PAGE_SIZE,
                        reveal_pii=reveal_pii,
                    )
                else:
                    narrative = analytics.events_narrative_v1(
                        account_email=account_emails[0],
                        account_emails=account_emails,
                        window_days=window_days,
                        event_filter=event_filter,
                        page=page,
                        page_size=EVENTS_GROUP_PAGE_SIZE,
                        reveal_pii=reveal_pii,
                    )
                _EVENTS_NARRATIVE_CACHE.set(cache_key, narrative)
            if hasattr(analytics, "lane_counts"):
                lane_cache_key = (
                    "lane_counts",
                    str(app.config["DB_PATH"]),
                    tuple(account_emails),
                    window_days,
                )
                cached_counts = _COCKPIT_CACHE.get(lane_cache_key)
                if isinstance(cached_counts, Mapping):
                    lane_counts = {key: int(cached_counts.get(key) or 0) for key in LANE_KEYS}
                else:
                    lane_counts = analytics.lane_counts(
                        account_email=account_emails[0],
                        account_emails=account_emails,
                        window_days=window_days,
                    )
                    _COCKPIT_CACHE.set(lane_cache_key, lane_counts)

        groups: list[dict[str, object]] = []
        for group in narrative.get("groups", []):
            headline = group.get("headline") if isinstance(group, Mapping) else {}
            group_kind = group.get("group_kind")
            group_id = group.get("group_id")
            forensics_url = None
            if group_kind == "email":
                headline = _sanitize_event_headline(headline)
            if group_kind == "email" and group_id is not None:
                params = {
                    "account_emails": ",".join(account_emails),
                    "window_days": str(window_days or 30),
                }
                if reveal_pii:
                    params["pii"] = "1"
                params["id"] = str(int(group_id))
                forensics_url = url_for("email_detail_redirect", **params)
            status_label = ""
            if isinstance(headline, Mapping):
                if group_kind == "email":
                    status_label = str(headline.get("delivery_status") or "")
                else:
                    status_label = str(headline.get("status_label") or "")

            groups.append(
                {
                    "group_kind": group_kind,
                    "group_id": group_id,
                    "ts_first": _format_ts_utc(group.get("ts_first")),
                    "ts_last": _format_ts_utc(group.get("ts_last")),
                    "event_count": group.get("event_count") or 0,
                    "headline": headline or {},
                    "timeline": [
                        {
                            "ts": _format_ts_utc(item.get("ts_utc")),
                            "event_type": item.get("event_type") or "",
                            "stage": item.get("stage") or "",
                            "outcome": item.get("outcome") or "",
                            "notes_safe": item.get("notes_safe") or "",
                        }
                        for item in (group.get("timeline") or [])
                    ],
                    "forensics_url": forensics_url,
                    "status_label": status_label,
                    "status_class": _status_class_for_label(status_label),
                }
            )

        total_groups = int(narrative.get("total_groups") or 0)
        total_pages = max(1, int(math.ceil(total_groups / EVENTS_GROUP_PAGE_SIZE))) if total_groups else 1
        if page > total_pages:
            page = total_pages

        def _base_params() -> dict[str, str]:
            params: dict[str, str] = {}
            if account_emails:
                params["account_emails"] = ",".join(account_emails)
            if window_days:
                params["window_days"] = str(window_days)
            if lane:
                params["lane"] = lane
            if event_filter and event_filter != "all":
                params["type"] = event_filter
            if reveal_pii:
                params["pii"] = "1"
            return params

        base_params = _base_params()
        prev_url = None
        if page > 1:
            prev_params = dict(base_params)
            prev_params["page"] = str(page - 1)
            prev_url = url_for("events", **prev_params)
        next_url = None
        if page < total_pages:
            next_params = dict(base_params)
            next_params["page"] = str(page + 1)
            next_url = url_for("events", **next_params)

        return _render_template(
            app,
            "events.html",
            title=app.config["APP_TITLE"],
            page_title="Events Narrative",
            dashboard_vars=adjusted_dashboard,
            scope_hint=scope_hint,
            pii_allowed=bool(app.config.get("WEB_OBSERVABILITY_ALLOW_PII")),
            pii_enabled=reveal_pii,
            error=error_message,
            hide_limit=True,
            lane=lane,
            lane_pills=_build_lane_pills(
                selected_lane=lane,
                counts=lane_counts,
                base_params=base_params,
                endpoint="events",
            ),
            groups=groups,
            event_filter=event_filter,
            page=page,
            total_pages=total_pages,
            total_groups=total_groups,
            prev_url=prev_url,
            next_url=next_url,
            share_url=url_for("events", **base_params, page=page),
        )

    @app.route("/email", methods=["GET"])
    def email_detail_redirect():
        email_id_raw = request.args.get("id")
        try:
            email_id = int(email_id_raw or 0)
        except (TypeError, ValueError):
            email_id = 0
        if email_id <= 0:
            return redirect(url_for("archive"))
        params: dict[str, str] = {}
        for key in ("account_emails", "window_days", "status", "page", "mode", "pii"):
            value = request.args.get(key)
            if value:
                params[key] = value
        return redirect(url_for("email_details", email_id=email_id, **params))

    @app.route("/relationships", methods=["GET"])
    def relationships():
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_email, account_emails, window_days, error = _validate_latency_params(
            args=request.args,
            require_account=False,
            default_account=default_account,
            window_default=30,
        )
        limit, limit_error = _parse_limit(request.args.get("limit"), default=50, max_limit=200)
        error_message = error or limit_error or (
            "Account selection required" if not account_email else ""
        )
        analytics = _analytics()
        graph: dict[str, object] | None = None
        if not error_message and account_email:
            graph = analytics.relationship_graph(
                account_email=account_email,
                account_emails=account_emails,
                window_days=window_days or 30,
                limit=limit or 50,
            )
        account_options = _build_select_options(accounts, account_email)
        window_options = _build_window_options(window_days or 30)
        limit_value = str(limit or 50)
        error_block = f'<div class="alert">{html.escape(error_message)}</div>' if error_message else ""
        graph_json = json.dumps(graph or {}, ensure_ascii=False)
        return _render_template(
            app,
            "relationships.html",
            title=app.config["APP_TITLE"],
            static_url=_static_url(),
            latency_url=url_for("latency"),
            health_url=url_for("health"),
            attention_url=url_for("attention"),
            events_url=url_for("events"),
            relationships_url=url_for("relationships"),
            error_block=error_block,
            account_options=account_options,
            account_emails_value=",".join(account_emails),
            window_options=window_options,
            limit_value=limit_value,
            graph_json=graph_json,
        )

    @app.route("/relationships/contact", methods=["GET"])
    def relationships_contact():
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_email, account_emails, window_days, error = _validate_latency_params(
            args=request.args,
            require_account=False,
            default_account=default_account,
            window_default=30,
        )
        contact_id = request.args.get("contact_id", "")
        error_message = error or ("contact_id is required" if not contact_id else "")
        analytics = _analytics()
        detail: dict[str, object] | None = None
        if not error_message and account_email:
            detail = analytics.relationship_contact_detail(
                account_email=account_email,
                account_emails=account_emails,
                contact_id=contact_id,
                window_days=window_days or 30,
            )
            if not detail:
                error_message = "Contact unavailable"
        account_options = _build_select_options(accounts, account_email)
        window_options = _build_window_options(window_days or 30)
        error_block = f'<div class="alert">{html.escape(error_message)}</div>' if error_message else ""
        detail_json = json.dumps(detail or {}, ensure_ascii=False)
        return _render_template(
            app,
            "relationships_contact.html",
            title=app.config["APP_TITLE"],
            static_url=_static_url(),
            latency_url=url_for("latency"),
            health_url=url_for("health"),
            attention_url=url_for("attention"),
            events_url=url_for("events"),
            relationships_url=url_for("relationships"),
            error_block=error_block,
            account_options=account_options,
            account_emails_value=",".join(account_emails),
            window_options=window_options,
            contact_id=html.escape(contact_id),
            detail_json=detail_json,
        )

    return app


def _build_access_urls(*, bind_address: str, port: int) -> tuple[str, str | None]:
    if _is_loopback_bind(bind_address):
        return f"http://127.0.0.1:{port}/", None
    if bind_address == "0.0.0.0":
        detected = get_primary_ipv4()
        local_url = f"http://127.0.0.1:{port}/"
        if detected:
            return local_url, f"http://{detected}:{port}/"
        return local_url, None
    try:
        parsed = ipaddress.ip_address(bind_address)
    except ValueError:
        return f"http://127.0.0.1:{port}/", None
    if parsed.version == 4 and parsed.is_private:
        return f"http://127.0.0.1:{port}/", f"http://{bind_address}:{port}/"
    return f"http://127.0.0.1:{port}/", None




def main() -> None:
    require_runtime_for("web_ui")
    parser = argparse.ArgumentParser(description="Letterbot Observability Console")
    parser.add_argument("--db", type=Path, help="Path to SQLite database")
    parser.add_argument("--config", type=Path, default=CONFIG_DIR, help="Config directory")
    parser.add_argument("--config-yaml", type=Path, help="Path to config.yaml")
    parser.add_argument("--bind", help="Bind address (default from settings.ini [web].host)")
    parser.add_argument("--port", type=int, help="Port to listen on (default from settings.ini [web].port)")
    args = parser.parse_args()

    config_dir = args.config if args.config else CONFIG_DIR
    config_path = _resolve_yaml_config_path(args.config_yaml, config_dir)
    web_ui = _load_web_ui_settings(config_path)
    support_settings = _load_support_settings(config_path)
    if not web_ui.enabled:
        raise RuntimeError("web_ui.enabled=false: refusing to start Web UI")

    web_runtime = load_web_config(config_dir)

    bind_address = args.bind or web_runtime.host or web_ui.bind or "127.0.0.1"
    port = args.port or web_runtime.port or web_ui.port or 8787
    if not _is_loopback_bind(bind_address):
        if not web_ui.allow_lan:
            raise RuntimeError("web_ui.allow_lan=false: bind outside loopback refused")
        if not web_ui.allow_cidrs:
            raise RuntimeError("web_ui.allow_cidrs must be set when allow_lan=true")
        if not web_ui.prod_server:
            raise RuntimeError(
                "web_ui.prod_server=false: bind outside loopback requires waitress server (set web_ui.prod_server=true)"
            )
    if web_ui.allow_lan:
        logger.info(
            "WEB_UI_LAN_ENABLED bind=%s port=%s allow_cidrs=%s",
            bind_address,
            port,
            web_ui.allow_cidrs,
        )

    if web_ui.prod_server:
        require_runtime_for("web_ui_prod")

    secret_key, attention_cost = _load_web_ui_secrets(config_dir)
    env_password = str(os.environ.get("WEB_PASSWORD") or "").strip()
    yaml_password = str(web_ui.password or "").strip()
    ini_password = load_web_ui_password_from_ini(config_dir)
    if env_password:
        password = env_password
    elif yaml_password:
        password = yaml_password
    else:
        password = ini_password
    if not password:
        logger.warning("web_ui_password_not_configured_using_empty_password")

    if args.db:
        db_path = args.db
    else:
        storage = load_storage_config(config_dir)
        db_path = storage.db_path

    local_url, lan_url = _build_access_urls(bind_address=str(bind_address), port=int(port))
    logger.info("WEB_UI_URL_LOCAL %s", local_url)
    if lan_url:
        logger.info("WEB_UI_URL_LAN %s", lan_url)
        logger.info("WEB_UI_FIREWALL_HINT Firewall may block incoming connections; see docs.")

    project_root = Path(__file__).resolve().parents[2]
    app = create_app(
        db_path=db_path,
        password=password,
        secret_key=secret_key,
        attention_cost_per_hour=attention_cost,
        api_token=web_ui.api_token,
        allow_cidrs=web_ui.allow_cidrs,
        config_path=config_path,
        log_path=project_root / "mailbot.log",
        dist_root=project_root,
        web_ui_bind=bind_address,
        web_ui_port=int(port),
        support_settings=support_settings,
    )
    try:
        if web_ui.prod_server:
            from waitress import serve

            serve(app, host=str(bind_address), port=int(port))
            return

        app.run(
            host=str(bind_address),
            port=int(port),
            debug=False,
            use_reloader=False,
            threaded=True,
        )
    except OSError as exc:
        if "Address already in use" in str(exc):
            print(
                f"[ERROR] Порт {port} занят. Откройте settings.ini в каталоге конфигурации и измените [web] port = ..."
            )
            raise SystemExit(1)
        raise


if __name__ == "__main__":
    try:
        main()
    except DependencyError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2)
