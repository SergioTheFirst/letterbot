from __future__ import annotations

import argparse
import configparser
import html
import ipaddress
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Optional

try:
    from flask import (
        Flask,
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
        jsonify,
        redirect,
        render_template,
        request,
        session,
        url_for,
    )

    USING_FLASK_STUB = True

from mailbot_v26.config_loader import CONFIG_DIR, load_storage_config
from mailbot_v26.storage.analytics import KnowledgeAnalytics

logger = logging.getLogger(__name__)

ALLOWED_WINDOWS = {7, 30, 90}


def _open_readonly_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        conn.execute("PRAGMA query_only = ON")
    except sqlite3.Error:
        conn.close()
        raise
    return conn


def _render_template(app: Flask, template_name: str, **context: object) -> str:
    if USING_FLASK_STUB:
        template_path = Path(app.template_folder or "") / template_name
        return render_template(str(template_path), **context)
    return render_template(template_name, **context)


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


def _format_number(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "–"
    if number.is_integer():
        return str(int(number))
    return f"{number:.2f}"


def _metrics_block(summary: dict[str, object] | None) -> str:
    if not summary:
        return ""
    cards = []
    cards.append(
        """
        <div class="metric"><div class="label">Total avg (ms)</div><div class="value">{}</div></div>
        """.format(_format_number(summary.get("total_duration_ms_avg")))
    )
    cards.append(
        """
        <div class="metric"><div class="label">Total p50 / p90 / p95 (ms)</div>
        <div class="value">{} / {} / {}</div></div>
        """.format(
            _format_number(summary.get("total_duration_ms_p50")),
            _format_number(summary.get("total_duration_ms_p90")),
            _format_number(summary.get("total_duration_ms_p95")),
        )
    )
    cards.append(
        """
        <div class="metric"><div class="label">LLM avg (ms)</div><div class="value">{}</div></div>
        """.format(_format_number(summary.get("llm_latency_ms_avg")))
    )
    cards.append(
        """
        <div class="metric"><div class="label">LLM p50 / p90 / p95 (ms)</div>
        <div class="value">{} / {} / {}</div></div>
        """.format(
            _format_number(summary.get("llm_latency_ms_p50")),
            _format_number(summary.get("llm_latency_ms_p90")),
            _format_number(summary.get("llm_latency_ms_p95")),
        )
    )
    error_rate = summary.get("error_rate")
    fallback_rate = summary.get("fallback_rate")
    cards.append(
        """
        <div class="metric"><div class="label">Error rate</div><div class="value">{}%</div></div>
        """.format(
            _format_number(float(error_rate) * 100) if error_rate is not None else "–"
        )
    )
    cards.append(
        """
        <div class="metric"><div class="label">Fallback rate</div><div class="value">{}%</div></div>
        """.format(
            _format_number(float(fallback_rate) * 100) if fallback_rate is not None else "–"
        )
    )

    stage_rows = ""
    stage_data = summary.get("stage_durations") if isinstance(summary, dict) else None
    if isinstance(stage_data, dict) and stage_data:
        rows = []
        for stage, stats in stage_data.items():
            rows.append(
                """
                <tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>
                """.format(
                    html.escape(str(stage)),
                    _format_number(stats.get("avg")),
                    _format_number(stats.get("p50")),
                    _format_number(stats.get("p90")),
                    _format_number(stats.get("p95")),
                )
            )
        stage_rows = (
            "<h3>Stage durations</h3>"
            + "<table class=\"data-table\"><thead><tr><th>Stage</th><th>Avg (ms)</th><th>p50</th><th>p90</th><th>p95</th></tr></thead>"
            + f"<tbody>{''.join(rows)}</tbody></table>"
        )
    return f"<div class=\"metrics-grid\">{''.join(cards)}</div>{stage_rows}"


def _errors_block(recent_errors: list[dict[str, object]]) -> str:
    if not recent_errors:
        return ""
    rows = []
    for item in recent_errors:
        rows.append(
            """
            <tr><td>{}</td><td>{}</td><td>{}</td><td>{} {}</td><td>{}</td></tr>
            """.format(
                html.escape(str(item.get("ts_start") or "")),
                html.escape(str(item.get("outcome") or "")),
                html.escape(str(item.get("error_code") or "")),
                html.escape(str(item.get("llm_provider") or "")),
                html.escape(str(item.get("llm_model") or "")),
                _format_number(item.get("total_duration_ms")),
            )
        )
    return (
        "<table class=\"data-table\"><thead><tr><th>Timestamp (UTC)</th><th>Outcome</th><th>Error</th><th>LLM</th><th>Total ms</th></tr></thead>"
        + f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _slow_block(slowest: list[dict[str, object]]) -> str:
    if not slowest:
        return ""
    rows = []
    for item in slowest:
        rows.append(
            """
            <tr><td>{}</td><td>{}</td><td>{}</td><td>{} {}</td><td>{}</td></tr>
            """.format(
                html.escape(str(item.get("started_at") or "")),
                _format_number(item.get("total_ms")),
                html.escape(str(item.get("outcome") or "")),
                html.escape(str(item.get("llm_provider") or "")),
                html.escape(str(item.get("llm_model") or "")),
                html.escape(str(item.get("health_snapshot_id") or "")),
            )
        )
    return (
        "<table class=\"data-table\"><thead><tr><th>Started</th><th>Total ms</th><th>Outcome</th><th>LLM</th><th>Health snapshot</th></tr></thead>"
        + f"<tbody>{''.join(rows)}</tbody></table>"
    )


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


def _load_credentials(config_dir: Path) -> tuple[str, str, float]:
    parser = configparser.ConfigParser()
    config_path = config_dir / "config.ini"
    if config_path.exists():
        try:
            parser.read(config_path, encoding="utf-8")
        except (OSError, configparser.Error):
            logger.warning("Failed to read config.ini from %s", config_path)
    password = os.environ.get("WEB_PASSWORD") or parser.get(
        "general", "web_password", fallback=""
    )
    secret_key = os.environ.get("WEB_SECRET_KEY") or parser.get(
        "general", "web_secret_key", fallback=""
    )
    if not password:
        raise RuntimeError("WEB_PASSWORD environment variable or [general] web_password is required")
    if not secret_key:
        raise RuntimeError(
            "WEB_SECRET_KEY environment variable or [general] web_secret_key is required"
        )
    try:
        attention_cost = float(
            parser.get("general", "attention_cost_per_hour", fallback="0")
        )
    except (TypeError, ValueError, configparser.Error):
        attention_cost = 0.0
    attention_cost = max(0.0, attention_cost)
    return password, secret_key, attention_cost


def _parse_account_emails(raw: str | None) -> list[str]:
    if not raw:
        return []
    emails = [item.strip() for item in raw.split(",") if item.strip()]
    return sorted(dict.fromkeys(emails))


def _parse_window_days(
    raw: Optional[str], default: int = 7
) -> tuple[Optional[int], Optional[str]]:
    if raw is None or raw == "":
        if default not in ALLOWED_WINDOWS:
            return None, "window_days must be one of 7, 30, 90"
        return default, None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None, "window_days must be an integer"
    if value not in ALLOWED_WINDOWS:
        return None, "window_days must be one of 7, 30, 90"
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


def _validate_latency_params(
    *,
    args,
    require_account: bool = True,
    default_account: str | None = None,
    window_default: int = 7,
) -> tuple[Optional[str], list[str], Optional[int], Optional[str]]:
    account_email = (args.get("account_email") or "").strip()
    account_emails = _parse_account_emails(args.get("account_emails"))
    window_days, error = _parse_window_days(args.get("window_days"), window_default)
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


def _parse_include_anomalies(raw: Optional[str]) -> tuple[bool | None, Optional[str]]:
    if raw is None or raw == "":
        return False, None
    raw_clean = str(raw).strip()
    if raw_clean == "1":
        return True, None
    if raw_clean == "0":
        return False, None
    return None, "include_anomalies must be 0 or 1"


def _validate_attention_params(
    *,
    args,
    default_account: str | None = None,
) -> tuple[Optional[str], list[str], Optional[int], Optional[int], bool | None, Optional[str]]:
    account_email = (args.get("account_email") or "").strip()
    account_emails = _parse_account_emails(args.get("account_emails"))
    window_days, window_error = _parse_window_days(args.get("window_days"), 30)
    if window_error:
        return None, [], None, None, None, window_error
    limit, limit_error = _parse_limit(args.get("limit"), default=50, max_limit=200, min_value=5)
    if limit_error:
        return None, [], None, None, None, limit_error
    include_anomalies, anomalies_error = _parse_include_anomalies(args.get("include_anomalies"))
    if anomalies_error:
        return None, [], None, None, None, anomalies_error
    if account_emails and account_email and account_email not in account_emails:
        return None, [], None, None, None, "account_email must match one of account_emails"
    if not account_email and account_emails:
        account_email = account_emails[0]
    if not account_email and default_account:
        account_email = default_account
    if not account_emails and account_email:
        account_emails = [account_email]
    if not account_email:
        return None, [], None, None, None, "account_email is required"
    return account_email, account_emails, window_days, limit, bool(include_anomalies), None


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
    fallback_query = "SELECT DISTINCT account_id FROM events_v1 ORDER BY account_id ASC"
    try:
        with _open_readonly_connection(db_path) as conn:
            rows = conn.execute(fallback_query).fetchall()
    except sqlite3.OperationalError:
        return []
    return [str(row[0]) for row in rows if row and row[0]]


def _ensure_authenticated() -> bool:
    return bool(session.get("authenticated"))


def create_app(
    *,
    db_path: Path,
    password: str,
    secret_key: str,
    title: str = "Observability Console",
    attention_cost_per_hour: float = 0.0,
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
    app.secret_key = secret_key
    app.config["ANALYTICS_FACTORY"] = lambda: KnowledgeAnalytics(
        app.config["DB_PATH"], read_only=True
    )

    @app.before_request
    def _require_login():
        open_paths = {"login", "static"}
        if request.endpoint in open_paths or request.path.startswith("/static"):
            return None
        if not _ensure_authenticated():
            return redirect(url_for("login", next=request.path))
        return None

    @app.route("/login", methods=["GET", "POST"])
    def login():
        error: str | None = None
        if request.method == "POST":
            provided = request.form.get("password", "")
            if provided == app.config["WEB_PASSWORD"]:
                session["authenticated"] = True
                session.permanent = False
                next_path = request.args.get("next")
                return redirect(next_path or url_for("latency"))
            error = "Invalid password"
        error_block = f'<div class="alert">{html.escape(error)}</div>' if error else ""
        return _render_template(
            app,
            "login.html",
            title=app.config["APP_TITLE"],
            error_block=error_block,
            static_url=_static_url(),
        )

    @app.route("/")
    def index():
        return redirect(url_for("latency"))

    def _analytics() -> KnowledgeAnalytics:
        factory = app.config.get("ANALYTICS_FACTORY")
        if callable(factory):
            return factory()
        return KnowledgeAnalytics(app.config["DB_PATH"], read_only=True)

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
            args=request.args, require_account=True, window_default=30
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
        (
            account_email,
            account_emails,
            window_days,
            limit,
            include_anomalies,
            error,
        ) = _validate_attention_params(args=request.args)
        if error:
            resp = jsonify({"error": error})
            resp.status_code = 400
            return resp
        analytics = _analytics()
        resolved_window = window_days or 30
        resolved_limit = limit or 50
        summary = analytics.attention_economics_summary(
            account_emails=account_emails,
            window_days=resolved_window,
            limit=resolved_limit,
            include_anomalies=bool(include_anomalies),
            attention_cost_per_hour=float(app.config.get("ATTENTION_COST_PER_HOUR", 0.0)),
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
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_email, account_emails, window_days, error = _validate_latency_params(
            args=request.args, require_account=False, default_account=default_account
        )
        error_message = error or ("Account selection required" if not account_email else "")
        analytics = _analytics()
        summary: dict[str, object] | None = None
        recent_errors: list[dict[str, object]] = []
        slowest: list[dict[str, object]] = []
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
        account_options = _build_select_options(accounts, account_email)
        window_options = _build_window_options(window_days or 7)
        error_block = f'<div class="alert">{html.escape(error_message)}</div>' if error_message else ""
        return _render_template(
            app,
            "latency.html",
            title=app.config["APP_TITLE"],
            error_block=error_block,
            account_options=account_options,
            account_emails_value=",".join(account_emails),
            window_options=window_options,
            metrics_block=_metrics_block(summary),
            errors_block=_errors_block(recent_errors),
            slow_block=_slow_block(slowest),
            static_url=_static_url(),
            latency_url=url_for("latency"),
            health_url=url_for("health"),
            attention_url=url_for("attention"),
            events_url=url_for("events"),
        )

    @app.route("/attention", methods=["GET"])
    def attention():
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        (
            account_email,
            account_emails,
            window_days,
            limit,
            include_anomalies,
            error,
        ) = _validate_attention_params(args=request.args, default_account=default_account)
        error_message = error or ("Account selection required" if not account_email else "")
        analytics = _analytics()
        summary: dict[str, object] | None = None
        if not error_message and account_email:
            summary = analytics.attention_economics_summary(
                account_emails=account_emails,
                window_days=window_days or 30,
                limit=limit or 50,
                include_anomalies=bool(include_anomalies),
            attention_cost_per_hour=float(app.config.get("ATTENTION_COST_PER_HOUR", 0.0)),
        )
        fallback_generated = datetime.fromtimestamp(0, tz=timezone.utc).isoformat()
        if fallback_generated.endswith("+00:00"):
            fallback_generated = fallback_generated.replace("+00:00", "Z")
        resolved_summary = summary or {
            "window_days": window_days or 30,
            "account_emails": account_emails,
            "limit": limit or 50,
            "totals": {
                "estimated_read_minutes": 0.0,
                "message_count": 0,
                "attachment_count": 0,
                "deferred_count": 0,
            },
            "entities": [],
            "generated_at_utc": fallback_generated,
        }
        account_options = _build_select_options(accounts, account_email)
        window_options = _build_window_options(window_days or 30)
        error_block = f'<div class="alert">{html.escape(error_message)}</div>' if error_message else ""
        return _render_template(
            app,
            "attention.html",
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
            limit_value=str(limit or 50),
            include_anomalies_checked="checked" if include_anomalies else "",
            summary=resolved_summary,
            attention_cost_per_hour=float(app.config.get("ATTENTION_COST_PER_HOUR", 0.0)),
        )

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
        )

    @app.route("/health", methods=["GET"])
    def health():
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_email, account_emails, window_days, error = _validate_latency_params(
            args=request.args,
            require_account=False,
            default_account=default_account,
            window_default=30,
        )
        error_block = (
            f'<div class="alert">{html.escape(error)}</div>' if error else ""
        )
        analytics = _analytics()
        current: dict[str, object] | None = None
        timeline: list[dict[str, object]] = []
        if not error and account_email:
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
        account_options = _build_select_options(accounts, account_email)
        window_options = _build_window_options(window_days or 30)
        return _render_template(
            app,
            "health.html",
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
            current_block=_health_current_block(current),
            timeline_block=_health_timeline_block(timeline),
        )

    @app.route("/events", methods=["GET"])
    def events():
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_email, account_emails, window_days, error = _validate_latency_params(
            args=request.args,
            require_account=False,
            default_account=default_account,
            window_default=30,
        )
        limit, limit_error = _parse_limit(request.args.get("limit"), default=200, max_limit=500)
        error_message = error or limit_error or (
            "Account selection required" if not account_email else ""
        )
        analytics = _analytics()
        items: list[dict[str, object]] = []
        if not error_message and account_email:
            resolved_window = window_days or 30
            resolved_limit = limit or 200
            items = analytics.events_timeline(
                account_email=account_email,
                account_emails=account_emails,
                window_days=resolved_window,
                limit=resolved_limit,
            )
        account_options = _build_select_options(accounts, account_email)
        window_options = _build_window_options(window_days or 30)
        limit_value = str(limit or 200)
        error_block = f'<div class="alert">{html.escape(error_message)}</div>' if error_message else ""
        return _render_template(
            app,
            "events.html",
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
            events_block=_events_table(items),
        )

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


def main() -> None:
    parser = argparse.ArgumentParser(description="MailBot Observability Console")
    parser.add_argument("--db", type=Path, help="Path to SQLite database")
    parser.add_argument("--config", type=Path, default=CONFIG_DIR, help="Config directory")
    parser.add_argument("--bind", default="127.0.0.1", help="Bind address (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    args = parser.parse_args()

    bind_address = args.bind or "127.0.0.1"
    try:
        parsed_bind = ipaddress.ip_address(bind_address)
        if not parsed_bind.is_loopback:
            raise RuntimeError("Bind address must be loopback (127.0.0.1 or ::1)")
    except ValueError:
        if bind_address.lower() != "localhost":
            raise RuntimeError("Bind address must be loopback (127.0.0.1 or ::1)")

    config_dir = args.config if args.config else CONFIG_DIR
    password, secret_key, attention_cost = _load_credentials(config_dir)

    if args.db:
        db_path = args.db
    else:
        storage = load_storage_config(config_dir)
        db_path = storage.db_path

    app = create_app(
        db_path=db_path,
        password=password,
        secret_key=secret_key,
        attention_cost_per_hour=attention_cost,
    )
    app.run(host=str(bind_address), port=args.port)


if __name__ == "__main__":
    main()
