from __future__ import annotations

import argparse
import configparser
import html
import logging
import os
import sqlite3
from pathlib import Path
from typing import Iterable, Optional

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
        return '<p class="muted">No data available.</p>'
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
        return '<p class="muted">No recent errors.</p>'
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
        return '<p class="muted">No slow spans found.</p>'
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


def _load_credentials(config_dir: Path) -> tuple[str, str]:
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
    return password, secret_key


def _parse_account_emails(raw: str | None) -> list[str]:
    if not raw:
        return []
    emails = [item.strip() for item in raw.split(",") if item.strip()]
    return sorted(dict.fromkeys(emails))


def _parse_window_days(raw: Optional[str]) -> tuple[Optional[int], Optional[str]]:
    if raw is None or raw == "":
        return 7, None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None, "window_days must be an integer"
    if value not in ALLOWED_WINDOWS:
        return None, "window_days must be one of 7, 30, 90"
    return value, None


def _validate_latency_params(
    *,
    args,
    require_account: bool = True,
    default_account: str | None = None,
) -> tuple[Optional[str], list[str], Optional[int], Optional[str]]:
    account_email = (args.get("account_email") or "").strip()
    account_emails = _parse_account_emails(args.get("account_emails"))
    window_days, error = _parse_window_days(args.get("window_days"))
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


def _available_accounts(db_path: Path) -> list[str]:
    query = "SELECT DISTINCT account_id FROM processing_spans ORDER BY account_id ASC"
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            rows = conn.execute(query).fetchall()
    except sqlite3.OperationalError:
        return []
    return [str(row[0]) for row in rows if row and row[0]]


def _ensure_authenticated() -> bool:
    return bool(session.get("authenticated"))


def create_app(
    *, db_path: Path, password: str, secret_key: str, title: str = "Observability Console"
) -> Flask:
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parent / "templates"),
        static_folder=str(Path(__file__).parent / "static"),
    )
    app.config["DB_PATH"] = Path(db_path)
    app.config["WEB_PASSWORD"] = password
    app.config["APP_TITLE"] = title
    app.secret_key = secret_key

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
        return KnowledgeAnalytics(app.config["DB_PATH"])

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

    @app.route("/latency", methods=["GET"])
    def latency():
        accounts = _available_accounts(app.config["DB_PATH"])
        default_account = accounts[0] if accounts else None
        account_email, account_emails, window_days, error = _validate_latency_params(
            args=request.args, require_account=False, default_account=default_account
        )
        error_message = error or ("No account data available" if not account_email else "")
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
        )

    @app.route("/health", methods=["GET"])
    def health():
        return _render_template(
            app,
            "health.html",
            title=app.config["APP_TITLE"],
            static_url=_static_url(),
            latency_url=url_for("latency"),
            health_url=url_for("health"),
            error_block="",
        )

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="MailBot Observability Console")
    parser.add_argument("--db", type=Path, help="Path to SQLite database")
    parser.add_argument("--config", type=Path, default=CONFIG_DIR, help="Config directory")
    parser.add_argument("--bind", default="127.0.0.1", help="Bind address (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    args = parser.parse_args()

    config_dir = args.config if args.config else CONFIG_DIR
    password, secret_key = _load_credentials(config_dir)

    if args.db:
        db_path = args.db
    else:
        storage = load_storage_config(config_dir)
        db_path = storage.db_path

    app = create_app(db_path=db_path, password=password, secret_key=secret_key)
    app.run(host=args.bind, port=args.port)


if __name__ == "__main__":
    main()
