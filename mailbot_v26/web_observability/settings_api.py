"""Settings lane web routes (S1: read-only mailbox status).

GET /accounts renders the status page, GET /api/accounts/status returns the
same status as JSON, and the Jinja global ``settings_accounts_home_card``
feeds the Home-page card partial (templates/settings_accounts_card.html) that
the dashboard can include.

Access is the dashboard rule (CONSTITUTION IV rev. 2026-10): no password on
this computer (loopback), a logged-in session over the network. The global
request guard in app.py enforces that; these routes also refuse a request
without a client address (never treated as loopback) and a non-loopback
request that is not signed in (the local smoke bypass trusts
X-Forwarded-For, settings never do). Nothing here writes a file or the
database.
"""

from __future__ import annotations

import logging
from functools import partial
from pathlib import Path
from typing import Any

from mailbot_v26.web_observability import app as web_app
from mailbot_v26.web_observability.accounts_status import (
    build_home_card,
    collect_accounts_status,
    default_runtime_health_path,
    unavailable_home_card,
)
from mailbot_v26.web_observability.settings_text import resolve_locale, text

logger = logging.getLogger(__name__)

ACCOUNTS_TEMPLATE = "accounts.html"
HOME_CARD_GLOBAL = "settings_accounts_home_card"
_FORBIDDEN_TEXT = (
    "Forbidden: the Settings pages open only on this computer or after signing "
    "in at /login."
)


def register_settings_routes(app: Any, *, config_dir: Path | None) -> None:
    """Add the S1 routes and the Home-card Jinja global to ``app``."""
    app.config["SETTINGS_CONFIG_DIR"] = (
        Path(config_dir) if config_dir is not None else None
    )
    app.config.setdefault(
        "SETTINGS_RUNTIME_HEALTH_PATH", default_runtime_health_path()
    )

    @app.route("/accounts", methods=["GET"])
    def accounts():
        denial = _access_denial(app)
        if denial is not None:
            return denial
        locale = _locale(app)
        status = _status_or_none(app, locale)
        title = text("accounts.title", locale)
        body = web_app._render_template(
            app,
            ACCOUNTS_TEMPLATE,
            title=f"{title} · LetterBot.ru",
            page_title=title,
            status=status,
            st=partial(text, locale=locale),
            http_warning=_http_warning(),
            hide_limit=True,
        )
        return _no_store(
            web_app._text_response(body, 200 if status is not None else 500)
        )

    @app.route("/api/accounts/status", methods=["GET"])
    def api_accounts_status():
        denial = _access_denial(app)
        if denial is not None:
            return denial
        status = _status_or_none(app, _locale(app))
        if status is None:
            response = web_app.jsonify({"error": "status_unavailable"})
            response.status_code = 500
            return _no_store(response)
        return _no_store(web_app.jsonify(status))

    jinja_env = getattr(app, "jinja_env", None)
    if jinja_env is not None:
        jinja_env.globals[HOME_CARD_GLOBAL] = partial(_home_card, app)


def _locale(app: Any) -> str:
    return resolve_locale(app.config.get("UI_LOCALE"))


def _collect(app: Any, locale: str) -> dict[str, object]:
    return collect_accounts_status(
        config_dir=app.config.get("SETTINGS_CONFIG_DIR"),
        db_path=app.config.get("DB_PATH"),
        runtime_health_path=app.config.get("SETTINGS_RUNTIME_HEALTH_PATH"),
        locale=locale,
    )


def _where(exc: BaseException) -> str:
    """'file.py:123' of the innermost frame: debuggable without the message,
    which can quote a line of accounts.ini."""
    tb = exc.__traceback__
    while tb is not None and tb.tb_next is not None:
        tb = tb.tb_next
    if tb is None:
        return "-"
    return f"{Path(tb.tb_frame.f_code.co_filename).name}:{tb.tb_lineno}"


def _status_or_none(app: Any, locale: str) -> dict[str, object] | None:
    try:
        return _collect(app, locale)
    except Exception as exc:
        logger.error(
            "accounts_status_failed error_class=%s at=%s",
            type(exc).__name__,
            _where(exc),
        )
        return None


def _home_card(app: Any) -> dict[str, object]:
    locale = _locale(app)
    try:
        return build_home_card(_collect(app, locale), locale)
    except Exception as exc:
        logger.warning(
            "accounts_home_card_failed error_class=%s at=%s",
            type(exc).__name__,
            _where(exc),
        )
        return unavailable_home_card(locale)


def _access_denial(app: Any) -> Any | None:
    """None when this computer or a signed-in session asks; else a logged 403."""
    remote_addr = web_app._request_remote_addr()
    if remote_addr and (
        web_app._is_loopback(remote_addr) or web_app._ensure_authenticated(app)
    ):
        return None
    reason = "not_signed_in" if remote_addr else "no_client_address"
    logger.warning(
        "settings_access_denied reason=%s remote=%s path=%s",
        reason,
        remote_addr or "-",
        getattr(web_app.request, "path", "-"),
    )
    return web_app._text_response(_FORBIDDEN_TEXT, 403)


def _http_warning() -> bool:
    """A network client talks plain HTTP: warn that the traffic is not encrypted."""
    remote_addr = web_app._request_remote_addr()
    scheme = str(getattr(web_app.request, "scheme", "http") or "http")
    return bool(remote_addr) and not web_app._is_loopback(remote_addr) and scheme != "https"


def _no_store(response: Any) -> Any:
    headers = getattr(response, "headers", None)
    if headers is not None:
        headers["Cache-Control"] = "no-store"
    return response


__all__ = ["HOME_CARD_GLOBAL", "register_settings_routes"]
