from __future__ import annotations

import configparser
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from mailbot_v26.llm.providers import (
    CloudflareProvider,
    CloudflareProviderConfig,
    GigaChatProvider,
    GigaChatProviderConfig,
    LLMProvider,
    LLMProviderError,
)
from mailbot_v26.llm.runtime_flags import DEFAULT_RUNTIME_FLAGS_PATH, RuntimeFlagStore
from mailbot_v26.config_yaml import ConfigError as YamlConfigError
from mailbot_v26.config.ini_utils import read_user_ini_with_defaults
from mailbot_v26.config.paths import resolve_config_paths
from mailbot_v26.config_yaml import load_config as load_yaml_config
from mailbot_v26.config_yaml import validate_config as validate_yaml_config

logger = logging.getLogger(__name__)
DEFAULT_CLOUDFLARE_MODEL = "@cf/meta/llama-3-8b-instruct"
_SUPPORTED_LLM_PROVIDERS = {"cloudflare", "gigachat"}
_EMPTY_RESPONSE = "EmptyResponse"
_PROVIDER_MISSING = "ProviderMissing"
_ERROR_DETAIL_MAX_CHARS = 200
# GigaChat was not called at all for these reasons (vs. called and failed).
_GIGACHAT_SKIP_REASONS = frozenset({"circuit_open", "healthcheck_failed"})


@dataclass(frozen=True)
class LLMRouterConfig:
    primary: str = "cloudflare"
    fallback: str = "cloudflare"
    gigachat_enabled: bool = False
    gigachat_api_key: str = ""
    gigachat_base_url: str = (
        "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
    )
    gigachat_model: str | None = "GigaChat"
    cloudflare_enabled: bool = True
    cloudflare_account_id: str = ""
    cloudflare_api_key: str = ""
    cloudflare_model: str | None = DEFAULT_CLOUDFLARE_MODEL
    runtime_flags_path: Path = DEFAULT_RUNTIME_FLAGS_PATH
    runtime_flags_poll_sec: float = 1.0
    # In-memory GigaChat breaker: any failed call (or a slow answer, which is
    # still returned) opens it for gigachat_cooldown_sec, then one probe call
    # closes or reopens it. Reaching gigachat_max_consecutive_errors failed
    # calls logs the opening as a warning. Nothing is written to disk.
    gigachat_max_consecutive_errors: int = 3
    gigachat_max_latency_sec: int = 10
    gigachat_cooldown_sec: int = 600


class LLMRouter:
    def __init__(
        self,
        config: LLMRouterConfig,
        *,
        providers: dict[str, LLMProvider] | None = None,
    ) -> None:
        self.config = config
        self._providers = providers or self._build_providers(config)
        self._fallback_name = config.fallback
        self._primary_name = config.primary
        self._active_primary = self._fallback_name
        self._circuit_open_until: datetime | None = None
        self.last_provider: str | None = None
        self._runtime_flags = RuntimeFlagStore(
            config.runtime_flags_path, poll_interval_sec=config.runtime_flags_poll_sec
        )
        self._runtime_gigachat_enabled = False
        self._gigachat_consecutive_errors = 0
        self._refresh_runtime_flags(force=True)
        logger.info("[LLM-PRIMARY] provider=%s", self._primary_name)

    @classmethod
    def from_config_dir(cls, base_dir: Path) -> "LLMRouter":
        config = _load_llm_config(base_dir)
        return cls(config)

    def _build_providers(self, config: LLMRouterConfig) -> dict[str, LLMProvider]:
        providers: dict[str, LLMProvider] = {}
        if config.cloudflare_enabled:
            model = config.cloudflare_model or DEFAULT_CLOUDFLARE_MODEL
            providers["cloudflare"] = CloudflareProvider(
                CloudflareProviderConfig(
                    account_id=config.cloudflare_account_id,
                    api_token=config.cloudflare_api_key,
                    model=model,
                )
            )
        if config.gigachat_enabled or config.gigachat_api_key:
            providers["gigachat"] = GigaChatProvider(
                GigaChatProviderConfig(
                    api_key=config.gigachat_api_key,
                    base_url=config.gigachat_base_url,
                    model=config.gigachat_model,
                )
            )
        return providers

    def _is_circuit_open(self) -> bool:
        """True while the GigaChat breaker is open and still cooling down."""
        if not self._circuit_open_until:
            return False
        return datetime.now(timezone.utc) < self._circuit_open_until

    def _open_circuit(self, reason: str) -> None:
        """Open the in-memory GigaChat breaker for the cooldown.

        Replaces the old auto-disable that wrote enable_gigachat=false to
        runtime_flags.json for good: the operator's file is left alone and
        GigaChat is probed again once the cooldown is over.
        """
        self._circuit_open_until = datetime.now(timezone.utc) + timedelta(
            seconds=self.config.gigachat_cooldown_sec
        )
        persistent = reason == "consecutive_errors"
        logger.log(
            logging.WARNING if persistent else logging.INFO,
            "[LLM-CIRCUIT] state=open provider=gigachat reason=%s failures=%d "
            "until=%s",
            reason,
            self._gigachat_consecutive_errors,
            self._circuit_open_until.isoformat(),
        )

    def _close_circuit(self) -> None:
        if self._circuit_open_until is None:
            return
        self._circuit_open_until = None
        logger.info("[LLM-CIRCUIT] state=closed provider=gigachat")

    def _refresh_runtime_flags(self, *, force: bool = False) -> None:
        flags, changed = self._runtime_flags.get_flags(force=force)
        self._runtime_gigachat_enabled = flags.enable_gigachat
        if changed:
            logger.info(
                "[LLM-RUNTIME] enable_gigachat=%d source=runtime_flags.json",
                int(flags.enable_gigachat),
            )

    def _fallback_log(self, *, reason: str, source: str = "gigachat") -> None:
        breaker_until = (
            self._circuit_open_until.isoformat() if self._circuit_open_until else ""
        )
        logger.info(
            "[LLM-FALLBACK] from=%s to=%s reason=%s breaker_until=%s",
            source,
            self._fallback_name,
            reason,
            breaker_until,
        )

    def _log_provider_error(
        self,
        provider: str,
        error_class: str,
        detail: str,
        latency_sec: float,
        *,
        unexpected: bool = False,
    ) -> None:
        # Error class and a short detail only (provider messages carry the HTTP
        # status or error class); never the messages sent to the model.
        logger.log(
            logging.ERROR if unexpected else logging.WARNING,
            "[LLM-ERROR] provider=%s error_class=%s detail=%s latency_ms=%d",
            provider,
            error_class,
            detail,
            int(latency_sec * 1000),
            exc_info=unexpected,
        )

    def _call_provider(
        self,
        name: str,
        provider: LLMProvider,
        messages: list[dict],
        *,
        max_tokens: int | None,
        temperature: float | None,
    ) -> tuple[str, float, bool]:
        """Call one provider once. A raised error or an empty answer is a
        logged failure (ok=False), never a silent "" reported as ok."""
        start = time.monotonic()
        try:
            result = provider.complete(
                messages, max_tokens=max_tokens, temperature=temperature
            )
        except LLMProviderError as exc:
            latency = time.monotonic() - start
            self._log_provider_error(
                name, type(exc).__name__, _error_detail(exc), latency
            )
            return "", latency, False
        except Exception as exc:  # unexpected: keep the traceback
            latency = time.monotonic() - start
            self._log_provider_error(
                name, type(exc).__name__, _error_detail(exc), latency, unexpected=True
            )
            return "", latency, False
        latency = time.monotonic() - start
        text = result if isinstance(result, str) else str(result or "")
        if not text.strip():
            self._log_provider_error(
                name, _EMPTY_RESPONSE, "provider returned no text", latency
            )
            return "", latency, False
        return text, latency, True

    def _log_llm_result(self, provider: str, latency_sec: float, ok: bool) -> None:
        logger.info(
            "[LLM] provider=%s latency_ms=%d ok=%d",
            provider,
            int(latency_sec * 1000),
            int(ok),
        )

    def _call_gigachat(
        self,
        messages: list[dict],
        *,
        max_tokens: int | None,
        temperature: float | None,
        attempts: int,
    ) -> tuple[str, str | None]:
        """Call GigaChat under the in-memory breaker: (text, failure reason).

        The reason is None on success. Every route to GigaChat (primary or
        fallback) goes through here, so the breaker is never bypassed.
        """
        if self._is_circuit_open():
            return "", "circuit_open"
        provider = self._providers.get("gigachat")
        if provider is None:
            self._log_provider_error(
                "gigachat", _PROVIDER_MISSING, "provider not configured", 0.0
            )
            return "", "provider_missing"
        if not provider.healthcheck():
            self._open_circuit("healthcheck_failed")
            return "", "healthcheck_failed"
        if self._circuit_open_until is not None:
            # Cooldown over: a single probe call closes or reopens the breaker.
            logger.info("[LLM-CIRCUIT] state=half_open provider=gigachat")
            attempts = 1
        result, latency, ok = "", 0.0, False
        for _ in range(max(1, attempts)):
            result, latency, ok = self._call_provider(
                "gigachat",
                provider,
                messages,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            if ok:
                break
        self._log_llm_result("gigachat", latency, ok)
        if not ok:
            self._gigachat_consecutive_errors += 1
            reason = (
                "consecutive_errors"
                if self._gigachat_consecutive_errors
                >= self.config.gigachat_max_consecutive_errors
                else "runtime_error"
            )
            self._open_circuit(reason)
            return "", reason
        self._gigachat_consecutive_errors = 0
        self.last_provider = "gigachat"
        # Success is checked before latency: a slow answer is kept, and only
        # the next calls avoid GigaChat while it cools down.
        if latency > self.config.gigachat_max_latency_sec:
            self._open_circuit("latency_exceeded")
        else:
            self._close_circuit()
        return result, None

    def _fallback_complete(
        self,
        messages: list[dict],
        *,
        max_tokens: int | None,
        temperature: float | None,
    ) -> str:
        name = self._fallback_name
        if name == "gigachat":
            result, reason = self._call_gigachat(
                messages, max_tokens=max_tokens, temperature=temperature, attempts=1
            )
            if reason in _GIGACHAT_SKIP_REASONS:
                # No answer without blocking: the caller degrades to heuristics.
                logger.info(
                    "[LLM-FALLBACK] provider=gigachat skipped reason=%s", reason
                )
            return result
        provider = self._providers.get(name)
        if not provider:
            self._log_provider_error(
                name, _PROVIDER_MISSING, "provider not configured", 0.0
            )
            return ""
        result, latency, ok = self._call_provider(
            name, provider, messages, max_tokens=max_tokens, temperature=temperature
        )
        self.last_provider = name
        self._log_llm_result(name, latency, ok)
        return result

    def _complete_via_gigachat(
        self,
        messages: list[dict],
        *,
        max_tokens: int | None,
        temperature: float | None,
    ) -> str:
        if not self._runtime_gigachat_enabled:
            reason = "runtime_disabled"
        else:
            result, failure = self._call_gigachat(
                messages, max_tokens=max_tokens, temperature=temperature, attempts=2
            )
            if failure is None:
                return result
            reason = failure
        self._fallback_log(reason=reason)
        return self._fallback_complete(
            messages, max_tokens=max_tokens, temperature=temperature
        )

    def complete(
        self,
        messages: list[dict],
        *,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        """Return the model's answer, or "" when no provider produced one.

        Failures are logged ([LLM-ERROR], [LLM] ok=0) and routed to the
        fallback provider; GigaChat is guarded by an in-memory breaker.
        """
        self._refresh_runtime_flags()
        if self._primary_name == "gigachat":
            return self._complete_via_gigachat(
                messages, max_tokens=max_tokens, temperature=temperature
            )

        if self._primary_name == "cloudflare":
            provider = self._providers.get("cloudflare")
            if provider and provider.healthcheck():
                result, latency, ok = self._call_provider(
                    "cloudflare",
                    provider,
                    messages,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
                self.last_provider = "cloudflare"
                self._log_llm_result("cloudflare", latency, ok)
                if ok or self._fallback_name == "cloudflare":
                    return result
                self._fallback_log(reason="runtime_error", source="cloudflare")
            else:
                reason = "provider_missing" if provider is None else "healthcheck_failed"
                self._fallback_log(reason=reason, source="cloudflare")
            return self._fallback_complete(
                messages, max_tokens=max_tokens, temperature=temperature
            )

        return self._fallback_complete(
            messages, max_tokens=max_tokens, temperature=temperature
        )


def _error_detail(exc: BaseException) -> str:
    """One line, capped; the providers keep their messages secret-free."""
    detail = " ".join(str(exc).split())
    return detail[:_ERROR_DETAIL_MAX_CHARS] or "-"


def _load_llm_config(base_dir: Path) -> LLMRouterConfig:
    resolved = resolve_config_paths(base_dir)
    if not resolved.two_file_mode:
        try:
            yaml_config = _load_yaml_config(base_dir)
        except ValueError as exc:
            logger.warning("llm_yaml_config_invalid %s", exc)
            yaml_config = None
        if yaml_config:
            return yaml_config

    config_path = resolved.settings_path
    if not resolved.two_file_mode and not config_path.exists():
        config_path = resolved.legacy_ini_path
    parser = read_user_ini_with_defaults(
        config_path,
        logger=logger,
        scope_label="LLM config",
    )

    accounts_parser = read_user_ini_with_defaults(
        resolved.accounts_path,
        logger=logger,
        scope_label="LLM credentials (accounts.ini)",
    )
    keys_parser = configparser.ConfigParser()
    if resolved.keys_path.exists():
        keys_parser = read_user_ini_with_defaults(
            resolved.keys_path,
            logger=logger,
            scope_label="LLM keys",
        )

    if resolved.two_file_mode:
        # 2-file mode contract:
        # - provider routing lives in settings.ini [llm]
        # - runtime credentials live in accounts.ini
        # - keys.ini and settings.ini credentials remain legacy-only fallbacks
        llm_section = parser["llm"] if "llm" in parser else parser["DEFAULT"]
        cloudflare_creds_section = (
            accounts_parser["cloudflare"]
            if "cloudflare" in accounts_parser
            else (
                keys_parser["cloudflare"]
                if "cloudflare" in keys_parser
                else accounts_parser["DEFAULT"]
            )
        )
    else:
        llm_section = parser["llm"] if "llm" in parser else parser["DEFAULT"]
        cloudflare_creds_section = (
            keys_parser["cloudflare"]
            if "cloudflare" in keys_parser
            else keys_parser["DEFAULT"]
        )
        gigachat_creds_section = (
            parser["gigachat"] if "gigachat" in parser else parser["DEFAULT"]
        )

    gigachat_section = parser["gigachat"] if "gigachat" in parser else parser["DEFAULT"]
    if resolved.two_file_mode:
        if "gigachat" in accounts_parser and str(
            accounts_parser["gigachat"].get("api_key", "")
        ).strip():
            gigachat_creds_section = accounts_parser["gigachat"]
        elif "gigachat" in keys_parser and str(
            keys_parser["gigachat"].get("api_key", "")
        ).strip():
            gigachat_creds_section = keys_parser["gigachat"]
        else:
            gigachat_creds_section = gigachat_section
    else:
        gigachat_creds_section = gigachat_section
    cloudflare_section = (
        parser["cloudflare"] if "cloudflare" in parser else parser["DEFAULT"]
    )
    safety_section = (
        parser["llm_safety"] if "llm_safety" in parser else parser["DEFAULT"]
    )

    primary_provider = str(llm_section.get("primary", "cloudflare")).strip().lower()
    fallback_provider = str(llm_section.get("fallback", primary_provider)).strip().lower()
    accounts_llm_section = (
        accounts_parser["llm"]
        if "llm" in accounts_parser
        else accounts_parser["DEFAULT"]
    )
    preferred_primary = _normalize_provider_name(
        accounts_llm_section.get("llm_priority_primary")
        or accounts_llm_section.get("priority_llm_primary")
    )
    preferred_fallback = _normalize_provider_name(
        accounts_llm_section.get("llm_priority_secondary")
        or accounts_llm_section.get("priority_llm_secondary")
    )
    if preferred_primary:
        primary_provider = preferred_primary
        if not preferred_fallback:
            fallback_provider = primary_provider
    if preferred_fallback:
        fallback_provider = preferred_fallback
    if primary_provider not in _SUPPORTED_LLM_PROVIDERS:
        primary_provider = "cloudflare"
    if fallback_provider not in _SUPPORTED_LLM_PROVIDERS:
        fallback_provider = primary_provider

    return LLMRouterConfig(
        primary=primary_provider,
        fallback=fallback_provider,
        gigachat_enabled=_get_bool(gigachat_section, "enabled", False),
        gigachat_api_key=gigachat_creds_section.get("api_key", ""),
        gigachat_base_url=gigachat_section.get(
            "base_url", "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
        ),
        gigachat_model=gigachat_section.get("model", "GigaChat"),
        cloudflare_enabled=_get_bool(cloudflare_section, "enabled", True),
        cloudflare_account_id=cloudflare_creds_section.get("account_id", ""),
        cloudflare_api_key=cloudflare_creds_section.get(
            "api_key", cloudflare_creds_section.get("api_token", "")
        ),
        cloudflare_model=cloudflare_section.get("model", DEFAULT_CLOUDFLARE_MODEL),
        runtime_flags_path=Path(__file__).resolve().parents[1] / "runtime_flags.json",
        gigachat_max_consecutive_errors=_get_int(
            safety_section, "gigachat_max_consecutive_errors", 3
        ),
        gigachat_max_latency_sec=_get_int(
            safety_section, "gigachat_max_latency_sec", 10
        ),
        gigachat_cooldown_sec=_get_int(safety_section, "gigachat_cooldown_sec", 600),
    )


def _normalize_provider_name(raw: object) -> str:
    value = str(raw or "").strip().lower()
    if value in _SUPPORTED_LLM_PROVIDERS:
        return value
    return ""


def _load_yaml_config(base_dir: Path) -> LLMRouterConfig | None:
    config_path = resolve_config_paths(base_dir).yaml_path
    if not config_path:
        return None
    try:
        payload = load_yaml_config(config_path)
    except (FileNotFoundError, YamlConfigError) as exc:
        raise ValueError(str(exc)) from exc
    ok, error = validate_yaml_config(payload)
    if not ok:
        raise ValueError(error or "Invalid config.yaml")

    llm = payload.get("llm", {}) if isinstance(payload, dict) else {}
    provider = llm.get("provider", "cloudflare")
    cloudflare = llm.get("cloudflare", {}) if isinstance(llm, dict) else {}
    gigachat = llm.get("gigachat", {}) if isinstance(llm, dict) else {}

    cloudflare_model = cloudflare.get("model", DEFAULT_CLOUDFLARE_MODEL)
    gigachat_model = gigachat.get("model", "GigaChat")
    if gigachat_model is None:
        gigachat_model = None
    if cloudflare_model is None:
        cloudflare_model = DEFAULT_CLOUDFLARE_MODEL

    primary = str(provider).strip() or "cloudflare"
    fallback = primary

    return LLMRouterConfig(
        primary=primary,
        fallback=fallback,
        gigachat_enabled=provider == "gigachat",
        gigachat_api_key=str(gigachat.get("api_token", "")).strip(),
        gigachat_base_url=str(
            gigachat.get(
                "base_url",
                "https://gigachat.devices.sberbank.ru/api/v1/chat/completions",
            )
        ).strip(),
        gigachat_model=gigachat_model if gigachat_model != "" else None,
        cloudflare_enabled=True,
        cloudflare_account_id=str(cloudflare.get("account_id", "")).strip(),
        cloudflare_api_key=str(cloudflare.get("api_token", "")).strip(),
        cloudflare_model=cloudflare_model,
        runtime_flags_path=Path(__file__).resolve().parents[1] / "runtime_flags.json",
    )


def _get_int(section: Any, key: str, default: int) -> int:
    if section is None:
        return default
    if hasattr(section, "getint"):
        try:
            return int(section.getint(key, fallback=default))
        except Exception:
            return default
    raw = section.get(key, None)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return default


def _get_bool(section: Any, key: str, default: bool) -> bool:
    if section is None:
        return default
    raw = section.get(key, None)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


__all__ = ["LLMRouter", "LLMRouterConfig"]
