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
)
from mailbot_v26.llm.runtime_flags import DEFAULT_RUNTIME_FLAGS_PATH, RuntimeFlagStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LLMRouterConfig:
    primary: str = "cloudflare"
    fallback: str = "cloudflare"
    gigachat_enabled: bool = False
    gigachat_api_key: str = ""
    gigachat_base_url: str = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
    gigachat_model: str = "GigaChat"
    cloudflare_enabled: bool = True
    cloudflare_account_id: str = ""
    cloudflare_api_key: str = ""
    cloudflare_model: str = "@cf/meta/llama-3-8b-instruct"
    runtime_flags_path: Path = DEFAULT_RUNTIME_FLAGS_PATH
    runtime_flags_poll_sec: float = 1.0
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
        self._gigachat_last_latency_sec = 0.0
        self._refresh_runtime_flags(force=True)
        logger.info("[LLM-PRIMARY] provider=%s", self._primary_name)

    @classmethod
    def from_config_dir(cls, base_dir: Path) -> "LLMRouter":
        config = _load_llm_config(base_dir)
        return cls(config)

    def _build_providers(self, config: LLMRouterConfig) -> dict[str, LLMProvider]:
        providers: dict[str, LLMProvider] = {}
        if config.cloudflare_enabled:
            providers["cloudflare"] = CloudflareProvider(
                CloudflareProviderConfig(
                    account_id=config.cloudflare_account_id,
                    api_token=config.cloudflare_api_key,
                    model=config.cloudflare_model,
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
        if not self._circuit_open_until:
            return False
        return datetime.now(timezone.utc) < self._circuit_open_until

    def _open_circuit(self, cooldown_sec: int) -> None:
        now = datetime.now(timezone.utc)
        new_until = now + timedelta(seconds=cooldown_sec)
        if self._circuit_open_until and self._circuit_open_until > new_until:
            return
        self._circuit_open_until = new_until
        logger.info(
            "[LLM-CIRCUIT] state=open until=%s",
            self._circuit_open_until.isoformat(),
        )

    def _refresh_runtime_flags(self, *, force: bool = False) -> None:
        flags, changed = self._runtime_flags.get_flags(force=force)
        self._runtime_gigachat_enabled = flags.enable_gigachat
        if changed:
            logger.info(
                "[LLM-RUNTIME] enable_gigachat=%d source=runtime_flags.json",
                int(flags.enable_gigachat),
            )

    def _fallback_log(self, *, reason: str) -> None:
        breaker_until = self._circuit_open_until.isoformat() if self._circuit_open_until else ""
        logger.info(
            "[LLM-FALLBACK] from=gigachat to=%s reason=%s breaker_until=%s",
            self._fallback_name,
            reason,
            breaker_until,
        )

    def _auto_disable_gigachat(self, reason: str) -> None:
        self._runtime_flags.set_enable_gigachat(False)
        self._runtime_gigachat_enabled = False
        self._open_circuit(self.config.gigachat_cooldown_sec)
        logger.info("[LLM-SAFETY] gigachat auto-disabled: %s", reason)

    def _call_provider(
        self,
        provider: LLMProvider,
        messages: list[dict],
        *,
        max_tokens: int | None,
        temperature: float | None,
    ) -> tuple[str, float, bool]:
        start = time.monotonic()
        try:
            result = provider.complete(
                messages, max_tokens=max_tokens, temperature=temperature
            )
            ok = True
        except Exception:
            result = ""
            ok = False
        latency = time.monotonic() - start
        return result, latency, ok

    def _log_llm_result(self, provider: str, latency_sec: float, ok: bool) -> None:
        logger.info(
            "[LLM] provider=%s latency_ms=%d ok=%d",
            provider,
            int(latency_sec * 1000),
            int(ok),
        )

    def _fallback_complete(
        self,
        messages: list[dict],
        *,
        max_tokens: int | None,
        temperature: float | None,
    ) -> str:
        provider = self._providers.get(self._fallback_name)
        if not provider:
            return ""
        result, latency, ok = self._call_provider(
            provider, messages, max_tokens=max_tokens, temperature=temperature
        )
        self.last_provider = self._fallback_name
        self._log_llm_result(self._fallback_name, latency, ok)
        return result

    def complete(
        self,
        messages: list[dict],
        *,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        self._refresh_runtime_flags()
        if self._is_circuit_open():
            logger.info(
                "[LLM-CIRCUIT] state=open until=%s",
                self._circuit_open_until.isoformat() if self._circuit_open_until else "",
            )
            if self._primary_name == "gigachat":
                self._fallback_log(reason="circuit_open")
            return self._fallback_complete(
                messages, max_tokens=max_tokens, temperature=temperature
            )

        if self._primary_name == "gigachat":
            if not self._runtime_gigachat_enabled:
                self._fallback_log(reason="runtime_disabled")
                return self._fallback_complete(
                    messages, max_tokens=max_tokens, temperature=temperature
                )
            provider = self._providers.get("gigachat")
            if not provider or not provider.healthcheck():
                self._auto_disable_gigachat("healthcheck_failed")
                self._fallback_log(reason="healthcheck_failed")
                return self._fallback_complete(
                    messages, max_tokens=max_tokens, temperature=temperature
                )

            result = ""
            latency = 0.0
            ok = False
            for attempt in range(2):
                result, latency, ok = self._call_provider(
                    provider, messages, max_tokens=max_tokens, temperature=temperature
                )
                if ok:
                    break
                if attempt == 0:
                    continue

            if not ok:
                self._gigachat_consecutive_errors += 1
            else:
                self._gigachat_consecutive_errors = 0
            self._gigachat_last_latency_sec = latency

            if not ok and self._gigachat_consecutive_errors >= self.config.gigachat_max_consecutive_errors:
                self._auto_disable_gigachat("consecutive_errors")
                self._fallback_log(reason="consecutive_errors")
                return self._fallback_complete(
                    messages, max_tokens=max_tokens, temperature=temperature
                )

            if self._gigachat_last_latency_sec > self.config.gigachat_max_latency_sec:
                self._auto_disable_gigachat("latency_exceeded")
                self._fallback_log(reason="latency_exceeded")
                return self._fallback_complete(
                    messages, max_tokens=max_tokens, temperature=temperature
                )

            if ok:
                self.last_provider = "gigachat"
                self._log_llm_result("gigachat", latency, True)
                return result

            self._open_circuit(self.config.gigachat_cooldown_sec)
            self._fallback_log(reason="runtime_error")
            return self._fallback_complete(
                messages, max_tokens=max_tokens, temperature=temperature
            )

        if self._primary_name == "cloudflare":
            provider = self._providers.get("cloudflare")
            if provider and provider.healthcheck():
                result, latency, ok = self._call_provider(
                    provider, messages, max_tokens=max_tokens, temperature=temperature
                )
                self.last_provider = "cloudflare"
                self._log_llm_result("cloudflare", latency, ok)
                return result
            logger.info("[LLM-FALLBACK] reason=healthcheck_failed")
            return self._fallback_complete(
                messages, max_tokens=max_tokens, temperature=temperature
            )

        return self._fallback_complete(
            messages, max_tokens=max_tokens, temperature=temperature
        )


def _load_llm_config(base_dir: Path) -> LLMRouterConfig:
    config_path = base_dir / "config.ini"
    keys_path = base_dir / "keys.ini"
    parser = configparser.ConfigParser()
    keys = configparser.ConfigParser()
    if config_path.exists():
        parser.read(config_path, encoding="utf-8")
    if keys_path.exists():
        keys.read(keys_path, encoding="utf-8")

    llm_section = parser["llm"] if "llm" in parser else {}
    gigachat_section = parser["gigachat"] if "gigachat" in parser else {}
    cloudflare_section = parser["cloudflare"] if "cloudflare" in parser else {}
    safety_section = parser["llm_safety"] if "llm_safety" in parser else {}
    keys_cloudflare = keys["cloudflare"] if "cloudflare" in keys else {}

    return LLMRouterConfig(
        primary=llm_section.get("primary", "cloudflare"),
        fallback=llm_section.get("fallback", "cloudflare"),
        gigachat_enabled=_get_bool(gigachat_section, "enabled", False),
        gigachat_api_key=gigachat_section.get("api_key", ""),
        gigachat_base_url=gigachat_section.get(
            "base_url", "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
        ),
        gigachat_model=gigachat_section.get("model", "GigaChat"),
        cloudflare_enabled=_get_bool(cloudflare_section, "enabled", True),
        cloudflare_account_id=keys_cloudflare.get("account_id", ""),
        cloudflare_api_key=keys_cloudflare.get(
            "api_key", keys_cloudflare.get("api_token", "")
        ),
        cloudflare_model=cloudflare_section.get(
            "model", "@cf/meta/llama-3-8b-instruct"
        ),
        runtime_flags_path=Path(__file__).resolve().parents[1] / "runtime_flags.json",
        gigachat_max_consecutive_errors=safety_section.getint(
            "gigachat_max_consecutive_errors", fallback=3
        ),
        gigachat_max_latency_sec=safety_section.getint(
            "gigachat_max_latency_sec", fallback=10
        ),
        gigachat_cooldown_sec=safety_section.getint(
            "gigachat_cooldown_sec", fallback=600
        ),
    )


def _get_bool(section: Any, key: str, default: bool) -> bool:
    raw = section.get(key, None)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


__all__ = ["LLMRouter", "LLMRouterConfig"]
