from __future__ import annotations

import configparser
import logging
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
        self._active_primary = self._resolve_primary()
        self._circuit_open_until: datetime | None = None
        self.last_provider: str | None = None
        logger.info("[LLM-PRIMARY] provider=%s", self._active_primary)

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
        if config.gigachat_enabled:
            providers["gigachat"] = GigaChatProvider(
                GigaChatProviderConfig(
                    api_key=config.gigachat_api_key,
                    base_url=config.gigachat_base_url,
                    model=config.gigachat_model,
                )
            )
        return providers

    def _resolve_primary(self) -> str:
        if self._primary_name == "gigachat":
            if not self.config.gigachat_enabled:
                logger.info("[LLM-FALLBACK] reason=healthcheck_failed")
                return self._fallback_name
            provider = self._providers.get("gigachat")
            if provider and provider.healthcheck():
                return "gigachat"
            logger.info("[LLM-FALLBACK] reason=healthcheck_failed")
            return self._fallback_name
        if self._primary_name == "cloudflare":
            if not self.config.cloudflare_enabled:
                logger.info("[LLM-FALLBACK] reason=healthcheck_failed")
                return self._fallback_name
            provider = self._providers.get("cloudflare")
            if provider and provider.healthcheck():
                return "cloudflare"
            logger.info("[LLM-FALLBACK] reason=healthcheck_failed")
            return self._fallback_name
        return self._fallback_name

    def _is_circuit_open(self) -> bool:
        if not self._circuit_open_until:
            return False
        return datetime.now(timezone.utc) < self._circuit_open_until

    def _open_circuit(self) -> None:
        self._circuit_open_until = datetime.now(timezone.utc) + timedelta(minutes=10)
        logger.info(
            "[LLM-CIRCUIT] state=open until=%s",
            self._circuit_open_until.isoformat(),
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
        try:
            self.last_provider = self._fallback_name
            return provider.complete(
                messages, max_tokens=max_tokens, temperature=temperature
            )
        except Exception:
            return ""

    def complete(
        self,
        messages: list[dict],
        *,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        if self._is_circuit_open():
            logger.info(
                "[LLM-CIRCUIT] state=open until=%s",
                self._circuit_open_until.isoformat() if self._circuit_open_until else "",
            )
            return self._fallback_complete(
                messages, max_tokens=max_tokens, temperature=temperature
            )

        provider = self._providers.get(self._active_primary)
        if not provider:
            return self._fallback_complete(
                messages, max_tokens=max_tokens, temperature=temperature
            )

        for attempt in range(2):
            try:
                result = provider.complete(
                    messages, max_tokens=max_tokens, temperature=temperature
                )
                self.last_provider = self._active_primary
                return result
            except Exception:
                if attempt == 0:
                    continue
                logger.info("[LLM-FALLBACK] reason=runtime_error")
                self._open_circuit()
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
    )


def _get_bool(section: Any, key: str, default: bool) -> bool:
    raw = section.get(key, None)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


__all__ = ["LLMRouter", "LLMRouterConfig"]
