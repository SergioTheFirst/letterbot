"""Cloudflare AI client wrapper.

The client is deliberately lightweight and defensive. If credentials are
missing or a request fails, the caller receives an empty string to keep
pipeline stability, satisfying Constitution Section VI.1.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.llm.providers import CloudflareProvider, CloudflareProviderConfig

@dataclass
class CloudflareConfig:
    account_id: str
    api_token: str
    model: str = "@cf/meta/llama-3-8b-instruct"


class CloudflareLLMClient:
    """Minimal Cloudflare AI REST client."""

    def __init__(self, config: CloudflareConfig) -> None:
        self.config = config
        self._provider = CloudflareProvider(
            CloudflareProviderConfig(
                account_id=config.account_id,
                api_token=config.api_token,
                model=config.model,
            )
        )

    def generate(self, prompt: str, data: str) -> str:
        """Return model output or empty string on failure."""
        return self._provider.complete(
            [
                {"role": "system", "content": prompt},
                {"role": "user", "content": data},
            ]
        )


def load_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()
