from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class CommitmentChainDigestConfig:
    window_days: int = 30
    max_entities: int = 3
    max_items_per_entity: int = 2


def load_commitment_chain_digest_config(
    config_dir: Path | None = None,
) -> CommitmentChainDigestConfig:
    config_path = (config_dir or Path(__file__).resolve().parent) / "config.ini"
    parser = configparser.ConfigParser()
    if config_path.exists():
        parser.read(config_path, encoding="utf-8")
    section = (
        parser["commitment_chain_digest"]
        if "commitment_chain_digest" in parser
        else None
    )

    window_days = 30
    max_entities = 3
    max_items_per_entity = 2

    if section is not None:
        try:
            window_days = max(1, section.getint("window_days", fallback=30))
        except ValueError:
            window_days = 30
        try:
            max_entities = max(0, section.getint("max_entities", fallback=3))
        except ValueError:
            max_entities = 3
        try:
            max_items_per_entity = max(
                0, section.getint("max_items_per_entity", fallback=2)
            )
        except ValueError:
            max_items_per_entity = 2

    return CommitmentChainDigestConfig(
        window_days=window_days,
        max_entities=max_entities,
        max_items_per_entity=max_items_per_entity,
    )


__all__ = ["CommitmentChainDigestConfig", "load_commitment_chain_digest_config"]
