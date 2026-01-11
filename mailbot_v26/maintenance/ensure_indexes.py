from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from mailbot_v26.config_loader import load_maintenance_config, load_storage_config
from mailbot_v26.maintenance.indexes import ensure_indexes


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create maintenance indexes for events_v1 (idempotent)."
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="",
        help="Override database path (defaults to config.ini storage.db_path)",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    maintenance = load_maintenance_config()
    if not maintenance.maintenance_mode:
        sys.stderr.write(
            "maintenance_mode is disabled in config.ini; no changes applied.\n"
        )
        return 2
    storage = load_storage_config()
    db_path = Path(args.db_path) if args.db_path else storage.db_path
    result = ensure_indexes(str(db_path))
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    sys.stdout.write(payload + "\n")
    return 0 if not result.get("errors") else 1


if __name__ == "__main__":
    raise SystemExit(main())
