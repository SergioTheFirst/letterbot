from __future__ import annotations

import argparse
import sys

from mailbot_v26.deps import DependencyError, require_runtime_for
from mailbot_v26.version import __version__


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mailbot_v26")
    subparsers = parser.add_subparsers(dest="command")

    doctor_parser = subparsers.add_parser("doctor", help="Run system doctor checks.")
    doctor_parser.add_argument(
        "--print-lan-url",
        action="store_true",
        help="Print LAN-friendly Web UI URL based on config.yaml and exit.",
    )
    doctor_parser.add_argument(
        "--strict",
        action="store_true",
        help="Return non-zero exit code when doctor finds issues.",
    )
    subparsers.add_parser("init-config", help="Create configuration templates.")
    subparsers.add_parser("migrate-config", help="Migrate legacy config files to settings.ini/accounts.ini.")
    validate_parser = subparsers.add_parser("validate-config", help="Validate configuration files.")
    validate_parser.add_argument(
        "--compat",
        action="store_true",
        help="Print compact config schema compatibility report.",
    )
    validate_parser.add_argument(
        "--strict",
        action="store_true",
        help="Return non-zero exit code on validation warnings/errors.",
    )
    subparsers.add_parser("backup", help="Create a data backup archive.")

    restore_parser = subparsers.add_parser("restore", help="Restore data from a backup archive.")
    restore_parser.add_argument("path", help="Path to backup archive (.zip).")

    export_parser = subparsers.add_parser("export", help="Export events/commitments/snapshots.")
    export_parser.add_argument(
        "--since",
        default="30d",
        help="Time window to export (e.g. 30d, 12h). Default: 30d.",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Print version and exit.",
    )
    subparsers.add_parser("version", help="Print version and exit.")
    return parser


def _run() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.version or args.command == "version":
        print(__version__)
        return

    try:
        if args.command == "doctor":
            require_runtime_for("doctor")
            from mailbot_v26.doctor import print_lan_url, report_exit_code, run_doctor

            if args.print_lan_url:
                sys.exit(print_lan_url())
            report = run_doctor()
            sys.exit(report_exit_code(report, strict=bool(args.strict)))

        if args.command == "init-config":
            from mailbot_v26.tools.config_bootstrap import run_init_config

            run_init_config()
            return

        if args.command == "migrate-config":
            from mailbot_v26.tools.config_bootstrap import run_migrate_config

            run_migrate_config()
            return

        if args.command == "validate-config":
            require_runtime_for("validate_config")
            from mailbot_v26.tools.config_bootstrap import run_validate_config

            sys.exit(run_validate_config(compat=bool(args.compat), strict=bool(args.strict)))

        if args.command == "backup":
            from mailbot_v26.tools.backup import run_backup

            run_backup()
            return

        if args.command == "restore":
            from mailbot_v26.tools.restore import run_restore

            run_restore(args.path)
            return

        if args.command == "export":
            from mailbot_v26.tools.export_data import run_export

            run_export(args.since)
            return

        require_runtime_for("runtime")
        from mailbot_v26.start import main

        main()
    except DependencyError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    _run()
