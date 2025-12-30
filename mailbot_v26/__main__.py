from __future__ import annotations

import argparse
import sys

from mailbot_v26.doctor import run_doctor
from mailbot_v26.start import main
from mailbot_v26.version import __version__


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mailbot_v26")
    parser.add_argument(
        "command",
        nargs="?",
        choices=["doctor"],
        help="Optional command (doctor).",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Print version and exit.",
    )
    return parser


def _run() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.version:
        print(__version__)
        return

    if args.command == "doctor":
        run_doctor()
        return

    main()


if __name__ == "__main__":
    _run()
