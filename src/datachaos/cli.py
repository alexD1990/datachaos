"""Command line interface. Parses arguments and maps errors to exit codes."""

from __future__ import annotations

import argparse
import sys
import traceback
from typing import NoReturn

from datachaos import __version__
from datachaos.errors import DataChaosError


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        self.exit(2, f"datachaos: error: {message}\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = _Parser(
        prog="datachaos",
        description="Inject controlled data quality faults and check that pipelines catch them.",
    )
    parser.add_argument("--version", action="version", version=f"datachaos {__version__}")
    parser.add_argument(
        "--verbose", action="store_true", help="show a stack trace when a command fails"
    )
    return parser


def _dispatch(args: argparse.Namespace) -> int:
    raise DataChaosError("no command given (see --help)")


def _report(message: str, *, verbose: bool) -> None:
    if verbose:
        traceback.print_exc()
    print(f"datachaos: error: {message}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        return _dispatch(args)
    except DataChaosError as exc:
        _report(str(exc), verbose=args.verbose)
        return exc.exit_code
    except Exception as exc:
        _report(f"internal error: {exc}", verbose=args.verbose)
        return 2
