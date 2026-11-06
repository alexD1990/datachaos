"""Command line interface. Parses arguments and maps errors to exit codes."""

from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path
from typing import NoReturn

from datachaos import config as _config
from datachaos import engine
from datachaos._version import __version__
from datachaos.errors import DataChaosError

_FORMATS = {"csv": ("csv",), "jsonl": ("jsonl",), "both": ("csv", "jsonl")}


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
    commands = parser.add_subparsers(dest="command", title="commands")
    _add_generate(commands)
    return parser


def _add_generate(commands: argparse._SubParsersAction[_Parser]) -> None:
    parser = commands.add_parser("generate", help="write the clean baseline dataset")
    parser.add_argument(
        "--out", required=True, type=Path, help="output directory (missing or empty)"
    )
    parser.add_argument("--seed", type=int, default=_config.DEFAULT_SEED)
    parser.add_argument("--customers", type=int, default=_config.DEFAULT_CUSTOMERS)
    parser.add_argument("--orders", type=int, default=_config.DEFAULT_ORDERS)
    parser.add_argument("--batches", type=int, default=_config.DEFAULT_BATCHES)
    parser.add_argument("--batch-interval", default=_config.DEFAULT_BATCH_INTERVAL, help="e.g. 1d")
    parser.add_argument("--as-of", default=_config.DEFAULT_AS_OF, help="end of the time window")
    parser.add_argument("--format", choices=sorted(_FORMATS), default="csv")
    parser.add_argument(
        "--combined", action="store_true", help="also write one file per table with all batches"
    )
    parser.add_argument(
        "--emit-row-ids", action="store_true", help="add the column _chaos_rid to all data files"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=argparse.SUPPRESS,
        help="show a stack trace when the command fails",
    )


def _run_generate(args: argparse.Namespace) -> int:
    config = _config.RunConfig(
        seed=args.seed,
        dataset=_config.DatasetConfig(
            as_of=args.as_of,
            customers=args.customers,
            orders=args.orders,
            batches=args.batches,
            batch_interval=args.batch_interval,
        ),
        output=_config.OutputConfig(
            formats=_FORMATS[args.format], combined=args.combined, emit_row_ids=args.emit_row_ids
        ),
    )
    manifest = engine.generate(config, args.out)
    print(f"wrote {args.out} (run {manifest.run.run_id})")
    for name, counts in manifest.dataset.tables.items():
        print(f"  {name}: {counts.rows_final} rows")
    return 0


def _dispatch(args: argparse.Namespace) -> int:
    if args.command == "generate":
        return _run_generate(args)
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
