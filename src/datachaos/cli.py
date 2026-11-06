"""Command line interface. Parses arguments and maps errors to exit codes."""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from pathlib import Path
from typing import Any, NoReturn

from datachaos import config as _config
from datachaos import engine
from datachaos._version import __version__
from datachaos.errors import DataChaosError
from datachaos.model.manifest import Manifest

_FORMATS = {"csv": ("csv",), "jsonl": ("jsonl",), "both": ("csv", "jsonl")}
_NOT_WITH_CONFIG = {
    "--scenario": "scenario",
    "--table": "table",
    "--param": "param",
    "--seed": "seed",
    "--customers": "customers",
    "--orders": "orders",
    "--batches": "batches",
    "--batch-interval": "batch_interval",
    "--as-of": "as_of",
    "--format": "format",
    "--combined": "combined",
    "--emit-row-ids": "emit_row_ids",
}


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
    _add_run(commands)
    _add_list_scenarios(commands)
    _add_describe(commands)
    return parser


def _add_verbose(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=argparse.SUPPRESS,
        help="show a stack trace when the command fails",
    )


def _add_output(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output", choices=["text", "json"], default="text")


def _add_data_flags(parser: argparse.ArgumentParser) -> None:
    """The defaults are applied later, so that `run` can tell what was given with --config."""
    parser.add_argument("--seed", type=int, help=f"default: {_config.DEFAULT_SEED}")
    parser.add_argument("--customers", type=int, help=f"default: {_config.DEFAULT_CUSTOMERS}")
    parser.add_argument("--orders", type=int, help=f"default: {_config.DEFAULT_ORDERS}")
    parser.add_argument("--batches", type=int, help=f"default: {_config.DEFAULT_BATCHES}")
    parser.add_argument(
        "--batch-interval", help=f"e.g. 2d, default: {_config.DEFAULT_BATCH_INTERVAL}"
    )
    parser.add_argument("--as-of", help=f"end of the time window, default: {_config.DEFAULT_AS_OF}")
    parser.add_argument("--format", choices=sorted(_FORMATS), help="default: csv")
    parser.add_argument(
        "--combined", action="store_true", help="also write one file per table with all batches"
    )
    parser.add_argument(
        "--emit-row-ids", action="store_true", help="add the column _chaos_rid to all data files"
    )


def _add_generate(commands: argparse._SubParsersAction[_Parser]) -> None:
    parser = commands.add_parser("generate", help="write the clean baseline dataset")
    parser.add_argument(
        "--out", required=True, type=Path, help="output directory (missing or empty)"
    )
    _add_data_flags(parser)
    _add_verbose(parser)


def _add_run(commands: argparse._SubParsersAction[_Parser]) -> None:
    parser = commands.add_parser(
        "run", help="write a dataset with faults, from a config file or one --scenario"
    )
    parser.add_argument(
        "--out", required=True, type=Path, help="output directory (missing or empty)"
    )
    parser.add_argument("--config", type=Path, help="YAML config file, for several scenarios")
    parser.add_argument(
        "--force", action="store_true", help="replace earlier DataChaos output in the directory"
    )
    parser.add_argument("--scenario", help="the scenario to run, instead of --config")
    parser.add_argument("--table", help="the table the scenario applies to")
    parser.add_argument(
        "--param", action="append", default=[], metavar="KEY=VALUE", help="scenario parameter"
    )
    _add_data_flags(parser)
    _add_verbose(parser)


def _add_list_scenarios(commands: argparse._SubParsersAction[_Parser]) -> None:
    parser = commands.add_parser("list-scenarios", help="list the available scenarios")
    _add_output(parser)
    _add_verbose(parser)


def _add_describe(commands: argparse._SubParsersAction[_Parser]) -> None:
    parser = commands.add_parser("describe", help="describe one scenario and its parameters")
    parser.add_argument("scenario")
    _add_output(parser)
    _add_verbose(parser)


def _config_from_flags(
    args: argparse.Namespace, scenarios: tuple[_config.ScenarioConfig, ...] = ()
) -> _config.RunConfig:
    def given(value: object, default: object) -> Any:
        return default if value is None else value

    return _config.RunConfig(
        seed=given(args.seed, _config.DEFAULT_SEED),
        dataset=_config.DatasetConfig(
            as_of=given(args.as_of, _config.DEFAULT_AS_OF),
            customers=given(args.customers, _config.DEFAULT_CUSTOMERS),
            orders=given(args.orders, _config.DEFAULT_ORDERS),
            batches=given(args.batches, _config.DEFAULT_BATCHES),
            batch_interval=given(args.batch_interval, _config.DEFAULT_BATCH_INTERVAL),
        ),
        output=_config.OutputConfig(
            formats=_FORMATS[given(args.format, "csv")],
            combined=args.combined,
            emit_row_ids=args.emit_row_ids,
        ),
        scenarios=scenarios,
    )


def _report_run(out: Path, manifest: Manifest) -> None:
    print(f"wrote {out} (run {manifest.run.run_id})")
    for name, counts in manifest.dataset.tables.items():
        print(f"  {name}: {counts.rows_final} rows")
    for record in manifest.scenarios:
        print(
            f"  scenario {record['index']}: {record['name']} on {record['table']}, "
            f"{record['ledger_entries']} ledger lines"
        )
    for warning in manifest.warnings:
        print(f"datachaos: warning: {warning}", file=sys.stderr)


def _run_generate(args: argparse.Namespace) -> int:
    _report_run(args.out, engine.generate(_config_from_flags(args), args.out))
    return 0


def _run_run(args: argparse.Namespace) -> int:
    _report_run(args.out, engine.run(_run_config(args), args.out, force=args.force))
    return 0


def _run_config(args: argparse.Namespace) -> _config.RunConfig:
    if args.config is not None:
        for flag, name in _NOT_WITH_CONFIG.items():
            if getattr(args, name) not in (None, False, []):
                raise DataChaosError(f"{flag} cannot be combined with --config")
        return _config.load_config(args.config)
    if args.scenario is None:
        raise DataChaosError("give --config or --scenario")
    if args.table is None:
        raise DataChaosError("--scenario needs --table")
    params = _config.parse_params(args.param)
    return _config_from_flags(args, (_config.ScenarioConfig(args.scenario, args.table, params),))


def _run_list_scenarios(args: argparse.Namespace) -> int:
    scenarios = engine.scenario_list()
    if args.output == "json":
        _print_json({"scenarios": scenarios})
        return 0
    width = max(len(str(scenario["name"])) for scenario in scenarios)
    for scenario in scenarios:
        print(f"{str(scenario['name']).ljust(width)}  {scenario['summary']}")
    return 0


def _run_describe(args: argparse.Namespace) -> int:
    if args.output == "json":
        _print_json({"scenario": engine.scenario_details(args.scenario)})
    else:
        print(engine.scenario_text(args.scenario), end="")
    return 0


def _print_json(document: dict[str, object]) -> None:
    versions = {"schema_version": "1.0", "tool_version": __version__}
    print(json.dumps({**versions, **document}, ensure_ascii=False, indent=2))


def _dispatch(args: argparse.Namespace) -> int:
    if args.command == "generate":
        return _run_generate(args)
    if args.command == "run":
        return _run_run(args)
    if args.command == "list-scenarios":
        return _run_list_scenarios(args)
    if args.command == "describe":
        return _run_describe(args)
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
