"""Command line interface. Parses arguments and maps errors to exit codes."""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from pathlib import Path
from typing import Any, NoReturn

from datachaos import config as _config
from datachaos import dbtcheck, engine
from datachaos._version import __version__
from datachaos.errors import DataChaosError
from datachaos.inject import inject
from datachaos.land import land
from datachaos.model.manifest import Manifest
from datachaos.report import build_report, render_report
from datachaos.validate import validate_dir

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
    _add_validate(commands)
    _add_report(commands)
    _add_inject(commands)
    _add_land(commands)
    _add_check_dbt(commands)
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
    _add_output(parser)
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
    _add_output(parser)
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


def _add_validate(commands: argparse._SubParsersAction[_Parser]) -> None:
    parser = commands.add_parser(
        "validate",
        help="check output against its manifest and ledger",
        description=(
            "Without flags, checks only what can be decided from the files on disk: hashes, "
            "columns per batch schema, schema changes, and that every ledger line can be observed "
            "in the final state. A line that cannot be proved wrong without row IDs counts as "
            "unverifiable, with a warning. Scenario invariants are only checked with --reproduce."
        ),
    )
    parser.add_argument("directory", type=Path)
    parser.add_argument(
        "--reproduce",
        action="store_true",
        help=(
            "also run the config in the manifest again in a temp directory, which checks the "
            "invariant of every scenario, and require identical files"
        ),
    )
    _add_output(parser)
    _add_verbose(parser)


def _add_report(commands: argparse._SubParsersAction[_Parser]) -> None:
    parser = commands.add_parser(
        "report",
        help="summarize a run per scenario, from its manifest and ledger",
        description="Summarizes a run. Checks nothing; use validate for that.",
    )
    parser.add_argument("directory", type=Path)
    _add_output(parser)
    _add_verbose(parser)


def _add_inject(commands: argparse._SubParsersAction[_Parser]) -> None:
    parser = commands.add_parser(
        "inject",
        help="add a scenario to earlier output, in a new directory",
        description=(
            "Runs the config in the manifest of --input again, with the scenario at the end. "
            "First checks that this gives the content of --input, which needs the same DataChaos "
            "version. The new manifest records which run it builds on."
        ),
    )
    parser.add_argument("--input", required=True, type=Path, help="earlier DataChaos output")
    parser.add_argument(
        "--out", required=True, type=Path, help="output directory (missing or empty)"
    )
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument(
        "--param", action="append", default=[], metavar="KEY=VALUE", help="scenario parameter"
    )
    _add_output(parser)
    _add_verbose(parser)


def _add_land(commands: argparse._SubParsersAction[_Parser]) -> None:
    parser = commands.add_parser(
        "land",
        help="copy batches up to one to a landing directory",
        description=(
            "Copies the data and schema files of batch 0 up to --upto-batch to "
            "LANDING/<table>/ and LANDING/schemas/<table>/. Never deletes or overwrites: an equal "
            "file is skipped, and a different one stops it before anything is copied."
        ),
    )
    parser.add_argument("directory", type=Path)
    parser.add_argument("--upto-batch", required=True, type=int)
    parser.add_argument("--to", required=True, type=Path, metavar="LANDING_DIR")
    _add_output(parser)
    _add_verbose(parser)


def _add_check_dbt(commands: argparse._SubParsersAction[_Parser]) -> None:
    parser = commands.add_parser(
        "check-dbt",
        help="compare dbt results with the expectations in a config",
        description=(
            "Reads one or more dbt run_results.json files and merges them: a node failed if it has "
            "status fail or error in any of them. Give the files from both dbt run and dbt test, "
            "since a model error from dbt run can be followed by passing tests on the old table. "
            "Exit code 1 if an expected failure did not happen. A run without scenarios expects "
            "every node to pass."
        ),
    )
    parser.add_argument("--run", required=True, type=Path, metavar="DIR", help="DataChaos output")
    parser.add_argument(
        "--run-results",
        required=True,
        action="append",
        type=Path,
        metavar="PATH",
        help="a run_results.json file; give it once per dbt run",
    )
    parser.add_argument(
        "--config", required=True, type=Path, help="the config of the run, with its expectations"
    )
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


def _report_run(out: Path, manifest: Manifest, output: str) -> None:
    for warning in manifest.warnings:
        print(f"datachaos: warning: {warning}", file=sys.stderr)
    if output == "json":
        _print_json(
            {
                "status": "ok",
                "directory": str(out),
                "run_id": manifest.run.run_id,
                **({"based_on": dict(manifest.run.based_on)} if manifest.run.based_on else {}),
                "content_sha256": manifest.content_sha256,
                "tables": manifest.to_dict()["dataset"]["tables"],  # type: ignore[index]
                "scenarios": list(manifest.scenarios),
                "warnings": list(manifest.warnings),
            }
        )
        return
    print(f"wrote {out} (run {manifest.run.run_id})")
    if manifest.run.based_on:
        print(f"  based on run {manifest.run.based_on['run_id']}")
    for name, counts in manifest.dataset.tables.items():
        print(f"  {name}: {counts.rows_final} rows")
    for record in manifest.scenarios:
        print(
            f"  scenario {record['index']}: {record['name']} on {record['table']}, "
            f"{record['ledger_entries']} ledger lines"
        )


def _run_generate(args: argparse.Namespace) -> int:
    _report_run(args.out, engine.generate(_config_from_flags(args), args.out), args.output)
    return 0


def _run_run(args: argparse.Namespace) -> int:
    _report_run(args.out, engine.run(_run_config(args), args.out, force=args.force), args.output)
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


def _run_validate(args: argparse.Namespace) -> int:
    result = validate_dir(args.directory, reproduce=args.reproduce)
    for warning in result.warnings:
        print(f"datachaos: warning: {warning}", file=sys.stderr)
    if args.output == "json":
        _print_json(result.to_dict())
    else:
        for problem in result.problems:
            print(f"{problem.check}: {problem.path}: {problem.message}")
        counts = result.ledger
        checked = "files and ledger" + (", and reproduced" if args.reproduce else "")
        print(f"{'ok' if result.ok else 'failed'}: {args.directory} ({checked})")
        print(
            f"  ledger: {counts.total} lines, {counts.verified} verified, "
            f"{counts.superseded} superseded, {counts.unverifiable} unverifiable"
        )
    return 0 if result.ok else 1


def _run_report(args: argparse.Namespace) -> int:
    report = build_report(args.directory)
    if args.output == "json":
        _print_json(report)
    else:
        print(render_report(report), end="")
    return 0


def _run_inject(args: argparse.Namespace) -> int:
    scenario = _config.ScenarioConfig(args.scenario, args.table, _config.parse_params(args.param))
    _report_run(args.out, inject(args.input, args.out, scenario), args.output)
    return 0


def _run_land(args: argparse.Namespace) -> int:
    result = land(args.directory, args.upto_batch, args.to)
    if args.output == "json":
        _print_json(result.to_dict())
    else:
        print(
            f"landed batches up to {result.upto_batch} in {args.to}: "
            f"{result.copied} files copied, {result.skipped} already there"
        )
    return 0


def _run_check_dbt(args: argparse.Namespace) -> int:
    result = dbtcheck.check(args.run, args.run_results, args.config)
    if args.output == "json":
        _print_json(result.to_dict())
    else:
        print(dbtcheck.render_check(result), end="")
    return 0 if result.ok else 1


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
    if args.command == "validate":
        return _run_validate(args)
    if args.command == "report":
        return _run_report(args)
    if args.command == "inject":
        return _run_inject(args)
    if args.command == "land":
        return _run_land(args)
    if args.command == "check-dbt":
        return _run_check_dbt(args)
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
