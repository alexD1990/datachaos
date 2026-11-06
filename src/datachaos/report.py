"""A summary of a run per scenario, from the manifest and the ledger alone. Checks nothing."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from datachaos.errors import DataChaosError
from datachaos.validate import Problem, load_manifest, read_ledger

_EXAMPLES = 5


def build_report(directory: Path) -> dict[str, object]:
    manifest = load_manifest(directory)
    problems: list[Problem] = []
    lines = read_ledger(directory, problems)
    if lines is None:
        raise DataChaosError(f"{problems[0].path}: {problems[0].message}")
    try:
        scenarios = [_scenario(record, lines) for record in manifest["scenarios"]]
        run = manifest["run"]
        report: dict[str, object] = {
            "directory": str(directory),
            "run_id": run["run_id"],
            "seed": run["seed"],
            "created_at": run["created_at"],
        }
        if "based_on" in run:
            report["based_on"] = run["based_on"]
        report.update(
            as_of=manifest["dataset"]["as_of"],
            batches=manifest["dataset"]["batches"],
            tables=manifest["dataset"]["tables"],
            scenarios=scenarios,
            ledger_lines=len(lines),
            warnings=manifest.get("warnings", []),
        )
    except (KeyError, TypeError) as exc:
        manifest_path = directory / "chaos-manifest.json"
        raise DataChaosError(f"{manifest_path} is not a valid manifest: {exc!r}") from exc
    return report


def _scenario(record: dict[str, Any], lines: list[dict[str, Any]]) -> dict[str, object]:
    own = [line for line in lines if line["scenario_index"] == record["index"]]
    keys: list[dict[str, Any]] = []
    for line in own:
        if line["action"] != "schema_change" and line["key"] not in keys:
            keys.append(line["key"])
    return {
        "index": record["index"],
        "name": record["name"],
        "version": record["version"],
        "table": record["table"],
        "params": record["params"],
        "stats": record["stats"],
        "actions": dict(sorted(Counter(line["action"] for line in own).items())),
        "example_keys": keys[:_EXAMPLES],
        "schema_changes": [
            {
                "column": line["column"],
                "before": line["before"],
                "after": line["after"],
                "from_batch": line["batch"],
            }
            for line in own
            if line["action"] == "schema_change"
        ],
    }


def render_report(report: dict[str, Any]) -> str:
    lines = [
        f"run {report['run_id']}: seed {report['seed']}, as of {report['as_of']}, "
        f"{report['batches']} batches"
    ]
    if "based_on" in report:
        lines.append(f"based on run {report['based_on']['run_id']}")
    for name, counts in report["tables"].items():
        lines.append(f"  {name}: {counts['rows_baseline']} -> {counts['rows_final']} rows")
    for scenario in report["scenarios"]:
        lines.append(
            f"scenario {scenario['index']}: {scenario['name']} on {scenario['table']} "
            f"(version {scenario['version']})"
        )
        actions = ", ".join(f"{name} {count}" for name, count in scenario["actions"].items())
        lines.append(f"  ledger: {actions or 'no lines'}")
        for change in scenario["schema_changes"]:
            lines.append(
                f"  {change['column']}: {change['before']} -> {change['after']} "
                f"from batch {change['from_batch']}"
            )
        if scenario["example_keys"]:
            examples = ", ".join(
                "/".join(str(value) for value in key.values()) for key in scenario["example_keys"]
            )
            lines.append(f"  example keys: {examples}")
    lines.append(f"{report['ledger_lines']} ledger lines. Not checked; use validate for that.")
    return "\n".join(lines) + "\n"
