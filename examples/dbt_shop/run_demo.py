"""Runs the whole loop for every config: inject faults, land batch by batch, and ask dbt.

For each config in `configs/`: `datachaos run`, then `datachaos land --upto-batch k` and `dbt run`
for each batch, `dbt test` once after the last batch, `datachaos validate` and `datachaos check-dbt`
with the run_results of every dbt call. `dbt build` per batch would not do: it skips models
downstream of a failing test, which leaves the incremental models in another state than the run
alone would.

A config passes if what it expects was caught and nothing else failed, so a robust model failing
under a scenario fails the demo.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
CONFIGS = PROJECT / "configs"


class DemoError(Exception):
    """Something in the demo itself went wrong, as opposed to a config not passing."""


@dataclass(frozen=True)
class Outcome:
    config: str
    passed: bool
    detected: list[str]
    missed: list[str]
    unexpected: list[str]
    unverifiable: int
    validate_status: str
    check_status: str


def _datachaos(*args: object) -> subprocess.CompletedProcess[str]:
    command = [sys.executable, "-m", "datachaos", *map(str, args)]
    return subprocess.run(command, capture_output=True, text=True, encoding="utf-8")


def _must_datachaos(*args: object) -> str:
    result = _datachaos(*args)
    if result.returncode != 0:
        raise DemoError(f"datachaos {args[0]} failed: {result.stderr.strip()}")
    return result.stdout


def _dbt_executable() -> str:
    # Only from this environment: a dbt elsewhere on PATH may be another version or adapter.
    found = shutil.which("dbt", path=str(Path(sys.executable).parent))
    if found is None:
        raise DemoError('dbt is not installed here; run pip install -e ".[dev,dbt]"')
    return found


def _dbt(dbt: str, command: str, work: Path, landing: Path, saved_as: Path) -> None:
    """Exit code 1 is a model or a test that failed. That is what the demo is there to see."""
    target = work / "target"
    result = subprocess.run(
        [
            dbt,
            "--no-use-colors",
            command,
            "--project-dir",
            str(PROJECT),
            "--profiles-dir",
            str(PROJECT),
            "--target-path",
            str(target),
            "--log-path",
            str(work / "logs"),
            "--vars",
            json.dumps({"landing": landing.as_posix()}),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=PROJECT,
        env={
            **os.environ,
            "DATACHAOS_DEMO_DB": str(work / "demo.duckdb"),
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        },
    )
    results = target / "run_results.json"
    if result.returncode not in (0, 1) or not results.is_file():
        raise DemoError(f"dbt {command} failed with exit {result.returncode}: {result.stdout}")
    saved_as.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(results, saved_as)
    results.unlink()


def run_config(config: Path, dbt: str, work: Path) -> Outcome:
    out = work / "out"
    landing = work / "landing"
    results = work / "results"
    _must_datachaos("run", "--config", config, "--out", out)
    batches = json.loads((out / "chaos-manifest.json").read_bytes())["dataset"]["batches"]

    files = []
    for batch in range(batches):
        _must_datachaos("land", out, "--upto-batch", batch, "--to", landing)
        files.append(results / f"run_batch_{batch:04d}.json")
        _dbt(dbt, "run", work, landing, files[-1])
    files.append(results / "test.json")
    _dbt(dbt, "test", work, landing, files[-1])

    validated = _datachaos("validate", out, "--output", "json")
    if validated.returncode == 2:
        raise DemoError(f"datachaos validate failed: {validated.stderr.strip()}")
    validation = json.loads(validated.stdout)

    args: list[object] = ["check-dbt", "--run", out, "--config", config, "--output", "json"]
    for path in files:
        args += ["--run-results", path]
    checked = _datachaos(*args)
    if checked.returncode == 2:
        raise DemoError(f"{config.name}: {checked.stderr.strip()}")
    document = json.loads(checked.stdout)
    (work / "check-dbt.json").write_bytes(checked.stdout.encode("utf-8"))

    unexpected = [node["unique_id"] for node in document["unexpected"]]
    expectations = document["expectations"]
    return Outcome(
        config=config.stem,
        passed=(checked.returncode == 0 and not unexpected and validation["status"] == "ok"),
        detected=[item["failing_test"] for item in expectations if item["detected"]],
        missed=[item["failing_test"] for item in expectations if not item["detected"]],
        unexpected=unexpected,
        unverifiable=validation["unverifiable"],
        validate_status=validation["status"],
        check_status=document["status"],
    )


def _print_table(outcomes: list[Outcome]) -> None:
    width = max(len(outcome.config) for outcome in outcomes)
    print(f"{'config'.ljust(width)}  result  caught by")
    for outcome in outcomes:
        caught = ", ".join(_short(name) for name in outcome.detected) or "-"
        print(f"{outcome.config.ljust(width)}  {'ok' if outcome.passed else 'FAIL':<6}  {caught}")
        for name in outcome.missed:
            print(f"{''.ljust(width)}          not caught: {_short(name)}")
        for name in outcome.unexpected:
            print(f"{''.ljust(width)}          unexpected: {_short(name)}")
        if outcome.validate_status != "ok":
            print(f"{''.ljust(width)}          validate: {outcome.validate_status}")


def _short(name: str) -> str:
    return name.replace("test.dbt_shop.", "").replace("model.dbt_shop.", "model ").rstrip("*")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--only", action="append", default=[], help="run only this config")
    parser.add_argument("--work", type=Path, help="keep everything in this new directory")
    args = parser.parse_args(argv)

    configs = sorted(CONFIGS.glob("*.yml"))
    if args.only:
        known = {path.stem: path for path in configs}
        unknown = sorted(set(args.only) - set(known))
        if unknown:
            print(f"run_demo: error: unknown config {unknown[0]}", file=sys.stderr)
            return 2
        configs = [known[name] for name in args.only]

    try:
        dbt = _dbt_executable()
        with tempfile.TemporaryDirectory(prefix="datachaos-demo-") as temp:
            root = args.work if args.work is not None else Path(temp)
            root.mkdir(parents=True, exist_ok=True)
            outcomes = []
            for config in configs:
                work = root / config.stem
                work.mkdir()
                print(f"running {config.stem} ...", file=sys.stderr)
                outcomes.append(run_config(config, dbt, work))
            summary = json.dumps([asdict(outcome) for outcome in outcomes], indent=2) + "\n"
            (root / "summary.json").write_bytes(summary.encode("utf-8"))
    except (DemoError, OSError) as exc:
        print(f"run_demo: error: {exc}", file=sys.stderr)
        return 2
    _print_table(outcomes)
    return 0 if all(outcome.passed for outcome in outcomes) else 1


if __name__ == "__main__":
    raise SystemExit(main())
