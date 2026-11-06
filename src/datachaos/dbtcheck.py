"""Compares dbt results with what a config expects: did the pipeline catch the faults?"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from datachaos import engine
from datachaos.config import RunConfig, load_config
from datachaos.errors import DataChaosError
from datachaos.io.reader import read_json
from datachaos.validate import load_manifest

_FAILED = ("error", "fail")
# When a node ran more than once, the status that says most about it.
_PRECEDENCE = ("error", "fail", "warn", "skipped")


@dataclass(frozen=True)
class Node:
    unique_id: str
    status: str

    @property
    def failed(self) -> bool:
        return self.status in _FAILED

    def to_dict(self) -> dict[str, str]:
        return {"unique_id": self.unique_id, "status": self.status}


@dataclass(frozen=True)
class ExpectationResult:
    scenario: str
    failing_test: str
    nodes: tuple[Node, ...]

    @property
    def detected(self) -> bool:
        return any(node.failed for node in self.nodes)

    def to_dict(self) -> dict[str, object]:
        return {
            "scenario": self.scenario,
            "failing_test": self.failing_test,
            "detected": self.detected,
            "nodes": [node.to_dict() for node in self.nodes],
        }


@dataclass(frozen=True)
class CheckResult:
    directory: str
    run_results: tuple[str, ...]
    is_baseline: bool
    nodes: int
    expectations: tuple[ExpectationResult, ...]
    unexpected: tuple[Node, ...]

    @property
    def ok(self) -> bool:
        if self.is_baseline:
            return not self.unexpected
        return all(expectation.detected for expectation in self.expectations)

    def to_dict(self) -> dict[str, object]:
        return {
            "status": "ok" if self.ok else "failed",
            "directory": self.directory,
            "run_results": list(self.run_results),
            "baseline": self.is_baseline,
            "nodes": self.nodes,
            "expectations": [expectation.to_dict() for expectation in self.expectations],
            "unexpected": [node.to_dict() for node in self.unexpected],
        }


def read_run_results(paths: list[Path]) -> dict[str, list[str]]:
    """The statuses of every node, over all the files."""
    statuses: dict[str, list[str]] = {}
    for path in paths:
        document = read_json(path)
        results = document.get("results")
        if not isinstance(results, list):
            raise DataChaosError(f"{path} is not a dbt run_results file: it has no results")
        for result in results:
            if (
                not isinstance(result, dict)
                or not isinstance(result.get("unique_id"), str)
                or not isinstance(result.get("status"), str)
            ):
                raise DataChaosError(f"{path} is not a dbt run_results file: {result!r}")
            statuses.setdefault(result["unique_id"], []).append(result["status"])
    return statuses


def merge_status(statuses: list[str]) -> str:
    """A node failed if it failed in any run. A later pass does not undo a model error."""
    for status in _PRECEDENCE:
        if status in statuses:
            return status
    return statuses[-1]


def check(directory: Path, paths: list[Path], config_path: Path) -> CheckResult:
    """Raises DataChaosError for files that cannot be used or a pattern that matches nothing."""
    manifest = load_manifest(directory)
    config = load_config(config_path)
    _check_same_run(manifest, config, directory, config_path)
    nodes = {
        unique_id: Node(unique_id, merge_status(statuses))
        for unique_id, statuses in sorted(read_run_results(paths).items())
    }
    results = []
    matched: set[str] = set()
    for expectation in config.expectations:
        for pattern in expectation.failing_tests:
            hits = sorted(name for name in nodes if fnmatch.fnmatchcase(name, pattern))
            if not hits:
                raise DataChaosError(
                    f"{config_path}: {pattern!r} for {expectation.scenario} matches no node in "
                    "the dbt results, so it is not a test that passed but a wrong name"
                )
            matched.update(hits)
            results.append(
                ExpectationResult(
                    expectation.scenario, pattern, tuple(nodes[name] for name in hits)
                )
            )
    return CheckResult(
        directory=str(directory),
        run_results=tuple(str(path) for path in paths),
        is_baseline=not manifest["scenarios"],
        nodes=len(nodes),
        expectations=tuple(results),
        unexpected=tuple(
            node for name, node in nodes.items() if node.failed and name not in matched
        ),
    )


def render_check(result: CheckResult) -> str:
    lines = []
    for expectation in result.expectations:
        state = "detected" if expectation.detected else "NOT DETECTED"
        statuses = ", ".join(sorted({node.status for node in expectation.nodes}))
        lines.append(
            f"{expectation.scenario}: {expectation.failing_test}: {state} (status {statuses})"
        )
    for node in result.unexpected:
        lines.append(f"unexpected: {node.unique_id}: {node.status}")
    if result.is_baseline and not result.unexpected:
        lines.append("baseline: all nodes passed")
    lines.append(
        f"{'ok' if result.ok else 'failed'}: {result.nodes} nodes in "
        f"{len(result.run_results)} run_results files"
    )
    return "\n".join(lines) + "\n"


def _check_same_run(
    manifest: dict[str, Any], config: RunConfig, directory: Path, config_path: Path
) -> None:
    run_id = engine.resolved_run_id(config)
    if run_id != manifest["run"].get("run_id"):
        raise DataChaosError(
            f"{config_path} is not the config of the run in {directory}: it gives run {run_id}, "
            f"and the manifest says {manifest['run'].get('run_id')}"
        )
