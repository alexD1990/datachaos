"""The manifest, `chaos-manifest.json`, schema 1.0."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

SCHEMA_VERSION = "1.0"
MANIFEST_FILE = "chaos-manifest.json"
LEDGER_FILE = "chaos-ledger.jsonl"


@dataclass(frozen=True)
class FileEntry:
    path: str  # relative to the output directory, always with / as separator
    sha256: str
    rows: int | None = None

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {"path": self.path, "sha256": self.sha256}
        if self.rows is not None:
            result["rows"] = self.rows
        return result


@dataclass(frozen=True)
class TableCounts:
    rows_baseline: int
    rows_final: int


@dataclass(frozen=True)
class RunInfo:
    run_id: str
    created_at: str
    seed: int
    config: Mapping[str, object]
    based_on: Mapping[str, str] | None = None


@dataclass(frozen=True)
class DatasetInfo:
    name: str
    generator_version: int
    as_of: str
    batches: int
    batch_interval: str
    tables: Mapping[str, TableCounts]


@dataclass(frozen=True)
class Manifest:
    tool_version: str
    run: RunInfo
    dataset: DatasetInfo
    scenarios: tuple[Mapping[str, object], ...]
    files: tuple[FileEntry, ...]
    content_sha256: str
    warnings: tuple[str, ...] = ()
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "tool_version": self.tool_version,
            "run": {
                "run_id": self.run.run_id,
                "created_at": self.run.created_at,
                "seed": self.run.seed,
                "config": dict(self.run.config),
                **({"based_on": dict(self.run.based_on)} if self.run.based_on else {}),
            },
            "dataset": {
                "name": self.dataset.name,
                "generator_version": self.dataset.generator_version,
                "as_of": self.dataset.as_of,
                "batches": self.dataset.batches,
                "batch_interval": self.dataset.batch_interval,
                "tables": {
                    name: {"rows_baseline": counts.rows_baseline, "rows_final": counts.rows_final}
                    for name, counts in self.dataset.tables.items()
                },
            },
            "scenarios": [dict(scenario) for scenario in self.scenarios],
            "files": [entry.to_dict() for entry in self.files],
            "content_sha256": self.content_sha256,
            "warnings": list(self.warnings),
        }


def compute_content_sha256(files: Iterable[FileEntry]) -> str:
    """SHA-256 over the ledger and the data files: `<sha256>  <path>\\n`, sorted on path."""
    content = (entry for entry in files if _is_content(entry.path))
    lines = (f"{entry.sha256}  {entry.path}\n" for entry in sorted(content, key=lambda e: e.path))
    return hashlib.sha256("".join(lines).encode("utf-8")).hexdigest()


def _is_content(path: str) -> bool:
    return path == LEDGER_FILE or path.startswith("data/")
