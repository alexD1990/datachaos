"""JSON Lines for data and the ledger, and pretty JSON for manifests and schema files."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path

from datachaos.model.ledger import LedgerEntry
from datachaos.model.schema import TableSchema
from datachaos.model.table import Row


def write_jsonl(path: Path, schema: TableSchema, rows: Iterable[Row]) -> int:
    """Writes the rows with keys in schema order and returns how many there were."""
    names = [column.name for column in schema.columns]
    return _write_lines(path, ({name: row.values.get(name) for name in names} for row in rows))


def write_ledger(path: Path, entries: Iterable[LedgerEntry]) -> int:
    return _write_lines(path, (entry.to_dict() for entry in entries))


def write_json(path: Path, document: Mapping[str, object]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as file:
        file.write(json.dumps(document, ensure_ascii=False, indent=2) + "\n")


def _write_lines(path: Path, objects: Iterable[Mapping[str, object]]) -> int:
    count = 0
    with open(path, "w", encoding="utf-8", newline="") as file:
        for obj in objects:
            file.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
            count += 1
    return count
