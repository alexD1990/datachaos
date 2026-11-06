"""CSV as defined by the determinism contract. NULL is an empty field."""

from __future__ import annotations

import csv
from collections.abc import Iterable
from pathlib import Path

from datachaos.model.schema import TableSchema
from datachaos.model.table import Row, Value


def write_csv(path: Path, schema: TableSchema, rows: Iterable[Row]) -> int:
    """Writes the rows and returns how many there were."""
    names = [column.name for column in schema.columns]
    count = 0
    with open(path, "w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        writer.writerow(names)
        for row in rows:
            writer.writerow([_cell(row.values.get(name)) for name in names])
            count += 1
    return count


def _cell(value: Value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
