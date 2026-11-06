"""Reads DataChaos output back, typed by the schema file of the batch."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from datachaos.errors import DataChaosError
from datachaos.model.schema import Column, ColumnType, TableSchema
from datachaos.model.table import Value


def read_json(path: Path) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8", newline="") as file:
            document = json.load(file)
    except (OSError, ValueError) as exc:
        raise DataChaosError(f"cannot read {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise DataChaosError(f"cannot read {path}: expected a JSON object")
    return document


def read_schema(path: Path) -> TableSchema:
    try:
        return TableSchema.from_dict(read_json(path))
    except (KeyError, TypeError, ValueError) as exc:
        raise DataChaosError(f"invalid schema file {path}: {exc!r}") from exc


def read_csv(path: Path, schema: TableSchema) -> list[dict[str, Value]]:
    names = [column.name for column in schema.columns]
    try:
        with open(path, encoding="utf-8", newline="") as file:
            reader = csv.reader(file)
            header = next(reader, None)
            if header != names:
                raise DataChaosError(f"{path}: header {header} does not match schema {names}")
            return [_csv_row(path, reader.line_num, schema, cells) for cells in reader]
    except (OSError, ValueError, csv.Error) as exc:
        raise DataChaosError(f"cannot read {path}: {exc}") from exc


def read_jsonl(path: Path, schema: TableSchema) -> list[dict[str, Value]]:
    names = [column.name for column in schema.columns]
    rows = []
    try:
        with open(path, encoding="utf-8", newline="") as file:
            for number, line in enumerate(file, 1):
                row = json.loads(line)
                if not isinstance(row, dict) or list(row) != names:
                    raise DataChaosError(f"{path}:{number}: keys do not match schema {names}")
                rows.append(row)
    except (OSError, ValueError) as exc:
        raise DataChaosError(f"cannot read {path}: {exc}") from exc
    return rows


def _csv_row(path: Path, line: int, schema: TableSchema, cells: list[str]) -> dict[str, Value]:
    if len(cells) != len(schema.columns):
        raise DataChaosError(
            f"{path}:{line}: expected {len(schema.columns)} fields, got {len(cells)}"
        )
    return {
        column.name: _parse_cell(path, line, column, cell)
        for column, cell in zip(schema.columns, cells, strict=True)
    }


def _parse_cell(path: Path, line: int, column: Column, cell: str) -> Value:
    if cell == "":
        return None
    if column.type is ColumnType.BOOLEAN:
        if cell not in ("true", "false"):
            raise DataChaosError(f"{path}:{line}: {column.name}: {cell!r} is not a boolean")
        return cell == "true"
    if column.type is ColumnType.INTEGER:
        try:
            return int(cell)
        except ValueError as exc:
            raise DataChaosError(
                f"{path}:{line}: {column.name}: {cell!r} is not an integer"
            ) from exc
    return cell
