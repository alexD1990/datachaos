"""Rows, batches and tables."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from datachaos.model.schema import Role, TableSchema

# Timestamps, dates and decimals are canonical strings; no floats, so no rounding in output.
Value = str | int | bool | None


@dataclass(frozen=True)
class Row:
    rid: str
    values: Mapping[str, Value]


@dataclass(frozen=True)
class Batch:
    index: int
    schema: TableSchema
    rows: tuple[Row, ...]


@dataclass(frozen=True)
class Table:
    schema_by_batch: tuple[TableSchema, ...]
    rows: tuple[Row, ...]

    @property
    def name(self) -> str:
        return self.schema_by_batch[0].name


def sorted_rows(schema: TableSchema, rows: Iterable[Row]) -> tuple[Row, ...]:
    """The stable order of a table. Canonical timestamps sort chronologically as strings."""
    column = schema.column_with_role(Role.INGESTION_TIME).name
    return tuple(sorted(rows, key=lambda row: (str(row.values[column]), row.rid)))
