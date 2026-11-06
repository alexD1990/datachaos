"""Rows, batches and tables."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace

from datachaos.model.schema import Role, SchemaStep, TableSchema

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
    schema_steps: tuple[SchemaStep, ...] = ()

    @property
    def name(self) -> str:
        return self.schema_by_batch[0].name

    @property
    def ingestion_column(self) -> str:
        """The same in every batch, since batch membership follows from it."""
        return self.schema_by_batch[0].column_with_role(Role.INGESTION_TIME).name

    @property
    def stable_schema(self) -> TableSchema:
        """The columns that are identical in every batch.

        This always holds the key and the ingestion time, which schema-drift cannot change, and
        every column a scenario uses, since schema-drift cannot be combined with that scenario.
        """
        first = self.schema_by_batch[0]
        return replace(
            first,
            columns=tuple(
                column
                for column in first.columns
                if all(column in schema.columns for schema in self.schema_by_batch)
            ),
        )


def sorted_rows(schema: TableSchema, rows: Iterable[Row]) -> tuple[Row, ...]:
    """The stable order of a table. Canonical timestamps sort chronologically as strings."""
    column = schema.column_with_role(Role.INGESTION_TIME).name
    return tuple(sorted(rows, key=lambda row: (str(row.values[column]), row.rid)))
