"""Column types, roles and table schemas."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal, cast


class ColumnType(Enum):
    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"


class Role(Enum):
    KEY = "key"
    FOREIGN_KEY = "foreign_key"
    EVENT_TIME = "event_time"
    INGESTION_TIME = "ingestion_time"
    UPDATED_AT = "updated_at"
    ENUM = "enum"
    ATTRIBUTE = "attribute"


TableKind = Literal["entity", "event_log"]


@dataclass(frozen=True)
class Column:
    name: str
    type: ColumnType
    nullable: bool
    role: Role = Role.ATTRIBUTE
    references: str | None = None
    allowed_values: tuple[str, ...] = ()
    scale: int | None = None

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "name": self.name,
            "type": self.type.value,
            "nullable": self.nullable,
            "role": self.role.value,
        }
        if self.references is not None:
            result["references"] = self.references
        if self.allowed_values:
            result["allowed_values"] = list(self.allowed_values)
        if self.scale is not None:
            result["scale"] = self.scale
        return result

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Column:
        return cls(
            name=data["name"],
            type=ColumnType(data["type"]),
            nullable=data["nullable"],
            role=Role(data["role"]),
            references=data.get("references"),
            allowed_values=tuple(data.get("allowed_values", ())),
            scale=data.get("scale"),
        )


@dataclass(frozen=True)
class TableSchema:
    name: str
    columns: tuple[Column, ...]
    kind: TableKind

    def column_with_role(self, role: Role) -> Column:
        for column in self.columns:
            if column.role is role:
                return column
        raise ValueError(f"table {self.name} has no column with role {role.value}")

    def to_dict(self, *, schema_version: str, tool_version: str, batch: int) -> dict[str, object]:
        return {
            "schema_version": schema_version,
            "tool_version": tool_version,
            "table": self.name,
            "kind": self.kind,
            "batch": batch,
            "columns": [column.to_dict() for column in self.columns],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TableSchema:
        kind = data["kind"]
        if kind not in ("entity", "event_log"):
            raise ValueError(f"unknown table kind: {kind!r}")
        return cls(
            name=data["table"],
            columns=tuple(Column.from_dict(column) for column in data["columns"]),
            kind=cast(TableKind, kind),
        )
