"""The scenario framework. Scenarios do no IO and return new datasets, never mutating input."""

from __future__ import annotations

import difflib
import hashlib
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Protocol, TypeAlias, TypeVar, cast

from datachaos.errors import DataChaosError, FieldError
from datachaos.model.dataset import Dataset
from datachaos.model.ledger import LedgerEntry
from datachaos.model.schema import Column, ColumnType, Role, SchemaStep, TableSchema
from datachaos.model.table import Row, Table, Value, sorted_rows
from datachaos.rng import Rng
from datachaos.timeutil import batch_index, parse_duration, parse_timestamp, window_start

T = TypeVar("T")
ParamType: TypeAlias = "type | tuple[type, ...]"
ResolvedParams: TypeAlias = Mapping[str, object]
SchemaState: TypeAlias = Mapping[str, tuple[TableSchema, ...]]


@dataclass(frozen=True)
class ParamSpec:
    name: str
    type: ParamType
    required: bool
    default: object = None
    help: str = ""
    # Returns an error message or None. Gets the schema of the table's last batch.
    validate: Callable[[object, TableSchema], str | None] | None = None


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    version: int
    summary: str
    detects: str
    typical_dbt_test: str
    applies_to: frozenset[str]
    params: tuple[ParamSpec, ...]


@dataclass(frozen=True)
class ResolveContext:
    """What a scenario is validated against before any scenario runs.

    `dataset` is the baseline. `schemas` is the schema state after all earlier scenarios, table
    by table and batch by batch, so validate column names and types against `schemas`.
    """

    dataset: Dataset
    schemas: SchemaState

    def schema_of(self, table: str) -> TableSchema:
        if table not in self.schemas:
            raise FieldError("table", f"unknown table {table!r}{did_you_mean(table, self.schemas)}")
        return self.schemas[table][-1]


@dataclass(frozen=True)
class ScenarioContext:
    dataset: Dataset
    table: str
    params: ResolvedParams
    rng: Rng
    index: int
    eligible: Callable[[Row], bool]
    # The same rule for any table, for scenarios that also select from another table.
    eligible_in: Callable[[str], Callable[[Row], bool]]


@dataclass(frozen=True)
class Group:
    """Rows that share the values of some columns."""

    key: Mapping[str, Value]
    rows: tuple[Row, ...]


@dataclass(frozen=True)
class ScenarioStats:
    rows_before: int
    rows_after: int
    rows_selected: int
    rows_added: int
    values_changed: int
    rows_removed: int = 0

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "rows_before": self.rows_before,
            "rows_after": self.rows_after,
            "rows_selected": self.rows_selected,
            "rows_added": self.rows_added,
            "values_changed": self.values_changed,
        }
        if self.rows_removed:
            result["rows_removed"] = self.rows_removed
        return result


@dataclass(frozen=True)
class ScenarioOutcome:
    dataset: Dataset
    ledger: tuple[LedgerEntry, ...]
    stats: ScenarioStats
    warnings: tuple[str, ...] = ()


class Scenario(Protocol):
    spec: ScenarioSpec

    def resolve(
        self, ctx: ResolveContext, table: str, params: Mapping[str, object]
    ) -> ResolvedParams:
        """Fills in defaults and validates. Raises FieldError. Must be idempotent."""
        ...

    def schemas_after(self, ctx: ResolveContext, table: str, params: ResolvedParams) -> SchemaState:
        """The schema state later scenarios are validated against."""
        ...

    def apply(self, ctx: ScenarioContext) -> ScenarioOutcome: ...

    def check(self, ctx: ScenarioContext, outcome: ScenarioOutcome) -> list[str]:
        """The invariant. `ctx.dataset` is the state before. Each message is a bug in DataChaos."""
        ...


class BaseScenario:
    """Default behaviour: common parameters, and no change to the schema."""

    spec: ScenarioSpec

    def resolve(
        self, ctx: ResolveContext, table: str, params: Mapping[str, object]
    ) -> ResolvedParams:
        schema = ctx.schema_of(table)
        if schema.kind not in self.spec.applies_to:
            kinds = " or ".join(sorted(self.spec.applies_to))
            raise FieldError(
                "table",
                f"{self.spec.name} applies to {kinds} tables, and {table} is {schema.kind}",
            )
        resolved = resolve_params(self.spec, schema, params)
        self.resolve_more(ctx, table, resolved, params)
        # In spec order whatever was filled in when, so the manifest is the same on a rerun.
        return {
            param.name: resolved[param.name] for param in self.spec.params if param.name in resolved
        }

    def resolve_more(
        self,
        ctx: ResolveContext,
        table: str,
        resolved: dict[str, object],
        given: Mapping[str, object],
    ) -> None:
        """Validation across parameters. May adjust `resolved`."""

    def schemas_after(self, ctx: ResolveContext, table: str, params: ResolvedParams) -> SchemaState:
        return ctx.schemas


def did_you_mean(name: str, options: Iterable[str]) -> str:
    matches = difflib.get_close_matches(name, sorted(options), n=1)
    return f" (did you mean {matches[0]}?)" if matches else ""


def _check_rate(value: object, schema: TableSchema) -> str | None:
    rate = cast(float, value)
    return None if 0 < rate <= 1 else f"must be above 0 and at most 1, got {value!r}"


def _check_count(value: object, schema: TableSchema) -> str | None:
    return None if cast(int, value) >= 1 else f"must be at least 1, got {value!r}"


COMMON_PARAMS = (
    ParamSpec(
        "rate",
        (float, int),
        False,
        help="Share of the candidate rows to select. Give exactly one of rate and count.",
        validate=_check_rate,
    ),
    ParamSpec(
        "count",
        int,
        False,
        help="Number of candidate rows to select. Give exactly one of rate and count.",
        validate=_check_count,
    ),
    ParamSpec(
        "allow_overlap",
        bool,
        False,
        default=False,
        help="Also select rows whose key an earlier scenario has touched.",
    ),
)


def type_name(param_type: ParamType) -> str:
    types = param_type if isinstance(param_type, tuple) else (param_type,)
    return " or ".join(item.__name__ for item in types)


def resolve_params(
    spec: ScenarioSpec, schema: TableSchema, params: Mapping[str, object]
) -> dict[str, object]:
    known = {param.name: param for param in spec.params}
    for name in sorted(params):
        if name not in known:
            raise FieldError(f"params.{name}", f"unknown parameter{did_you_mean(name, known)}")
    resolved: dict[str, object] = {}
    for param in spec.params:
        if param.name in params:
            value = params[param.name]
        elif param.required:
            raise FieldError(f"params.{param.name}", "is required")
        elif param.default is None:
            continue
        else:
            value = param.default
        if isinstance(value, tuple):
            value = list(value)
        _check_value(param, value, schema)
        resolved[param.name] = value
    if ("rate" in resolved) == ("count" in resolved):
        raise FieldError("params", "give exactly one of rate and count")
    return resolved


def _check_value(param: ParamSpec, value: object, schema: TableSchema) -> None:
    types = param.type if isinstance(param.type, tuple) else (param.type,)
    # bool is a subclass of int, but true is not a number in a config file.
    wrong_bool = isinstance(value, bool) and bool not in types
    if wrong_bool or not isinstance(value, types):
        raise FieldError(f"params.{param.name}", f"must be {type_name(param.type)}, got {value!r}")
    if param.validate is not None:
        message = param.validate(value, schema)
        if message is not None:
            raise FieldError(f"params.{param.name}", message)


def select_rows(
    rows: Iterable[Row],
    rng: Rng,
    *,
    rate: float | None = None,
    count: int | None = None,
    eligible: Callable[[Row], bool],
) -> list[Row]:
    """The selected rows, sorted on rid. Candidates are sorted on rid before drawing."""
    candidates = sorted((row for row in rows if eligible(row)), key=lambda row: row.rid)
    return _draw(candidates, rng, rate, count, "rows")


def _draw(
    candidates: Sequence[T], rng: Rng, rate: float | None, count: int | None, noun: str
) -> list[T]:
    if (rate is None) == (count is None):
        raise DataChaosError("give exactly one of rate and count")
    if rate is not None:
        wanted = math.floor(rate * len(candidates) + 0.5)
        if wanted == 0:
            raise DataChaosError(
                f"rate {rate} of {len(candidates)} candidate {noun} selects no {noun}; use count"
            )
    else:
        wanted = cast(int, count)
        if wanted > len(candidates):
            raise DataChaosError(
                f"count {wanted} is more than the {len(candidates)} candidate {noun}"
            )
    return [candidates[index] for index in rng.sample_indices(len(candidates), wanted)]


def groups_of(rows: Iterable[Row], columns: Sequence[str]) -> list[Group]:
    """Groups sorted on their values, with rows sorted on rid. NULL belongs to no group."""
    members: dict[tuple[Value, ...], list[Row]] = {}
    for row in rows:
        values = tuple(row.values.get(column) for column in columns)
        if all(value is not None for value in values):
            members.setdefault(values, []).append(row)
    return [
        Group(
            dict(zip(columns, values, strict=True)),
            tuple(sorted(members[values], key=lambda row: row.rid)),
        )
        for values in sorted(members, key=lambda values: tuple(str(value) for value in values))
    ]


def select_groups(
    rows: Iterable[Row],
    columns: Sequence[str],
    params: ResolvedParams,
    rng: Rng,
    *,
    eligible: Callable[[Row], bool],
    candidate: Callable[[Group], bool] | None = None,
) -> list[Group]:
    """Groups selected by rate or count. A group is a candidate only if all its rows are."""
    candidates = [
        group
        for group in groups_of(rows, columns)
        if all(eligible(row) for row in group.rows) and (candidate is None or candidate(group))
    ]
    rate = cast("float | None", params.get("rate"))
    return _draw(candidates, rng, rate, cast("int | None", params.get("count")), "groups")


def select(ctx: ScenarioContext, candidate: Callable[[Row], bool] | None = None) -> list[Row]:
    """Rows selected by the common parameters among eligible rows that are also candidates."""
    extra = candidate if candidate is not None else lambda row: True
    return select_rows(
        ctx.dataset.tables[ctx.table].rows,
        ctx.rng,
        rate=cast("float | None", ctx.params.get("rate")),
        count=cast("int | None", ctx.params.get("count")),
        eligible=lambda row: ctx.eligible(row) and extra(row),
    )


def new_rid(index: int, number: int) -> str:
    """Fixed width, so that sorting on rid is numeric."""
    return f"s{index:04d}-{number:07d}"


def key_of(row: Row, schema: TableSchema) -> dict[str, Value]:
    return {
        column.name: row.values.get(column.name)
        for column in schema.columns
        if column.role is Role.KEY
    }


def ingestion_column(schema: TableSchema) -> str:
    return schema.column_with_role(Role.INGESTION_TIME).name


def batch_bounds(dataset: Dataset) -> tuple[int, int]:
    """The start of the window and the length of a batch, in seconds."""
    interval = parse_duration(dataset.batch_interval)
    return window_start(parse_timestamp(dataset.as_of), dataset.batches, interval), interval


def batch_of(dataset: Dataset, table: Table, row: Row) -> int:
    start, interval = batch_bounds(dataset)
    return batch_index(parse_timestamp(str(row.values[table.ingestion_column])), start, interval)


def with_rows(dataset: Dataset, table: str, rows: Sequence[Row]) -> Dataset:
    current = dataset.tables[table]
    updated = replace(current, rows=sorted_rows(current.stable_schema, rows))
    return replace(dataset, tables={**dataset.tables, table: updated})


def column_named(schema: TableSchema, name: object) -> Column | None:
    return next((column for column in schema.columns if column.name == name), None)


def other_value(column: Column, value: Value, rng: Rng) -> Value:
    """A valid value of the column that differs from `value`."""
    if column.role is Role.ENUM:
        return rng.choice([option for option in column.allowed_values if option != value])
    if column.type is ColumnType.BOOLEAN:
        return not value
    if column.type is ColumnType.INTEGER:
        return cast(int, value) + rng.between(1, 100)
    if column.type is ColumnType.DECIMAL:
        units = int(str(value).replace(".", "")) + rng.between(1, 100)
        scale = column.scale or 0
        text = str(units).rjust(scale + 1, "0")
        return f"{text[:-scale]}.{text[-scale:]}" if scale else text
    return f"{value}-changed"


def apply_step(step: SchemaStep, values: Mapping[str, Value], rid: str) -> dict[str, Value]:
    """The values of a row after a schema change."""
    result = dict(values)
    if step.operation == "add_column":
        result[step.column] = None if step.fill == "null" else _generated(step, rid)
    elif step.operation == "drop_column":
        del result[step.column]
    elif step.operation == "rename_column":
        result[cast(str, step.new_name)] = result.pop(step.column)
    else:
        result[step.column] = convert_value(cast(str, step.to), result[step.column])
    return result


def conform_row(table: Table, row: Row, from_batch: int, to_batch: int) -> Row:
    """The row in the form of a later batch. Rows only ever move forward between batches."""
    values: Mapping[str, Value] = row.values
    for step in table.schema_steps:
        if from_batch < step.from_batch <= to_batch:
            values = apply_step(step, values, row.rid)
    return row if values is row.values else replace(row, values=values)


def convert_value(to: str, value: Value) -> Value:
    if value is None:
        return None
    if to == "string_comma_decimal":
        return str(value).replace(".", ",")
    if to == "string_local_format":
        text = str(value)
        return f"{text[8:10]}.{text[5:7]}.{text[:4]} {text[11:16]}"
    if to == "string_yes_no":
        return "yes" if value else "no"
    return str(value)


def _generated(step: SchemaStep, rid: str) -> str:
    return hashlib.sha256(f"{step.fill_seed}:{rid}".encode()).hexdigest()[:8]
