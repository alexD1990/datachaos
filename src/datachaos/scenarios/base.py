"""The scenario framework. Scenarios do no IO and return new datasets, never mutating input."""

from __future__ import annotations

import difflib
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Protocol, TypeAlias, cast

from datachaos.errors import DataChaosError, FieldError
from datachaos.model.dataset import Dataset
from datachaos.model.ledger import LedgerEntry
from datachaos.model.schema import Role, TableSchema
from datachaos.model.table import Row, Table, Value, sorted_rows
from datachaos.rng import Rng
from datachaos.timeutil import batch_index, parse_duration, parse_timestamp, window_start

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


@dataclass(frozen=True)
class ScenarioStats:
    rows_before: int
    rows_after: int
    rows_selected: int
    rows_added: int
    values_changed: int

    def to_dict(self) -> dict[str, object]:
        return {
            "rows_before": self.rows_before,
            "rows_after": self.rows_after,
            "rows_selected": self.rows_selected,
            "rows_added": self.rows_added,
            "values_changed": self.values_changed,
        }


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
        return resolved

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
    if (rate is None) == (count is None):
        raise DataChaosError("give exactly one of rate and count")
    candidates = sorted((row for row in rows if eligible(row)), key=lambda row: row.rid)
    if rate is not None:
        wanted = math.floor(rate * len(candidates) + 0.5)
        if wanted == 0:
            raise DataChaosError(
                f"rate {rate} of {len(candidates)} candidate rows selects no rows; use count"
            )
    else:
        wanted = cast(int, count)
        if wanted > len(candidates):
            raise DataChaosError(
                f"count {wanted} is more than the {len(candidates)} candidate rows"
            )
    return [candidates[index] for index in rng.sample_indices(len(candidates), wanted)]


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
    column = ingestion_column(table.schema_by_batch[0])
    return batch_index(parse_timestamp(str(row.values[column])), start, interval)


def with_rows(dataset: Dataset, table: str, rows: Sequence[Row]) -> Dataset:
    current = dataset.tables[table]
    updated = replace(current, rows=sorted_rows(current.schema_by_batch[0], rows))
    return replace(dataset, tables={**dataset.tables, table: updated})
