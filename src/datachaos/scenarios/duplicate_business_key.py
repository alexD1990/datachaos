"""duplicate-business-key: a second row with the same key, as an upstream retry would send."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import cast

from datachaos.errors import DataChaosError, FieldError
from datachaos.model.ledger import LedgerEntry
from datachaos.model.schema import Column, ColumnType, Role, TableSchema
from datachaos.model.table import Row, Value
from datachaos.scenarios.base import (
    COMMON_PARAMS,
    BaseScenario,
    ParamSpec,
    ResolveContext,
    ScenarioContext,
    ScenarioOutcome,
    ScenarioSpec,
    ScenarioStats,
    batch_bounds,
    batch_of,
    ingestion_column,
    key_of,
    new_rid,
    other_value,
    select,
    with_rows,
)
from datachaos.timeutil import batch_index, format_timestamp, parse_timestamp

_MODES = ("exact_copy", "mutate_non_key")
_MUTABLE_ROLES = (Role.ENUM, Role.ATTRIBUTE)
_MUTABLE_TYPES = (ColumnType.STRING, ColumnType.BOOLEAN, ColumnType.DECIMAL, ColumnType.INTEGER)


def _check_key_columns(value: object, schema: TableSchema) -> str | None:
    names = [column.name for column in schema.columns]
    columns = cast("list[object]", value)
    if not columns or not all(isinstance(column, str) for column in columns):
        return f"must be a non-empty list of column names, got {value!r}"
    if len(set(columns)) != len(columns):
        return f"must not repeat a column, got {value!r}"
    for column in columns:
        if column not in names:
            return f"table {schema.name} has no column {column!r}"
    return None


def _check_mode(value: object, schema: TableSchema) -> str | None:
    return None if value in _MODES else f"must be one of {list(_MODES)}, got {value!r}"


class DuplicateBusinessKey(BaseScenario):
    spec = ScenarioSpec(
        name="duplicate-business-key",
        version=1,
        summary="Adds a second row with the same business key.",
        detects="Broken grain, and staging models that do not deduplicate.",
        typical_dbt_test="unique",
        applies_to=frozenset({"entity"}),
        params=(
            *COMMON_PARAMS,
            ParamSpec(
                "key_columns",
                list,
                False,
                help="Columns that identify a row. Defaults to the columns with role KEY.",
                validate=_check_key_columns,
            ),
            ParamSpec(
                "mutation_mode",
                str,
                False,
                default="exact_copy",
                help="exact_copy, or mutate_non_key to change one other column on the copy.",
                validate=_check_mode,
            ),
            ParamSpec(
                "same_batch",
                bool,
                False,
                default=True,
                help="Keep the copy in the batch of the original, or else in a later one.",
            ),
        ),
    )

    def resolve_more(
        self,
        ctx: ResolveContext,
        table: str,
        resolved: dict[str, object],
        given: Mapping[str, object],
    ) -> None:
        schema = ctx.schema_of(table)
        if "key_columns" not in resolved:
            resolved["key_columns"] = [
                column.name for column in schema.columns if column.role is Role.KEY
            ]
        key_columns = cast("list[str]", resolved["key_columns"])
        baseline = ctx.dataset.tables[table]
        known = {column.name for column in baseline.schema_by_batch[0].columns}
        # A column an earlier scenario added has no baseline values to check.
        if set(key_columns) <= known:
            keys = Counter(_key(row, key_columns) for row in baseline.rows)
            if any(count > 1 for count in keys.values()):
                raise FieldError(
                    "params.key_columns",
                    f"{key_columns} does not identify the rows of {table} uniquely",
                )
        if not resolved["same_batch"] and ctx.dataset.batches == 1:
            raise FieldError("params.same_batch", "false needs more than one batch")

    def apply(self, ctx: ScenarioContext) -> ScenarioOutcome:
        dataset = ctx.dataset
        table = dataset.tables[ctx.table]
        schema = table.schema_by_batch[0]
        same_batch = bool(ctx.params["same_batch"])
        last = dataset.batches - 1
        start, interval = batch_bounds(dataset)
        ingestion = ingestion_column(schema)
        mutate = ctx.params["mutation_mode"] == "mutate_non_key"
        key_columns = cast("list[str]", ctx.params["key_columns"])

        picked = select(
            ctx, None if same_batch else lambda row: batch_of(dataset, table, row) < last
        )
        copies = []
        ledger = []
        for number, row in enumerate(picked, 1):
            source_batch = batch_of(dataset, table, row)
            if same_batch:
                ingested_at = parse_timestamp(str(row.values[ingestion])) + 1
                # One second later may be the next batch, or as_of. Stay in the batch.
                if batch_index(ingested_at, start, interval) != source_batch:
                    ingested_at -= 1
                target = source_batch
            else:
                target = ctx.rng.between(source_batch + 1, last)
                ingested_at = start + target * interval + ctx.rng.below(interval)
            values = {**row.values, ingestion: format_timestamp(ingested_at)}
            column: str | None = None
            before: Value = None
            after: Value = None
            if mutate:
                chosen = ctx.rng.choice(_mutable_columns(schema, row, key_columns))
                column, before = chosen.name, row.values[chosen.name]
                after = other_value(chosen, before, ctx.rng)
                values[chosen.name] = after
            rid = new_rid(ctx.index, number)
            copies.append(Row(rid, values))
            ledger.append(
                LedgerEntry(
                    scenario_index=ctx.index,
                    scenario=self.spec.name,
                    table=ctx.table,
                    action="insert_row",
                    rid=rid,
                    source_rid=row.rid,
                    key=key_of(row, schema),
                    batch=target,
                    column=column,
                    before=before,
                    after=after,
                )
            )
        stats = ScenarioStats(
            rows_before=len(table.rows),
            rows_after=len(table.rows) + len(copies),
            rows_selected=len(picked),
            rows_added=len(copies),
            values_changed=len(copies) if mutate else 0,
        )
        return ScenarioOutcome(
            with_rows(dataset, ctx.table, [*table.rows, *copies]), tuple(ledger), stats
        )

    def check(self, ctx: ScenarioContext, outcome: ScenarioOutcome) -> list[str]:
        before = ctx.dataset.tables[ctx.table]
        after = outcome.dataset.tables[ctx.table]
        schema = before.schema_by_batch[0]
        ingestion = ingestion_column(schema)
        key_columns = cast("list[str]", ctx.params["key_columns"])
        same_batch = bool(ctx.params["same_batch"])
        problems = []
        if len(after.rows) != len(before.rows) + len(outcome.ledger):
            problems.append(f"expected {len(before.rows) + len(outcome.ledger)} rows")
        by_rid = {row.rid: row for row in after.rows}
        for row in before.rows:
            if by_rid.get(row.rid) != row:
                problems.append(f"original row {row.rid} changed")
        counts_before = Counter(_key(row, key_columns) for row in before.rows)
        counts_after = Counter(_key(row, key_columns) for row in after.rows)
        originals = {row.rid: row for row in before.rows}
        added = Counter(
            _key(originals[entry.source_rid or ""], key_columns)
            for entry in outcome.ledger
            if entry.source_rid in originals
        )
        for entry in outcome.ledger:
            copy = by_rid.get(entry.rid)
            source = originals.get(entry.source_rid or "")
            if copy is None or source is None:
                problems.append(f"copy {entry.rid} or its source is missing")
                continue
            key = _key(copy, key_columns)
            if key != _key(source, key_columns):
                problems.append(f"copy {entry.rid} has another key")
            # With allow_overlap, a key can already be duplicated, and be selected more than once.
            expected_count = counts_before[key] + added[key] if ctx.params["allow_overlap"] else 2
            if counts_after[key] != expected_count:
                problems.append(f"key {key} occurs {counts_after[key]} times")
            changed = {
                name for name in copy.values if copy.values[name] != source.values.get(name)
            } - {ingestion}
            expected = {entry.column} if entry.column else set()
            if changed != expected:
                problems.append(f"copy {entry.rid} differs in {sorted(changed)}")
            batch = batch_of(outcome.dataset, after, copy)
            source_batch = batch_of(ctx.dataset, before, source)
            if batch != entry.batch or (batch == source_batch) != same_batch:
                problems.append(f"copy {entry.rid} is in batch {batch}")
            if not same_batch and batch < source_batch:
                problems.append(f"copy {entry.rid} is before its source")
        return problems


def _key(row: Row, key_columns: list[str]) -> tuple[Value, ...]:
    return tuple(row.values.get(column) for column in key_columns)


def _mutable_columns(schema: TableSchema, row: Row, key_columns: list[str]) -> list[Column]:
    columns = [
        column
        for column in schema.columns
        if column.role in _MUTABLE_ROLES
        and column.name not in key_columns
        and column.type in _MUTABLE_TYPES
        and row.values.get(column.name) is not None
        and (column.role is not Role.ENUM or len(column.allowed_values) > 1)
    ]
    if not columns:
        raise DataChaosError(f"row {row.rid} has no column that mutate_non_key can change")
    return columns
