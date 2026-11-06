"""schema-drift: the schema of a table changes between two deliveries."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import replace
from typing import cast

from datachaos.errors import FieldError
from datachaos.model.ledger import LedgerEntry
from datachaos.model.schema import (
    Column,
    ColumnType,
    Role,
    SchemaStep,
    StepFill,
    StepOperation,
    TableSchema,
)
from datachaos.model.table import Row, Value
from datachaos.scenarios.base import (
    BaseScenario,
    ParamSpec,
    ResolveContext,
    ResolvedParams,
    ScenarioContext,
    ScenarioOutcome,
    ScenarioSpec,
    ScenarioStats,
    SchemaState,
    apply_step,
    batch_of,
    column_named,
)

_OPERATIONS = ("add_column", "drop_column", "rename_column", "change_type")
_CONVERSIONS = {
    "string_comma_decimal": (ColumnType.DECIMAL,),
    "string_local_format": (ColumnType.TIMESTAMP,),
    "string": (ColumnType.INTEGER, ColumnType.DECIMAL),
    "string_yes_no": (ColumnType.BOOLEAN,),
}
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_RESERVED = "_chaos_rid"


def _check_operation(value: object, schema: TableSchema) -> str | None:
    return None if value in _OPERATIONS else f"must be one of {list(_OPERATIONS)}, got {value!r}"


def _check_to(value: object, schema: TableSchema) -> str | None:
    return (
        None if value in _CONVERSIONS else f"must be one of {sorted(_CONVERSIONS)}, got {value!r}"
    )


def _check_fill(value: object, schema: TableSchema) -> str | None:
    # YAML reads an unquoted null as None.
    return (
        None
        if value in (None, "null", "generated")
        else f"must be null or generated, got {value!r}"
    )


def _step(params: ResolvedParams, fill_seed: int = 0) -> SchemaStep:
    return SchemaStep(
        from_batch=cast(int, params["from_batch"]),
        operation=cast(StepOperation, params["operation"]),
        column=cast(str, params["column"]),
        new_name=cast("str | None", params.get("new_name")),
        to=cast("str | None", params.get("to")),
        fill=cast(StepFill, params.get("fill", "null")),
        fill_seed=fill_seed,
    )


def _changed_schema(schema: TableSchema, step: SchemaStep) -> TableSchema:
    if step.operation == "add_column":
        return replace(
            schema, columns=(*schema.columns, Column(step.column, ColumnType.STRING, True))
        )
    if step.operation == "drop_column":
        columns = tuple(column for column in schema.columns if column.name != step.column)
    elif step.operation == "rename_column":
        columns = tuple(
            replace(column, name=cast(str, step.new_name)) if column.name == step.column else column
            for column in schema.columns
        )
    else:
        columns = tuple(
            replace(column, type=ColumnType.STRING, scale=None)
            if column.name == step.column
            else column
            for column in schema.columns
        )
    return replace(schema, columns=columns)


def _evolved(schemas: tuple[TableSchema, ...], step: SchemaStep) -> tuple[TableSchema, ...]:
    return tuple(
        _changed_schema(schema, step) if batch >= step.from_batch else schema
        for batch, schema in enumerate(schemas)
    )


class SchemaDrift(BaseScenario):
    spec = ScenarioSpec(
        name="schema-drift",
        version=1,
        summary="Changes the schema of a table from one batch on.",
        detects="Pipelines that fail or silently lose data when a schema changes.",
        typical_dbt_test="none; often shows up as a model with status error",
        applies_to=frozenset({"entity", "event_log"}),
        changes_schema=True,
        params=(
            ParamSpec(
                "operation",
                str,
                True,
                help="add_column, drop_column, rename_column or change_type.",
                validate=_check_operation,
            ),
            ParamSpec(
                "column",
                str,
                True,
                help=(
                    "The column to change, or the name of the new one. The ingestion time can "
                    "never change, and a key cannot be dropped or renamed."
                ),
            ),
            ParamSpec(
                "from_batch",
                int,
                False,
                help="The first batch with the new schema. Defaults to the last one.",
            ),
            ParamSpec("new_name", str, False, help="The new name, with rename_column."),
            ParamSpec(
                "to",
                str,
                False,
                help=(
                    "With change_type: string_comma_decimal from decimal, string_local_format "
                    "from timestamp, string from integer or decimal, string_yes_no from boolean."
                ),
                validate=_check_to,
            ),
            ParamSpec(
                "fill",
                (str, type(None)),
                False,
                help="With add_column: null (default), or generated for a value derived per row.",
                validate=_check_fill,
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
        batches = ctx.dataset.batches
        from_batch = cast(int, resolved.setdefault("from_batch", batches - 1))
        if not 0 <= from_batch < batches:
            raise FieldError(
                "params.from_batch", f"must be from 0 to {batches - 1}, got {from_batch}"
            )
        operation = resolved["operation"]
        for name, allowed in (
            ("new_name", "rename_column"),
            ("to", "change_type"),
            ("fill", "add_column"),
        ):
            if name in given and operation != allowed:
                raise FieldError(f"params.{name}", f"only applies to {allowed}")
        name = cast(str, resolved["column"])
        schema = ctx.schema_of(table)
        if operation == "add_column":
            self._check_free(ctx, table, "params.column", name)
            resolved["fill"] = "null" if resolved.get("fill") is None else resolved["fill"]
            return
        column = column_named(schema, name)
        if column is None:
            raise FieldError("params.column", f"table {table} has no column {name!r}")
        if column.role is Role.INGESTION_TIME:
            raise FieldError(
                "params.column", f"{name} decides the batch of a row, so it cannot change"
            )
        if column.role is Role.KEY and operation != "change_type":
            raise FieldError("params.column", f"{name} is a key column, so it cannot change")
        if operation == "rename_column":
            if "new_name" not in resolved:
                raise FieldError("params.new_name", "is required with rename_column")
            self._check_free(ctx, table, "params.new_name", cast(str, resolved["new_name"]))
        if operation == "change_type":
            if "to" not in resolved:
                raise FieldError("params.to", "is required with change_type")
            sources = _CONVERSIONS[cast(str, resolved["to"])]
            if column.type not in sources:
                raise FieldError(
                    "params.to",
                    f"{resolved['to']} does not apply to {name}, which is {column.type.value}",
                )

    def _check_free(self, ctx: ResolveContext, table: str, field: str, name: str) -> None:
        if not _NAME.fullmatch(name) or name == _RESERVED:
            raise FieldError(field, f"{name!r} cannot be a column name")
        for schema in ctx.schemas[table]:
            if column_named(schema, name) is not None:
                raise FieldError(field, f"table {table} already has a column {name!r}")

    def schemas_after(self, ctx: ResolveContext, table: str, params: ResolvedParams) -> SchemaState:
        return {**ctx.schemas, table: _evolved(ctx.schemas[table], _step(params))}

    def columns_used(
        self, ctx: ResolveContext, table: str, params: ResolvedParams
    ) -> frozenset[str]:
        names = {cast(str, params["column"])}
        if "new_name" in params:
            names.add(cast(str, params["new_name"]))
        return frozenset(names)

    def apply(self, ctx: ScenarioContext) -> ScenarioOutcome:
        dataset = ctx.dataset
        table = dataset.tables[ctx.table]
        step = _step(ctx.params, ctx.rng.below(1 << 53))
        rows = []
        changed = 0
        for row in table.rows:
            if batch_of(dataset, table, row) >= step.from_batch:
                rows.append(Row(row.rid, apply_step(step, row.values, row.rid)))
                changed += 1
            else:
                rows.append(row)
        updated = replace(
            table,
            schema_by_batch=_evolved(table.schema_by_batch, step),
            rows=tuple(rows),
            schema_steps=(*table.schema_steps, step),
        )
        before, after = _described(step, table.schema_by_batch[-1])
        ledger = (
            LedgerEntry(
                scenario_index=ctx.index,
                scenario=self.spec.name,
                table=ctx.table,
                action="schema_change",
                rid="",
                key={},
                batch=step.from_batch,
                column=step.column,
                before=before,
                after=after,
            ),
        )
        rewrites = step.operation == "change_type" or step.fill == "generated"
        stats = ScenarioStats(
            rows_before=len(rows),
            rows_after=len(rows),
            rows_selected=changed,
            rows_added=0,
            values_changed=changed if rewrites else 0,
        )
        tables = {**dataset.tables, ctx.table: updated}
        return ScenarioOutcome(replace(dataset, tables=tables), ledger, stats)

    def check(self, ctx: ScenarioContext, outcome: ScenarioOutcome) -> list[str]:
        table = ctx.dataset.tables[ctx.table]
        after = outcome.dataset.tables[ctx.table]
        step = after.schema_steps[-1]
        problems = []
        if after.schema_by_batch != _evolved(table.schema_by_batch, step):
            problems.append("the schemas do not reflect the change")
        current = {row.rid: row for row in after.rows}
        if len(current) != len(table.rows):
            problems.append(f"expected {len(table.rows)} rows, got {len(current)}")
        for row in table.rows:
            found = current.get(row.rid)
            if found is None:
                problems.append(f"row {row.rid} is gone")
                continue
            batch = batch_of(ctx.dataset, table, row)
            if batch < step.from_batch:
                if found != row:
                    problems.append(f"row {row.rid} before from_batch changed")
            elif found.values != apply_step(step, row.values, row.rid):
                problems.append(f"row {row.rid} does not have the new form")
        for name, other in ctx.dataset.tables.items():
            if name != ctx.table and outcome.dataset.tables[name] != other:
                problems.append(f"table {name} changed")
        return problems


def _described(step: SchemaStep, schema: TableSchema) -> tuple[Value, Value]:
    if step.operation == "add_column":
        return None, step.column
    if step.operation == "drop_column":
        return step.column, None
    if step.operation == "rename_column":
        return step.column, step.new_name
    column = column_named(schema, step.column)
    assert column is not None
    return column.type.value, step.to
