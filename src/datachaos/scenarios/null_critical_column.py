"""null-critical-column: NULL where the pipeline expects a value."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import cast

from datachaos.errors import FieldError
from datachaos.model.ledger import LedgerEntry
from datachaos.model.schema import Role, TableSchema
from datachaos.scenarios.base import (
    COMMON_PARAMS,
    BaseScenario,
    ParamSpec,
    ResolveContext,
    ResolvedParams,
    ScenarioContext,
    ScenarioOutcome,
    ScenarioSpec,
    ScenarioStats,
    batch_of,
    key_of,
    select,
    with_rows,
)


def _check_column(value: object, schema: TableSchema) -> str | None:
    for column in schema.columns:
        if column.name == value:
            if column.role is Role.INGESTION_TIME:
                return f"{value} decides the batch of a row, so it cannot be NULL"
            return None
    return f"table {schema.name} has no column {value!r}"


class NullCriticalColumn(BaseScenario):
    spec = ScenarioSpec(
        name="null-critical-column",
        version=1,
        summary="Sets a column to NULL, whether or not the schema allows it.",
        detects="Missing not_null tests, and joins that lose rows on NULL.",
        typical_dbt_test="not_null",
        applies_to=frozenset({"entity", "event_log"}),
        params=(
            *COMMON_PARAMS,
            ParamSpec(
                "column", str, True, help="The column to set to NULL.", validate=_check_column
            ),
            ParamSpec(
                "allow_key",
                bool,
                False,
                default=False,
                help="Allow a column with role KEY.",
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
        name = resolved["column"]
        for column in ctx.schema_of(table).columns:
            if column.name == name and column.role is Role.KEY and not resolved["allow_key"]:
                raise FieldError("params.column", f"{name} is a key column; set allow_key: true")

    def columns_used(
        self, ctx: ResolveContext, table: str, params: ResolvedParams
    ) -> frozenset[str]:
        return frozenset({cast(str, params["column"])})

    def apply(self, ctx: ScenarioContext) -> ScenarioOutcome:
        dataset = ctx.dataset
        table = dataset.tables[ctx.table]
        schema = table.stable_schema
        name = str(ctx.params["column"])
        # A row that already holds NULL would not change.
        picked = select(ctx, lambda row: row.values.get(name) is not None)
        rids = {row.rid for row in picked}
        rows = [
            replace(row, values={**row.values, name: None}) if row.rid in rids else row
            for row in table.rows
        ]
        ledger = tuple(
            LedgerEntry(
                scenario_index=ctx.index,
                scenario=self.spec.name,
                table=ctx.table,
                action="set_value",
                rid=row.rid,
                key=key_of(row, schema),
                batch=batch_of(dataset, table, row),
                column=name,
                before=row.values[name],
                after=None,
            )
            for row in picked
        )
        stats = ScenarioStats(
            rows_before=len(table.rows),
            rows_after=len(rows),
            rows_selected=len(picked),
            rows_added=0,
            values_changed=len(picked),
        )
        column = next(item for item in schema.columns if item.name == name)
        warnings = (
            ()
            if column.nullable
            else (
                f"scenarios[{ctx.index}]: {ctx.table}.{name} is not nullable in the schema, "
                f"but {len(picked)} rows now hold NULL",
            )
        )
        return ScenarioOutcome(with_rows(dataset, ctx.table, rows), ledger, stats, warnings)

    def check(self, ctx: ScenarioContext, outcome: ScenarioOutcome) -> list[str]:
        name = str(ctx.params["column"])
        before = ctx.dataset.tables[ctx.table].rows
        after = {row.rid: row for row in outcome.dataset.tables[ctx.table].rows}
        problems = []
        if len(after) != len(before):
            problems.append(f"expected {len(before)} rows, got {len(after)}")
        changed = {entry.rid for entry in outcome.ledger}
        for row in before:
            current = after.get(row.rid)
            if current is None:
                problems.append(f"row {row.rid} is gone")
            elif row.rid not in changed:
                if current != row:
                    problems.append(f"row {row.rid} changed without being selected")
            elif current.values != {**row.values, name: None} or row.values[name] is None:
                problems.append(f"row {row.rid} did not go from a value to NULL only")
        nulls_before = sum(row.values.get(name) is None for row in before)
        nulls_after = sum(row.values.get(name) is None for row in after.values())
        if nulls_after != nulls_before + len(changed):
            problems.append(f"expected {nulls_before + len(changed)} NULLs, got {nulls_after}")
        return problems
