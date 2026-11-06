"""invalid-enum: a category the pipeline does not know."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import cast

from datachaos.errors import FieldError
from datachaos.model.ledger import LedgerEntry
from datachaos.model.schema import Column, Role, TableSchema
from datachaos.rng import Rng
from datachaos.scenarios.base import (
    COMMON_PARAMS,
    BaseScenario,
    ParamSpec,
    ResolveContext,
    ScenarioContext,
    ScenarioOutcome,
    ScenarioSpec,
    ScenarioStats,
    batch_of,
    key_of,
    select,
    with_rows,
)

_VARIANTS = ("literal", "case_change", "whitespace")


def _check_column(value: object, schema: TableSchema) -> str | None:
    for column in schema.columns:
        if column.name == value:
            return None if column.role is Role.ENUM else f"{value} does not have role ENUM"
    return f"table {schema.name} has no column {value!r}"


def _check_values(value: object, schema: TableSchema) -> str | None:
    values = cast("list[object]", value)
    if not values:
        return "must not be empty"
    for item in values:
        if isinstance(item, bool):
            return f"must be strings, got {item!r} (YAML reads NO and yes as booleans, so quote it)"
        if not isinstance(item, str):
            return f"must be strings, got {item!r}"
        if item == "":
            return "must not hold an empty string, which is NULL in CSV"
    return None


def _check_variant(value: object, schema: TableSchema) -> str | None:
    return None if value in _VARIANTS else f"must be one of {list(_VARIANTS)}, got {value!r}"


def _column(schema: TableSchema, name: object) -> Column:
    return next(column for column in schema.columns if column.name == name)


class InvalidEnum(BaseScenario):
    spec = ScenarioSpec(
        name="invalid-enum",
        version=1,
        summary="Replaces a category with a value outside the allowed values.",
        detects="Unknown categories that fall out of CASE expressions and mappings.",
        typical_dbt_test="accepted_values",
        applies_to=frozenset({"entity", "event_log"}),
        params=(
            *COMMON_PARAMS,
            ParamSpec("column", str, True, help="A column with role ENUM.", validate=_check_column),
            ParamSpec(
                "values",
                list,
                False,
                default=("UNKNOWN",),
                help="The values to use with variant literal. Not allowed with other variants.",
                validate=_check_values,
            ),
            ParamSpec(
                "variant",
                str,
                False,
                default="literal",
                help=(
                    "literal uses values. case_change turns paid into PAID, and NO into no. "
                    "whitespace turns paid into 'paid '."
                ),
                validate=_check_variant,
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
        if resolved["variant"] != "literal":
            if "values" in given:
                raise FieldError(
                    "params.values", f"cannot be combined with variant {resolved['variant']}"
                )
            del resolved["values"]
            return
        allowed = _column(ctx.schema_of(table), resolved["column"]).allowed_values
        for value in cast("list[str]", resolved["values"]):
            if value in allowed:
                raise FieldError("params.values", f"{value!r} is an allowed value of the column")

    def apply(self, ctx: ScenarioContext) -> ScenarioOutcome:
        dataset = ctx.dataset
        table = dataset.tables[ctx.table]
        schema = table.schema_by_batch[0]
        name = str(ctx.params["column"])
        allowed = _column(schema, name).allowed_values
        # Only valid values, so that the result is always a change, and never valid by accident.
        picked = select(ctx, lambda row: row.values.get(name) in allowed)
        changed = {row.rid: self._invalid(ctx, str(row.values[name]), ctx.rng) for row in picked}
        rows = [
            replace(row, values={**row.values, name: changed[row.rid]})
            if row.rid in changed
            else row
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
                after=changed[row.rid],
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
        return ScenarioOutcome(with_rows(dataset, ctx.table, rows), ledger, stats)

    def check(self, ctx: ScenarioContext, outcome: ScenarioOutcome) -> list[str]:
        name = str(ctx.params["column"])
        allowed = _column(ctx.dataset.tables[ctx.table].schema_by_batch[0], name).allowed_values
        before = ctx.dataset.tables[ctx.table].rows
        after = {row.rid: row for row in outcome.dataset.tables[ctx.table].rows}
        problems = []
        if len(after) != len(before):
            problems.append(f"expected {len(before)} rows, got {len(after)}")
        changed = {entry.rid: entry for entry in outcome.ledger}
        for row in before:
            current = after.get(row.rid)
            if current is None:
                problems.append(f"row {row.rid} is gone")
                continue
            entry = changed.get(row.rid)
            if entry is None:
                if current != row:
                    problems.append(f"row {row.rid} changed without being selected")
                continue
            value = current.values.get(name)
            if value in allowed:
                problems.append(f"row {row.rid} still has the allowed value {value!r}")
            if value != entry.after or current.values != {**row.values, name: entry.after}:
                problems.append(f"row {row.rid} does not match its ledger line")
        return problems

    def _invalid(self, ctx: ScenarioContext, value: str, rng: Rng) -> str:
        variant = ctx.params["variant"]
        if variant == "literal":
            return rng.choice(cast("list[str]", ctx.params["values"]))
        if variant == "whitespace":
            return f"{value} "
        return value.upper() if value != value.upper() else value.lower()
