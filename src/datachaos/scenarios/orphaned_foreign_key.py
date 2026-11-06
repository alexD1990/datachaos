"""orphaned-foreign-key: references to a parent that does not exist."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace

from datachaos.errors import FieldError
from datachaos.model.dataset import Dataset
from datachaos.model.ledger import LedgerEntry
from datachaos.model.schema import Column, Role, TableSchema
from datachaos.model.table import Row
from datachaos.rng import Rng
from datachaos.scenarios.base import (
    COMMON_PARAMS,
    BaseScenario,
    Group,
    ParamSpec,
    ResolveContext,
    ScenarioContext,
    ScenarioOutcome,
    ScenarioSpec,
    ScenarioStats,
    batch_of,
    column_named,
    key_of,
    select,
    select_groups,
    with_rows,
)

_MODES = ("nonexistent", "deleted_parent")


def _check_column(value: object, schema: TableSchema) -> str | None:
    column = column_named(schema, value)
    if column is None:
        return f"table {schema.name} has no column {value!r}"
    if column.role is not Role.FOREIGN_KEY or column.references is None:
        return f"{value} is not a foreign key"
    return None


def _check_mode(value: object, schema: TableSchema) -> str | None:
    return None if value in _MODES else f"must be one of {list(_MODES)}, got {value!r}"


def _parent_of(column: Column) -> tuple[str, str]:
    table, _, name = (column.references or "").partition(".")
    return table, name


def _reserved_id(parent_id: str, rng: Rng) -> str:
    """An ID in the 9xxxxx series, which the generator never uses. Rows may share one."""
    return f"{parent_id[0]}9{rng.below(100_000):05d}"


class OrphanedForeignKey(BaseScenario):
    spec = ScenarioSpec(
        name="orphaned-foreign-key",
        version=1,
        summary="Makes foreign keys point at a parent that does not exist.",
        detects="Inner joins that lose rows, and missing referential integrity.",
        typical_dbt_test="relationships",
        applies_to=frozenset({"entity", "event_log"}),
        params=(
            *COMMON_PARAMS,
            ParamSpec(
                "column",
                str,
                False,
                help="The foreign key column. Defaults to the first one in the table.",
                validate=_check_column,
            ),
            ParamSpec(
                "mode",
                str,
                False,
                default="nonexistent",
                help=(
                    "nonexistent points selected rows at an ID that does not exist. "
                    "deleted_parent deletes selected parents, so all their children are orphaned. "
                    "rate and count then select parents, among those with children."
                ),
                validate=_check_mode,
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
        if "column" not in resolved:
            schema = ctx.schema_of(table)
            keys = [column.name for column in schema.columns if column.role is Role.FOREIGN_KEY]
            if not keys:
                raise FieldError("table", f"{table} has no foreign key column")
            resolved["column"] = keys[0]

    def apply(self, ctx: ScenarioContext) -> ScenarioOutcome:
        if ctx.params["mode"] == "deleted_parent":
            return self._delete_parents(ctx)
        return self._point_at_nothing(ctx)

    def _point_at_nothing(self, ctx: ScenarioContext) -> ScenarioOutcome:
        dataset = ctx.dataset
        table = dataset.tables[ctx.table]
        schema = table.stable_schema
        column = _column(ctx)
        name = column.name
        parents = _parent_ids(dataset, column)
        # Only rows that still point at a parent, so that the change is real.
        picked = select(ctx, lambda row: row.values.get(name) in parents)
        changed = {row.rid: _reserved_id(str(row.values[name]), ctx.rng) for row in picked}
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

    def _delete_parents(self, ctx: ScenarioContext) -> ScenarioOutcome:
        dataset = ctx.dataset
        children = dataset.tables[ctx.table]
        child_schema = children.stable_schema
        column = _column(ctx)
        name = column.name
        parent_name, parent_column = _parent_of(column)
        parents = dataset.tables[parent_name]
        parent_schema = parents.stable_schema

        by_parent: dict[object, list[Row]] = {}
        for row in children.rows:
            by_parent.setdefault(row.values.get(name), []).append(row)

        def has_free_children(group: Group) -> bool:
            kids = by_parent.get(group.key[parent_column], [])
            return bool(kids) and all(ctx.eligible(kid) for kid in kids)

        picked = select_groups(
            parents.rows,
            [parent_column],
            ctx.params,
            ctx.rng,
            eligible=ctx.eligible_in(parent_name),
            candidate=has_free_children,
        )
        ledger = []
        removed = set()
        for group in picked:
            for row in group.rows:
                removed.add(row.rid)
                ledger.append(
                    LedgerEntry(
                        scenario_index=ctx.index,
                        scenario=self.spec.name,
                        table=parent_name,
                        action="delete_row",
                        rid=row.rid,
                        key=key_of(row, parent_schema),
                        batch=batch_of(dataset, parents, row),
                    )
                )
            for kid in by_parent[group.key[parent_column]]:
                ledger.append(
                    LedgerEntry(
                        scenario_index=ctx.index,
                        scenario=self.spec.name,
                        table=ctx.table,
                        action="orphan_row",
                        rid=kid.rid,
                        key=key_of(kid, child_schema),
                        batch=batch_of(dataset, children, kid),
                        column=name,
                        before=kid.values[name],
                        after=kid.values[name],
                    )
                )
        stats = ScenarioStats(
            rows_before=len(children.rows),
            rows_after=len(children.rows),
            rows_selected=len(picked),
            rows_added=0,
            values_changed=0,
            rows_removed=len(removed),
        )
        remaining = [row for row in parents.rows if row.rid not in removed]
        return ScenarioOutcome(with_rows(dataset, parent_name, remaining), tuple(ledger), stats)

    def check(self, ctx: ScenarioContext, outcome: ScenarioOutcome) -> list[str]:
        column = _column(ctx)
        name = column.name
        parent_name, _ = _parent_of(column)
        after_parents = _parent_ids(outcome.dataset, column)
        before_children = {row.rid: row for row in ctx.dataset.tables[ctx.table].rows}
        after_children = {row.rid: row for row in outcome.dataset.tables[ctx.table].rows}
        problems = []
        if set(before_children) != set(after_children):
            problems.append(f"the rows of {ctx.table} changed")
        for table in ctx.dataset.tables:
            if table not in (ctx.table, parent_name) and (
                ctx.dataset.tables[table] != outcome.dataset.tables[table]
            ):
                problems.append(f"table {table} changed")

        child_lines = {
            entry.rid: entry
            for entry in outcome.ledger
            if entry.table == ctx.table and entry.action != "delete_row"
        }
        for rid, entry in child_lines.items():
            current = after_children.get(rid)
            if current is None:
                problems.append(f"row {rid} is gone")
            elif current.values.get(name) in after_parents:
                problems.append(f"row {rid} still has a parent")
            elif current.values.get(name) != entry.after:
                problems.append(f"row {rid} does not match its ledger line")
        for rid, row in before_children.items():
            current = after_children.get(rid)
            if rid in child_lines:
                expected = child_lines[rid].after
                if current is not None and current.values != {**row.values, name: expected}:
                    problems.append(f"row {rid} changed more than {name}")
            elif current != row:
                problems.append(f"row {rid} changed without being selected")

        if ctx.params["mode"] == "deleted_parent":
            problems.extend(self._check_deleted(ctx, outcome, column, child_lines))
        elif ctx.dataset.tables[parent_name] != outcome.dataset.tables[parent_name]:
            problems.append(f"parent table {parent_name} changed")
        return problems

    def _check_deleted(
        self,
        ctx: ScenarioContext,
        outcome: ScenarioOutcome,
        column: Column,
        child_lines: Mapping[str, LedgerEntry],
    ) -> list[str]:
        name = column.name
        parent_name, _ = _parent_of(column)
        problems = []
        before = {row.rid: row for row in ctx.dataset.tables[parent_name].rows}
        after = {row.rid: row for row in outcome.dataset.tables[parent_name].rows}
        deleted = {entry.rid for entry in outcome.ledger if entry.action == "delete_row"}
        if set(before) - set(after) != deleted or set(after) - set(before):
            problems.append(f"the ledger does not match the rows removed from {parent_name}")
        if any(after[rid] != before[rid] for rid in after if rid in before):
            problems.append(f"a remaining row of {parent_name} changed")
        parents_before = _parent_ids(ctx.dataset, column)
        parents_after = _parent_ids(outcome.dataset, column)
        lost = parents_before - parents_after
        for row in ctx.dataset.tables[ctx.table].rows:
            if row.values.get(name) in lost and row.rid not in child_lines:
                problems.append(f"orphaned row {row.rid} has no ledger line")
        return problems


def _column(ctx: ScenarioContext) -> Column:
    schema = ctx.dataset.tables[ctx.table].stable_schema
    column = column_named(schema, ctx.params["column"])
    assert column is not None
    return column


def _parent_ids(dataset: Dataset, column: Column) -> set[object]:
    table, name = _parent_of(column)
    return {row.values.get(name) for row in dataset.tables[table].rows}
