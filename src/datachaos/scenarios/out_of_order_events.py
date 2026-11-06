"""out-of-order-events: an event that lands after the event that followed it."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace

from datachaos.errors import FieldError
from datachaos.model.ledger import LedgerEntry
from datachaos.model.schema import Role, TableSchema
from datachaos.model.table import Row
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
    batch_bounds,
    column_named,
    groups_of,
    ingestion_column,
    key_of,
    select_groups,
    with_rows,
)
from datachaos.timeutil import batch_index, format_timestamp, parse_timestamp

_MAX_HOLD_BACK = 6 * 3600


def _check_group_by(value: object, schema: TableSchema) -> str | None:
    column = column_named(schema, value)
    if column is None:
        return f"table {schema.name} has no column {value!r}"
    if column.role is Role.KEY:
        return f"{value} is the key, so every group would be one row"
    return None


class OutOfOrderEvents(BaseScenario):
    spec = ScenarioSpec(
        name="out-of-order-events",
        version=1,
        summary="Holds back the next to last event of a group, so that it lands after the last.",
        detects=(
            "Latest-event-wins logic that sorts on ingested_at instead of event time, or that "
            "assumes events arrive in order."
        ),
        typical_dbt_test="singular test that the current status comes from the latest event time",
        applies_to=frozenset({"event_log"}),
        params=(
            *COMMON_PARAMS,
            ParamSpec(
                "group_by",
                str,
                False,
                help=(
                    "The column that defines a group. Defaults to the foreign key. rate and count "
                    "select groups, among those with two events in order and room before as_of."
                ),
                validate=_check_group_by,
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
        if "group_by" in resolved:
            return
        schema = ctx.schema_of(table)
        keys = [column.name for column in schema.columns if column.role is Role.FOREIGN_KEY]
        if not keys:
            raise FieldError("params.group_by", f"{table} has no foreign key, so give a column")
        resolved["group_by"] = keys[0]

    def apply(self, ctx: ScenarioContext) -> ScenarioOutcome:
        dataset = ctx.dataset
        table = dataset.tables[ctx.table]
        schema = table.schema_by_batch[0]
        event_time = schema.column_with_role(Role.EVENT_TIME).name
        ingestion = ingestion_column(schema)
        as_of = parse_timestamp(dataset.as_of)
        start, interval = batch_bounds(dataset)

        def is_candidate(group: Group) -> bool:
            return _last_two(group.rows, event_time, ingestion, as_of) is not None

        picked = select_groups(
            table.rows,
            [str(ctx.params["group_by"])],
            ctx.params,
            ctx.rng,
            eligible=ctx.eligible,
            candidate=is_candidate,
        )
        moved = {}
        ledger = []
        for group in picked:
            pair = _last_two(group.rows, event_time, ingestion, as_of)
            assert pair is not None
            earlier, last = pair
            before = parse_timestamp(str(earlier.values[ingestion]))
            floor = parse_timestamp(str(last.values[ingestion])) + 1
            after = ctx.rng.between(floor, min(floor + _MAX_HOLD_BACK, as_of - 1))
            old_batch = batch_index(before, start, interval)
            new_batch = batch_index(after, start, interval)
            moved[earlier.rid] = format_timestamp(after)
            ledger.append(
                LedgerEntry(
                    scenario_index=ctx.index,
                    scenario=self.spec.name,
                    table=ctx.table,
                    action="set_value",
                    rid=earlier.rid,
                    key=key_of(earlier, schema),
                    group=group.key,
                    batch=new_batch,
                    column=ingestion,
                    before=earlier.values[ingestion],
                    after=moved[earlier.rid],
                )
            )
            if new_batch != old_batch:
                ledger.append(
                    LedgerEntry(
                        scenario_index=ctx.index,
                        scenario=self.spec.name,
                        table=ctx.table,
                        action="move_batch",
                        rid=earlier.rid,
                        key=key_of(earlier, schema),
                        group=group.key,
                        batch=new_batch,
                        before=old_batch,
                        after=new_batch,
                    )
                )
        rows = [
            replace(row, values={**row.values, ingestion: moved[row.rid]})
            if row.rid in moved
            else row
            for row in table.rows
        ]
        stats = ScenarioStats(
            rows_before=len(table.rows),
            rows_after=len(rows),
            rows_selected=len(picked),
            rows_added=0,
            values_changed=len(picked),
        )
        return ScenarioOutcome(with_rows(dataset, ctx.table, rows), tuple(ledger), stats)

    def check(self, ctx: ScenarioContext, outcome: ScenarioOutcome) -> list[str]:
        table = ctx.dataset.tables[ctx.table]
        schema = table.schema_by_batch[0]
        event_time = schema.column_with_role(Role.EVENT_TIME).name
        ingestion = ingestion_column(schema)
        as_of = parse_timestamp(ctx.dataset.as_of)
        group_by = [str(ctx.params["group_by"])]
        after_rows = outcome.dataset.tables[ctx.table].rows
        after = {row.rid: row for row in after_rows}
        changed = {entry.rid for entry in outcome.ledger if entry.action == "set_value"}
        problems = []
        if len(after_rows) != len(table.rows):
            problems.append(f"expected {len(table.rows)} rows, got {len(after_rows)}")
        for row in table.rows:
            current = after.get(row.rid)
            if current is None:
                problems.append(f"row {row.rid} is gone")
            elif row.rid not in changed:
                if current != row:
                    problems.append(f"row {row.rid} changed without being selected")
            elif {**current.values, ingestion: None} != {**row.values, ingestion: None}:
                problems.append(f"row {row.rid} changed more than {ingestion}")
            elif parse_timestamp(str(current.values[ingestion])) >= as_of:
                problems.append(f"row {row.rid} landed on or after as_of")
        groups = {tuple(group.key.values()): group for group in groups_of(after_rows, group_by)}
        for entry in outcome.ledger:
            group = groups.get(tuple((entry.group or {}).values()))
            if group is None:
                problems.append(f"the group of {entry.rid} is gone")
            elif _in_order(_by_event_time(group.rows, event_time)[-2:], ingestion):
                problems.append(f"group {entry.group} is still in order")
        return problems


def _by_event_time(rows: Sequence[Row], event_time: str) -> list[Row]:
    return sorted(rows, key=lambda row: (str(row.values[event_time]), row.rid))


def _in_order(pair: Sequence[Row], ingestion: str) -> bool:
    return parse_timestamp(str(pair[0].values[ingestion])) < parse_timestamp(
        str(pair[1].values[ingestion])
    )


def _last_two(
    rows: Sequence[Row], event_time: str, ingestion: str, as_of: int
) -> tuple[Row, Row] | None:
    """The two latest events, if holding back the earlier of them is a real change that fits."""
    if len(rows) < 2 or any(row.values.get(event_time) is None for row in rows):
        return None
    earlier, last = _by_event_time(rows, event_time)[-2:]
    if earlier.values[event_time] == last.values[event_time]:
        return None
    if not _in_order((earlier, last), ingestion):
        return None
    if parse_timestamp(str(last.values[ingestion])) + 1 >= as_of:
        return None
    return earlier, last
