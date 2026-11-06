"""historical-update: a new version of an old row, as a source with incremental extraction sends."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import cast

from datachaos.errors import FieldError
from datachaos.model.ledger import LedgerEntry
from datachaos.model.schema import Role, TableSchema
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
    column_named,
    ingestion_column,
    key_of,
    new_rid,
    other_value,
    select,
    with_rows,
)
from datachaos.timeutil import format_timestamp, parse_duration, parse_timestamp

_CHANGEABLE_ROLES = (Role.ENUM, Role.ATTRIBUTE)
_UPDATE_DELAY_MAX = 1800


def _check_older_than(value: object, schema: TableSchema) -> str | None:
    try:
        seconds = parse_duration(cast(str, value))
    except ValueError as exc:
        return str(exc)
    return None if seconds > 0 else f"must be above zero, got {value!r}"


def _check_columns(value: object, schema: TableSchema) -> str | None:
    columns = cast("list[object]", value)
    if not columns or not all(isinstance(column, str) for column in columns):
        return f"must be a non-empty list of column names, got {value!r}"
    if len(set(columns)) != len(columns):
        return f"must not repeat a column, got {value!r}"
    for name in columns:
        column = column_named(schema, name)
        if column is None:
            return f"table {schema.name} has no column {name!r} (give the columns to change)"
        if column.role not in _CHANGEABLE_ROLES:
            return f"{name} has role {column.role.value}; only enum and attribute can change"
    return None


class HistoricalUpdate(BaseScenario):
    spec = ScenarioSpec(
        name="historical-update",
        version=1,
        summary="Adds a new version of old rows in a later batch, with a new updated_at.",
        detects=(
            "Incremental models that only see new keys, or that filter on event time instead of "
            "updated_at."
        ),
        typical_dbt_test="singular test comparing the incremental model with a full refresh",
        applies_to=frozenset({"entity"}),
        params=(
            *COMMON_PARAMS,
            ParamSpec(
                "older_than",
                str,
                True,
                help="Candidates have an event time this long before as_of, such as 3d.",
                validate=_check_older_than,
            ),
            ParamSpec(
                "columns",
                list,
                False,
                default=("status", "amount"),
                help="The columns that change in the new version. They must be enum or attribute.",
                validate=_check_columns,
            ),
            ParamSpec(
                "in_batch",
                int,
                False,
                help=(
                    "The batch of the new versions. Defaults to the last one. Candidates are in "
                    "an earlier batch."
                ),
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
        if batches == 1:
            raise FieldError("params.in_batch", "needs at least two batches")
        in_batch = cast(int, resolved.setdefault("in_batch", batches - 1))
        if not 1 <= in_batch < batches:
            raise FieldError("params.in_batch", f"must be from 1 to {batches - 1}, got {in_batch}")
        baseline = ctx.dataset.tables[table]
        event_time = baseline.schema_by_batch[0].column_with_role(Role.EVENT_TIME).name
        cutoff = _cutoff(ctx.dataset.as_of, resolved)
        if not any(parse_timestamp(str(row.values[event_time])) < cutoff for row in baseline.rows):
            raise FieldError(
                "params.older_than",
                f"no row of {table} is {resolved['older_than']} older than as_of",
            )

    def apply(self, ctx: ScenarioContext) -> ScenarioOutcome:
        dataset = ctx.dataset
        table = dataset.tables[ctx.table]
        schema = table.schema_by_batch[0]
        event_time = schema.column_with_role(Role.EVENT_TIME).name
        updated_at = schema.column_with_role(Role.UPDATED_AT).name
        ingestion = ingestion_column(schema)
        names = cast("list[str]", ctx.params["columns"])
        in_batch = cast(int, ctx.params["in_batch"])
        cutoff = _cutoff(dataset.as_of, ctx.params)
        start, interval = batch_bounds(dataset)

        def is_candidate(row: Row) -> bool:
            required: list[Value] = [row.values.get(name) for name in names]
            required += [row.values.get(event_time), row.values.get(updated_at)]
            return (
                all(value is not None for value in required)
                and parse_timestamp(str(row.values[event_time])) < cutoff
                and batch_of(dataset, table, row) < in_batch
            )

        picked = select(ctx, is_candidate)
        versions = []
        ledger = []
        for number, row in enumerate(picked, 1):
            rid = new_rid(ctx.index, number)
            ingested_at = start + in_batch * interval + ctx.rng.below(interval)
            earliest = max(
                parse_timestamp(str(row.values[updated_at])) + 1, ingested_at - _UPDATE_DELAY_MAX
            )
            values = {
                **row.values,
                updated_at: format_timestamp(ctx.rng.between(earliest, ingested_at)),
                ingestion: format_timestamp(ingested_at),
            }
            key = key_of(row, schema)
            ledger.append(
                LedgerEntry(
                    scenario_index=ctx.index,
                    scenario=self.spec.name,
                    table=ctx.table,
                    action="insert_row",
                    rid=rid,
                    source_rid=row.rid,
                    key=key,
                    batch=in_batch,
                )
            )
            for name in names:
                column = column_named(schema, name)
                assert column is not None
                values[name] = other_value(column, row.values[name], ctx.rng)
                ledger.append(
                    LedgerEntry(
                        scenario_index=ctx.index,
                        scenario=self.spec.name,
                        table=ctx.table,
                        action="set_value",
                        rid=rid,
                        key=key,
                        batch=in_batch,
                        column=name,
                        before=row.values[name],
                        after=values[name],
                    )
                )
            versions.append(Row(rid, values))
        stats = ScenarioStats(
            rows_before=len(table.rows),
            rows_after=len(table.rows) + len(versions),
            rows_selected=len(picked),
            rows_added=len(versions),
            values_changed=len(versions) * len(names),
        )
        return ScenarioOutcome(
            with_rows(dataset, ctx.table, [*table.rows, *versions]), tuple(ledger), stats
        )

    def check(self, ctx: ScenarioContext, outcome: ScenarioOutcome) -> list[str]:
        table = ctx.dataset.tables[ctx.table]
        schema = table.schema_by_batch[0]
        event_time = schema.column_with_role(Role.EVENT_TIME).name
        updated_at = schema.column_with_role(Role.UPDATED_AT).name
        ingestion = ingestion_column(schema)
        names = cast("list[str]", ctx.params["columns"])
        in_batch = cast(int, ctx.params["in_batch"])
        cutoff = _cutoff(ctx.dataset.as_of, ctx.params)
        after_table = outcome.dataset.tables[ctx.table]
        after = {row.rid: row for row in after_table.rows}
        before = {row.rid: row for row in table.rows}
        problems = []
        for rid, row in before.items():
            if after.get(rid) != row:
                problems.append(f"original row {rid} changed")
        inserts = [entry for entry in outcome.ledger if entry.action == "insert_row"]
        if len(after) != len(before) + len(inserts):
            problems.append(f"expected {len(before) + len(inserts)} rows, got {len(after)}")
        changes = Counter(entry.rid for entry in outcome.ledger if entry.action == "set_value")
        for entry in inserts:
            new = after.get(entry.rid)
            source = before.get(entry.source_rid or "")
            if new is None or source is None:
                problems.append(f"version {entry.rid} or its source is missing")
                continue
            if key_of(new, schema) != key_of(source, schema):
                problems.append(f"version {entry.rid} has another key")
            if new.values.get(event_time) != source.values.get(event_time):
                problems.append(f"version {entry.rid} changed its {event_time}")
            if parse_timestamp(str(source.values[event_time])) >= cutoff:
                problems.append(f"the source of {entry.rid} is too new")
            old_updated = parse_timestamp(str(source.values[updated_at]))
            new_updated = parse_timestamp(str(new.values[updated_at]))
            new_ingested = parse_timestamp(str(new.values[ingestion]))
            if not old_updated < new_updated <= new_ingested:
                problems.append(f"version {entry.rid} has a wrong {updated_at}")
            if batch_of(outcome.dataset, after_table, new) != in_batch:
                problems.append(f"version {entry.rid} is not in batch {in_batch}")
            if batch_of(ctx.dataset, table, source) >= in_batch:
                problems.append(f"the source of {entry.rid} is not in an earlier batch")
            differing = {
                name for name in new.values if new.values[name] != source.values.get(name)
            } - {updated_at, ingestion}
            if differing != set(names) or changes[entry.rid] != len(names):
                problems.append(f"version {entry.rid} differs in {sorted(differing)}")
        return problems


def _cutoff(as_of: str, params: Mapping[str, object]) -> int:
    return parse_timestamp(as_of) - parse_duration(cast(str, params["older_than"]))
