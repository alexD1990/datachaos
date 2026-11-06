"""late-arriving-data: rows that land long after the event they describe."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import cast

from datachaos.errors import FieldError
from datachaos.model.ledger import LedgerEntry
from datachaos.model.schema import ColumnType, TableSchema
from datachaos.model.table import Row
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
    conform_row,
    ingestion_column,
    key_of,
    select,
    with_rows,
)
from datachaos.timeutil import batch_index, format_timestamp, parse_duration, parse_timestamp


def _check_delay(value: object, schema: TableSchema) -> str | None:
    try:
        seconds = parse_duration(cast(str, value))
    except ValueError as exc:
        return str(exc)
    return None if seconds > 0 else f"must be above zero, got {value!r}"


class LateArrivingData(BaseScenario):
    spec = ScenarioSpec(
        name="late-arriving-data",
        version=1,
        summary="Delays ingested_at, so that rows land in a later batch than their event time.",
        detects="Incremental models with a watermark on event time, which never pick up late rows.",
        typical_dbt_test="singular test comparing the incremental model with a full refresh",
        applies_to=frozenset({"entity", "event_log"}),
        params=(
            *COMMON_PARAMS,
            ParamSpec(
                "delay_min",
                str,
                True,
                help="The shortest delay, such as 6h.",
                validate=_check_delay,
            ),
            ParamSpec(
                "delay_max",
                str,
                True,
                help=(
                    "The longest delay, such as 2d. Candidates are the rows that stay before "
                    "as_of with this delay, and rate is a share of those."
                ),
                validate=_check_delay,
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
        shortest = parse_duration(cast(str, resolved["delay_min"]))
        longest = parse_duration(cast(str, resolved["delay_max"]))
        if shortest > longest:
            raise FieldError(
                "params.delay_min",
                f"{resolved['delay_min']} is longer than delay_max {resolved['delay_max']}",
            )
        baseline = ctx.dataset.tables[table]
        column = ingestion_column(baseline.stable_schema)
        latest = parse_timestamp(ctx.dataset.as_of) - longest
        if not any(parse_timestamp(str(row.values[column])) < latest for row in baseline.rows):
            raise FieldError(
                "params.delay_max",
                f"{resolved['delay_max']} would push every row of {table} past as_of "
                f"{ctx.dataset.as_of}",
            )

    def apply(self, ctx: ScenarioContext) -> ScenarioOutcome:
        dataset = ctx.dataset
        table = dataset.tables[ctx.table]
        schema = table.stable_schema
        column = ingestion_column(schema)
        start, interval = batch_bounds(dataset)
        shortest = parse_duration(cast(str, ctx.params["delay_min"]))
        longest = parse_duration(cast(str, ctx.params["delay_max"]))
        latest = parse_timestamp(dataset.as_of) - longest

        def fits(row: Row) -> bool:
            return parse_timestamp(str(row.values[column])) < latest

        picked = select(ctx, fits)
        moved = {}
        ledger = []
        for row in picked:
            before = parse_timestamp(str(row.values[column]))
            after = before + ctx.rng.between(shortest, longest)
            old_batch = batch_index(before, start, interval)
            new_batch = batch_index(after, start, interval)
            moved[row.rid] = conform_row(
                table,
                replace(row, values={**row.values, column: format_timestamp(after)}),
                old_batch,
                new_batch,
            )
            common = {
                "scenario_index": ctx.index,
                "scenario": self.spec.name,
                "table": ctx.table,
                "rid": row.rid,
                "key": key_of(row, schema),
                "batch": new_batch,
            }
            ledger.append(
                LedgerEntry(
                    action="set_value",
                    column=column,
                    before=row.values[column],
                    after=moved[row.rid].values[column],
                    **common,  # type: ignore[arg-type]
                )
            )
            if new_batch != old_batch:
                ledger.append(
                    LedgerEntry(
                        action="move_batch",
                        before=old_batch,
                        after=new_batch,
                        **common,  # type: ignore[arg-type]
                    )
                )
        rows = [moved.get(row.rid, row) for row in table.rows]
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
        schema = table.stable_schema
        column = ingestion_column(schema)
        shortest = parse_duration(cast(str, ctx.params["delay_min"]))
        longest = parse_duration(cast(str, ctx.params["delay_max"]))
        as_of = parse_timestamp(ctx.dataset.as_of)
        after_table = outcome.dataset.tables[ctx.table]
        after = {row.rid: row for row in after_table.rows}
        changed = {entry.rid for entry in outcome.ledger if entry.action == "set_value"}
        moves = {entry.rid: entry for entry in outcome.ledger if entry.action == "move_batch"}
        problems = []
        if len(after) != len(table.rows):
            problems.append(f"expected {len(table.rows)} rows, got {len(after)}")
        times = [c.name for c in schema.columns if c.type is ColumnType.TIMESTAMP]
        for row in table.rows:
            current = after.get(row.rid)
            if current is None:
                problems.append(f"row {row.rid} is gone")
                continue
            if row.rid not in changed:
                if current != row:
                    problems.append(f"row {row.rid} changed without being selected")
                continue
            old_batch = batch_of(ctx.dataset, table, row)
            new_batch = batch_of(outcome.dataset, after_table, current)
            shaped = conform_row(table, row, old_batch, new_batch)
            for name in times:
                if name != column and current.values.get(name) != shaped.values.get(name):
                    problems.append(f"row {row.rid} changed its {name}")
            if {**current.values, column: None} != {**shaped.values, column: None}:
                problems.append(f"row {row.rid} changed more than {column}")
            old = parse_timestamp(str(row.values[column]))
            new = parse_timestamp(str(current.values[column]))
            if not shortest <= new - old <= longest:
                problems.append(f"row {row.rid} was delayed {new - old} seconds")
            if new >= as_of:
                problems.append(f"row {row.rid} landed on or after as_of")
            move = moves.get(row.rid)
            if (old_batch != new_batch) != (move is not None):
                problems.append(f"row {row.rid} is in batch {new_batch} against its ledger")
            elif move is not None and (move.before, move.after) != (old_batch, new_batch):
                problems.append(f"row {row.rid} has a wrong move_batch line")
        return problems
