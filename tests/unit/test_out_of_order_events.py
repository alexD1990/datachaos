from __future__ import annotations

import copy
from collections import defaultdict
from dataclasses import replace

import pytest
from helpers import apply_all, by_rid, config, context, set_values, tamper

from datachaos import engine
from datachaos.config import ScenarioConfig
from datachaos.errors import DataChaosError
from datachaos.scenarios import SCENARIOS
from datachaos.scenarios.base import batch_of
from datachaos.timeutil import parse_timestamp

NAME = "out-of-order-events"
TABLE = "order_events"


def _ooo(**params: object) -> ScenarioConfig:
    return ScenarioConfig(NAME, TABLE, params)


def _time(row, column: str) -> int:  # type: ignore[no-untyped-def]
    return parse_timestamp(str(row.values[column]))


def _groups(dataset) -> dict[object, list]:  # type: ignore[no-untyped-def, type-arg]
    groups: dict[object, list] = defaultdict(list)  # type: ignore[type-arg]
    for row in dataset.tables[TABLE].rows:
        groups[row.values["order_id"]].append(row)
    for rows in groups.values():
        rows.sort(key=lambda row: _time(row, "event_time"))
    return groups


def test_the_next_to_last_event_lands_after_the_last() -> None:
    result = apply_all(_ooo(count=10))
    groups = _groups(result.dataset)
    touched = {entry.group["order_id"] for entry in result.ledger}  # type: ignore[index]
    assert len(touched) == 10
    for order in touched:
        earlier, last = groups[order][-2:]
        assert _time(earlier, "ingested_at") > _time(last, "ingested_at")


def test_only_ingested_at_of_the_next_to_last_event_changes() -> None:
    result = apply_all(_ooo(count=10))
    before = by_rid(result.baseline, TABLE)
    after = by_rid(result.dataset, TABLE)
    changed = {entry.rid for entry in result.ledger}
    assert len(changed) == 10
    for rid, row in after.items():
        if rid in changed:
            assert {**row.values, "ingested_at": None} == {
                **before[rid].values,
                "ingested_at": None,
            }
            assert row.values["ingested_at"] != before[rid].values["ingested_at"]
        else:
            assert row == before[rid]


def test_no_event_lands_before_its_own_event_time() -> None:
    result = apply_all(_ooo(rate=1.0))
    for row in result.dataset.tables[TABLE].rows:
        assert _time(row, "ingested_at") >= _time(row, "event_time")


def test_nothing_lands_on_or_after_as_of_and_the_hold_back_is_at_most_6h() -> None:
    result = apply_all(_ooo(rate=1.0))
    as_of = parse_timestamp(result.baseline.as_of)
    before = _groups(result.baseline)
    after = by_rid(result.dataset, TABLE)
    for entry in result.ledger:
        if entry.action != "set_value":
            continue
        row = after[entry.rid]
        assert _time(row, "ingested_at") < as_of
        last = before[row.values["order_id"]][-1]
        assert _time(row, "ingested_at") - _time(last, "ingested_at") <= 6 * 3600 + 1


def test_rate_and_count_select_groups() -> None:
    assert apply_all(_ooo(count=7)).records[0]["stats"]["rows_selected"] == 7  # type: ignore[index]
    groups = _groups(engine._build_dataset(config()))
    multi = sum(len(rows) >= 2 for rows in groups.values())
    selected = apply_all(_ooo(rate=1.0)).records[0]["stats"]["rows_selected"]  # type: ignore[index]
    assert 0 < selected <= multi


def test_a_group_with_one_event_is_no_candidate() -> None:
    baseline = engine._build_dataset(config())
    groups = _groups(baseline)
    table = baseline.tables[TABLE]
    single = next(order for order, rows in groups.items() if len(rows) >= 2)
    rows = [
        row
        for row in table.rows
        if row.values["order_id"] != single or row.rid == groups[single][0].rid
    ]
    dataset = replace(baseline, tables={**baseline.tables, TABLE: replace(table, rows=tuple(rows))})
    ctx = context(NAME, TABLE, {"group_by": "order_id", "rate": 1.0}, dataset)
    outcome = SCENARIOS[NAME].apply(ctx)
    assert single not in {entry.group["order_id"] for entry in outcome.ledger}  # type: ignore[index]


def test_a_group_with_no_room_before_as_of_is_no_candidate() -> None:
    baseline = engine._build_dataset(config())
    order, rows = next((order, rows) for order, rows in _groups(baseline).items() if len(rows) >= 2)
    dataset = set_values(baseline, TABLE, rows[-1].rid, ingested_at="2026-01-07T23:59:59Z")
    ctx = context(NAME, TABLE, {"group_by": "order_id", "rate": 1.0}, dataset)
    outcome = SCENARIOS[NAME].apply(ctx)
    assert order not in {entry.group["order_id"] for entry in outcome.ledger}  # type: ignore[index]


def test_a_group_already_out_of_order_is_no_candidate_even_with_overlap() -> None:
    first = _ooo(rate=1.0)
    done = apply_all(first)
    assert done.ledger
    with pytest.raises(DataChaosError, match="selects no groups|more than the"):
        apply_all(first, _ooo(rate=1.0, allow_overlap=True))
    with pytest.raises(DataChaosError, match="more than the 0 candidate groups"):
        apply_all(first, _ooo(count=1, allow_overlap=True))


def test_a_group_with_a_null_event_time_is_no_candidate() -> None:
    nulled = ScenarioConfig("null-critical-column", TABLE, {"column": "event_time", "rate": 1.0})
    with pytest.raises(DataChaosError, match="more than the 0 candidate groups"):
        apply_all(nulled, _ooo(count=1, allow_overlap=True))


def test_rows_with_a_null_group_belong_to_no_group() -> None:
    nulled = ScenarioConfig("null-critical-column", TABLE, {"column": "order_id", "rate": 1.0})
    with pytest.raises(DataChaosError, match="more than the 0 candidate groups"):
        apply_all(nulled, _ooo(count=1, allow_overlap=True))


def test_ledger_lines_carry_the_group_and_move_batch_when_the_batch_changes() -> None:
    result = apply_all(_ooo(rate=1.0))
    before_table = result.baseline.tables[TABLE]
    after_table = result.dataset.tables[TABLE]
    before = by_rid(result.baseline, TABLE)
    after = by_rid(result.dataset, TABLE)
    values = {e.rid: e for e in result.ledger if e.action == "set_value"}
    moves = {e.rid: e for e in result.ledger if e.action == "move_batch"}
    assert moves
    for rid, entry in values.items():
        assert entry.group == {"order_id": before[rid].values["order_id"]}
        assert entry.column == "ingested_at"
        assert entry.before == before[rid].values["ingested_at"]
        assert entry.after == after[rid].values["ingested_at"]
        new_batch = batch_of(result.dataset, after_table, after[rid])
        old_batch = batch_of(result.baseline, before_table, before[rid])
        assert entry.batch == new_batch
        if new_batch != old_batch:
            assert (moves[rid].before, moves[rid].after) == (old_batch, new_batch)
            assert moves[rid].group == entry.group
        else:
            assert rid not in moves


def test_later_row_scenario_stays_out_of_the_whole_group() -> None:
    later = ScenarioConfig("invalid-enum", TABLE, {"column": "event_type", "rate": 1.0})
    result = apply_all(_ooo(count=20), later)
    touched = {
        entry.group["order_id"]  # type: ignore[index]
        for entry in result.ledger
        if entry.scenario == NAME
    }
    before = by_rid(result.baseline, TABLE)
    for entry in result.ledger:
        if entry.scenario == "invalid-enum":
            assert before[entry.rid].values["order_id"] not in touched
    expected = sum(
        row.values["order_id"] not in touched for row in result.baseline.tables[TABLE].rows
    )
    assert sum(entry.scenario == "invalid-enum" for entry in result.ledger) == expected


def test_a_group_with_a_row_touched_earlier_is_no_candidate() -> None:
    earlier = ScenarioConfig("invalid-enum", TABLE, {"column": "event_type", "count": 30})
    result = apply_all(earlier, _ooo(rate=1.0))
    before = by_rid(result.baseline, TABLE)
    touched = {
        before[entry.rid].values["order_id"]
        for entry in result.ledger
        if entry.scenario == "invalid-enum"
    }
    groups = {
        entry.group["order_id"]  # type: ignore[index]
        for entry in result.ledger
        if entry.scenario == NAME
    }
    assert groups
    assert not touched & groups


def test_group_by_is_resolved_and_idempotent() -> None:
    run_config = config(_ooo(count=2))
    baseline = engine._build_dataset(run_config)
    once = engine.resolve_scenarios(run_config, baseline)[0].params
    assert once == {"count": 2, "allow_overlap": False, "group_by": "order_id"}
    again = engine.resolve_scenarios(config(_ooo(**once)), baseline)[0].params
    assert again == once
    assert list(again) == list(once)


def test_the_input_is_not_mutated() -> None:
    run_config = config(_ooo(count=5))
    baseline = engine._build_dataset(run_config)
    snapshot = copy.deepcopy(baseline)
    engine._apply_scenarios(run_config, baseline, engine.resolve_scenarios(run_config, baseline))
    assert baseline == snapshot


@pytest.mark.parametrize(
    ("scenario", "message"),
    [
        (ScenarioConfig(NAME, "orders", {"count": 1}), r"\.table: .* applies to event_log tables"),
        (_ooo(count=1, group_by="event_id"), r"\.params\.group_by: event_id is the key"),
        (_ooo(count=1, group_by="nope"), r"\.params\.group_by: table order_events has no column"),
        (_ooo(), r"\.params: give exactly one of rate and count"),
        (_ooo(count=1000), "more than the .* candidate groups"),
    ],
)
def test_invalid_use_gives_a_clear_error(scenario: ScenarioConfig, message: str) -> None:
    with pytest.raises(DataChaosError, match=message):
        apply_all(scenario)


def _applied() -> tuple:  # type: ignore[type-arg]
    baseline = engine._build_dataset(config())
    ctx = context(NAME, TABLE, {"group_by": "order_id", "count": 5}, baseline)
    scenario = SCENARIOS[NAME]
    outcome = scenario.apply(ctx)
    assert scenario.check(ctx, outcome) == []
    return scenario, ctx, outcome


def _set_line(outcome):  # type: ignore[no-untyped-def]
    return next(entry for entry in outcome.ledger if entry.action == "set_value")


def test_check_notices_a_group_still_in_order() -> None:
    scenario, ctx, outcome = _applied()
    rid = _set_line(outcome).rid
    original = by_rid(ctx.dataset, TABLE)[rid].values["ingested_at"]
    bad = tamper(outcome, TABLE, rid, ingested_at=original)
    assert any("still in order" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_change_to_event_time() -> None:
    scenario, ctx, outcome = _applied()
    bad = tamper(outcome, TABLE, _set_line(outcome).rid, event_time="2020-01-01T00:00:00Z")
    assert any("changed more than ingested_at" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_row_past_as_of() -> None:
    scenario, ctx, outcome = _applied()
    bad = tamper(outcome, TABLE, _set_line(outcome).rid, ingested_at="2026-01-08T00:00:00Z")
    assert any("on or after as_of" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_an_unselected_row_that_changed() -> None:
    scenario, ctx, outcome = _applied()
    changed = {entry.rid for entry in outcome.ledger}
    rid = next(row.rid for row in ctx.dataset.tables[TABLE].rows if row.rid not in changed)
    bad = tamper(outcome, TABLE, rid, event_type=None)
    assert any("without being selected" in problem for problem in scenario.check(ctx, bad))
