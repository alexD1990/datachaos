from __future__ import annotations

import copy
from dataclasses import replace
from pathlib import Path

import pytest
from helpers import SMALL, apply_all, by_rid, config, context, tamper

from datachaos import engine
from datachaos.config import DatasetConfig, ScenarioConfig
from datachaos.errors import DataChaosError
from datachaos.io.reader import read_csv, read_schema
from datachaos.scenarios import SCENARIOS
from datachaos.scenarios.base import batch_of
from datachaos.timeutil import parse_duration, parse_timestamp

NAME = "late-arriving-data"
DELAYS = {"delay_min": "6h", "delay_max": "1d"}


def _late(table: str = "orders", **params: object) -> ScenarioConfig:
    return ScenarioConfig(NAME, table, {**DELAYS, **params})


def _seconds(row, column: str = "ingested_at") -> int:  # type: ignore[no-untyped-def]
    return parse_timestamp(str(row.values[column]))


def test_delays_are_within_the_range_and_only_ingested_at_changes() -> None:
    result = apply_all(_late(rate=1.0))
    before = by_rid(result.baseline, "orders")
    after = by_rid(result.dataset, "orders")
    touched = {entry.rid for entry in result.ledger}
    assert touched
    for rid, row in after.items():
        if rid not in touched:
            assert row == before[rid]
            continue
        delay = _seconds(row) - _seconds(before[rid])
        assert parse_duration("6h") <= delay <= parse_duration("1d")
        assert {**row.values, "ingested_at": None} == {**before[rid].values, "ingested_at": None}


def test_event_time_is_not_touched_on_the_event_log() -> None:
    result = apply_all(_late("order_events", rate=1.0))
    before = by_rid(result.baseline, "order_events")
    for rid, row in by_rid(result.dataset, "order_events").items():
        assert row.values["event_time"] == before[rid].values["event_time"]


def test_nothing_lands_on_or_after_as_of() -> None:
    result = apply_all(_late(rate=1.0))
    as_of = parse_timestamp(SMALL.as_of)
    assert all(_seconds(row) < as_of for row in result.dataset.tables["orders"].rows)


def test_candidates_are_the_rows_that_stay_before_as_of() -> None:
    baseline = engine._build_dataset(config())
    latest = parse_timestamp(SMALL.as_of) - parse_duration("1d")
    fitting = {row.rid for row in baseline.tables["orders"].rows if _seconds(row) < latest}
    result = apply_all(_late(rate=1.0))
    assert {entry.rid for entry in result.ledger} == fitting
    assert 0 < len(fitting) < 100
    with pytest.raises(DataChaosError, match=f"more than the {len(fitting)} candidate rows"):
        apply_all(_late(count=len(fitting) + 1))


def test_rate_is_a_share_of_the_candidates() -> None:
    all_of_them = apply_all(_late(rate=1.0)).records[0]["stats"]["rows_selected"]  # type: ignore[index]
    half = apply_all(_late(rate=0.5)).records[0]["stats"]["rows_selected"]  # type: ignore[index]
    assert half == int(all_of_them * 0.5 + 0.5)


def test_rows_land_in_the_right_later_batch_in_the_files(tmp_path: Path) -> None:
    out = tmp_path / "out"
    run_config = config(
        ScenarioConfig(NAME, "orders", {"delay_min": "1d", "delay_max": "1d", "rate": 1.0})
    )
    engine.run(run_config, out)
    baseline = engine._build_dataset(run_config)
    table = baseline.tables["orders"]
    expected_batch = {
        row.values["order_id"]: batch_of(baseline, table, row) + 1
        for row in table.rows
        if _seconds(row) < parse_timestamp(SMALL.as_of) - 86400
    }
    assert expected_batch
    seen = 0
    for k in range(SMALL.batches):
        schema = read_schema(out / "schemas" / "orders" / f"batch_{k:04d}.json")
        for row in read_csv(out / "data" / "orders" / f"batch_{k:04d}.csv", schema):
            if row["order_id"] in expected_batch:
                assert expected_batch[row["order_id"]] == k
                seen += 1
    assert seen == len(expected_batch)


def test_ledger_has_a_value_line_and_a_move_line_only_when_the_batch_changes() -> None:
    result = apply_all(_late(rate=1.0, delay_min="1h", delay_max="1d"))
    before_table = result.baseline.tables["orders"]
    after_table = result.dataset.tables["orders"]
    before = by_rid(result.baseline, "orders")
    after = by_rid(result.dataset, "orders")
    values = {e.rid: e for e in result.ledger if e.action == "set_value"}
    moves = {e.rid: e for e in result.ledger if e.action == "move_batch"}
    assert moves
    assert set(moves) < set(values)
    for rid, entry in values.items():
        new_batch = batch_of(result.dataset, after_table, after[rid])
        old_batch = batch_of(result.baseline, before_table, before[rid])
        assert entry.column == "ingested_at"
        assert entry.before == before[rid].values["ingested_at"]
        assert entry.after == after[rid].values["ingested_at"]
        assert entry.batch == new_batch
        if new_batch != old_batch:
            assert moves[rid].column is None
            assert (moves[rid].before, moves[rid].after) == (old_batch, new_batch)
            assert moves[rid].batch == new_batch
        else:
            assert rid not in moves


def test_a_delay_that_keeps_the_batch_gives_no_move_line() -> None:
    result = apply_all(_late(rate=1.0, delay_min="1s", delay_max="1s"))
    assert result.ledger
    moved = [entry for entry in result.ledger if entry.action == "move_batch"]
    assert len(moved) < len(result.ledger) / 2


def test_too_large_delay_max_is_an_error_before_anything_is_written(tmp_path: Path) -> None:
    run_config = config(_late(rate=0.5, delay_max="3d"))
    with pytest.raises(
        DataChaosError,
        match=r"scenarios\[0\]\.params\.delay_max: 3d would push every row of orders past as_of",
    ):
        engine.run(run_config, tmp_path / "out")
    assert list(tmp_path.iterdir()) == []


def test_the_validation_also_runs_before_earlier_scenarios(tmp_path: Path) -> None:
    earlier = ScenarioConfig("null-critical-column", "orders", {"column": "amount", "count": 1})
    with pytest.raises(DataChaosError, match=r"scenarios\[1\]\.params\.delay_max"):
        engine.run(config(earlier, _late(rate=0.5, delay_max="30d")), tmp_path / "out")
    assert list(tmp_path.iterdir()) == []


def test_with_one_batch_rows_can_stay_in_it() -> None:
    settings = DatasetConfig(
        as_of=SMALL.as_of, customers=5, orders=50, batches=1, batch_interval="7d"
    )
    result = apply_all(_late(rate=0.5), settings=settings)
    assert result.ledger
    assert all(entry.action == "set_value" for entry in result.ledger)


def test_exclusivity_against_an_earlier_scenario() -> None:
    earlier = ScenarioConfig("null-critical-column", "orders", {"column": "amount", "rate": 1.0})
    with pytest.raises(DataChaosError, match="more than the 0 candidate rows"):
        apply_all(earlier, _late(count=1))
    assert apply_all(earlier, _late(count=1, allow_overlap=True)).ledger


def test_stats_and_resolved_params() -> None:
    record = apply_all(_late(count=4)).records[0]
    assert record["stats"] == {
        "rows_before": 100,
        "rows_after": 100,
        "rows_selected": 4,
        "rows_added": 0,
        "values_changed": 4,
    }
    assert record["params"] == {
        "count": 4,
        "allow_overlap": False,
        "delay_min": "6h",
        "delay_max": "1d",
    }


def test_the_input_is_not_mutated() -> None:
    run_config = config(_late(count=5))
    baseline = engine._build_dataset(run_config)
    snapshot = copy.deepcopy(baseline)
    engine._apply_scenarios(run_config, baseline, engine.resolve_scenarios(run_config, baseline))
    assert baseline == snapshot


@pytest.mark.parametrize(
    ("scenario", "message"),
    [
        (
            ScenarioConfig(NAME, "orders", {"count": 1, "delay_max": "1d"}),
            r"params\.delay_min: is required",
        ),
        (
            ScenarioConfig(NAME, "orders", {"count": 1, "delay_min": "1d"}),
            r"params\.delay_max: is required",
        ),
        (_late(count=1, delay_min=6), r"params\.delay_min: must be str, got 6"),
        (_late(count=1, delay_max="2days"), r"params\.delay_max: '2days' is not a duration"),
        (_late(count=1, delay_min="0s"), r"params\.delay_min: must be above zero"),
        (_late(count=1, delay_min="2d", delay_max="1d"), r"params\.delay_min: 2d is longer than"),
        (_late(), r"params: give exactly one of rate and count"),
    ],
)
def test_invalid_use_gives_a_clear_error(scenario: ScenarioConfig, message: str) -> None:
    with pytest.raises(DataChaosError, match=message):
        apply_all(scenario)


def _applied() -> tuple:  # type: ignore[type-arg]
    baseline = engine._build_dataset(config())
    ctx = context(NAME, "orders", {**DELAYS, "count": 5}, baseline)
    scenario = SCENARIOS[NAME]
    outcome = scenario.apply(ctx)
    assert scenario.check(ctx, outcome) == []
    return scenario, ctx, outcome


def _value_line(outcome):  # type: ignore[no-untyped-def]
    return next(entry for entry in outcome.ledger if entry.action == "set_value")


def test_check_notices_a_changed_event_time() -> None:
    scenario, ctx, outcome = _applied()
    bad = tamper(
        outcome, "orders", _value_line(outcome).rid, order_timestamp="2020-01-01T00:00:00Z"
    )
    assert any("changed its order_timestamp" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_delay_outside_the_range() -> None:
    scenario, ctx, outcome = _applied()
    rid = _value_line(outcome).rid
    current = by_rid(ctx.dataset, "orders")[rid].values["ingested_at"]
    bad = tamper(outcome, "orders", rid, ingested_at=current)
    assert any("was delayed 0 seconds" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_row_past_as_of() -> None:
    scenario, ctx, outcome = _applied()
    bad = tamper(outcome, "orders", _value_line(outcome).rid, ingested_at="2026-01-08T00:00:00Z")
    assert any("on or after as_of" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_batch_change_without_a_move_line() -> None:
    scenario, ctx, outcome = _applied()
    bad = replace(outcome, ledger=tuple(e for e in outcome.ledger if e.action != "move_batch"))
    assert any("against its ledger" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_an_unselected_row_that_changed() -> None:
    scenario, ctx, outcome = _applied()
    touched = {entry.rid for entry in outcome.ledger}
    rid = next(row.rid for row in ctx.dataset.tables["orders"].rows if row.rid not in touched)
    bad = tamper(outcome, "orders", rid, status="paid")
    assert any("without being selected" in problem for problem in scenario.check(ctx, bad))
