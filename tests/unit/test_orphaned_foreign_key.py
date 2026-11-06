from __future__ import annotations

import copy
import json
import re
from dataclasses import replace
from pathlib import Path

import pytest
from helpers import SMALL, apply_all, by_rid, config, context, tamper

from datachaos import engine
from datachaos.config import DatasetConfig, ScenarioConfig
from datachaos.errors import DataChaosError
from datachaos.scenarios import SCENARIOS

NAME = "orphaned-foreign-key"


def _orphan(table: str = "orders", **params: object) -> ScenarioConfig:
    return ScenarioConfig(NAME, table, params)


def _ids(dataset, table: str, column: str) -> set[object]:  # type: ignore[no-untyped-def]
    return {row.values[column] for row in dataset.tables[table].rows}


def test_nonexistent_points_at_an_id_that_does_not_exist() -> None:
    result = apply_all(_orphan(count=10))
    customers = _ids(result.baseline, "customers", "customer_id")
    assert len(result.ledger) == 10
    rows = by_rid(result.dataset, "orders")
    for entry in result.ledger:
        assert entry.action == "set_value"
        assert entry.column == "customer_id"
        assert re.fullmatch(r"C9[0-9]{5}", str(entry.after))
        assert entry.after not in customers
        assert rows[entry.rid].values["customer_id"] == entry.after


def test_nonexistent_uses_the_prefix_of_the_parent() -> None:
    result = apply_all(_orphan("order_events", count=5))
    assert result.ledger[0].column == "order_id"
    assert all(re.fullmatch(r"O9[0-9]{5}", str(entry.after)) for entry in result.ledger)


def test_nonexistent_leaves_everything_else_alone() -> None:
    result = apply_all(_orphan(count=10))
    assert result.dataset.tables["customers"] == result.baseline.tables["customers"]
    assert result.dataset.tables["order_events"] == result.baseline.tables["order_events"]
    before = by_rid(result.baseline, "orders")
    after = by_rid(result.dataset, "orders")
    touched = {entry.rid: entry for entry in result.ledger}
    for rid, row in after.items():
        expected = (
            {**before[rid].values, "customer_id": touched[rid].after}
            if rid in touched
            else before[rid].values
        )
        assert row.values == expected


def test_rows_with_null_or_orphaned_values_are_no_candidates_even_with_overlap() -> None:
    nulled = ScenarioConfig(
        "null-critical-column", "orders", {"column": "customer_id", "count": 60}
    )
    result = apply_all(nulled, _orphan(count=40, allow_overlap=True))
    assert len({entry.rid for entry in result.ledger}) == 100
    with pytest.raises(DataChaosError, match="more than the 40 candidate rows"):
        apply_all(nulled, _orphan(count=41, allow_overlap=True))
    first = _orphan(count=60)
    with pytest.raises(DataChaosError, match="more than the 40 candidate rows"):
        apply_all(first, _orphan(count=41, allow_overlap=True))


def test_deleted_parent_removes_the_parent_and_orphans_all_its_children() -> None:
    result = apply_all(_orphan(mode="deleted_parent", count=3))
    deleted = [entry for entry in result.ledger if entry.action == "delete_row"]
    assert len(deleted) == 3
    assert {entry.table for entry in deleted} == {"customers"}
    gone = {entry.key["customer_id"] for entry in deleted}
    assert not gone & _ids(result.dataset, "customers", "customer_id")
    assert len(result.dataset.tables["customers"].rows) == 17
    children = {
        row.rid
        for row in result.baseline.tables["orders"].rows
        if row.values["customer_id"] in gone
    }
    assert children
    lines = [entry for entry in result.ledger if entry.action == "orphan_row"]
    assert {entry.rid for entry in lines} == children
    assert {entry.table for entry in lines} == {"orders"}


def test_orphan_lines_are_correct_and_the_children_are_unchanged() -> None:
    result = apply_all(_orphan(mode="deleted_parent", count=2))
    before = by_rid(result.baseline, "orders")
    assert result.dataset.tables["orders"] == result.baseline.tables["orders"]
    for entry in result.ledger:
        if entry.action == "orphan_row":
            assert entry.column == "customer_id"
            assert entry.before == entry.after == before[entry.rid].values["customer_id"]
            assert entry.key == {"order_id": before[entry.rid].values["order_id"]}


def test_deleted_parent_only_selects_parents_with_children() -> None:
    settings = DatasetConfig(as_of=SMALL.as_of, customers=100, orders=20, batches=3)
    result = apply_all(_orphan(mode="deleted_parent", rate=1.0), settings=settings)
    with_children = _ids(result.baseline, "orders", "customer_id")
    everyone = _ids(result.baseline, "customers", "customer_id")
    assert with_children < everyone
    assert _ids(result.dataset, "customers", "customer_id") == everyone - with_children
    assert {
        entry.key["customer_id"] for entry in result.ledger if entry.action == "delete_row"
    } == with_children


def test_deleted_parent_works_on_the_event_log_without_touching_customers() -> None:
    result = apply_all(_orphan("order_events", mode="deleted_parent", count=4))
    assert len(result.dataset.tables["orders"].rows) == 96
    assert result.dataset.tables["customers"] == result.baseline.tables["customers"]
    assert result.dataset.tables["order_events"] == result.baseline.tables["order_events"]
    record = result.records[0]
    assert record["stats"]["rows_removed"] == 4  # type: ignore[index]
    assert record["stats"]["rows_selected"] == 4  # type: ignore[index]


def test_deleted_parent_removes_every_row_of_a_duplicated_parent() -> None:
    duplicated = ScenarioConfig("duplicate-business-key", "customers", {"rate": 1.0})
    result = apply_all(duplicated, _orphan(mode="deleted_parent", rate=1.0, allow_overlap=True))
    deleted = [entry for entry in result.ledger if entry.action == "delete_row"]
    assert result.records[1]["stats"]["rows_removed"] == len(deleted)  # type: ignore[index]
    assert len(deleted) == 2 * result.records[1]["stats"]["rows_selected"]  # type: ignore[index]


def test_later_scenario_on_the_child_table_avoids_the_orphans() -> None:
    orphaning = _orphan(mode="deleted_parent", count=3)
    first = apply_all(orphaning)
    orphans = {entry.rid for entry in first.ledger if entry.action == "orphan_row"}
    free = 100 - len(orphans)
    later = ScenarioConfig("null-critical-column", "orders", {"column": "amount", "rate": 1.0})
    result = apply_all(orphaning, later)
    touched = [entry for entry in result.ledger if entry.scenario == "null-critical-column"]
    assert len(touched) == free
    assert not {entry.rid for entry in touched} & orphans


def test_later_scenario_on_the_parent_table_avoids_nothing_it_can_still_see() -> None:
    orphaning = _orphan(mode="deleted_parent", count=3)
    later = ScenarioConfig("duplicate-business-key", "customers", {"rate": 1.0})
    result = apply_all(orphaning, later)
    gone = {entry.key["customer_id"] for entry in result.ledger if entry.action == "delete_row"}
    copied = {entry.key["customer_id"] for entry in result.ledger if entry.action == "insert_row"}
    assert len(copied) == 17
    assert not gone & copied


def test_a_parent_touched_earlier_is_no_candidate() -> None:
    earlier = ScenarioConfig("invalid-enum", "customers", {"column": "country_code", "rate": 1.0})
    with pytest.raises(DataChaosError, match="more than the 0 candidate groups"):
        apply_all(earlier, _orphan(mode="deleted_parent", count=1))


def test_a_parent_with_a_touched_child_is_no_candidate() -> None:
    earlier = ScenarioConfig("invalid-enum", "orders", {"column": "status", "count": 1})
    result = apply_all(earlier, _orphan(mode="deleted_parent", rate=1.0))
    touched = next(entry for entry in result.ledger if entry.scenario == "invalid-enum")
    parent = by_rid(result.baseline, "orders")[touched.rid].values["customer_id"]
    assert parent in _ids(result.dataset, "customers", "customer_id")


def test_ledger_lines_sort_per_table_and_rid(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.run(config(_orphan(mode="deleted_parent", count=3)), out)
    entries = [json.loads(line) for line in (out / "chaos-ledger.jsonl").read_bytes().splitlines()]
    keys = [(entry["table"], entry["rid"]) for entry in entries]
    assert keys == sorted(keys)
    assert {entry["table"] for entry in entries} == {"customers", "orders"}


def test_manifest_counts_the_rows_removed_from_the_parent(tmp_path: Path) -> None:
    manifest = engine.run(config(_orphan(mode="deleted_parent", count=3)), tmp_path / "out")
    assert manifest.dataset.tables["customers"].rows_baseline == 20
    assert manifest.dataset.tables["customers"].rows_final == 17
    assert manifest.dataset.tables["orders"].rows_final == 100


def test_stats_without_removed_rows_leave_the_field_out() -> None:
    assert "rows_removed" not in apply_all(_orphan(count=1)).records[0]["stats"]  # type: ignore[operator]


def test_resolve_fills_in_and_is_idempotent() -> None:
    run_config = config(_orphan(count=2))
    baseline = engine._build_dataset(run_config)
    once = engine.resolve_scenarios(run_config, baseline)[0].params
    assert once == {
        "count": 2,
        "allow_overlap": False,
        "column": "customer_id",
        "mode": "nonexistent",
    }
    again = engine.resolve_scenarios(config(_orphan(**once)), baseline)[0].params
    assert again == once
    assert list(again) == list(once)


@pytest.mark.parametrize("mode", ["nonexistent", "deleted_parent"])
def test_the_input_is_not_mutated(mode: str) -> None:
    run_config = config(_orphan(count=3, mode=mode))
    baseline = engine._build_dataset(run_config)
    snapshot = copy.deepcopy(baseline)
    engine._apply_scenarios(run_config, baseline, engine.resolve_scenarios(run_config, baseline))
    assert baseline == snapshot


@pytest.mark.parametrize(
    ("scenario", "message"),
    [
        (_orphan("customers", count=1), r"\.table: customers has no foreign key column"),
        (_orphan(column="amount", count=1), r"\.params\.column: amount is not a foreign key"),
        (_orphan(column="nope", count=1), r"\.params\.column: table orders has no column 'nope'"),
        (_orphan(mode="gone", count=1), r"\.params\.mode: must be one of"),
        (_orphan(), r"\.params: give exactly one of rate and count"),
        (_orphan(mode="deleted_parent", count=1000), "more than the .* candidate groups"),
        (_orphan(count=1000), "more than the 100 candidate rows"),
    ],
)
def test_invalid_use_gives_a_clear_error(scenario: ScenarioConfig, message: str) -> None:
    with pytest.raises(DataChaosError, match=message):
        apply_all(scenario)


def _applied(mode: str) -> tuple:  # type: ignore[type-arg]
    baseline = engine._build_dataset(config())
    params = {"column": "customer_id", "mode": mode, "count": 3}
    ctx = context(NAME, "orders", params, baseline)
    scenario = SCENARIOS[NAME]
    outcome = scenario.apply(ctx)
    assert scenario.check(ctx, outcome) == []
    return scenario, ctx, outcome


def test_check_notices_a_row_that_still_has_a_parent() -> None:
    scenario, ctx, outcome = _applied("nonexistent")
    bad = tamper(outcome, "orders", outcome.ledger[0].rid, customer_id="C000001")
    assert any("still has a parent" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_changed_parent_table_in_nonexistent() -> None:
    scenario, ctx, outcome = _applied("nonexistent")
    rid = ctx.dataset.tables["customers"].rows[0].rid
    bad = tamper(outcome, "customers", rid, is_active=None)
    assert any("parent table customers changed" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_another_table_that_changed() -> None:
    scenario, ctx, outcome = _applied("nonexistent")
    rid = ctx.dataset.tables["order_events"].rows[0].rid
    bad = tamper(outcome, "order_events", rid, event_type=None)
    assert any("table order_events changed" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_an_unselected_row_that_changed() -> None:
    scenario, ctx, outcome = _applied("nonexistent")
    touched = {entry.rid for entry in outcome.ledger}
    rid = next(row.rid for row in ctx.dataset.tables["orders"].rows if row.rid not in touched)
    bad = tamper(outcome, "orders", rid, status="paid")
    assert any("without being selected" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_an_orphan_without_a_ledger_line() -> None:
    scenario, ctx, outcome = _applied("deleted_parent")
    orphan = next(entry for entry in outcome.ledger if entry.action == "orphan_row")
    bad = replace(
        outcome, ledger=tuple(entry for entry in outcome.ledger if entry.rid != orphan.rid)
    )
    assert any("has no ledger line" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_ledger_that_does_not_match_the_removed_rows() -> None:
    scenario, ctx, outcome = _applied("deleted_parent")
    deleted = next(entry for entry in outcome.ledger if entry.action == "delete_row")
    bad = replace(outcome, ledger=tuple(entry for entry in outcome.ledger if entry is not deleted))
    assert any("does not match the rows removed" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_remaining_parent_that_changed() -> None:
    scenario, ctx, outcome = _applied("deleted_parent")
    rid = outcome.dataset.tables["customers"].rows[0].rid
    bad = tamper(outcome, "customers", rid, is_active=None)
    assert any("remaining row" in problem for problem in scenario.check(ctx, bad))
