from __future__ import annotations

import copy
from dataclasses import replace

import pytest
from helpers import apply_all, by_rid, config, context, tamper

from datachaos import engine
from datachaos.config import ScenarioConfig
from datachaos.errors import DataChaosError
from datachaos.scenarios import SCENARIOS
from datachaos.scenarios.base import batch_of

NAME = "null-critical-column"


def _null(table: str = "orders", **params: object) -> ScenarioConfig:
    return ScenarioConfig(NAME, table, params)


def _nulls(rows: tuple, column: str) -> int:  # type: ignore[type-arg]
    return sum(row.values[column] is None for row in rows)


def test_nulls_increase_by_n_and_nothing_else_changes() -> None:
    result = apply_all(_null(column="customer_id", count=10))
    before = by_rid(result.baseline, "orders")
    after = by_rid(result.dataset, "orders")
    assert _nulls(tuple(after.values()), "customer_id") == 10
    assert set(after) == set(before)
    touched = {entry.rid for entry in result.ledger}
    assert len(touched) == 10
    for rid, row in after.items():
        expected = (
            {**before[rid].values, "customer_id": None} if rid in touched else before[rid].values
        )
        assert row.values == expected


def test_ledger_has_before_and_after() -> None:
    result = apply_all(_null(column="customer_id", count=5))
    before = by_rid(result.baseline, "orders")
    table = result.baseline.tables["orders"]
    for entry in result.ledger:
        row = before[entry.rid]
        assert entry.action == "set_value"
        assert entry.column == "customer_id"
        assert entry.before == row.values["customer_id"]
        assert entry.after is None
        assert entry.key == {"order_id": row.values["order_id"]}
        assert entry.batch == batch_of(result.baseline, table, row)


def test_stats_and_resolved_params() -> None:
    record = apply_all(_null(column="amount", rate=0.1)).records[0]
    assert record["stats"] == {
        "rows_before": 100,
        "rows_after": 100,
        "rows_selected": 10,
        "rows_added": 0,
        "values_changed": 10,
    }
    assert record["params"] == {
        "rate": 0.1,
        "allow_overlap": False,
        "column": "amount",
        "allow_key": False,
    }


def test_the_manifest_records_a_broken_contract() -> None:
    result = apply_all(_null(column="customer_id", count=4))
    assert result.warnings == (
        "scenarios[0]: orders.customer_id is not nullable in the schema, but 4 rows now hold NULL",
    )


def test_a_key_needs_allow_key_and_keeps_its_original_value_in_the_ledger() -> None:
    with pytest.raises(DataChaosError, match=r"params\.column: order_id is a key column"):
        apply_all(_null(column="order_id", count=1))
    result = apply_all(_null(column="order_id", count=3, allow_key=True))
    before = by_rid(result.baseline, "orders")
    assert _nulls(result.dataset.tables["orders"].rows, "order_id") == 3
    for entry in result.ledger:
        assert entry.key == {"order_id": before[entry.rid].values["order_id"]}
        assert entry.before == before[entry.rid].values["order_id"]


def test_a_row_with_a_nulled_key_stays_excluded_from_later_scenarios() -> None:
    result = apply_all(
        _null(column="order_id", count=60, allow_key=True), _null(column="amount", count=40)
    )
    assert len({entry.rid for entry in result.ledger}) == 100
    with pytest.raises(DataChaosError, match="more than the 40 candidate rows"):
        apply_all(
            _null(column="order_id", count=60, allow_key=True), _null(column="amount", count=41)
        )


def test_ingestion_time_is_always_refused() -> None:
    for params in ({}, {"allow_key": True}, {"allow_overlap": True}):
        with pytest.raises(DataChaosError, match=r"params\.column: ingested_at decides the batch"):
            apply_all(_null(column="ingested_at", count=1, **params))


def test_rows_already_null_are_no_candidates_even_with_overlap() -> None:
    result = apply_all(
        _null(column="amount", count=60),
        _null(column="amount", count=40, allow_overlap=True),
    )
    assert _nulls(result.dataset.tables["orders"].rows, "amount") == 100
    with pytest.raises(DataChaosError, match="more than the 40 candidate rows"):
        apply_all(
            _null(column="amount", count=60),
            _null(column="amount", count=41, allow_overlap=True),
        )


def test_works_on_event_logs() -> None:
    result = apply_all(_null("order_events", column="event_type", count=2))
    assert _nulls(result.dataset.tables["order_events"].rows, "event_type") == 2


def test_null_is_written_as_an_empty_field(tmp_path) -> None:  # type: ignore[no-untyped-def]
    out = tmp_path / "out"
    engine.run(config(_null(column="amount", rate=1.0)), out)
    header, *rows = (out / "data" / "orders" / "batch_0002.csv").read_bytes().splitlines()
    position = header.split(b",").index(b"amount")
    assert rows
    assert all(row.split(b",")[position] == b"" for row in rows)


def test_the_input_is_not_mutated() -> None:
    run_config = config(_null(column="amount", count=5))
    baseline = engine._build_dataset(run_config)
    snapshot = copy.deepcopy(baseline)
    engine._apply_scenarios(run_config, baseline, engine.resolve_scenarios(run_config, baseline))
    assert baseline == snapshot


@pytest.mark.parametrize(
    ("scenario", "message"),
    [
        (_null(count=1), r"\.params\.column: is required"),
        (_null(column=1, count=1), r"\.params\.column: must be str"),
        (_null(column="nope", count=1), r"\.params\.column: table orders has no column 'nope'"),
        (_null(column="amount"), r"\.params: give exactly one of rate and count"),
        (_null(column="amount", count=1, allow_key="yes"), r"\.params\.allow_key: must be bool"),
        (_null(column="amount", cout=1), r"\.params\.cout: unknown parameter \(did you mean count"),
        (_null(column="amount", rate=0.001), "selects no rows; use count"),
    ],
)
def test_invalid_use_gives_a_clear_error(scenario: ScenarioConfig, message: str) -> None:
    with pytest.raises(DataChaosError, match=message):
        apply_all(scenario)


def _applied() -> tuple:  # type: ignore[type-arg]
    baseline = engine._build_dataset(config())
    ctx = context(NAME, "orders", {"column": "amount", "count": 3}, baseline)
    scenario = SCENARIOS[NAME]
    outcome = scenario.apply(ctx)
    assert scenario.check(ctx, outcome) == []
    return scenario, ctx, outcome


def test_check_notices_an_unselected_row_that_changed() -> None:
    scenario, ctx, outcome = _applied()
    touched = {entry.rid for entry in outcome.ledger}
    rid = next(row.rid for row in ctx.dataset.tables["orders"].rows if row.rid not in touched)
    bad = tamper(outcome, "orders", rid, status="paid")
    assert any("without being selected" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_another_change_on_a_selected_row() -> None:
    scenario, ctx, outcome = _applied()
    bad = tamper(outcome, "orders", outcome.ledger[0].rid, status="paid")
    assert any("to NULL only" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_wrong_number_of_nulls() -> None:
    scenario, ctx, outcome = _applied()
    bad = tamper(outcome, "orders", outcome.ledger[0].rid, amount="1.00")
    assert any("expected 3 NULLs" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_lost_row() -> None:
    scenario, ctx, outcome = _applied()
    table = outcome.dataset.tables["orders"]
    bad = replace(
        outcome,
        dataset=replace(
            outcome.dataset,
            tables={**outcome.dataset.tables, "orders": replace(table, rows=table.rows[1:])},
        ),
    )
    assert any("is gone" in problem for problem in scenario.check(ctx, bad))
