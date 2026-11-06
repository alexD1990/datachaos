from __future__ import annotations

import copy
from dataclasses import replace

import pytest
from helpers import apply_all, by_rid, config, context, tamper

from datachaos import engine
from datachaos.config import ScenarioConfig
from datachaos.errors import DataChaosError
from datachaos.io.reader import read_csv, read_schema
from datachaos.scenarios import SCENARIOS

NAME = "invalid-enum"


def _enum(table: str = "orders", **params: object) -> ScenarioConfig:
    return ScenarioConfig(NAME, table, params)


def _allowed(result, table: str, column: str) -> tuple[str, ...]:  # type: ignore[no-untyped-def]
    schema = result.dataset.tables[table].schema_by_batch[0]
    return next(item.allowed_values for item in schema.columns if item.name == column)


@pytest.mark.parametrize(
    ("table", "column"),
    [
        ("orders", "status"),
        ("orders", "currency"),
        ("orders", "source_system"),
        ("customers", "country_code"),
        ("customers", "customer_segment"),
        ("order_events", "event_type"),
    ],
)
@pytest.mark.parametrize("variant", ["literal", "case_change", "whitespace"])
def test_every_variant_gives_an_invalid_value_on_every_enum(
    table: str, column: str, variant: str
) -> None:
    result = apply_all(_enum(table, column=column, rate=1.0, variant=variant))
    allowed = _allowed(result, table, column)
    assert result.ledger
    for row in result.dataset.tables[table].rows:
        assert row.values[column] not in allowed


def test_only_the_column_changes_and_only_on_selected_rows() -> None:
    result = apply_all(_enum(column="status", count=10))
    before = by_rid(result.baseline, "orders")
    after = by_rid(result.dataset, "orders")
    touched = {entry.rid: entry for entry in result.ledger}
    assert len(touched) == 10
    for rid, row in after.items():
        expected = (
            {**before[rid].values, "status": touched[rid].after}
            if rid in touched
            else before[rid].values
        )
        assert row.values == expected


def test_literal_uses_unknown_by_default_and_listed_values_otherwise() -> None:
    assert {entry.after for entry in apply_all(_enum(column="status", count=5)).ledger} == {
        "UNKNOWN"
    }
    result = apply_all(_enum(column="status", rate=1.0, values=["a", "b"]))
    assert {entry.after for entry in result.ledger} == {"a", "b"}


def test_case_change_turns_lower_to_upper_and_upper_to_lower() -> None:
    lower = apply_all(_enum(column="source_system", rate=1.0, variant="case_change"))
    assert all(entry.after == str(entry.before).upper() for entry in lower.ledger)
    upper = apply_all(_enum("customers", column="country_code", rate=1.0, variant="case_change"))
    assert all(entry.after == str(entry.before).lower() for entry in upper.ledger)


def test_whitespace_adds_a_trailing_space_that_survives_csv(tmp_path) -> None:  # type: ignore[no-untyped-def]
    out = tmp_path / "out"
    engine.run(config(_enum(column="source_system", rate=1.0, variant="whitespace")), out)
    schema = read_schema(out / "schemas" / "orders" / "batch_0002.json")
    rows = read_csv(out / "data" / "orders" / "batch_0002.csv", schema)
    assert rows
    assert all(str(row["source_system"]).endswith(" ") for row in rows)


def test_ledger_lines_are_correct() -> None:
    result = apply_all(_enum(column="status", count=3))
    before = by_rid(result.baseline, "orders")
    for entry in result.ledger:
        assert entry.action == "set_value"
        assert entry.column == "status"
        assert entry.before == before[entry.rid].values["status"]
        assert entry.after == "UNKNOWN"
        assert entry.key == {"order_id": before[entry.rid].values["order_id"]}


def test_resolved_params_are_recorded_and_idempotent() -> None:
    run_config = config(_enum(column="status", count=2))
    baseline = engine._build_dataset(run_config)
    once = engine.resolve_scenarios(run_config, baseline)[0].params
    assert once == {
        "count": 2,
        "allow_overlap": False,
        "column": "status",
        "values": ["UNKNOWN"],
        "variant": "literal",
    }
    again = engine.resolve_scenarios(config(_enum(**once)), baseline)[0].params
    assert again == once
    assert list(again) == list(once)


def test_other_variants_resolve_without_values_and_are_idempotent() -> None:
    run_config = config(_enum(column="status", count=2, variant="whitespace"))
    baseline = engine._build_dataset(run_config)
    once = engine.resolve_scenarios(run_config, baseline)[0].params
    assert "values" not in once
    assert engine.resolve_scenarios(config(_enum(**once)), baseline)[0].params == once


def test_already_invalid_rows_are_no_candidates_even_with_overlap() -> None:
    first = _enum(column="status", count=60)
    result = apply_all(first, _enum(column="status", count=40, allow_overlap=True))
    assert len({entry.rid for entry in result.ledger}) == 100
    with pytest.raises(DataChaosError, match="more than the 40 candidate rows"):
        apply_all(first, _enum(column="status", count=41, allow_overlap=True))


def test_null_rows_are_no_candidates() -> None:
    nulled = ScenarioConfig("null-critical-column", "orders", {"column": "status", "count": 60})
    with pytest.raises(DataChaosError, match="more than the 40 candidate rows"):
        apply_all(nulled, _enum(column="status", count=41, allow_overlap=True))


def test_exclusivity_against_an_earlier_scenario() -> None:
    nulled = ScenarioConfig("null-critical-column", "orders", {"column": "amount", "count": 90})
    result = apply_all(nulled, _enum(column="status", count=10))
    assert len({entry.rid for entry in result.ledger}) == 100
    with pytest.raises(DataChaosError, match="more than the 10 candidate rows"):
        apply_all(nulled, _enum(column="status", count=11))


def test_the_input_is_not_mutated() -> None:
    run_config = config(_enum(column="status", count=5))
    baseline = engine._build_dataset(run_config)
    snapshot = copy.deepcopy(baseline)
    engine._apply_scenarios(run_config, baseline, engine.resolve_scenarios(run_config, baseline))
    assert baseline == snapshot


@pytest.mark.parametrize(
    ("scenario", "message"),
    [
        (_enum(count=1), r"\.params\.column: is required"),
        (_enum(column="amount", count=1), r"\.params\.column: amount does not have role ENUM"),
        (_enum(column="nope", count=1), r"\.params\.column: table orders has no column 'nope'"),
        (_enum(column="status", count=1, variant="x"), r"\.params\.variant: must be one of"),
        (_enum(column="status", count=1, values="x"), r"\.params\.values: must be list"),
        (_enum(column="status", count=1, values=[]), r"\.params\.values: must not be empty"),
        (_enum(column="status", count=1, values=[""]), r"\.params\.values: must not hold an empty"),
        (_enum(column="status", count=1, values=[1]), r"\.params\.values: must be strings, got 1"),
        (
            _enum(column="status", count=1, values=[False]),
            r"\.params\.values: must be strings, got False \(YAML reads NO",
        ),
        (
            _enum(column="status", count=1, values=["paid"]),
            r"\.params\.values: 'paid' is an allowed value",
        ),
        (
            _enum(column="status", count=1, values=["x"], variant="whitespace"),
            r"\.params\.values: cannot be combined with variant whitespace",
        ),
        (_enum(column="status"), r"\.params: give exactly one of rate and count"),
    ],
)
def test_invalid_use_gives_a_clear_error(scenario: ScenarioConfig, message: str) -> None:
    with pytest.raises(DataChaosError, match=message):
        apply_all(scenario)


def _applied() -> tuple:  # type: ignore[type-arg]
    baseline = engine._build_dataset(config())
    params = {"column": "status", "count": 3, "values": ["UNKNOWN"], "variant": "literal"}
    ctx = context(NAME, "orders", params, baseline)
    scenario = SCENARIOS[NAME]
    outcome = scenario.apply(ctx)
    assert scenario.check(ctx, outcome) == []
    return scenario, ctx, outcome


def test_check_notices_a_value_that_is_still_allowed() -> None:
    scenario, ctx, outcome = _applied()
    bad = tamper(outcome, "orders", outcome.ledger[0].rid, status="paid")
    assert any("still has the allowed value" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_another_changed_column() -> None:
    scenario, ctx, outcome = _applied()
    bad = tamper(outcome, "orders", outcome.ledger[0].rid, amount="1.00")
    assert any("does not match its ledger" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_an_unselected_row_that_changed() -> None:
    scenario, ctx, outcome = _applied()
    touched = {entry.rid for entry in outcome.ledger}
    rid = next(row.rid for row in ctx.dataset.tables["orders"].rows if row.rid not in touched)
    bad = tamper(outcome, "orders", rid, status="paid")
    assert any("without being selected" in problem for problem in scenario.check(ctx, bad))


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
