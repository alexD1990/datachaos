from __future__ import annotations

import copy
from collections import Counter
from dataclasses import replace

import pytest
from helpers import SMALL, apply_all, by_rid, config, context, set_values, tamper

from datachaos import engine
from datachaos.config import DatasetConfig, ScenarioConfig
from datachaos.errors import DataChaosError
from datachaos.scenarios import SCENARIOS
from datachaos.scenarios.base import batch_of

NAME = "duplicate-business-key"


def _dup(table: str = "orders", **params: object) -> ScenarioConfig:
    return ScenarioConfig(NAME, table, params)


def _key_counts(rows: tuple, column: str) -> Counter:  # type: ignore[type-arg]
    return Counter(row.values[column] for row in rows)


def test_each_selected_key_occurs_exactly_twice() -> None:
    result = apply_all(_dup(count=10))
    before = result.baseline.tables["orders"]
    after = result.dataset.tables["orders"]
    assert len(after.rows) == len(before.rows) + 10
    counts = _key_counts(after.rows, "order_id")
    assert Counter(counts.values()) == {1: 90, 2: 10}
    assert {key for key, count in counts.items() if count == 2} == {
        entry.key["order_id"] for entry in result.ledger
    }


def test_original_rows_are_unchanged() -> None:
    result = apply_all(_dup(count=10))
    after = by_rid(result.dataset, "orders")
    for row in result.baseline.tables["orders"].rows:
        assert after[row.rid] == row


def test_exact_copy_differs_only_by_at_most_one_second_of_ingested_at() -> None:
    result = apply_all(_dup(rate=0.5))
    after = by_rid(result.dataset, "orders")
    baseline = by_rid(result.baseline, "orders")
    for entry in result.ledger:
        copied = dict(after[entry.rid].values)
        source = dict(baseline[entry.source_rid or ""].values)
        assert copied.pop("ingested_at") >= source.pop("ingested_at")
        assert copied == source
        assert entry.column is None
        assert entry.before is None
        assert entry.after is None


def test_ledger_lines_are_correct() -> None:
    result = apply_all(_dup(count=3))
    table = result.dataset.tables["orders"]
    rows = by_rid(result.dataset, "orders")
    assert [entry.rid for entry in result.ledger] == [
        "s0000-0000001",
        "s0000-0000002",
        "s0000-0000003",
    ]
    for entry in result.ledger:
        assert entry.action == "insert_row"
        assert entry.scenario == NAME
        assert entry.scenario_index == 0
        assert entry.table == "orders"
        assert entry.key == {"order_id": rows[entry.rid].values["order_id"]}
        assert entry.batch == batch_of(result.dataset, table, rows[entry.rid])


def test_stats_and_record() -> None:
    result = apply_all(_dup(count=4))
    record = result.records[0]
    assert record["stats"] == {
        "rows_before": 100,
        "rows_after": 104,
        "rows_selected": 4,
        "rows_added": 4,
        "values_changed": 0,
    }
    assert record["ledger_entries"] == 4
    assert record["version"] == 1
    assert record["params"] == {
        "count": 4,
        "allow_overlap": False,
        "key_columns": ["order_id"],
        "mutation_mode": "exact_copy",
        "same_batch": True,
    }


def test_same_batch_keeps_the_batch_of_the_source() -> None:
    result = apply_all(_dup(rate=1.0))
    before = result.baseline.tables["orders"]
    after = result.dataset.tables["orders"]
    source = by_rid(result.baseline, "orders")
    rows = by_rid(result.dataset, "orders")
    for entry in result.ledger:
        assert batch_of(result.dataset, after, rows[entry.rid]) == batch_of(
            result.baseline, before, source[entry.source_rid or ""]
        )


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        # Last second of batch 0: one second later would be batch 1.
        ("2026-01-05T23:59:59Z", "2026-01-05T23:59:59Z"),
        # Last second before as_of: one second later would be as_of.
        ("2026-01-07T23:59:59Z", "2026-01-07T23:59:59Z"),
        # Anywhere else: one second later.
        ("2026-01-06T12:00:00Z", "2026-01-06T12:00:01Z"),
    ],
)
def test_copy_stays_in_the_batch_at_its_boundaries(timestamp: str, expected: str) -> None:
    baseline = engine._build_dataset(config())
    rid = baseline.tables["orders"].rows[0].rid
    dataset = set_values(baseline, "orders", rid, ingested_at=timestamp)
    only_rid = replace(
        context(
            NAME,
            "orders",
            {
                "count": 1,
                "key_columns": ["order_id"],
                "mutation_mode": "exact_copy",
                "same_batch": True,
            },
            dataset,
        ),
        eligible=lambda row: row.rid == rid,
    )
    scenario = SCENARIOS[NAME]
    outcome = scenario.apply(only_rid)
    assert scenario.check(only_rid, outcome) == []
    copied = by_rid(outcome.dataset, "orders")["s0000-0000001"]
    assert copied.values["ingested_at"] == expected
    assert outcome.dataset.tables["orders"].rows[-1].values["ingested_at"] < "2026-01-08T00:00:00Z"


def test_later_batch_puts_every_copy_in_a_later_batch() -> None:
    result = apply_all(_dup(rate=0.5, same_batch=False))
    before = result.baseline.tables["orders"]
    after = result.dataset.tables["orders"]
    source = by_rid(result.baseline, "orders")
    rows = by_rid(result.dataset, "orders")
    assert result.ledger
    for entry in result.ledger:
        batch = batch_of(result.dataset, after, rows[entry.rid])
        assert batch == entry.batch
        assert batch > batch_of(result.baseline, before, source[entry.source_rid or ""])


def test_rows_in_the_last_batch_are_no_candidates_for_a_later_batch() -> None:
    baseline = engine._build_dataset(config())
    table = baseline.tables["orders"]
    not_last = [row for row in table.rows if batch_of(baseline, table, row) < 2]
    result = apply_all(_dup(rate=1.0, same_batch=False))
    assert len(result.ledger) == len(not_last)
    too_many = _dup(count=len(not_last) + 1, same_batch=False)
    with pytest.raises(DataChaosError, match=f"more than the {len(not_last)} candidate rows"):
        apply_all(too_many)


def test_mutate_non_key_changes_one_other_column_to_a_valid_value() -> None:
    result = apply_all(_dup(rate=1.0, mutation_mode="mutate_non_key"))
    schema = result.dataset.tables["orders"].schema_by_batch[0]
    columns = {column.name: column for column in schema.columns}
    rows = by_rid(result.dataset, "orders")
    source = by_rid(result.baseline, "orders")
    used = set()
    for entry in result.ledger:
        original = source[entry.source_rid or ""]
        copied = rows[entry.rid]
        assert entry.column is not None
        used.add(entry.column)
        assert entry.column not in ("order_id", "customer_id", "ingested_at")
        assert entry.before == original.values[entry.column]
        assert entry.after == copied.values[entry.column]
        assert entry.before != entry.after
        different = {k for k in copied.values if copied.values[k] != original.values[k]}
        assert different - {"ingested_at"} == {entry.column}
        allowed = columns[entry.column].allowed_values
        if allowed:
            assert entry.after in allowed
        if entry.column == "amount":
            assert str(entry.after).split(".")[1].isdigit()
            assert len(str(entry.after).split(".")[1]) == 2
    assert used == {"status", "amount", "currency", "source_system"}
    assert result.records[0]["stats"]["values_changed"] == len(result.ledger)  # type: ignore[index]


def test_mutate_non_key_on_customers_can_flip_the_boolean() -> None:
    result = apply_all(_dup("customers", rate=1.0, mutation_mode="mutate_non_key"))
    columns = {entry.column for entry in result.ledger}
    assert columns == {"country_code", "customer_segment", "is_active"}
    flips = [entry for entry in result.ledger if entry.column == "is_active"]
    assert all(entry.after is (not entry.before) for entry in flips)


def test_later_scenarios_avoid_the_duplicated_key() -> None:
    result = apply_all(_dup(count=60), _dup(count=40))
    assert len({entry.key["order_id"] for entry in result.ledger}) == 100
    with pytest.raises(DataChaosError, match="more than the 40 candidate rows"):
        apply_all(_dup(count=60), _dup(count=41))


def test_allow_overlap_duplicates_a_duplicate_with_a_relative_invariant() -> None:
    result = apply_all(_dup(rate=1.0), _dup(count=120, allow_overlap=True))
    counts = _key_counts(result.dataset.tables["orders"].rows, "order_id")
    assert sum(counts.values()) == 100 + 100 + 120
    assert max(counts.values()) >= 3


def test_the_input_is_not_mutated() -> None:
    run_config = config(_dup(count=5, mutation_mode="mutate_non_key"))
    baseline = engine._build_dataset(run_config)
    snapshot = copy.deepcopy(baseline)
    engine._apply_scenarios(run_config, baseline, engine.resolve_scenarios(run_config, baseline))
    assert baseline == snapshot


def test_explicit_key_columns_are_accepted() -> None:
    result = apply_all(_dup(count=1, key_columns=["order_id"]))
    assert result.records[0]["params"]["key_columns"] == ["order_id"]  # type: ignore[index]


def test_resolve_is_idempotent_and_keeps_the_order() -> None:
    run_config = config(_dup(count=2, mutation_mode="mutate_non_key"))
    baseline = engine._build_dataset(run_config)
    once = engine.resolve_scenarios(run_config, baseline)[0].params
    again_config = config(ScenarioConfig(NAME, "orders", once))
    again = engine.resolve_scenarios(again_config, baseline)[0].params
    assert again == once
    assert list(again) == list(once)


@pytest.mark.parametrize(
    ("scenario", "settings", "message"),
    [
        (_dup("order_events", count=1), SMALL, r"\.table: .* applies to entity tables"),
        (_dup(count=1, mutation_mode="nope"), SMALL, r"\.params\.mutation_mode: must be one of"),
        (_dup(count=1, same_batch="no"), SMALL, r"\.params\.same_batch: must be bool"),
        (_dup(count=1, key_columns="order_id"), SMALL, r"\.params\.key_columns: must be list"),
        (_dup(count=1, key_columns=[]), SMALL, r"\.params\.key_columns: must be a non-empty"),
        (_dup(count=1, key_columns=[1]), SMALL, r"\.params\.key_columns: must be a non-empty"),
        (
            _dup(count=1, key_columns=["order_id", "order_id"]),
            SMALL,
            r"\.params\.key_columns: must not repeat",
        ),
        (_dup(count=1, key_columns=["nope"]), SMALL, r"\.params\.key_columns: table orders has no"),
        (
            _dup(count=1, key_columns=["customer_id"]),
            SMALL,
            r"\.params\.key_columns: \['customer_id'\] does not identify",
        ),
        (
            _dup(count=1, same_batch=False),
            DatasetConfig(customers=2, orders=2, batches=1),
            r"\.params\.same_batch: false needs more than one batch",
        ),
        (_dup(), SMALL, r"\.params: give exactly one of rate and count"),
        (_dup(count=1, keys=[]), SMALL, r"\.params\.keys: unknown parameter"),
        (_dup(count=1000), SMALL, "count 1000 is more than the 100 candidate rows"),
    ],
)
def test_invalid_use_gives_a_clear_error(
    scenario: ScenarioConfig, settings: DatasetConfig, message: str
) -> None:
    with pytest.raises(DataChaosError, match=message):
        apply_all(scenario, settings=settings)


def _applied_context() -> tuple:  # type: ignore[type-arg]
    baseline = engine._build_dataset(config())
    params = {
        "count": 3,
        "allow_overlap": False,
        "key_columns": ["order_id"],
        "mutation_mode": "exact_copy",
        "same_batch": True,
    }
    ctx = context(NAME, "orders", params, baseline)
    scenario = SCENARIOS[NAME]
    outcome = scenario.apply(ctx)
    assert scenario.check(ctx, outcome) == []
    return scenario, ctx, outcome


def test_check_notices_a_changed_original() -> None:
    scenario, ctx, outcome = _applied_context()
    rid = ctx.dataset.tables["orders"].rows[0].rid
    bad = tamper(outcome, "orders", rid, status="paid")
    assert any("original row" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_copy_with_another_key() -> None:
    scenario, ctx, outcome = _applied_context()
    bad = tamper(outcome, "orders", "s0000-0000001", order_id="O999999")
    problems = scenario.check(ctx, bad)
    assert any("another key" in problem for problem in problems)


def test_check_notices_an_unexpected_difference_on_the_copy() -> None:
    scenario, ctx, outcome = _applied_context()
    bad = tamper(outcome, "orders", "s0000-0000001", status="paid")
    assert any("differs in ['status']" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_copy_in_another_batch() -> None:
    scenario, ctx, outcome = _applied_context()
    copied = by_rid(outcome.dataset, "orders")["s0000-0000001"]
    day = "2026-01-05" if str(copied.values["ingested_at"]) >= "2026-01-06" else "2026-01-07"
    bad = tamper(outcome, "orders", "s0000-0000001", ingested_at=f"{day}T12:00:00Z")
    assert any("in batch" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_key_that_does_not_occur_exactly_twice() -> None:
    scenario, ctx, outcome = _applied_context()
    rid = ctx.dataset.tables["orders"].rows[0].rid
    key = by_rid(outcome.dataset, "orders")["s0000-0000001"].values["order_id"]
    if by_rid(ctx.dataset, "orders")[rid].values["order_id"] == key:
        rid = ctx.dataset.tables["orders"].rows[1].rid
    bad = tamper(outcome, "orders", rid, order_id=key)
    assert any("occurs 3 times" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_missing_rows() -> None:
    scenario, ctx, outcome = _applied_context()
    table = outcome.dataset.tables["orders"]
    shorter = replace(table, rows=table.rows[:-1])
    bad = replace(
        outcome,
        dataset=replace(outcome.dataset, tables={**outcome.dataset.tables, "orders": shorter}),
    )
    assert scenario.check(ctx, bad)
