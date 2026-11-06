from __future__ import annotations

import copy
from collections import Counter

import pytest
from helpers import SMALL, apply_all, by_rid, config, context, tamper

from datachaos import engine
from datachaos.config import DatasetConfig, ScenarioConfig
from datachaos.errors import DataChaosError
from datachaos.scenarios import SCENARIOS
from datachaos.scenarios.base import batch_of
from datachaos.timeutil import parse_duration, parse_timestamp

NAME = "historical-update"
TIME_COLUMNS = {"updated_at", "ingested_at"}


def _update(table: str = "orders", **params: object) -> ScenarioConfig:
    return ScenarioConfig(NAME, table, {"older_than": "1d", **params})


def _time(row, column: str) -> int:  # type: ignore[no-untyped-def]
    return parse_timestamp(str(row.values[column]))


def _inserts(result):  # type: ignore[no-untyped-def]
    return [entry for entry in result.ledger if entry.action == "insert_row"]


def test_originals_stay_and_a_new_version_is_added() -> None:
    result = apply_all(_update(count=10))
    before = by_rid(result.baseline, "orders")
    after = by_rid(result.dataset, "orders")
    assert len(after) == 110
    for rid, row in before.items():
        assert after[rid] == row
    counts = Counter(row.values["order_id"] for row in after.values())
    assert Counter(counts.values()) == {1: 90, 2: 10}


def test_a_version_keeps_key_and_event_time_and_gets_a_new_updated_at() -> None:
    result = apply_all(_update(count=10))
    before = by_rid(result.baseline, "orders")
    after = by_rid(result.dataset, "orders")
    inserts = _inserts(result)
    assert len(inserts) == 10
    for entry in inserts:
        new = after[entry.rid]
        source = before[entry.source_rid]
        assert new.values["order_id"] == source.values["order_id"]
        assert new.values["order_timestamp"] == source.values["order_timestamp"]
        assert new.values["customer_id"] == source.values["customer_id"]
        assert _time(source, "updated_at") < _time(new, "updated_at") <= _time(new, "ingested_at")
        assert _time(new, "ingested_at") - _time(new, "updated_at") <= 1800


def test_the_default_columns_change_to_valid_values() -> None:
    result = apply_all(_update(rate=1.0))
    after = by_rid(result.dataset, "orders")
    schema = result.dataset.tables["orders"].schema_by_batch[0]
    allowed = next(c.allowed_values for c in schema.columns if c.name == "status")
    for entry in _inserts(result):
        new = after[entry.rid]
        source = by_rid(result.baseline, "orders")[entry.source_rid]
        assert new.values["status"] in allowed
        assert new.values["status"] != source.values["status"]
        assert new.values["amount"] != source.values["amount"]


def test_only_the_listed_columns_change() -> None:
    result = apply_all(_update(rate=0.5, columns=["amount"]))
    after = by_rid(result.dataset, "orders")
    before = by_rid(result.baseline, "orders")
    for entry in _inserts(result):
        differing = {
            name
            for name in after[entry.rid].values
            if after[entry.rid].values[name] != before[entry.source_rid].values[name]
        }
        assert differing - TIME_COLUMNS == {"amount"}


def test_ledger_has_an_insert_and_one_value_line_per_column() -> None:
    result = apply_all(_update(count=3))
    after = by_rid(result.dataset, "orders")
    before = by_rid(result.baseline, "orders")
    assert len(result.ledger) == 9
    by_new = Counter(entry.rid for entry in result.ledger if entry.action == "set_value")
    assert set(by_new.values()) == {2}
    for entry in result.ledger:
        assert entry.batch == 2
        assert entry.key == {"order_id": after[entry.rid].values["order_id"]}
        if entry.action == "set_value":
            insert = next(e for e in _inserts(result) if e.rid == entry.rid)
            assert entry.column in ("status", "amount")
            assert entry.before == before[insert.source_rid].values[entry.column]
            assert entry.after == after[entry.rid].values[entry.column]


def test_new_rids_are_fixed_width_and_the_lines_sort_insert_first() -> None:
    result = apply_all(_update(count=2))
    assert result.ledger[0].rid == "s0000-0000001"
    assert result.ledger[0].action == "insert_row"
    assert [entry.column for entry in result.ledger[:3]] == [None, "amount", "status"]


def test_candidates_are_old_enough() -> None:
    result = apply_all(_update(rate=1.0, older_than="2d"))
    before = by_rid(result.baseline, "orders")
    cutoff = parse_timestamp(SMALL.as_of) - parse_duration("2d")
    assert _inserts(result)
    for entry in _inserts(result):
        assert _time(before[entry.source_rid], "order_timestamp") < cutoff
    assert {entry.source_rid for entry in _inserts(result)} == {
        rid
        for rid, row in before.items()
        if _time(row, "order_timestamp") < cutoff
        and batch_of(result.baseline, result.baseline.tables["orders"], row) < 2
    }


def test_candidates_are_in_an_earlier_batch_than_the_new_version() -> None:
    result = apply_all(_update(rate=1.0, older_than="1s", in_batch=1))
    table = result.baseline.tables["orders"]
    before = by_rid(result.baseline, "orders")
    assert _inserts(result)
    for entry in _inserts(result):
        assert batch_of(result.baseline, table, before[entry.source_rid]) == 0
        assert entry.batch == 1


def test_versions_land_in_the_chosen_batch() -> None:
    result = apply_all(_update(count=5, in_batch=1, older_than="1s"))
    table = result.dataset.tables["orders"]
    rows = by_rid(result.dataset, "orders")
    for entry in _inserts(result):
        assert batch_of(result.dataset, table, rows[entry.rid]) == 1


def test_resolve_defaults_to_the_last_batch_and_is_idempotent() -> None:
    run_config = config(_update(count=2))
    baseline = engine._build_dataset(run_config)
    once = engine.resolve_scenarios(run_config, baseline)[0].params
    assert once == {
        "count": 2,
        "allow_overlap": False,
        "older_than": "1d",
        "columns": ["status", "amount"],
        "in_batch": 2,
    }
    again = engine.resolve_scenarios(config(_update(**once)), baseline)[0].params
    assert again == once
    assert list(again) == list(once)


def test_stats() -> None:
    assert apply_all(_update(count=4)).records[0]["stats"] == {
        "rows_before": 100,
        "rows_after": 104,
        "rows_selected": 4,
        "rows_added": 4,
        "values_changed": 8,
    }


def test_customers_need_their_own_columns() -> None:
    with pytest.raises(
        DataChaosError, match=r"params\.columns: table customers has no column 'status'.*give"
    ):
        apply_all(_update("customers", count=1))
    result = apply_all(_update("customers", count=2, columns=["customer_segment", "is_active"]))
    assert len(_inserts(result)) == 2


def test_exclusivity_against_an_earlier_scenario() -> None:
    earlier = ScenarioConfig(
        "null-critical-column", "orders", {"column": "source_system", "rate": 1.0}
    )
    with pytest.raises(DataChaosError, match="more than the 0 candidate rows"):
        apply_all(earlier, _update(count=1))
    assert _inserts(apply_all(earlier, _update(count=1, allow_overlap=True)))


def test_a_later_scenario_avoids_the_updated_key() -> None:
    later = ScenarioConfig(
        "null-critical-column", "orders", {"column": "source_system", "rate": 1.0}
    )
    result = apply_all(_update(count=10), later)
    updated = {entry.key["order_id"] for entry in _inserts(result)}
    for entry in result.ledger:
        if entry.scenario == "null-critical-column":
            assert entry.key["order_id"] not in updated


def test_rows_with_a_null_in_a_changed_column_are_no_candidates() -> None:
    nulled = ScenarioConfig("null-critical-column", "orders", {"column": "amount", "rate": 1.0})
    with pytest.raises(DataChaosError, match="more than the 0 candidate rows"):
        apply_all(nulled, _update(count=1, allow_overlap=True))


def test_the_input_is_not_mutated() -> None:
    run_config = config(_update(count=5))
    baseline = engine._build_dataset(run_config)
    snapshot = copy.deepcopy(baseline)
    engine._apply_scenarios(run_config, baseline, engine.resolve_scenarios(run_config, baseline))
    assert baseline == snapshot


@pytest.mark.parametrize(
    ("scenario", "settings", "message"),
    [
        (ScenarioConfig(NAME, "orders", {"count": 1}), SMALL, r"params\.older_than: is required"),
        (_update(count=1, older_than="3days"), SMALL, r"params\.older_than: '3days' is not"),
        (_update(count=1, older_than="0d"), SMALL, r"params\.older_than: must be above zero"),
        (_update(count=1, older_than="30d"), SMALL, r"params\.older_than: no row of orders"),
        (_update(count=1, columns="status"), SMALL, r"params\.columns: must be list"),
        (_update(count=1, columns=[]), SMALL, r"params\.columns: must be a non-empty"),
        (
            _update(count=1, columns=["amount", "amount"]),
            SMALL,
            r"params\.columns: must not repeat",
        ),
        (_update(count=1, columns=["nope"]), SMALL, r"params\.columns: table orders has no column"),
        (_update(count=1, columns=["customer_id"]), SMALL, r"customer_id has role foreign_key"),
        (_update(count=1, columns=["order_id"]), SMALL, r"order_id has role key"),
        (_update(count=1, columns=["updated_at"]), SMALL, r"updated_at has role updated_at"),
        (_update(count=1, in_batch=0), SMALL, r"params\.in_batch: must be from 1 to 2, got 0"),
        (_update(count=1, in_batch=3), SMALL, r"params\.in_batch: must be from 1 to 2, got 3"),
        (_update(count=1, in_batch="1"), SMALL, r"params\.in_batch: must be int"),
        (
            _update(count=1),
            DatasetConfig(customers=5, orders=5, batches=1),
            r"params\.in_batch: needs at least two batches",
        ),
        (
            ScenarioConfig(NAME, "order_events", {"older_than": "1d", "count": 1}),
            SMALL,
            r"\.table: .* applies to entity tables",
        ),
        (_update(), SMALL, r"params: give exactly one of rate and count"),
    ],
)
def test_invalid_use_gives_a_clear_error(
    scenario: ScenarioConfig, settings: DatasetConfig, message: str
) -> None:
    with pytest.raises(DataChaosError, match=message):
        apply_all(scenario, settings=settings)


def _applied() -> tuple:  # type: ignore[type-arg]
    baseline = engine._build_dataset(config())
    params = {"older_than": "1d", "count": 3, "columns": ["status", "amount"], "in_batch": 2}
    ctx = context(NAME, "orders", params, baseline)
    scenario = SCENARIOS[NAME]
    outcome = scenario.apply(ctx)
    assert scenario.check(ctx, outcome) == []
    return scenario, ctx, outcome


def _new_rid(outcome) -> str:  # type: ignore[no-untyped-def]
    return next(entry.rid for entry in outcome.ledger if entry.action == "insert_row")


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"order_id": "O999999"}, "another key"),
        ({"order_timestamp": "2020-01-01T00:00:00Z"}, "changed its order_timestamp"),
        ({"updated_at": "2020-01-01T00:00:00Z"}, "wrong updated_at"),
        ({"ingested_at": "2026-01-05T12:00:00Z"}, "is not in batch 2"),
        ({"source_system": "changed"}, "differs in"),
    ],
)
def test_check_notices_a_wrong_version(change: dict[str, object], message: str) -> None:
    scenario, ctx, outcome = _applied()
    bad = tamper(outcome, "orders", _new_rid(outcome), **change)
    assert any(message in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_changed_original() -> None:
    scenario, ctx, outcome = _applied()
    rid = ctx.dataset.tables["orders"].rows[0].rid
    bad = tamper(outcome, "orders", rid, status="paid")
    assert any("original row" in problem for problem in scenario.check(ctx, bad))
