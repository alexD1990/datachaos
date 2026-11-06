from __future__ import annotations

import copy
import json
import re
from dataclasses import replace
from pathlib import Path

import pytest
from helpers import SMALL, apply_all, by_rid, config, context, tamper

from datachaos import cli, engine
from datachaos.config import DatasetConfig, OutputConfig, RunConfig, ScenarioConfig
from datachaos.errors import DataChaosError
from datachaos.io.reader import read_csv, read_jsonl, read_schema
from datachaos.scenarios import SCENARIOS
from datachaos.scenarios.base import batch_of, conform_row

NAME = "schema-drift"
SEVEN = DatasetConfig(as_of=SMALL.as_of, customers=20, orders=200, batches=7)


def _drift(table: str = "orders", **params: object) -> ScenarioConfig:
    return ScenarioConfig(NAME, table, params)


RENAME = {
    "operation": "rename_column",
    "column": "source_system",
    "new_name": "channel",
    "from_batch": 1,
}
COMMA = {
    "operation": "change_type",
    "column": "amount",
    "to": "string_comma_decimal",
    "from_batch": 1,
}


def _columns(result, table: str, batch: int) -> list[str]:  # type: ignore[no-untyped-def]
    return [c.name for c in result.dataset.tables[table].schema_by_batch[batch].columns]


def _row_batches(result, table: str):  # type: ignore[no-untyped-def]
    current = result.dataset.tables[table]
    for row in current.rows:
        yield batch_of(result.dataset, current, row), row


def test_rename_changes_schema_and_rows_from_the_batch_on() -> None:
    result = apply_all(_drift(**RENAME))
    assert "source_system" in _columns(result, "orders", 0)
    assert "channel" not in _columns(result, "orders", 0)
    for batch in (1, 2):
        assert "channel" in _columns(result, "orders", batch)
        assert "source_system" not in _columns(result, "orders", batch)
    before = by_rid(result.baseline, "orders")
    for batch, row in _row_batches(result, "orders"):
        if batch == 0:
            assert row == before[row.rid]
        else:
            assert row.values["channel"] == before[row.rid].values["source_system"]
            assert "source_system" not in row.values


def test_drop_removes_the_column_from_the_batch_on() -> None:
    result = apply_all(_drift(operation="drop_column", column="source_system", from_batch=2))
    assert "source_system" in _columns(result, "orders", 1)
    assert "source_system" not in _columns(result, "orders", 2)
    for batch, row in _row_batches(result, "orders"):
        assert ("source_system" in row.values) == (batch < 2)


def test_add_with_null_fill_is_a_nullable_string_at_the_end() -> None:
    result = apply_all(_drift(operation="add_column", column="region", from_batch=1))
    schema = result.dataset.tables["orders"].schema_by_batch[1]
    assert schema.columns[-1].name == "region"
    assert schema.columns[-1].nullable
    assert schema.columns[-1].type.value == "string"
    for batch, row in _row_batches(result, "orders"):
        assert ("region" in row.values) == (batch >= 1)
        assert row.values.get("region") is None


def test_add_with_generated_fill_gives_a_value_per_row() -> None:
    result = apply_all(
        _drift(operation="add_column", column="region", fill="generated", from_batch=1)
    )
    values = [row.values["region"] for batch, row in _row_batches(result, "orders") if batch >= 1]
    assert all(re.fullmatch("[0-9a-f]{8}", str(value)) for value in values)
    assert len(set(values)) > 1


@pytest.mark.parametrize(
    ("table", "column", "to", "check"),
    [
        ("orders", "amount", "string_comma_decimal", lambda v: re.fullmatch(r"\d+,\d{2}", v)),
        ("orders", "amount", "string", lambda v: re.fullmatch(r"\d+\.\d{2}", v)),
        (
            "orders",
            "order_timestamp",
            "string_local_format",
            lambda v: re.fullmatch(r"\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}", v),
        ),
        ("customers", "is_active", "string_yes_no", lambda v: v in ("yes", "no")),
    ],
)
def test_change_type_converts_values_and_the_schema_says_string(
    table: str, column: str, to: str, check
) -> None:  # type: ignore[no-untyped-def]
    result = apply_all(_drift(table, operation="change_type", column=column, to=to, from_batch=1))
    schemas = result.dataset.tables[table].schema_by_batch
    assert next(c for c in schemas[0].columns if c.name == column).type.value != "string"
    changed = next(c for c in schemas[1].columns if c.name == column)
    assert changed.type.value == "string"
    assert changed.scale is None
    before = by_rid(result.baseline, table)
    seen = 0
    for batch, row in _row_batches(result, table):
        if batch == 0:
            assert row == before[row.rid]
        else:
            assert check(row.values[column])
            seen += 1
    assert seen


def test_ledger_has_one_schema_change_line_that_locks_no_rows() -> None:
    result = apply_all(_drift(**RENAME))
    assert len(result.ledger) == 1
    entry = result.ledger[0]
    assert entry.action == "schema_change"
    assert entry.rid == ""
    assert entry.key == {}
    assert entry.batch == 1
    assert (entry.column, entry.before, entry.after) == (
        "source_system",
        "source_system",
        "channel",
    )
    later = ScenarioConfig("invalid-enum", "orders", {"column": "status", "rate": 1.0})
    assert len(apply_all(_drift(**RENAME), later).ledger) == 101


@pytest.mark.parametrize(
    ("params", "before", "after"),
    [
        ({"operation": "drop_column", "column": "source_system"}, "source_system", None),
        ({"operation": "add_column", "column": "region"}, None, "region"),
        ({"operation": "change_type", "column": "amount", "to": "string"}, "decimal", "string"),
    ],
)
def test_ledger_describes_each_operation(
    params: dict[str, object], before: object, after: object
) -> None:
    entry = apply_all(_drift(**params)).ledger[0]
    assert (entry.before, entry.after) == (before, after)


def test_from_batch_defaults_to_the_last_batch_and_is_recorded() -> None:
    run_config = config(_drift(operation="drop_column", column="source_system"))
    baseline = engine._build_dataset(run_config)
    once = engine.resolve_scenarios(run_config, baseline)[0].params
    assert once == {"operation": "drop_column", "column": "source_system", "from_batch": 2}


@pytest.mark.parametrize(
    "params",
    [
        RENAME,
        COMMA,
        {"operation": "drop_column", "column": "source_system"},
        {"operation": "add_column", "column": "region"},
        {"operation": "add_column", "column": "region", "fill": "generated"},
    ],
)
def test_resolve_is_idempotent(params: dict[str, object]) -> None:
    run_config = config(_drift(**params))
    baseline = engine._build_dataset(run_config)
    once = engine.resolve_scenarios(run_config, baseline)[0].params
    again = engine.resolve_scenarios(config(_drift(**once)), baseline)[0].params
    assert again == once
    assert list(again) == list(once)


@pytest.mark.parametrize("fill", [None, "null"])
def test_an_unquoted_yaml_null_means_null_fill(fill: object) -> None:
    result = apply_all(_drift(operation="add_column", column="region", fill=fill))
    assert result.records[0]["params"]["fill"] == "null"  # type: ignore[index]


def test_the_input_is_not_mutated() -> None:
    run_config = config(_drift(**COMMA))
    baseline = engine._build_dataset(run_config)
    snapshot = copy.deepcopy(baseline)
    engine._apply_scenarios(run_config, baseline, engine.resolve_scenarios(run_config, baseline))
    assert baseline == snapshot


@pytest.mark.parametrize(
    ("params", "message"),
    [
        ({"column": "amount"}, r"params\.operation: is required"),
        ({"operation": "drop_column"}, r"params\.column: is required"),
        ({"operation": "explode", "column": "amount"}, r"params\.operation: must be one of"),
        ({"operation": "drop_column", "column": "nope"}, r"params\.column: table orders has no"),
        ({"operation": "drop_column", "column": "amount", "from_batch": 3}, r"from 0 to 2, got 3"),
        (
            {"operation": "drop_column", "column": "amount", "from_batch": -1},
            r"from 0 to 2, got -1",
        ),
        ({"operation": "drop_column", "column": "amount", "rate": 0.5}, r"params\.rate: unknown"),
        ({"operation": "drop_column", "column": "amount", "count": 1}, r"params\.count: unknown"),
        (
            {"operation": "drop_column", "column": "amount", "allow_overlap": True},
            r"allow_overlap: unknown",
        ),
        (
            {"operation": "drop_column", "column": "amount", "new_name": "x"},
            r"only applies to rename_column",
        ),
        (
            {"operation": "drop_column", "column": "amount", "to": "string"},
            r"only applies to change_type",
        ),
        (
            {"operation": "drop_column", "column": "amount", "fill": "null"},
            r"only applies to add_column",
        ),
        ({"operation": "rename_column", "column": "amount"}, r"params\.new_name: is required"),
        (
            {"operation": "rename_column", "column": "amount", "new_name": "status"},
            r"already has a column 'status'",
        ),
        (
            {"operation": "rename_column", "column": "amount", "new_name": "_chaos_rid"},
            r"cannot be a column name",
        ),
        (
            {"operation": "rename_column", "column": "amount", "new_name": "a b"},
            r"cannot be a column name",
        ),
        ({"operation": "add_column", "column": "amount"}, r"already has a column 'amount'"),
        ({"operation": "add_column", "column": "_chaos_rid"}, r"cannot be a column name"),
        (
            {"operation": "add_column", "column": "x", "fill": "maybe"},
            r"params\.fill: must be null or generated",
        ),
        ({"operation": "change_type", "column": "amount"}, r"params\.to: is required"),
        (
            {"operation": "change_type", "column": "amount", "to": "number"},
            r"params\.to: must be one of",
        ),
        (
            {"operation": "change_type", "column": "status", "to": "string"},
            r"does not apply to status, which is string",
        ),
        (
            {"operation": "change_type", "column": "amount", "to": "string_yes_no"},
            r"does not apply to amount",
        ),
        (
            {"operation": "change_type", "column": "order_timestamp", "to": "string_comma_decimal"},
            r"does not apply",
        ),
        (
            {"operation": "change_type", "column": "order_id", "to": "string"},
            r"does not apply to order_id",
        ),
        ({"operation": "drop_column", "column": "ingested_at"}, r"decides the batch of a row"),
        (
            {"operation": "rename_column", "column": "ingested_at", "new_name": "x"},
            r"decides the batch",
        ),
        (
            {"operation": "change_type", "column": "ingested_at", "to": "string_local_format"},
            r"decides the batch",
        ),
        ({"operation": "drop_column", "column": "order_id"}, r"order_id is a key column"),
        (
            {"operation": "rename_column", "column": "order_id", "new_name": "id"},
            r"order_id is a key column",
        ),
    ],
)
def test_invalid_use_gives_a_clear_error(params: dict[str, object], message: str) -> None:
    with pytest.raises(DataChaosError, match=message):
        apply_all(_drift(**params))


def test_a_foreign_key_can_be_renamed() -> None:
    result = apply_all(
        _drift(operation="rename_column", column="customer_id", new_name="client", from_batch=1)
    )
    assert "client" in _columns(result, "orders", 2)


# Conflicts, in both orders.

OTHERS = [
    ScenarioConfig("null-critical-column", "orders", {"column": "amount", "count": 1}),
    ScenarioConfig(
        "historical-update", "orders", {"older_than": "1d", "count": 1, "columns": ["amount"]}
    ),
    ScenarioConfig("historical-update", "orders", {"older_than": "1d", "count": 1}),
]


@pytest.mark.parametrize("other", OTHERS, ids=lambda o: f"{o.name}-{sorted(o.params)}")
def test_a_scenario_on_the_same_column_conflicts_in_both_orders(other: ScenarioConfig) -> None:
    drift = _drift(**COMMA)
    with pytest.raises(
        DataChaosError, match=r"scenarios\[1\]\.params: schema-drift and scenarios\[0\]"
    ):
        apply_all(other, drift)
    with pytest.raises(DataChaosError, match=r"schema-drift\) both use orders\.amount"):
        apply_all(drift, other)


@pytest.mark.parametrize(
    ("other", "drift"),
    [
        (ScenarioConfig("invalid-enum", "orders", {"column": "source_system", "count": 1}), RENAME),
        (
            ScenarioConfig("orphaned-foreign-key", "orders", {"count": 1}),
            {"operation": "drop_column", "column": "customer_id"},
        ),
        (
            ScenarioConfig("out-of-order-events", "order_events", {"count": 1}),
            {"operation": "change_type", "column": "event_time", "to": "string_local_format"},
        ),
        (
            ScenarioConfig("out-of-order-events", "order_events", {"count": 1}),
            {"operation": "rename_column", "column": "order_id", "new_name": "o"},
        ),
        (
            ScenarioConfig(
                "historical-update",
                "orders",
                {"older_than": "1d", "count": 1, "columns": ["status"]},
            ),
            {"operation": "rename_column", "column": "updated_at", "new_name": "changed_at"},
        ),
        (
            ScenarioConfig(
                "historical-update",
                "orders",
                {"older_than": "1d", "count": 1, "columns": ["status"]},
            ),
            {"operation": "change_type", "column": "order_timestamp", "to": "string_local_format"},
        ),
    ],
)
def test_columns_a_scenario_must_read_also_conflict(
    other: ScenarioConfig, drift: dict[str, object]
) -> None:
    with pytest.raises(DataChaosError, match="cannot be combined"):
        apply_all(other, ScenarioConfig(NAME, other.table, drift))


def test_a_column_already_renamed_or_dropped_names_the_drift() -> None:
    later = ScenarioConfig(
        "null-critical-column", "orders", {"column": "source_system", "count": 1}
    )
    with pytest.raises(
        DataChaosError,
        match=r"scenarios\[1\]\.params\.column: table orders has no column 'source_system'; "
        r"schema-drift in scenarios\[0\] changed this table",
    ):
        apply_all(_drift(**RENAME), later)


def test_the_new_name_conflicts_too() -> None:
    later = ScenarioConfig("null-critical-column", "orders", {"column": "channel", "count": 1})
    with pytest.raises(DataChaosError, match=r"both use orders\.channel"):
        apply_all(_drift(**RENAME), later)


def test_two_drifts_on_the_same_column_conflict() -> None:
    with pytest.raises(DataChaosError, match=r"both use orders\.amount"):
        apply_all(_drift(**COMMA), _drift(operation="drop_column", column="amount"))


def test_two_drifts_on_different_columns_combine() -> None:
    result = apply_all(_drift(**COMMA), _drift(**RENAME))
    assert {"amount", "channel"} <= set(_columns(result, "orders", 2))


def test_same_column_name_in_another_table_is_no_conflict() -> None:
    drift = _drift(operation="rename_column", column="customer_id", new_name="client")
    other = ScenarioConfig(
        "null-critical-column", "customers", {"column": "country_code", "count": 1}
    )
    assert apply_all(drift, other).ledger
    keyed = ScenarioConfig("duplicate-business-key", "customers", {"count": 1})
    assert apply_all(drift, keyed).ledger


# Crossing the batch where the schema changes.

CROSSING = {"delay_min": "1d", "delay_max": "1d"}


def _assert_form(result, table: str) -> None:  # type: ignore[no-untyped-def]
    """Every row has exactly the columns of the batch it is in."""
    for batch, row in _row_batches(result, table):
        names = set(_columns(result, table, batch))
        assert set(row.values) == names, (batch, row.rid)


@pytest.mark.parametrize(
    "params",
    [
        RENAME,
        COMMA,
        {"operation": "drop_column", "column": "source_system", "from_batch": 1},
        {"operation": "add_column", "column": "region", "from_batch": 1},
        {"operation": "add_column", "column": "region", "fill": "generated", "from_batch": 1},
    ],
)
@pytest.mark.parametrize("drift_first", [True, False])
def test_a_copy_in_a_later_batch_has_the_form_of_that_batch(
    params: dict[str, object], drift_first: bool
) -> None:
    drift = _drift(**params)
    duplicate = ScenarioConfig(
        "duplicate-business-key", "orders", {"rate": 1.0, "same_batch": False}
    )
    result = apply_all(*((drift, duplicate) if drift_first else (duplicate, drift)))
    _assert_form(result, "orders")
    assert result.dataset.tables["orders"].rows


def test_an_exact_copy_is_equal_after_conversion() -> None:
    duplicate = ScenarioConfig(
        "duplicate-business-key", "orders", {"rate": 1.0, "same_batch": False}
    )
    result = apply_all(_drift(**COMMA), duplicate)
    table = result.dataset.tables["orders"]
    rows = by_rid(result.dataset, "orders")
    crossed = 0
    for entry in result.ledger:
        if entry.action != "insert_row":
            continue
        source = rows[entry.source_rid or ""]
        copied = rows[entry.rid]
        source_batch = batch_of(result.dataset, table, source)
        target = batch_of(result.dataset, table, copied)
        shaped = conform_row(table, source, source_batch, target)
        assert {**copied.values, "ingested_at": None} == {**shaped.values, "ingested_at": None}
        if source_batch < 1 <= target:
            crossed += 1
            assert "," in str(copied.values["amount"])
            assert "." in str(source.values["amount"])
    assert crossed


def test_a_drift_after_the_copies_gives_the_same_form_as_before_them() -> None:
    duplicate = ScenarioConfig(
        "duplicate-business-key", "orders", {"rate": 1.0, "same_batch": False}
    )
    first = apply_all(_drift(**RENAME), duplicate)
    second = apply_all(duplicate, _drift(**RENAME))
    assert {tuple(sorted(row.values)) for _, row in _row_batches(first, "orders")} == {
        tuple(sorted(row.values)) for _, row in _row_batches(second, "orders")
    }


def test_mutate_non_key_never_chooses_a_changed_column() -> None:
    duplicate = ScenarioConfig(
        "duplicate-business-key", "orders", {"rate": 1.0, "mutation_mode": "mutate_non_key"}
    )
    result = apply_all(_drift(**COMMA), duplicate)
    columns = {entry.column for entry in result.ledger if entry.action == "insert_row"}
    assert columns == {"status", "currency", "source_system"}


def test_mutate_non_key_conflicts_with_a_later_drift_on_a_column_it_may_change() -> None:
    duplicate = ScenarioConfig(
        "duplicate-business-key", "orders", {"count": 1, "mutation_mode": "mutate_non_key"}
    )
    with pytest.raises(DataChaosError, match=r"both use orders\.amount"):
        apply_all(duplicate, _drift(**COMMA))
    assert apply_all(_drift(operation="add_column", column="region"), duplicate).ledger


def test_a_late_row_gets_the_form_of_its_new_batch() -> None:
    late = ScenarioConfig("late-arriving-data", "orders", {**CROSSING, "rate": 1.0})
    result = apply_all(_drift(**COMMA), late)
    _assert_form(result, "orders")
    before = by_rid(result.baseline, "orders")
    crossed = [e for e in result.ledger if e.action == "move_batch" and e.before == 0]
    assert crossed
    rows = by_rid(result.dataset, "orders")
    for entry in crossed:
        assert rows[entry.rid].values["amount"] == str(before[entry.rid].values["amount"]).replace(
            ".", ","
        )


def test_a_row_moved_in_gets_the_same_generated_value_as_if_it_had_been_there() -> None:
    drift = _drift(operation="add_column", column="region", fill="generated", from_batch=1)
    late = ScenarioConfig("late-arriving-data", "orders", {**CROSSING, "rate": 1.0})
    result = apply_all(drift, late)
    _assert_form(result, "orders")
    moved = [e for e in result.ledger if e.action == "move_batch" and e.before == 0]
    assert moved
    rows = by_rid(result.dataset, "orders")
    assert all(re.fullmatch("[0-9a-f]{8}", str(rows[e.rid].values["region"])) for e in moved)


def test_an_event_held_back_gets_the_form_of_its_new_batch() -> None:
    drift = _drift("order_events", operation="add_column", column="region", from_batch=1)
    held = ScenarioConfig("out-of-order-events", "order_events", {"rate": 1.0})
    result = apply_all(drift, held)
    _assert_form(result, "order_events")
    assert any(e.action == "move_batch" and e.before == 0 for e in result.ledger)


# historical-update across a schema change.


def _versions(result):  # type: ignore[no-untyped-def]
    return [e for e in result.ledger if e.action == "insert_row"]


def test_a_version_after_from_batch_has_the_new_form_of_other_columns() -> None:
    update = ScenarioConfig(
        "historical-update", "orders", {"older_than": "1d", "rate": 1.0, "columns": ["status"]}
    )
    result = apply_all(_drift(**RENAME), update)
    _assert_form(result, "orders")
    rows = by_rid(result.dataset, "orders")
    source_batch_zero = 0
    table = result.dataset.tables["orders"]
    for entry in _versions(result):
        version = rows[entry.rid]
        assert "channel" in version.values
        if batch_of(result.dataset, table, rows[entry.source_rid or ""]) == 0:
            source_batch_zero += 1
            assert "source_system" in rows[entry.source_rid or ""].values
    assert source_batch_zero


def test_a_version_before_from_batch_keeps_the_old_form() -> None:
    drift = _drift(
        operation="rename_column", column="source_system", new_name="channel", from_batch=2
    )
    update = ScenarioConfig(
        "historical-update",
        "orders",
        {"older_than": "1s", "rate": 1.0, "columns": ["status"], "in_batch": 1},
    )
    result = apply_all(drift, update)
    _assert_form(result, "orders")
    rows = by_rid(result.dataset, "orders")
    assert _versions(result)
    for entry in _versions(result):
        assert "source_system" in rows[entry.rid].values
        assert "channel" not in rows[entry.rid].values


@pytest.mark.parametrize(
    "drift",
    [
        COMMA,
        {"operation": "rename_column", "column": "amount", "new_name": "total", "from_batch": 1},
        {"operation": "drop_column", "column": "amount", "from_batch": 1},
    ],
)
@pytest.mark.parametrize("params", [{}, {"columns": ["amount"]}], ids=["default", "explicit"])
def test_a_changed_column_in_columns_is_an_error_at_startup(
    tmp_path: Path, drift: dict[str, object], params: dict[str, object]
) -> None:
    update = ScenarioConfig(
        "historical-update", "orders", {"older_than": "1d", "count": 1, **params}
    )
    with pytest.raises(DataChaosError, match=r"scenarios\[0\]"):
        engine.run(config(_drift(**drift), update), tmp_path / "out")
    assert list(tmp_path.iterdir()) == []


# Acceptance: what is written.


def test_batches_before_from_batch_are_byte_identical_to_a_run_without_drift(
    tmp_path: Path,
) -> None:
    others = (
        ScenarioConfig("invalid-enum", "orders", {"column": "status", "count": 5}),
        ScenarioConfig("null-critical-column", "customers", {"column": "country_code", "count": 2}),
    )
    engine.run(config(*others, settings=SEVEN), tmp_path / "plain")
    engine.run(
        config(
            *others,
            _drift(
                operation="rename_column", column="source_system", new_name="channel", from_batch=4
            ),
            settings=SEVEN,
        ),
        tmp_path / "drift",
    )
    for table in ("customers", "orders", "order_events"):
        for batch in range(SEVEN.batches):
            name = f"batch_{batch:04d}"
            data = (f"data/{table}/{name}.csv", f"schemas/{table}/{name}.json")
            for path in data:
                same = (tmp_path / "plain" / path).read_bytes() == (
                    tmp_path / "drift" / path
                ).read_bytes()
                assert same == (table != "orders" or batch < 4), path


def test_schema_files_reflect_the_change_from_the_batch_on(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.run(config(_drift(**COMMA)), out)
    old = read_schema(out / "schemas" / "orders" / "batch_0000.json")
    new = read_schema(out / "schemas" / "orders" / "batch_0001.json")
    assert next(c for c in old.columns if c.name == "amount").type.value == "decimal"
    assert next(c for c in new.columns if c.name == "amount").type.value == "string"
    assert next(c for c in old.columns if c.name == "amount").scale == 2
    assert next(c for c in new.columns if c.name == "amount").scale is None


def test_comma_decimals_are_quoted_in_csv_and_read_back_unchanged(tmp_path: Path) -> None:
    out = tmp_path / "out"
    settings = OutputConfig(formats=("csv", "jsonl"), emit_row_ids=True)
    run_config = RunConfig(seed=5, dataset=SMALL, output=settings, scenarios=(_drift(**COMMA),))
    engine.run(run_config, out)
    baseline = engine._build_dataset(run_config)
    table = baseline.tables["orders"]
    source = {
        row.rid: str(row.values["amount"]).replace(".", ",")
        for row in table.rows
        if batch_of(baseline, table, row) >= 1
    }
    seen = {"csv": 0, "jsonl": 0}
    for batch in (1, 2):
        name = f"batch_{batch:04d}"
        schema = read_schema(out / "schemas" / "orders" / f"{name}.json")
        csv_path = out / "data" / "orders" / f"{name}.csv"
        lines = csv_path.read_bytes().decode().splitlines()[1:]
        assert lines
        rows = read_csv(csv_path, schema)
        for line, row in zip(lines, rows, strict=True):
            expected = source[str(row["_chaos_rid"])]
            assert f',"{expected}",'.encode() in line.encode()
            assert re.fullmatch(r"\d+,\d{2}", expected)
            assert row["amount"] == expected
            seen["csv"] += 1
        jsonl_path = out / "data" / "orders" / f"{name}.jsonl"
        raw = jsonl_path.read_bytes().decode().splitlines()
        rows = read_jsonl(jsonl_path, schema)
        for line, row in zip(raw, rows, strict=True):
            expected = source[str(row["_chaos_rid"])]
            assert f'"amount":"{expected}"' in line
            assert row["amount"] == expected
            assert json.loads(line)["amount"] == expected
            seen["jsonl"] += 1
    assert seen["csv"] == seen["jsonl"] == len(source) > 0


def test_decimals_before_from_batch_stay_unquoted(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.run(config(_drift(**COMMA)), out)
    schema = read_schema(out / "schemas" / "orders" / "batch_0000.json")
    rows = read_csv(out / "data" / "orders" / "batch_0000.csv", schema)
    assert rows
    assert all(re.fullmatch(r"\d+\.\d{2}", str(row["amount"])) for row in rows)
    assert b'"' not in (out / "data" / "orders" / "batch_0000.csv").read_bytes()


def test_emit_row_ids_keeps_the_rid_last_after_an_added_column(tmp_path: Path) -> None:
    out = tmp_path / "out"
    settings = OutputConfig(emit_row_ids=True)
    run_config = RunConfig(
        seed=5,
        dataset=SMALL,
        output=settings,
        scenarios=(_drift(operation="add_column", column="region", from_batch=1),),
    )
    engine.run(run_config, out)
    schema = read_schema(out / "schemas" / "orders" / "batch_0002.json")
    assert [c.name for c in schema.columns][-2:] == ["region", "_chaos_rid"]
    assert read_csv(out / "data" / "orders" / "batch_0002.csv", schema)


def _write_config(tmp_path: Path, *lines: str) -> Path:
    head = [
        "version: 1",
        "seed: 3",
        'dataset: {as_of: "2026-01-08T00:00:00Z", customers: 5, orders: 20, batches: 3}',
        "output: {combined: true}",
        "scenarios:",
    ]
    path = tmp_path / "chaos.yml"
    path.write_bytes(("\n".join([*head, *lines]) + "\n").encode())
    return path


def test_combined_refuses_with_an_explanation_when_the_schema_varies(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _write_config(
        tmp_path,
        "  - {name: schema-drift, table: orders, params: {operation: drop_column, column: status}}",
    )
    out = tmp_path / "out"
    assert cli.main(["run", "--config", str(path), "--out", str(out)]) == 2
    err = capsys.readouterr().err
    assert "cannot combine orders: its schema differs between batches" in err
    assert "--combined" in err
    assert err.count("\n") == 1
    assert not out.exists()


def test_combined_is_allowed_when_the_change_covers_every_batch(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path,
        "  - name: schema-drift",
        "    table: orders",
        "    params: {operation: drop_column, column: status, from_batch: 0}",
    )
    out = tmp_path / "out"
    assert cli.main(["run", "--config", str(path), "--out", str(out)]) == 0
    assert b"status" not in (out / "data" / "orders.csv").read_bytes().splitlines()[0]


def test_stats() -> None:
    result = apply_all(_drift(**COMMA))
    changed = sum(batch >= 1 for batch, _ in _row_batches(result, "orders"))
    assert result.records[0]["stats"] == {
        "rows_before": 100,
        "rows_after": 100,
        "rows_selected": changed,
        "rows_added": 0,
        "values_changed": changed,
    }
    assert apply_all(_drift(**RENAME)).records[0]["stats"]["values_changed"] == 0  # type: ignore[index]


def test_the_key_and_ingestion_columns_are_stable_after_any_drift() -> None:
    result = apply_all(_drift(**COMMA), _drift(**RENAME))
    table = result.dataset.tables["orders"]
    names = [column.name for column in table.stable_schema.columns]
    assert "order_id" in names
    assert "ingested_at" in names
    assert "amount" not in names
    assert "source_system" not in names


def _applied(params: dict[str, object] = COMMA) -> tuple:  # type: ignore[type-arg]
    baseline = engine._build_dataset(config())
    ctx = context(NAME, "orders", params, baseline)
    scenario = SCENARIOS[NAME]
    outcome = scenario.apply(ctx)
    assert scenario.check(ctx, outcome) == []
    return scenario, ctx, outcome


def _rid_in(dataset, batch: int) -> str:  # type: ignore[no-untyped-def]
    table = dataset.tables["orders"]
    return next(row.rid for row in table.rows if batch_of(dataset, table, row) == batch)


def test_check_notices_a_row_before_from_batch_that_changed() -> None:
    scenario, ctx, outcome = _applied()
    bad = tamper(outcome, "orders", _rid_in(outcome.dataset, 0), status="paid")
    assert any("before from_batch changed" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_row_without_the_new_form() -> None:
    scenario, ctx, outcome = _applied()
    bad = tamper(outcome, "orders", _rid_in(outcome.dataset, 2), amount="1.00")
    assert any("does not have the new form" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_schema_without_the_change() -> None:
    scenario, ctx, outcome = _applied()
    table = outcome.dataset.tables["orders"]
    plain = ctx.dataset.tables["orders"].schema_by_batch
    wrong = replace(table, schema_by_batch=plain)
    bad = replace(
        outcome,
        dataset=replace(outcome.dataset, tables={**outcome.dataset.tables, "orders": wrong}),
    )
    assert any("do not reflect the change" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_another_table_that_changed() -> None:
    scenario, ctx, outcome = _applied()
    rid = outcome.dataset.tables["customers"].rows[0].rid
    bad = tamper(outcome, "customers", rid, is_active=None)
    assert any("table customers changed" in problem for problem in scenario.check(ctx, bad))


def test_check_notices_a_lost_row() -> None:
    scenario, ctx, outcome = _applied()
    table = outcome.dataset.tables["orders"]
    shorter = replace(table, rows=table.rows[1:])
    bad = replace(
        outcome,
        dataset=replace(outcome.dataset, tables={**outcome.dataset.tables, "orders": shorter}),
    )
    assert any("is gone" in problem for problem in scenario.check(ctx, bad))
