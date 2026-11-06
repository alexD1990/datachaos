from __future__ import annotations

import re
from collections import defaultdict

import pytest

from datachaos.config import MAX_ROWS
from datachaos.datasets.shop import (
    _EVENT_GAP_MAX,
    _INGEST_DELAY_MAX,
    _LIFECYCLES,
    _event_ids,
    build_shop,
)
from datachaos.engine import partition
from datachaos.model.dataset import Dataset
from datachaos.model.schema import ColumnType, Role
from datachaos.model.table import Row
from datachaos.timeutil import parse_duration, parse_timestamp, window_start

CONFIGS = [
    {"seed": 42, "customers": 100, "orders": 1000, "batches": 1, "batch_interval": "1d"},
    {"seed": 1, "customers": 200, "orders": 2000, "batches": 7, "batch_interval": "1d"},
    {"seed": 7, "customers": 50, "orders": 500, "batches": 3, "batch_interval": "1h"},
    {"seed": 3, "customers": 1, "orders": 1, "batches": 1, "batch_interval": "3h"},
]
AS_OF = "2026-01-08T00:00:00Z"


@pytest.fixture(params=CONFIGS, ids=lambda c: f"{c['batches']}x{c['batch_interval']}")
def dataset(request: pytest.FixtureRequest) -> Dataset:
    return build_shop(as_of=AS_OF, **request.param)


def _rows(dataset: Dataset, table: str) -> tuple[Row, ...]:
    return dataset.tables[table].rows


def _time(row: Row, column: str) -> int:
    return parse_timestamp(str(row.values[column]))


def _events_by_order(dataset: Dataset) -> dict[str, list[Row]]:
    events: dict[str, list[Row]] = defaultdict(list)
    for row in _rows(dataset, "order_events"):
        events[str(row.values["order_id"])].append(row)
    for rows in events.values():
        rows.sort(key=lambda row: _time(row, "event_time"))
    return events


def test_keys_are_unique(dataset: Dataset) -> None:
    for name, table in dataset.tables.items():
        column = table.schema_by_batch[0].column_with_role(Role.KEY).name
        keys = [row.values[column] for row in table.rows]
        assert len(keys) == len(set(keys)), name


def test_rids_are_unique_and_rows_are_sorted(dataset: Dataset) -> None:
    for table in dataset.tables.values():
        keys = [(row.values["ingested_at"], row.rid) for row in table.rows]
        assert keys == sorted(keys)
        assert len({row.rid for row in table.rows}) == len(table.rows)


def test_values_follow_the_schema(dataset: Dataset) -> None:
    for table in dataset.tables.values():
        schema = table.schema_by_batch[0]
        for row in table.rows:
            assert list(row.values) == [column.name for column in schema.columns]
            for column in schema.columns:
                value = row.values[column.name]
                assert value is not None
                assert value != ""
                if column.role is Role.ENUM:
                    assert value in column.allowed_values
                if column.type is ColumnType.DECIMAL:
                    assert re.fullmatch(r"[0-9]+\.[0-9]{2}", str(value))
                if column.type is ColumnType.BOOLEAN:
                    assert isinstance(value, bool)


def test_foreign_keys_resolve(dataset: Dataset) -> None:
    customers = {row.values["customer_id"] for row in _rows(dataset, "customers")}
    orders = {row.values["order_id"] for row in _rows(dataset, "orders")}
    assert all(row.values["customer_id"] in customers for row in _rows(dataset, "orders"))
    assert all(row.values["order_id"] in orders for row in _rows(dataset, "order_events"))


def test_every_row_is_inside_the_window(dataset: Dataset) -> None:
    as_of = parse_timestamp(dataset.as_of)
    start = window_start(as_of, dataset.batches, parse_duration(dataset.batch_interval))
    for table in dataset.tables.values():
        for row in table.rows:
            assert start <= _time(row, "ingested_at") < as_of


def test_no_row_holds_information_from_after_its_ingested_at(dataset: Dataset) -> None:
    for table in dataset.tables.values():
        schema = table.schema_by_batch[0]
        times = [column.name for column in schema.columns if column.type is ColumnType.TIMESTAMP]
        for row in table.rows:
            ingested_at = _time(row, "ingested_at")
            assert all(_time(row, name) <= ingested_at for name in times)


def test_orders_are_their_state_at_creation(dataset: Dataset) -> None:
    for row in _rows(dataset, "orders"):
        assert row.values["status"] == "created"
        assert row.values["updated_at"] == row.values["order_timestamp"]


def test_customers_are_their_state_at_creation(dataset: Dataset) -> None:
    for row in _rows(dataset, "customers"):
        assert row.values["updated_at"] == row.values["created_at"]


def test_ingest_delay_is_at_most_30_minutes(dataset: Dataset) -> None:
    event_columns = {"customers": "created_at", "orders": "order_timestamp"}
    event_columns["order_events"] = "event_time"
    for name, column in event_columns.items():
        for row in _rows(dataset, name):
            assert 0 <= _time(row, "ingested_at") - _time(row, column) <= _INGEST_DELAY_MAX


def test_a_customer_exists_before_its_orders(dataset: Dataset) -> None:
    customers = {row.values["customer_id"]: row for row in _rows(dataset, "customers")}
    for order in _rows(dataset, "orders"):
        customer = customers[order.values["customer_id"]]
        assert _time(customer, "created_at") <= _time(order, "order_timestamp")


def test_a_parent_is_never_ingested_after_its_child(dataset: Dataset) -> None:
    customers = {row.values["customer_id"]: row for row in _rows(dataset, "customers")}
    orders = {row.values["order_id"]: row for row in _rows(dataset, "orders")}
    for order in orders.values():
        customer = customers[order.values["customer_id"]]
        assert _time(customer, "ingested_at") <= _time(order, "ingested_at")
    for event in _rows(dataset, "order_events"):
        order = orders[event.values["order_id"]]
        assert _time(order, "ingested_at") <= _time(event, "ingested_at")


def test_references_resolve_in_every_landed_prefix_of_batches(dataset: Dataset) -> None:
    customers: set[object] = set()
    orders: set[object] = set()
    parts = {name: partition(table, dataset) for name, table in dataset.tables.items()}
    for k in range(dataset.batches):
        customers |= {row.values["customer_id"] for row in parts["customers"][k].rows}
        orders |= {row.values["order_id"] for row in parts["orders"][k].rows}
        assert all(row.values["customer_id"] in customers for row in parts["orders"][k].rows)
        assert all(row.values["order_id"] in orders for row in parts["order_events"][k].rows)


def test_every_order_has_a_first_event_at_its_order_timestamp(dataset: Dataset) -> None:
    events = _events_by_order(dataset)
    for order in _rows(dataset, "orders"):
        first = events[str(order.values["order_id"])][0]
        assert first.values["event_type"] == "created"
        assert first.values["event_time"] == order.values["order_timestamp"]


def test_events_are_strictly_increasing_in_both_times(dataset: Dataset) -> None:
    for rows in _events_by_order(dataset).values():
        for earlier, later in zip(rows, rows[1:], strict=False):
            assert _time(earlier, "event_time") < _time(later, "event_time")
            assert _time(earlier, "ingested_at") < _time(later, "ingested_at")


def test_events_follow_a_lifecycle_prefix_with_1_to_5_events(dataset: Dataset) -> None:
    prefixes = {life[:n] for life in _LIFECYCLES for n in range(1, len(life) + 1)}
    for rows in _events_by_order(dataset).values():
        assert 1 <= len(rows) <= 5
        assert tuple(row.values["event_type"] for row in rows) in prefixes


def test_only_events_past_as_of_are_cut(dataset: Dataset) -> None:
    as_of = parse_timestamp(dataset.as_of)
    for rows in _events_by_order(dataset).values():
        types = tuple(row.values["event_type"] for row in rows)
        if types not in _LIFECYCLES:
            # The next event could have reached as_of, or nothing justified cutting it.
            assert _time(rows[-1], "event_time") + _EVENT_GAP_MAX + _INGEST_DELAY_MAX >= as_of


def test_old_orders_are_never_cut() -> None:
    dataset = build_shop(
        as_of=AS_OF, seed=1, customers=200, orders=2000, batches=7, batch_interval="1d"
    )
    as_of = parse_timestamp(AS_OF)
    latest_possible = 4 * _EVENT_GAP_MAX + _INGEST_DELAY_MAX
    events = _events_by_order(dataset)
    old = [
        rows for rows in events.values() if _time(rows[0], "event_time") + latest_possible < as_of
    ]
    assert old
    assert all(tuple(row.values["event_type"] for row in rows) in _LIFECYCLES for rows in old)


def test_a_tight_window_cuts_down_to_one_event_and_a_wide_one_reaches_five() -> None:
    tight = build_shop(
        as_of=AS_OF, seed=7, customers=50, orders=500, batches=3, batch_interval="1h"
    )
    wide = build_shop(
        as_of=AS_OF, seed=1, customers=200, orders=2000, batches=7, batch_interval="1d"
    )
    assert min(len(rows) for rows in _events_by_order(tight).values()) == 1
    assert max(len(rows) for rows in _events_by_order(wide).values()) == 5


def test_event_ids_above_999999_keep_fixed_width_and_sort_numerically() -> None:
    largest = MAX_ROWS * max(len(lifecycle) for lifecycle in _LIFECYCLES)
    numbers = [1, 999_999, 1_000_000, largest]
    ids = [_event_ids(number) for number in numbers]
    assert ids[2] == ("e1000000", "E1000000")
    assert all(len(rid) == 8 and len(event_id) == 8 for rid, event_id in ids)
    assert [rid for rid, _ in ids] == sorted(rid for rid, _ in ids)
    assert [event_id for _, event_id in ids] == sorted(event_id for _, event_id in ids)


def test_generated_events_use_the_fixed_width_ids(dataset: Dataset) -> None:
    rows = sorted(_rows(dataset, "order_events"), key=lambda row: row.rid)
    for number, row in enumerate(rows, 1):
        assert (row.rid, row.values["event_id"]) == _event_ids(number)


def test_orders_are_spread_evenly_over_the_batches() -> None:
    dataset = build_shop(
        as_of=AS_OF, seed=1, customers=200, orders=7000, batches=7, batch_interval="1d"
    )
    sizes = [len(batch.rows) for batch in partition(dataset.tables["orders"], dataset)]
    assert all(800 <= size <= 1200 for size in sizes), sizes


def test_the_first_customer_exists_from_the_start_of_the_window(dataset: Dataset) -> None:
    start = window_start(
        parse_timestamp(dataset.as_of), dataset.batches, parse_duration(dataset.batch_interval)
    )
    first = next(row for row in _rows(dataset, "customers") if row.rid == "c000001")
    assert _time(first, "created_at") == start


def test_an_order_chooses_among_all_customers_that_exist_by_then() -> None:
    dataset = build_shop(
        as_of=AS_OF, seed=1, customers=50, orders=5000, batches=7, batch_interval="1d"
    )
    assert {row.values["customer_id"] for row in _rows(dataset, "orders")} > {"C000001"}
    late = [
        row for row in _rows(dataset, "orders") if str(row.values["order_timestamp"]) > "2026-01-07"
    ]
    ids = {row.values["customer_id"] for row in late}
    assert len(ids) > 25


def test_seeds_are_per_purpose() -> None:
    base = {"as_of": AS_OF, "seed": 5, "customers": 20, "batches": 3, "batch_interval": "1d"}
    fewer = build_shop(orders=10, **base)  # type: ignore[arg-type]
    more = build_shop(orders=20, **base)  # type: ignore[arg-type]
    assert _rows(fewer, "customers") == _rows(more, "customers")
    by_rid = {row.rid: row for row in _rows(more, "orders")}
    assert all(by_rid[row.rid] == row for row in _rows(fewer, "orders"))


def test_same_arguments_give_equal_datasets_and_another_seed_does_not() -> None:
    base = {"as_of": AS_OF, "customers": 20, "orders": 50, "batches": 3, "batch_interval": "1d"}
    assert build_shop(seed=1, **base) == build_shop(seed=1, **base)  # type: ignore[arg-type]
    assert build_shop(seed=1, **base) != build_shop(seed=2, **base)  # type: ignore[arg-type]
