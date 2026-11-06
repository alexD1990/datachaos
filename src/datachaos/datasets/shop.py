"""The built-in `shop` dataset: customers, orders and order_events.

The baseline is clean: every natural unique, not_null, relationships and accepted_values test
holds, also over the batches landed so far. No row holds information from after its own
ingested_at, so orders are their state at creation and later statuses exist only as events.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass

from datachaos.model.dataset import Dataset
from datachaos.model.schema import Column, ColumnType, Role, TableSchema
from datachaos.model.table import Row, Table, sorted_rows
from datachaos.rng import Rng, derive_seed
from datachaos.timeutil import format_timestamp, parse_duration, parse_timestamp, window_start

GENERATOR_VERSION = 2

_INGEST_DELAY_MAX = 1800
# Longer than the largest ingest delay, so ingested_at follows event_time within an order.
_EVENT_GAP_MIN = _INGEST_DELAY_MAX + 60
_EVENT_GAP_MAX = 6 * 3600

_COUNTRIES = ("NO", "SE", "DK", "FI", "DE")
_COUNTRY_WEIGHTS = (40, 25, 15, 10, 10)
_CURRENCY_BY_COUNTRY = {"NO": "NOK", "SE": "SEK", "DK": "DKK", "FI": "EUR", "DE": "EUR"}
_CURRENCIES = ("NOK", "SEK", "DKK", "EUR")
_SEGMENTS = ("consumer", "smb", "enterprise")
_SEGMENT_WEIGHTS = (70, 20, 10)
_SOURCES = ("webshop", "pos", "b2b_api")
_SOURCE_WEIGHTS = (70, 20, 10)
_STATUSES = ("created", "paid", "shipped", "delivered", "cancelled")

_AMOUNT_MIN_CENTS = 1_000
_AMOUNT_MAX_CENTS = 5_000_000

_LIFECYCLES = (
    ("created", "paid"),
    ("created", "paid", "shipped"),
    ("created", "paid", "shipped", "delivered"),
    ("created", "cancelled"),
    ("created", "paid", "cancelled"),
    ("created", "paid", "shipped", "delivered", "cancelled"),
)
_LIFECYCLE_WEIGHTS = (10, 15, 50, 10, 10, 5)

CUSTOMERS_SCHEMA = TableSchema(
    name="customers",
    kind="entity",
    columns=(
        Column("customer_id", ColumnType.STRING, False, Role.KEY),
        Column("created_at", ColumnType.TIMESTAMP, False, Role.EVENT_TIME),
        Column("updated_at", ColumnType.TIMESTAMP, False, Role.UPDATED_AT),
        Column("country_code", ColumnType.STRING, False, Role.ENUM, allowed_values=_COUNTRIES),
        Column("customer_segment", ColumnType.STRING, False, Role.ENUM, allowed_values=_SEGMENTS),
        Column("is_active", ColumnType.BOOLEAN, False),
        Column("ingested_at", ColumnType.TIMESTAMP, False, Role.INGESTION_TIME),
    ),
)

ORDERS_SCHEMA = TableSchema(
    name="orders",
    kind="entity",
    columns=(
        Column("order_id", ColumnType.STRING, False, Role.KEY),
        Column(
            "customer_id",
            ColumnType.STRING,
            False,
            Role.FOREIGN_KEY,
            references="customers.customer_id",
        ),
        Column("order_timestamp", ColumnType.TIMESTAMP, False, Role.EVENT_TIME),
        Column("updated_at", ColumnType.TIMESTAMP, False, Role.UPDATED_AT),
        Column("status", ColumnType.STRING, False, Role.ENUM, allowed_values=_STATUSES),
        Column("amount", ColumnType.DECIMAL, False, scale=2),
        Column("currency", ColumnType.STRING, False, Role.ENUM, allowed_values=_CURRENCIES),
        Column("source_system", ColumnType.STRING, False, Role.ENUM, allowed_values=_SOURCES),
        Column("ingested_at", ColumnType.TIMESTAMP, False, Role.INGESTION_TIME),
    ),
)

ORDER_EVENTS_SCHEMA = TableSchema(
    name="order_events",
    kind="event_log",
    columns=(
        Column("event_id", ColumnType.STRING, False, Role.KEY),
        Column(
            "order_id", ColumnType.STRING, False, Role.FOREIGN_KEY, references="orders.order_id"
        ),
        Column("event_type", ColumnType.STRING, False, Role.ENUM, allowed_values=_STATUSES),
        Column("event_time", ColumnType.TIMESTAMP, False, Role.EVENT_TIME),
        Column("ingested_at", ColumnType.TIMESTAMP, False, Role.INGESTION_TIME),
    ),
)


@dataclass(frozen=True)
class _Customer:
    customer_id: str
    created_at: int
    ingested_at: int
    country: str


@dataclass(frozen=True)
class _Order:
    order_id: str
    order_timestamp: int
    ingested_at: int


def build_shop(
    *, seed: int, as_of: str, customers: int, orders: int, batches: int, batch_interval: str
) -> Dataset:
    as_of_seconds = parse_timestamp(as_of)
    start = window_start(as_of_seconds, batches, parse_duration(batch_interval))
    # An event at this time or earlier is ingested before as_of whatever its delay.
    latest = as_of_seconds - _INGEST_DELAY_MAX - 1

    customer_rows, parents = _customers(
        Rng(derive_seed(seed, "dataset:customers")), customers, start, latest
    )
    order_rows, placed = _orders(
        Rng(derive_seed(seed, "dataset:orders")), orders, parents, start, latest
    )
    event_rows = _events(Rng(derive_seed(seed, "dataset:order_events")), placed, as_of_seconds)

    return Dataset(
        name="shop",
        generator_version=GENERATOR_VERSION,
        as_of=as_of,
        batches=batches,
        batch_interval=batch_interval,
        tables={
            "customers": _table(CUSTOMERS_SCHEMA, batches, customer_rows),
            "orders": _table(ORDERS_SCHEMA, batches, order_rows),
            "order_events": _table(ORDER_EVENTS_SCHEMA, batches, event_rows),
        },
    )


def _customers(rng: Rng, count: int, start: int, latest: int) -> tuple[list[Row], list[_Customer]]:
    rows = []
    customers = []
    for number in range(1, count + 1):
        # The first customer exists from the start, so every order has a customer to choose.
        created_at = start if number == 1 else rng.between(start, latest)
        ingested_at = created_at + rng.between(0, _INGEST_DELAY_MAX)
        country = rng.weighted_choice(_COUNTRIES, _COUNTRY_WEIGHTS)
        customer = _Customer(f"C{number:06d}", created_at, ingested_at, country)
        customers.append(customer)
        rows.append(
            Row(
                rid=f"c{number:06d}",
                values={
                    "customer_id": customer.customer_id,
                    "created_at": format_timestamp(created_at),
                    "updated_at": format_timestamp(created_at),
                    "country_code": country,
                    "customer_segment": rng.weighted_choice(_SEGMENTS, _SEGMENT_WEIGHTS),
                    "is_active": rng.weighted_choice((True, False), (90, 10)),
                    "ingested_at": format_timestamp(ingested_at),
                },
            )
        )
    return rows, customers


def _orders(
    rng: Rng, count: int, customers: list[_Customer], start: int, latest: int
) -> tuple[list[Row], list[_Order]]:
    rows = []
    orders = []
    by_age = sorted(customers, key=lambda customer: (customer.created_at, customer.customer_id))
    created = [customer.created_at for customer in by_age]
    for number in range(1, count + 1):
        order_timestamp = rng.between(start, latest)
        customer = by_age[rng.below(bisect_right(created, order_timestamp))]
        # Within the usual delay, but never ahead of the customer it refers to.
        ingested_at = rng.between(
            max(order_timestamp, customer.ingested_at), order_timestamp + _INGEST_DELAY_MAX
        )
        order = _Order(f"O{number:06d}", order_timestamp, ingested_at)
        orders.append(order)
        rows.append(
            Row(
                rid=f"o{number:06d}",
                values={
                    "order_id": order.order_id,
                    "customer_id": customer.customer_id,
                    "order_timestamp": format_timestamp(order_timestamp),
                    "updated_at": format_timestamp(order_timestamp),
                    "status": "created",
                    "amount": _format_cents(rng.between(_AMOUNT_MIN_CENTS, _AMOUNT_MAX_CENTS)),
                    "currency": _CURRENCY_BY_COUNTRY[customer.country],
                    "source_system": rng.weighted_choice(_SOURCES, _SOURCE_WEIGHTS),
                    "ingested_at": format_timestamp(ingested_at),
                },
            )
        )
    return rows, orders


def _events(rng: Rng, orders: list[_Order], as_of: int) -> list[Row]:
    rows: list[Row] = []
    for order in orders:
        lifecycle = rng.weighted_choice(_LIFECYCLES, _LIFECYCLE_WEIGHTS)
        event_time = order.order_timestamp
        # The first event is never ahead of its order, and is always ingested before as_of.
        ingested_at = rng.between(order.ingested_at, event_time + _INGEST_DELAY_MAX)
        timeline = [(lifecycle[0], event_time, ingested_at)]
        # Every draw is made before cutting, so a cut does not shift the orders after this one.
        for event_type in lifecycle[1:]:
            event_time += rng.between(_EVENT_GAP_MIN, _EVENT_GAP_MAX)
            timeline.append(
                (event_type, event_time, event_time + rng.between(0, _INGEST_DELAY_MAX))
            )
        for event_type, event_time, ingested_at in timeline:
            if ingested_at >= as_of:
                break
            rid, event_id = _event_ids(len(rows) + 1)
            rows.append(
                Row(
                    rid=rid,
                    values={
                        "event_id": event_id,
                        "order_id": order.order_id,
                        "event_type": event_type,
                        "event_time": format_timestamp(event_time),
                        "ingested_at": format_timestamp(ingested_at),
                    },
                )
            )
    return rows


def _event_ids(number: int) -> tuple[str, str]:
    """The rid and event_id. Seven digits hold 5 events for each of the 899 999 possible orders."""
    return f"e{number:07d}", f"E{number:07d}"


def _table(schema: TableSchema, batches: int, rows: list[Row]) -> Table:
    return Table(schema_by_batch=(schema,) * batches, rows=sorted_rows(schema, rows))


def _format_cents(cents: int) -> str:
    return f"{cents // 100}.{cents % 100:02d}"
