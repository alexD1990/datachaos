from __future__ import annotations

import re
from collections.abc import Mapping

import pytest

from datachaos.errors import DataChaosError, FieldError
from datachaos.model.schema import Column, ColumnType, Role, TableSchema
from datachaos.model.table import Row
from datachaos.rng import Rng
from datachaos.scenarios.base import (
    COMMON_PARAMS,
    Group,
    ParamSpec,
    ScenarioSpec,
    groups_of,
    new_rid,
    other_value,
    resolve_params,
    select_groups,
    select_rows,
    type_name,
)

SCHEMA = TableSchema(
    "t",
    (
        Column("id", ColumnType.STRING, False, Role.KEY),
        Column("at", ColumnType.TIMESTAMP, False, Role.INGESTION_TIME),
    ),
    "entity",
)


def _check_column(value: object, schema: TableSchema) -> str | None:
    names = [column.name for column in schema.columns]
    return None if value in names else f"no column {value!r}"


SPEC = ScenarioSpec(
    name="probe",
    version=1,
    summary="",
    detects="",
    typical_dbt_test="",
    applies_to=frozenset({"entity"}),
    params=(
        *COMMON_PARAMS,
        ParamSpec("column", str, True, validate=_check_column),
        ParamSpec("names", list, False, default=("a", "b")),
    ),
)


def _rows(count: int) -> list[Row]:
    return [Row(f"r{number:03d}", {"id": str(number)}) for number in range(count)]


def _all(row: Row) -> bool:
    return True


def _resolve(params: Mapping[str, object]) -> dict[str, object]:
    return resolve_params(SPEC, SCHEMA, params)


@pytest.mark.parametrize(
    ("rate", "candidates", "expected"),
    [(0.5, 3, 2), (0.5, 5, 3), (0.1, 5, 1), (0.1, 4, 0), (1, 7, 7), (0.25, 10, 3)],
)
def test_rate_rounds_half_up(rate: float, candidates: int, expected: int) -> None:
    if expected == 0:
        with pytest.raises(DataChaosError, match="selects no rows; use count"):
            select_rows(_rows(candidates), Rng(1), rate=rate, eligible=_all)
    else:
        assert len(select_rows(_rows(candidates), Rng(1), rate=rate, eligible=_all)) == expected


def test_count_selects_exactly_and_refuses_too_many() -> None:
    assert len(select_rows(_rows(5), Rng(1), count=5, eligible=_all)) == 5
    with pytest.raises(DataChaosError, match="count 6 is more than the 5 candidate rows"):
        select_rows(_rows(5), Rng(1), count=6, eligible=_all)


@pytest.mark.parametrize("kwargs", [{}, {"rate": 0.5, "count": 1}])
def test_exactly_one_of_rate_and_count(kwargs: dict[str, float]) -> None:
    with pytest.raises(DataChaosError, match="exactly one of rate and count"):
        select_rows(_rows(5), Rng(1), eligible=_all, **kwargs)  # type: ignore[arg-type]


def test_selection_ignores_input_order_and_is_sorted_on_rid() -> None:
    rows = _rows(50)
    first = select_rows(rows, Rng(3), count=10, eligible=_all)
    second = select_rows(list(reversed(rows)), Rng(3), count=10, eligible=_all)
    assert first == second
    assert [row.rid for row in first] == sorted(row.rid for row in first)


def test_selection_respects_eligible() -> None:
    rows = _rows(10)
    picked = select_rows(rows, Rng(1), count=5, eligible=lambda row: row.rid != "r000")
    assert all(row.rid != "r000" for row in picked)
    with pytest.raises(DataChaosError, match="count 10 is more than the 9"):
        select_rows(rows, Rng(1), count=10, eligible=lambda row: row.rid != "r000")


def test_resolve_fills_defaults_in_spec_order() -> None:
    assert _resolve({"column": "id", "rate": 0.5}) == {
        "rate": 0.5,
        "allow_overlap": False,
        "column": "id",
        "names": ["a", "b"],
    }


def test_resolve_is_idempotent() -> None:
    once = _resolve({"column": "id", "count": 2})
    assert _resolve(once) == once


@pytest.mark.parametrize(
    ("params", "field", "message"),
    [
        ({"column": "id", "rate": 0.5, "colum": "x"}, "params.colum", "did you mean column?"),
        ({"rate": 0.5}, "params.column", "is required"),
        ({"column": "id", "rate": "many"}, "params.rate", "must be float or int, got 'many'"),
        ({"column": "id", "rate": True}, "params.rate", "must be float or int, got True"),
        ({"column": "id", "count": True}, "params.count", "must be int, got True"),
        ({"column": "id", "count": 1.5}, "params.count", "must be int, got 1.5"),
        ({"column": "id", "rate": 0}, "params.rate", "above 0"),
        ({"column": "id", "rate": 1.5}, "params.rate", "at most 1"),
        ({"column": "id", "count": 0}, "params.count", "at least 1"),
        ({"column": "id", "rate": 0.5, "allow_overlap": 1}, "params.allow_overlap", "must be bool"),
        ({"column": "nope", "rate": 0.5}, "params.column", "no column 'nope'"),
        ({"column": "id"}, "params", "exactly one of rate and count"),
        ({"column": "id", "rate": 0.5, "count": 1}, "params", "exactly one of rate and count"),
    ],
)
def test_resolve_errors_name_the_parameter(
    params: dict[str, object], field: str, message: str
) -> None:
    with pytest.raises(FieldError) as info:
        _resolve(params)
    assert info.value.field == field
    assert message in info.value.message


def test_new_rid_has_fixed_width_and_sorts_numerically() -> None:
    assert new_rid(0, 1) == "s0000-0000001"
    rids = [new_rid(2, 9), new_rid(2, 10), new_rid(10, 1)]
    assert rids == sorted(rids)
    assert len({len(rid) for rid in rids}) == 1


def _members() -> list[Row]:
    return [
        Row("r3", {"g": "b"}),
        Row("r1", {"g": "a"}),
        Row("r2", {"g": "a"}),
        Row("r4", {"g": None}),
        Row("r5", {"g": "c"}),
    ]


def test_groups_are_sorted_and_null_belongs_to_no_group() -> None:
    assert groups_of(_members(), ["g"]) == [
        Group({"g": "a"}, (Row("r1", {"g": "a"}), Row("r2", {"g": "a"}))),
        Group({"g": "b"}, (Row("r3", {"g": "b"}),)),
        Group({"g": "c"}, (Row("r5", {"g": "c"}),)),
    ]


def test_select_groups_counts_groups_not_rows() -> None:
    picked = select_groups(_members(), ["g"], {"count": 2}, Rng(1), eligible=_all)
    assert len(picked) == 2
    assert len(select_groups(_members(), ["g"], {"rate": 1.0}, Rng(1), eligible=_all)) == 3


def test_a_group_with_an_ineligible_row_is_no_candidate() -> None:
    picked = select_groups(
        _members(), ["g"], {"rate": 1.0}, Rng(1), eligible=lambda row: row.rid != "r2"
    )
    assert [group.key["g"] for group in picked] == ["b", "c"]


def test_select_groups_applies_the_candidate_rule() -> None:
    picked = select_groups(
        _members(),
        ["g"],
        {"rate": 1.0},
        Rng(1),
        eligible=_all,
        candidate=lambda group: len(group.rows) >= 2,
    )
    assert [group.key["g"] for group in picked] == ["a"]


def test_select_groups_reports_groups_in_its_errors() -> None:
    with pytest.raises(DataChaosError, match="count 4 is more than the 3 candidate groups"):
        select_groups(_members(), ["g"], {"count": 4}, Rng(1), eligible=_all)


@pytest.mark.parametrize(
    ("column", "value", "check"),
    [
        (
            Column("e", ColumnType.STRING, False, Role.ENUM, allowed_values=("a", "b")),
            "a",
            lambda v: v == "b",
        ),
        (Column("b", ColumnType.BOOLEAN, False), True, lambda v: v is False),
        (Column("i", ColumnType.INTEGER, False), 5, lambda v: v > 5),
        (Column("d", ColumnType.DECIMAL, False, scale=2), "0.05", lambda v: float(v) > 0.05),
        (Column("s", ColumnType.STRING, False), "x", lambda v: v == "x-changed"),
    ],
)
def test_other_value_differs_and_stays_valid(column, value, check) -> None:  # type: ignore[no-untyped-def]
    assert check(other_value(column, value, Rng(1)))


def test_other_value_keeps_the_scale_of_a_decimal() -> None:
    column = Column("d", ColumnType.DECIMAL, False, scale=2)
    assert re.fullmatch(r"[0-9]+\.[0-9]{2}", str(other_value(column, "0.05", Rng(1))))


def test_type_name() -> None:
    assert type_name(str) == "str"
    assert type_name((float, int)) == "float or int"
