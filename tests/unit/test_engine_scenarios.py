"""The engine's handling of scenarios, with fake scenarios that show what it guarantees."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path

import pytest

from datachaos import engine
from datachaos.config import DatasetConfig, RunConfig, ScenarioConfig
from datachaos.errors import DataChaosError, FieldError
from datachaos.io.reader import read_json
from datachaos.model.ledger import LedgerEntry
from datachaos.model.schema import TableSchema
from datachaos.model.table import Row
from datachaos.scenarios import SCENARIOS
from datachaos.scenarios.base import (
    COMMON_PARAMS,
    BaseScenario,
    ParamSpec,
    ResolveContext,
    ResolvedParams,
    ScenarioContext,
    ScenarioOutcome,
    ScenarioSpec,
    ScenarioStats,
    SchemaState,
    batch_of,
    key_of,
    select,
    with_rows,
)

SMALL = DatasetConfig(customers=20, orders=50, batches=3)


def _spec(name: str, *params: ParamSpec) -> ScenarioSpec:
    return ScenarioSpec(
        name=name,
        version=1,
        summary="",
        detects="",
        typical_dbt_test="",
        applies_to=frozenset({"entity"}),
        params=(*COMMON_PARAMS, *params),
    )


class Touch(BaseScenario):
    """Sets source_system to "touched" on the selected rows."""

    spec = _spec("touch")

    def apply(self, ctx: ScenarioContext) -> ScenarioOutcome:
        table = ctx.dataset.tables[ctx.table]
        schema = table.schema_by_batch[0]
        picked = {row.rid: row for row in select(ctx)}
        rows = [
            replace(row, values={**row.values, "source_system": "touched"})
            if row.rid in picked
            else row
            for row in table.rows
        ]
        ledger = tuple(
            LedgerEntry(
                scenario_index=ctx.index,
                scenario=self.spec.name,
                table=ctx.table,
                action="set_value",
                rid=row.rid,
                key=key_of(row, schema),
                batch=batch_of(ctx.dataset, table, row),
                column="source_system",
                before=row.values["source_system"],
                after="touched",
            )
            for row in picked.values()
        )
        stats = ScenarioStats(len(rows), len(rows), len(picked), 0, len(picked))
        return ScenarioOutcome(with_rows(ctx.dataset, ctx.table, rows), ledger, stats)

    def check(self, ctx: ScenarioContext, outcome: ScenarioOutcome) -> list[str]:
        return []


class Rename(Touch):
    """A stand-in for schema-drift: renames a column, in the schema state and in the data."""

    spec = _spec("rename", ParamSpec("column", str, True), ParamSpec("new_name", str, True))

    def schemas_after(self, ctx: ResolveContext, table: str, params: ResolvedParams) -> SchemaState:
        return {**ctx.schemas, table: self._renamed(ctx.schemas[table], params)}

    def _renamed(
        self, schemas: tuple[TableSchema, ...], params: ResolvedParams
    ) -> tuple[TableSchema, ...]:
        return tuple(
            replace(
                schema,
                columns=tuple(
                    replace(column, name=str(params["new_name"]))
                    if column.name == params["column"]
                    else column
                    for column in schema.columns
                ),
            )
            for schema in schemas
        )

    def apply(self, ctx: ScenarioContext) -> ScenarioOutcome:
        table = ctx.dataset.tables[ctx.table]
        old, new = str(ctx.params["column"]), str(ctx.params["new_name"])
        renamed = replace(
            table,
            schema_by_batch=self._renamed(table.schema_by_batch, ctx.params),
            rows=tuple(
                Row(row.rid, {(new if k == old else k): v for k, v in row.values.items()})
                for row in table.rows
            ),
        )
        dataset = replace(ctx.dataset, tables={**ctx.dataset.tables, ctx.table: renamed})
        return ScenarioOutcome(dataset, (), ScenarioStats(0, 0, 0, 0, 0))


class NeedsColumn(Touch):
    """Validates its column against the schema state it is given, as real scenarios do."""

    spec = _spec("needs-column", ParamSpec("column", str, True))

    def resolve_more(
        self,
        ctx: ResolveContext,
        table: str,
        resolved: dict[str, object],
        given: Mapping[str, object],
    ) -> None:
        names = [column.name for column in ctx.schema_of(table).columns]
        if resolved["column"] not in names:
            raise FieldError("params.column", f"no column {resolved['column']!r}")


class Broken(Touch):
    spec = _spec("broken")

    def check(self, ctx: ScenarioContext, outcome: ScenarioOutcome) -> list[str]:
        return ["the invariant does not hold"]


class Selfish(Touch):
    spec = _spec("selfish")

    def apply(self, ctx: ScenarioContext) -> ScenarioOutcome:
        raise DataChaosError("cannot find rows")


@pytest.fixture(autouse=True)
def _registry(monkeypatch: pytest.MonkeyPatch) -> None:
    for scenario in (Touch(), Rename(), NeedsColumn(), Broken(), Selfish()):
        monkeypatch.setitem(SCENARIOS, scenario.spec.name, scenario)


def _config(*scenarios: ScenarioConfig) -> RunConfig:
    return RunConfig(seed=5, dataset=SMALL, scenarios=scenarios)


def _touch(**params: object) -> ScenarioConfig:
    return ScenarioConfig("touch", "orders", params)


def _ledger(out: Path) -> bytes:
    return (out / "chaos-ledger.jsonl").read_bytes()


def test_resolve_sees_the_schema_state_after_earlier_scenarios() -> None:
    rename = ScenarioConfig(
        "rename", "orders", {"column": "amount", "new_name": "total", "count": 1}
    )
    good = _config(
        rename, ScenarioConfig("needs-column", "orders", {"column": "total", "count": 1})
    )
    bad = _config(
        rename, ScenarioConfig("needs-column", "orders", {"column": "amount", "count": 1})
    )
    baseline = engine._build_dataset(good)
    assert [item.scenario.spec.name for item in engine.resolve_scenarios(good, baseline)] == [
        "rename",
        "needs-column",
    ]
    with pytest.raises(DataChaosError, match=r"scenarios\[1\]\.params\.column: no column 'amount'"):
        engine.resolve_scenarios(bad, baseline)


def test_a_scenario_cannot_see_later_schema_changes(tmp_path: Path) -> None:
    early = ScenarioConfig("needs-column", "orders", {"column": "total", "count": 1})
    rename = ScenarioConfig(
        "rename", "orders", {"column": "amount", "new_name": "total", "count": 1}
    )
    with pytest.raises(DataChaosError, match=r"scenarios\[0\]\.params\.column: no column 'total'"):
        engine.run(_config(early, rename), tmp_path / "out")


def test_later_scenarios_are_resolved_before_any_runs(tmp_path: Path) -> None:
    config = _config(ScenarioConfig("broken", "orders", {"count": 1}), _touch(rate=5))
    with pytest.raises(DataChaosError, match=r"scenarios\[1\]\.params\.rate: must be above 0"):
        engine.run(config, tmp_path / "out")
    assert list(tmp_path.iterdir()) == []


def test_baseline_schema_state_reaches_a_scenario_unchanged(tmp_path: Path) -> None:
    config = _config(ScenarioConfig("needs-column", "orders", {"column": "amount", "count": 1}))
    baseline = engine._build_dataset(config)
    assert engine.resolve_scenarios(config, baseline)[0].params["column"] == "amount"


@pytest.mark.parametrize(
    ("scenario", "message"),
    [
        (
            ScenarioConfig("touhc", "orders", {"count": 1}),
            r"scenarios\[0\]\.name: unknown scenario 'touhc' \(did you mean touch\?\)",
        ),
        (
            ScenarioConfig("touch", "ordres", {"count": 1}),
            r"scenarios\[0\]\.table: unknown table 'ordres' \(did you mean orders\?\)",
        ),
        (
            ScenarioConfig("touch", "order_events", {"count": 1}),
            r"scenarios\[0\]\.table: touch applies to entity tables, and order_events is event_log",
        ),
        (
            ScenarioConfig("touch", "orders", {"rte": 1}),
            r"scenarios\[0\]\.params\.rte: unknown parameter \(did you mean rate\?\)",
        ),
    ],
)
def test_invalid_scenarios_name_their_path(
    tmp_path: Path, scenario: ScenarioConfig, message: str
) -> None:
    with pytest.raises(DataChaosError, match=message):
        engine.run(_config(scenario), tmp_path / "out")
    assert list(tmp_path.iterdir()) == []


def test_a_broken_invariant_is_an_internal_error_and_writes_nothing(tmp_path: Path) -> None:
    config = _config(ScenarioConfig("broken", "orders", {"count": 1}))
    with pytest.raises(DataChaosError, match="internal error: scenario broken broke its invariant"):
        engine.run(config, tmp_path / "out")
    assert list(tmp_path.iterdir()) == []


def test_an_error_while_applying_names_the_scenario(tmp_path: Path) -> None:
    config = _config(ScenarioConfig("selfish", "orders", {"count": 1}))
    with pytest.raises(DataChaosError, match=r"scenarios\[0\]: cannot find rows"):
        engine.run(config, tmp_path / "out")
    assert list(tmp_path.iterdir()) == []


def test_manifest_records_the_resolved_scenario(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.run(_config(_touch(count=3)), out)
    manifest = read_json(out / "chaos-manifest.json")
    record = manifest["scenarios"][0]
    assert record["index"] == 0
    assert record["name"] == "touch"
    assert record["version"] == 1
    assert record["table"] == "orders"
    assert record["params"] == {"count": 3, "allow_overlap": False}
    assert record["stats"]["rows_selected"] == 3
    assert record["ledger_entries"] == 3
    assert manifest["run"]["config"]["scenarios"] == [
        {"name": "touch", "table": "orders", "params": {"count": 3, "allow_overlap": False}}
    ]
    assert len(_ledger(out).splitlines()) == 3


def test_running_the_resolved_config_gives_the_same_run(tmp_path: Path) -> None:
    config = _config(_touch(count=3))
    first = engine.run(config, tmp_path / "a")
    again = _config(
        ScenarioConfig("touch", "orders", first.to_dict()["scenarios"][0]["params"])  # type: ignore[index, call-overload]
    )
    second = engine.run(again, tmp_path / "b")
    assert first.run.run_id == second.run.run_id
    assert first.content_sha256 == second.content_sha256


def test_exclusivity_keeps_later_scenarios_away_from_touched_keys(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.run(_config(_touch(count=25), _touch(count=25)), out)
    lines = _ledger(out).splitlines()
    keys = [line.split(b'"key":')[1].split(b"}")[0] for line in lines]
    assert len(keys) == 50
    assert len(set(keys)) == 50


def test_exclusivity_shrinks_the_candidates(tmp_path: Path) -> None:
    config = _config(_touch(count=30), _touch(count=30))
    with pytest.raises(
        DataChaosError, match=r"scenarios\[1\]: count 30 is more than the 20 candidate rows"
    ):
        engine.run(config, tmp_path / "out")


def test_allow_overlap_lets_a_later_scenario_touch_them_again(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.run(_config(_touch(count=30), _touch(count=30, allow_overlap=True)), out)
    assert len(_ledger(out).splitlines()) == 60


def test_exclusivity_is_per_table() -> None:
    config = _config(_touch(count=1))
    baseline = engine._build_dataset(config)
    row = baseline.tables["customers"].rows[0]
    touched = LedgerEntry(
        scenario_index=0,
        scenario="touch",
        table="orders",
        action="set_value",
        rid=row.rid,
        key={"customer_id": row.values["customer_id"]},
        batch=0,
    )
    other = engine._Planned(1, Touch(), "customers", {"allow_overlap": False})
    same = engine._Planned(1, Touch(), "orders", {"allow_overlap": False})
    assert engine._eligibility(baseline, other, [touched])(row)
    assert not engine._eligibility(baseline, same, [touched])(row)


def test_a_touched_row_stays_excluded_after_its_key_has_changed() -> None:
    config = _config(_touch(count=1))
    baseline = engine._build_dataset(config)
    row = baseline.tables["orders"].rows[0]
    touched = LedgerEntry(
        scenario_index=0,
        scenario="touch",
        table="orders",
        action="set_value",
        rid=row.rid,
        key={"order_id": row.values["order_id"]},
        batch=0,
        column="order_id",
        before=row.values["order_id"],
    )
    nulled = replace(row, values={**row.values, "order_id": None})
    planned = engine._Planned(1, Touch(), "orders", {"allow_overlap": False})
    assert not engine._eligibility(baseline, planned, [touched])(nulled)


def test_ledger_is_sorted(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.run(_config(_touch(count=5), _touch(count=5)), out)
    document = read_json(out / "chaos-manifest.json")
    assert document["scenarios"][1]["ledger_entries"] == 5
    entries = [line.decode() for line in _ledger(out).splitlines()]
    prefix = [entry.split('"rid":"')[1].split('"')[0] for entry in entries]
    assert prefix[:5] == sorted(prefix[:5])
    assert prefix[5:] == sorted(prefix[5:])
    assert all('"scenario_index":0' in entry for entry in entries[:5])


def test_later_data_is_unchanged_when_a_scenario_is_added_at_the_end(tmp_path: Path) -> None:
    engine.run(_config(_touch(count=5)), tmp_path / "a")
    engine.run(_config(_touch(count=5), _touch(count=5)), tmp_path / "b")
    first = _ledger(tmp_path / "a").splitlines()
    second = _ledger(tmp_path / "b").splitlines()
    assert second[:5] == first


def test_the_input_dataset_is_not_mutated() -> None:
    config = _config(_touch(count=5))
    baseline = engine._build_dataset(config)
    snapshot = copy.deepcopy(baseline)
    engine._apply_scenarios(config, baseline, engine.resolve_scenarios(config, baseline))
    assert baseline == snapshot


def test_row_counts_in_the_manifest_cover_baseline_and_final(tmp_path: Path) -> None:
    manifest = engine.run(_config(_touch(count=5)), tmp_path / "out")
    assert manifest.dataset.tables["orders"].rows_baseline == 50
    assert manifest.dataset.tables["orders"].rows_final == 50
