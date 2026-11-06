"""Shared by the scenario tests: run scenarios on a small dataset, without writing anything."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace

from datachaos import engine
from datachaos.config import DatasetConfig, RunConfig, ScenarioConfig
from datachaos.model.dataset import Dataset
from datachaos.model.ledger import LedgerEntry
from datachaos.model.table import Row
from datachaos.rng import Rng, derive_seed
from datachaos.scenarios import SCENARIOS
from datachaos.scenarios.base import ScenarioContext, ScenarioOutcome, resolve_params, with_rows

SMALL = DatasetConfig(as_of="2026-01-08T00:00:00Z", customers=20, orders=100, batches=3)


def config(*scenarios: ScenarioConfig, settings: DatasetConfig = SMALL) -> RunConfig:
    return RunConfig(seed=5, dataset=settings, scenarios=scenarios)


@dataclass(frozen=True)
class Applied:
    baseline: Dataset
    dataset: Dataset
    ledger: tuple[LedgerEntry, ...]
    records: tuple[dict[str, object], ...]
    warnings: tuple[str, ...]


def apply_all(*scenarios: ScenarioConfig, settings: DatasetConfig = SMALL) -> Applied:
    run_config = config(*scenarios, settings=settings)
    baseline = engine._build_dataset(run_config)
    planned = engine.resolve_scenarios(run_config, baseline)
    applied = engine._apply_scenarios(run_config, baseline, planned)
    return Applied(baseline, applied.dataset, applied.ledger, applied.records, applied.warnings)


def context(
    name: str, table: str, params: Mapping[str, object], dataset: Dataset
) -> ScenarioContext:
    """A context for calling one scenario directly on a hand-made dataset."""
    schema = dataset.tables[table].schema_by_batch[-1]
    scenario = SCENARIOS[name]
    resolved = dict(resolve_params(scenario.spec, schema, params))
    return ScenarioContext(
        dataset=dataset,
        table=table,
        params=resolved,
        rng=Rng(derive_seed(5, name)),
        index=0,
        eligible=lambda row: True,
    )


def set_values(dataset: Dataset, table: str, rid: str, **values: object) -> Dataset:
    rows = [
        replace(row, values={**row.values, **values}) if row.rid == rid else row  # type: ignore[arg-type]
        for row in dataset.tables[table].rows
    ]
    return with_rows(dataset, table, rows)


def by_rid(dataset: Dataset, table: str) -> dict[str, Row]:
    return {row.rid: row for row in dataset.tables[table].rows}


def tamper(outcome: ScenarioOutcome, table: str, rid: str, **values: object) -> ScenarioOutcome:
    """The same outcome with one row changed, to show that `check` notices."""
    return replace(outcome, dataset=set_values(outcome.dataset, table, rid, **values))
