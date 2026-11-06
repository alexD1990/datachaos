"""Registry of scenarios, by name."""

from __future__ import annotations

from datachaos.scenarios.base import Scenario
from datachaos.scenarios.duplicate_business_key import DuplicateBusinessKey
from datachaos.scenarios.historical_update import HistoricalUpdate
from datachaos.scenarios.invalid_enum import InvalidEnum
from datachaos.scenarios.late_arriving_data import LateArrivingData
from datachaos.scenarios.null_critical_column import NullCriticalColumn
from datachaos.scenarios.orphaned_foreign_key import OrphanedForeignKey
from datachaos.scenarios.out_of_order_events import OutOfOrderEvents

SCENARIOS: dict[str, Scenario] = {
    scenario.spec.name: scenario
    for scenario in (
        DuplicateBusinessKey(),
        NullCriticalColumn(),
        InvalidEnum(),
        OrphanedForeignKey(),
        LateArrivingData(),
        OutOfOrderEvents(),
        HistoricalUpdate(),
    )
}
