"""Registry of scenarios, by name."""

from __future__ import annotations

from datachaos.scenarios.base import Scenario
from datachaos.scenarios.duplicate_business_key import DuplicateBusinessKey
from datachaos.scenarios.invalid_enum import InvalidEnum
from datachaos.scenarios.null_critical_column import NullCriticalColumn
from datachaos.scenarios.orphaned_foreign_key import OrphanedForeignKey

SCENARIOS: dict[str, Scenario] = {
    scenario.spec.name: scenario
    for scenario in (
        DuplicateBusinessKey(),
        NullCriticalColumn(),
        InvalidEnum(),
        OrphanedForeignKey(),
    )
}
