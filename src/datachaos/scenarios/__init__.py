"""Registry of scenarios, by name."""

from __future__ import annotations

from datachaos.scenarios.base import Scenario
from datachaos.scenarios.duplicate_business_key import DuplicateBusinessKey

SCENARIOS: dict[str, Scenario] = {
    scenario.spec.name: scenario for scenario in (DuplicateBusinessKey(),)
}
