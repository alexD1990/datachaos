"""Registry of scenarios, by name."""

from __future__ import annotations

from datachaos.scenarios.base import Scenario

SCENARIOS: dict[str, Scenario] = {}
