"""One line per change in `chaos-ledger.jsonl`."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from datachaos.model.table import Value

LedgerAction = Literal[
    "insert_row", "delete_row", "set_value", "move_batch", "orphan_row", "schema_change"
]


@dataclass(frozen=True)
class LedgerEntry:
    scenario_index: int
    scenario: str
    table: str
    action: LedgerAction
    rid: str
    key: Mapping[str, Value]
    batch: int
    column: str | None = None
    before: Value = None
    after: Value = None
    source_rid: str | None = None
    # The group a scenario worked on. Later scenarios stay away from all of its rows.
    group: Mapping[str, Value] | None = None

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "scenario_index": self.scenario_index,
            "scenario": self.scenario,
            "table": self.table,
            "action": self.action,
            "rid": self.rid,
        }
        if self.source_rid is not None:
            result["source_rid"] = self.source_rid
        result["key"] = dict(self.key)
        if self.group is not None:
            result["group"] = dict(self.group)
        result.update(
            batch=self.batch,
            column=self.column,
            before=self.before,
            after=self.after,
        )
        return result
