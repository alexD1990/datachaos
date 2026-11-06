"""A named collection of tables and how they are divided into batches."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from datachaos.model.table import Table


@dataclass(frozen=True)
class Dataset:
    name: str
    generator_version: int
    as_of: str
    batches: int
    batch_interval: str
    tables: Mapping[str, Table]
