"""Registry of built-in datasets."""

from __future__ import annotations

from collections.abc import Callable

from datachaos.datasets.shop import build_shop
from datachaos.model.dataset import Dataset

DATASETS: dict[str, Callable[..., Dataset]] = {"shop": build_shop}
