"""Deterministic randomness, stable across Python versions and operating systems.

Python only guarantees the sequence from `random.Random.random()` across versions, so everything
here is built on that method alone.
"""

from __future__ import annotations

import hashlib
import random
from collections.abc import Sequence
from typing import TypeVar

T = TypeVar("T")


def derive_seed(global_seed: int, purpose: str) -> int:
    digest = hashlib.sha256(f"datachaos:{global_seed}:{purpose}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


class Rng:
    def __init__(self, seed: int) -> None:
        self._r = random.Random(seed)

    def random(self) -> float:
        return self._r.random()

    def below(self, n: int) -> int:
        if n <= 0:
            raise ValueError(f"n must be positive, got {n}")
        return int(self._r.random() * n)

    def between(self, lo: int, hi: int) -> int:
        """An integer in [lo, hi], both inclusive."""
        if hi < lo:
            raise ValueError(f"empty range: {lo}..{hi}")
        return lo + self.below(hi - lo + 1)

    def choice(self, seq: Sequence[T]) -> T:
        return seq[self.below(len(seq))]

    def weighted_choice(self, items: Sequence[T], weights: Sequence[int]) -> T:
        if len(items) != len(weights):
            raise ValueError("items and weights must have the same length")
        if any(weight < 0 for weight in weights):
            raise ValueError("weights must not be negative")
        target = self.below(sum(weights))
        cumulative = 0
        for item, weight in zip(items, weights, strict=True):
            cumulative += weight
            if target < cumulative:
                return item
        raise AssertionError("target is below the sum of the weights")

    def sample_indices(self, n: int, k: int) -> list[int]:
        """k distinct indices from range(n), sorted, by a partial Fisher-Yates shuffle."""
        if not 0 <= k <= n:
            raise ValueError(f"cannot sample {k} indices from {n}")
        swapped: dict[int, int] = {}
        picked = []
        for i in range(k):
            j = i + self.below(n - i)
            picked.append(swapped.get(j, j))
            swapped[j] = swapped.get(i, i)
        return sorted(picked)

    def shuffle(self, items: list[T]) -> None:
        for i in range(len(items) - 1, 0, -1):
            j = self.below(i + 1)
            items[i], items[j] = items[j], items[i]
