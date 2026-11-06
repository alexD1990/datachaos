from __future__ import annotations

import pytest

from datachaos.rng import Rng, derive_seed

# Hardcoded on purpose: a change here breaks the determinism contract. CI runs these on every
# supported Python version.


def test_derive_seed_is_a_known_value() -> None:
    assert derive_seed(42, "dataset:customers") == 1221815275088747662


def test_derive_seed_differs_by_seed_and_purpose() -> None:
    seeds = {derive_seed(1, "a"), derive_seed(2, "a"), derive_seed(1, "b")}
    assert len(seeds) == 3


def test_random_sequence() -> None:
    rng = Rng(42)
    assert [rng.random() for _ in range(3)] == [
        0.6394267984578837,
        0.025010755222666936,
        0.27502931836911926,
    ]


def test_below_sequence() -> None:
    rng = Rng(42)
    assert [rng.below(100) for _ in range(8)] == [63, 2, 27, 22, 73, 67, 89, 8]


def test_between_sequence_is_inclusive() -> None:
    rng = Rng(42)
    assert [rng.between(5, 9) for _ in range(8)] == [8, 5, 6, 6, 8, 8, 9, 5]
    assert Rng(1).between(3, 3) == 3


def test_choice_sequence() -> None:
    rng = Rng(42)
    assert [rng.choice("abcde") for _ in range(8)] == ["d", "a", "b", "b", "d", "d", "e", "a"]


def test_weighted_choice_sequence_skips_zero_weight() -> None:
    rng = Rng(42)
    assert [rng.weighted_choice("abc", (1, 0, 3)) for _ in range(8)] == [
        "c",
        "a",
        "c",
        "a",
        "c",
        "c",
        "c",
        "a",
    ]


def test_sample_indices_are_known_distinct_and_sorted() -> None:
    assert Rng(42).sample_indices(20, 5) == [1, 2, 6, 12, 15]
    assert Rng(42).sample_indices(5, 5) == [0, 1, 2, 3, 4]
    assert Rng(42).sample_indices(5, 0) == []


def test_shuffle_sequence() -> None:
    items = list(range(8))
    Rng(42).shuffle(items)
    assert items == [6, 4, 3, 2, 7, 1, 0, 5]


@pytest.mark.parametrize(
    "call",
    [
        lambda rng: rng.below(0),
        lambda rng: rng.between(2, 1),
        lambda rng: rng.choice([]),
        lambda rng: rng.weighted_choice("ab", (1,)),
        lambda rng: rng.weighted_choice("ab", (0, 0)),
        lambda rng: rng.weighted_choice("ab", (2, -1)),
        lambda rng: rng.sample_indices(2, 3),
    ],
)
def test_invalid_arguments_raise(call) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(ValueError):
        call(Rng(1))
