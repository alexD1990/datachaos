"""Byte-exact output for small, fixed configs. Update with DATACHAOS_UPDATE_GOLDEN=1."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from datachaos import engine
from datachaos.config import DatasetConfig, OutputConfig, RunConfig

GOLDEN = Path(__file__).resolve().parent
CREATED_AT = "2000-01-01T00:00:00Z"
CASES = {
    "baseline_small": RunConfig(
        seed=20260101,
        dataset=DatasetConfig(
            as_of="2026-01-08T00:00:00Z", customers=20, orders=50, batches=3, batch_interval="1d"
        ),
        output=OutputConfig(formats=("csv", "jsonl")),
    ),
}


def _tree(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


@pytest.mark.parametrize("case", sorted(CASES))
def test_output_matches_golden_files(case: str, tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.generate(CASES[case], out, created_at=CREATED_AT)
    expected = GOLDEN / case
    if os.environ.get("DATACHAOS_UPDATE_GOLDEN") == "1":
        if expected.exists():
            shutil.rmtree(expected)
        shutil.copytree(out, expected)
    actual_files = _tree(out)
    expected_files = _tree(expected)
    assert sorted(actual_files) == sorted(expected_files)
    for path, content in expected_files.items():
        assert actual_files[path] == content, path
