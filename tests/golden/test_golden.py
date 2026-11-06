"""Byte-exact output for small, fixed configs. Update with DATACHAOS_UPDATE_GOLDEN=1."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from datachaos import engine
from datachaos.config import load_config

GOLDEN = Path(__file__).resolve().parent
CREATED_AT = "2000-01-01T00:00:00Z"
CASES = sorted(path.parent.name for path in GOLDEN.glob("*/chaos.yml"))


def _tree(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_the_cases_are_found() -> None:
    assert "baseline_small" in CASES


@pytest.mark.parametrize("case", CASES)
def test_output_matches_golden_files(case: str, tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.run(load_config(GOLDEN / case / "chaos.yml"), out, created_at=CREATED_AT)
    expected = GOLDEN / case / "expected"
    if os.environ.get("DATACHAOS_UPDATE_GOLDEN") == "1":
        if expected.exists():
            shutil.rmtree(expected)
        shutil.copytree(out, expected)
    actual_files = _tree(out)
    expected_files = _tree(expected)
    assert sorted(actual_files) == sorted(expected_files)
    for path, content in expected_files.items():
        assert actual_files[path] == content, path
