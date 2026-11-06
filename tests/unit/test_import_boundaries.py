"""The layers from the build plan, section 4, checked on the imports of each module."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src" / "datachaos"

# First part below `datachaos` that a module in each layer may import.
ALLOWED = {
    "model": {"model"},
    "datasets": {"model", "rng", "timeutil", "errors", "datasets"},
    "scenarios": {"model", "rng", "timeutil", "errors", "scenarios"},
    "io": {"model", "errors", "io", "timeutil"},
    "rng": set(),
    "timeutil": set(),
    "errors": set(),
    "config": {"errors", "timeutil"},
    "cli": {"_version", "config", "engine", "errors", "model", "validate", "dbtcheck", "land"},
}


def _layer(path: Path) -> str:
    relative = path.relative_to(SRC)
    return relative.parts[0] if len(relative.parts) > 1 else relative.stem


def _internal_imports(path: Path) -> set[str]:
    found = set()
    for node in ast.walk(ast.parse(path.read_bytes())):
        names: list[str] = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, f"{path}: use absolute imports"
            module = node.module or ""
            names = [f"{module}.{alias.name}" for alias in node.names]
        for name in names:
            parts = name.split(".")
            if parts[0] == "datachaos":
                assert len(parts) > 1, f"{path}: bare import of datachaos"
                found.add(parts[1])
    return found


MODULES = [p for p in sorted(SRC.rglob("*.py")) if _layer(p) in ALLOWED]


@pytest.mark.parametrize("path", MODULES, ids=lambda p: str(p.relative_to(SRC)))
def test_layer_imports_only_what_it_may(path: Path) -> None:
    extra = _internal_imports(path) - ALLOWED[_layer(path)]
    assert not extra, f"{path} imports {sorted(extra)}"


def test_the_checks_cover_every_layer_that_exists() -> None:
    assert {"model", "datasets", "io", "rng", "timeutil", "errors", "config", "cli"} <= {
        _layer(path) for path in MODULES
    }
