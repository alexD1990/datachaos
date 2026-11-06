"""The determinism and file rules from CLAUDE.md, checked on the syntax tree of the code."""

from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SRC = REPO / "src" / "datachaos"


def _python_files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.py"))


def _parse(path: Path) -> ast.Module:
    return ast.parse(path.read_bytes(), filename=str(path))


def _calls(tree: ast.AST) -> Iterator[ast.Call]:
    return (node for node in ast.walk(tree) if isinstance(node, ast.Call))


SOURCES = _python_files(SRC)
ALL_CODE = SOURCES + _python_files(REPO / "tests")


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_random_module_is_only_used_in_rng(path: Path) -> None:
    tree = _parse(path)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert node.module != "random", f"{path}:{node.lineno}"
        if isinstance(node, ast.Import) and path.name != "rng.py":
            assert "random" not in [alias.name for alias in node.names], f"{path}:{node.lineno}"


def test_rng_uses_only_random_random_from_the_module() -> None:
    tree = _parse(SRC / "rng.py")
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id == "random":
                assert node.attr == "Random", f"line {node.lineno}"
            if isinstance(node.value, ast.Attribute) and node.value.attr == "_r":
                assert node.attr == "random", f"line {node.lineno}"


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_builtin_hash_is_not_called(path: Path) -> None:
    for call in _calls(_parse(path)):
        if isinstance(call.func, ast.Name):
            assert call.func.id != "hash", f"{path}:{call.lineno}"


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_no_iteration_over_a_set_without_sorted(path: Path) -> None:
    for node in ast.walk(_parse(path)):
        iterables = []
        if isinstance(node, (ast.For, ast.comprehension)):
            iterables.append(node.iter)
        for iterable in iterables:
            is_call = (
                isinstance(iterable, ast.Call)
                and isinstance(iterable.func, ast.Name)
                and iterable.func.id in ("set", "frozenset")
            )
            assert not is_call and not isinstance(iterable, (ast.Set, ast.SetComp)), str(path)


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_system_time_is_only_read_in_timeutil(path: Path) -> None:
    if path.name == "timeutil.py":
        return
    for call in _calls(_parse(path)):
        if isinstance(call.func, ast.Attribute):
            assert call.func.attr not in (
                "now",
                "utcnow",
                "today",
                "time",
                "time_ns",
            ), f"{path}:{call.lineno}"


def test_system_time_in_timeutil_is_only_read_by_utc_now() -> None:
    tree = _parse(SRC / "timeutil.py")
    for function in (node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)):
        if function.name == "utc_now":
            continue
        for call in _calls(function):
            if isinstance(call.func, ast.Attribute):
                assert call.func.attr not in ("now", "utcnow", "today"), function.name


@pytest.mark.parametrize("path", ALL_CODE, ids=lambda p: str(p.relative_to(REPO)))
def test_text_files_are_opened_with_utf8_and_no_newline_translation(path: Path) -> None:
    for call in _calls(_parse(path)):
        if isinstance(call.func, ast.Attribute):
            assert call.func.attr not in ("read_text", "write_text"), f"{path}:{call.lineno}"
        is_open = (isinstance(call.func, ast.Name) and call.func.id == "open") or (
            isinstance(call.func, ast.Attribute) and call.func.attr == "open"
        )
        if not is_open:
            continue
        mode = call.args[1].value if len(call.args) > 1 else "r"  # type: ignore[attr-defined]
        if "b" in mode:
            continue
        keywords = {
            keyword.arg: keyword.value.value  # type: ignore[attr-defined]
            for keyword in call.keywords
        }
        assert keywords.get("encoding") == "utf-8", f"{path}:{call.lineno}"
        assert keywords.get("newline") == "", f"{path}:{call.lineno}"


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: str(p.relative_to(SRC)))
def test_only_the_table_model_reads_the_schema_of_the_first_batch(path: Path) -> None:
    if path.relative_to(SRC).as_posix() == "model/table.py":
        return
    for node in ast.walk(_parse(path)):
        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.value, ast.Attribute)
            and node.value.attr == "schema_by_batch"
            and isinstance(node.slice, ast.Constant)
            and node.slice.value == 0
        ):
            raise AssertionError(f"{path}:{node.lineno}: use stable_schema or ingestion_column")


def test_binary_mode_is_only_used_for_hashing() -> None:
    for path in SOURCES:
        for call in _calls(_parse(path)):
            if isinstance(call.func, ast.Name) and call.func.id == "open" and len(call.args) > 1:
                mode = call.args[1].value  # type: ignore[attr-defined]
                if "b" in mode:
                    assert (path.name, mode) == ("output_dir.py", "rb"), f"{path}:{call.lineno}"
