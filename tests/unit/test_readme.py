"""The README shows real output: its commands run here, and the output has to be the same.

The output of these commands is deterministic and uses the relative paths in the commands, so
nothing needs to be normalized. `run_id` depends on the version, so a new version updates it too.
"""

from __future__ import annotations

import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from datachaos.scenarios import SCENARIOS

ROOT = Path(__file__).resolve().parents[2]
README = (ROOT / "README.md").read_bytes().decode().replace("\r\n", "\n")


def _commands() -> list[tuple[str, str]]:
    """Every `$ datachaos` line in a console block, with the output shown under it."""
    commands = []
    for block in re.findall(r"```console\n(.*?)\n```", README, flags=re.S):
        current: str | None = None
        output: list[str] = []
        for line in [*block.split("\n"), "$ "]:
            if line.startswith("$ "):
                if current is not None and current.startswith("datachaos "):
                    commands.append((current, "\n".join(output)))
                current = line[2:]
                output = []
            else:
                output.append(line)
    return commands


def _run(command: str, cwd: Path) -> str:
    """Warnings first, as in a terminal, where they are written before the result."""
    args = shlex.split(command)[1:]
    result = subprocess.run(
        [sys.executable, "-m", "datachaos", *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=cwd,
    )
    assert result.returncode == 0, f"{command}\n{result.stderr}"
    return (result.stderr + result.stdout).rstrip("\n").replace("\r\n", "\n")


def test_the_commands_in_the_readme_give_the_output_it_shows(tmp_path: Path) -> None:
    config = re.search(r"```yaml\n(.*?)\n```", README, flags=re.S)
    assert config is not None
    (tmp_path / "chaos.yml").write_bytes((config.group(1) + "\n").encode())
    commands = _commands()
    assert len(commands) >= 9
    for command, shown in commands:
        assert _run(command, tmp_path) == shown, command


def test_the_quickstart_comes_first_and_installs_from_pypi() -> None:
    sections = re.findall(r"^## (.+)$", README, flags=re.M)
    assert sections[0] == "Quickstart"
    quickstart = README.split("## Quickstart")[1].split("\n## ")[0]
    assert "$ pip install datachaos\n" in quickstart
    assert "$ datachaos generate" in quickstart
    assert "$ datachaos run" in quickstart
    assert "$ datachaos validate" in quickstart


def test_the_demo_has_a_section_of_its_own() -> None:
    assert "## The dbt demo\n" in README
    assert "run_demo.py" in README.split("## The dbt demo")[1].split("\n## ")[0]
    assert "run_demo.py" not in README.split("## The dbt demo")[0]


def test_links_work_on_pypi_too() -> None:
    for target in re.findall(r"\]\(([^)]+)\)", README):
        assert target.startswith(("https://", "#")), target


def test_links_inside_the_readme_point_at_sections() -> None:
    anchors = {
        re.sub(r"[^a-z0-9 -]", "", title.lower()).replace(" ", "-")
        for title in re.findall(r"^#+ (.+)$", README, flags=re.M)
    }
    for target in re.findall(r"\]\(#([^)]+)\)", README):
        assert target in anchors, target


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_the_scenario_table_follows_the_specs(name: str) -> None:
    spec = SCENARIOS[name].spec
    row = f"| `{name}` | {spec.detects} | {spec.typical_dbt_test} |"
    assert row in README


def test_the_scenario_table_has_every_scenario_once() -> None:
    assert re.search(r"```console\n\$ datachaos list-scenarios\n", README)
    section = README.split("## Scenarios")[1].split("\n## ")[0]
    rows = re.findall(r"^\| `([a-z-]+)` \|", section, flags=re.M)
    assert sorted(rows) == sorted(SCENARIOS)


def test_the_output_format_covers_every_ledger_action_and_command() -> None:
    text = (ROOT / "docs" / "output-format.md").read_bytes().decode()
    for action in (
        "insert_row",
        "set_value",
        "move_batch",
        "delete_row",
        "orphan_row",
        "schema_change",
    ):
        assert f"`{action}`" in text
    for command in ("generate", "validate", "report", "land", "check-dbt", "list-scenarios"):
        assert f"`{command}`" in text
