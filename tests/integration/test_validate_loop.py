"""Acceptance for phase 5, section 13.2 of the build plan, through the command line."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

GOLDEN = Path(__file__).resolve().parents[1] / "golden"
EIGHT = GOLDEN / "all_eight_scenarios" / "chaos.yml"
PLAIN = """\
version: 1
seed: {seed}
dataset: {{as_of: "2026-01-08T00:00:00Z", customers: 20, orders: 100, batches: 3}}
scenarios:
  - name: duplicate-business-key
    table: orders
    params: {{count: 3}}
  - name: null-critical-column
    table: customers
    params: {{column: country_code, count: 2}}
"""


def _datachaos(*args: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "datachaos", *map(str, args)], capture_output=True, text=True
    )


def _ok(*args: object) -> str:
    result = _datachaos(*args)
    assert result.returncode == 0, result.stderr
    return result.stdout


def _plain(tmp_path: Path, seed: int = 1) -> Path:
    path = tmp_path / f"plain-{seed}.yml"
    path.write_bytes(PLAIN.format(seed=seed).encode())
    return path


def test_all_eight_scenarios_reproduce_with_exit_0(tmp_path: Path) -> None:
    out = tmp_path / "out"
    _ok("run", "--config", EIGHT, "--out", out)
    document = json.loads(_ok("validate", out, "--reproduce", "--output", "json"))
    assert document["status"] == "ok"
    assert document["reproduce"] is True
    assert document["unverifiable"] == 0


def test_without_row_ids_shared_keys_are_unverifiable_and_exit_0(tmp_path: Path) -> None:
    out = tmp_path / "out"
    _ok("run", "--config", _plain(tmp_path), "--out", out)
    result = _datachaos("validate", out, "--reproduce")
    assert result.returncode == 0
    assert "3 unverifiable" in result.stdout
    assert "3 ledger lines cannot be verified" in result.stderr


def test_one_changed_byte_is_exit_1_and_names_the_file(tmp_path: Path) -> None:
    out = tmp_path / "out"
    _ok("run", "--config", EIGHT, "--out", out)
    path = out / "data" / "customers" / "batch_0000.csv"
    data = bytearray(path.read_bytes())
    data[-3] ^= 1
    path.write_bytes(bytes(data))
    result = _datachaos("validate", out)
    assert result.returncode == 1
    assert "data/customers/batch_0000.csv" in result.stdout


def test_the_same_config_twice_gives_the_same_content_and_another_seed_does_not(
    tmp_path: Path,
) -> None:
    hashes = []
    for name, seed in (("a", 1), ("b", 1), ("c", 2)):
        document = json.loads(
            _ok(
                "run",
                "--config",
                _plain(tmp_path, seed),
                "--out",
                tmp_path / name,
                "--output",
                "json",
            )
        )
        hashes.append(document["content_sha256"])
    assert hashes[0] == hashes[1]
    assert hashes[0] != hashes[2]


def test_the_whole_loop_inject_validate_report_land(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    landing = tmp_path / "landing"
    _ok("run", "--config", _plain(tmp_path), "--out", first)
    _ok(
        "inject",
        "--input",
        first,
        "--out",
        second,
        "--scenario",
        "late-arriving-data",
        "--table",
        "order_events",
        "--param",
        "count=5",
        "--param",
        "delay_min=6h",
        "--param",
        "delay_max=1d",
    )
    assert json.loads(_ok("validate", second, "--reproduce", "--output", "json"))["status"] == "ok"
    report = json.loads(_ok("report", second, "--output", "json"))
    assert [scenario["index"] for scenario in report["scenarios"]] == [0, 1, 2]
    assert (
        report["based_on"]["content_sha256"]
        == json.loads((first / "chaos-manifest.json").read_bytes())["content_sha256"]
    )
    for batch in (0, 1, 2):
        _ok("land", second, "--upto-batch", batch, "--to", landing)
    assert sorted(path.name for path in (landing / "orders").iterdir()) == [
        "batch_0000.csv",
        "batch_0001.csv",
        "batch_0002.csv",
    ]
    assert (landing / "schemas" / "orders" / "batch_0002.json").exists()


@pytest.mark.parametrize(
    "command",
    [
        ("validate", "{dir}"),
        ("report", "{dir}"),
        ("land", "{dir}", "--upto-batch", "0", "--to", "{dir}-landing"),
        ("inject", "--input", "{dir}", "--out", "{dir}-out", "--scenario", "x", "--table", "t"),
    ],
)
def test_every_command_gives_exit_2_on_a_directory_that_is_not_output(
    tmp_path: Path, command: tuple[str, ...]
) -> None:
    args = [part.format(dir=tmp_path / "plain") for part in command]
    (tmp_path / "plain").mkdir()
    result = _datachaos(*args)
    assert result.returncode == 2
    assert result.stderr.startswith("datachaos: error:")
    assert result.stdout == ""
