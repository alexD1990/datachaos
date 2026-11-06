from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from datachaos import cli, engine
from datachaos.config import DatasetConfig, OutputConfig, RunConfig, ScenarioConfig
from datachaos.errors import DataChaosError
from datachaos.land import land
from datachaos.validate import validate_dir

SMALL = DatasetConfig(as_of="2026-01-08T00:00:00Z", customers=20, orders=100, batches=3)
DROP = ScenarioConfig(
    "schema-drift", "orders", {"operation": "drop_column", "column": "status", "from_batch": 1}
)


def _run(tmp_path: Path, name: str = "run", seed: int = 5, **output: object) -> Path:
    path = tmp_path / name
    settings = OutputConfig(**output)  # type: ignore[arg-type]
    engine.run(RunConfig(seed=seed, dataset=SMALL, output=settings), path)
    return path


def _files(root: Path) -> list[str]:
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file())


def test_files_land_per_table_with_schemas_next_to_them(tmp_path: Path) -> None:
    run = _run(tmp_path)
    to = tmp_path / "landing"
    result = land(run, 0, to)
    assert _files(to) == sorted(
        [f"{table}/batch_0000.csv" for table in ("customers", "orders", "order_events")]
        + [f"schemas/{table}/batch_0000.json" for table in ("customers", "orders", "order_events")]
    )
    assert result.copied == 6
    assert result.skipped == 0
    for table in ("customers", "orders", "order_events"):
        assert (to / table / "batch_0000.csv").read_bytes() == (
            run / "data" / table / "batch_0000.csv"
        ).read_bytes()
        assert (to / "schemas" / table / "batch_0000.json").read_bytes() == (
            run / "schemas" / table / "batch_0000.json"
        ).read_bytes()


def test_upto_batch_includes_all_batches_up_to_it(tmp_path: Path) -> None:
    to = tmp_path / "landing"
    land(_run(tmp_path), 1, to)
    assert (to / "orders" / "batch_0001.csv").exists()
    assert (to / "schemas" / "orders" / "batch_0001.json").exists()
    assert not (to / "orders" / "batch_0002.csv").exists()
    assert not (to / "schemas" / "orders" / "batch_0002.json").exists()


def test_repeated_calls_with_a_growing_batch_work(tmp_path: Path) -> None:
    run = _run(tmp_path)
    to = tmp_path / "landing"
    assert (land(run, 0, to).copied, land(run, 0, to).skipped) == (6, 6)
    second = land(run, 1, to)
    assert (second.copied, second.skipped) == (6, 6)
    third = land(run, 2, to)
    assert (third.copied, third.skipped) == (6, 12)
    assert len(_files(to)) == 18


def test_a_lower_batch_afterwards_deletes_nothing(tmp_path: Path) -> None:
    run = _run(tmp_path)
    to = tmp_path / "landing"
    land(run, 2, to)
    before = _files(to)
    assert land(run, 0, to).copied == 0
    assert _files(to) == before


def test_a_different_file_is_exit_2_and_nothing_is_copied_or_overwritten(tmp_path: Path) -> None:
    run = _run(tmp_path)
    to = tmp_path / "landing"
    land(run, 0, to)
    changed = to / "orders" / "batch_0000.csv"
    changed.write_bytes(b"mine")
    with pytest.raises(DataChaosError, match="exists with other content; land never overwrites"):
        land(run, 2, to)
    assert changed.read_bytes() == b"mine"
    assert not (to / "orders" / "batch_0001.csv").exists()
    assert not (to / "customers" / "batch_0001.csv").exists()


def test_another_run_into_the_same_landing_is_refused(tmp_path: Path) -> None:
    to = tmp_path / "landing"
    land(_run(tmp_path), 0, to)
    before = _files(to)
    with pytest.raises(DataChaosError, match="exists with other content"):
        land(_run(tmp_path, "other", seed=6), 0, to)
    assert _files(to) == before


def test_a_directory_at_a_landing_path_is_refused(tmp_path: Path) -> None:
    to = tmp_path / "landing"
    (to / "orders" / "batch_0000.csv").mkdir(parents=True)
    with pytest.raises(DataChaosError, match="exists with other content"):
        land(_run(tmp_path), 0, to)


def test_a_source_that_does_not_match_its_hash_lands_nothing(tmp_path: Path) -> None:
    run = _run(tmp_path)
    path = run / "data" / "orders" / "batch_0001.csv"
    path.write_bytes(path.read_bytes() + b"x")
    to = tmp_path / "landing"
    with pytest.raises(DataChaosError, match="batch_0001.csv does not match its hash.*validate"):
        land(run, 2, to)
    assert not to.exists()


def test_a_later_batch_that_is_damaged_does_not_stop_an_earlier_one(tmp_path: Path) -> None:
    run = _run(tmp_path)
    path = run / "data" / "orders" / "batch_0002.csv"
    path.write_bytes(path.read_bytes() + b"x")
    assert land(run, 1, tmp_path / "landing").copied == 12


def test_a_missing_source_lands_nothing(tmp_path: Path) -> None:
    run = _run(tmp_path)
    (run / "schemas" / "orders" / "batch_0000.json").unlink()
    with pytest.raises(DataChaosError, match="is missing"):
        land(run, 0, tmp_path / "landing")


def test_a_manifest_that_points_outside_is_refused(tmp_path: Path) -> None:
    run = _run(tmp_path)
    (tmp_path / "secret.csv").write_bytes(b"secret")
    path = run / "chaos-manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["files"].append({"path": "data/orders/../../../secret.csv", "sha256": "0" * 64})
    path.write_bytes(json.dumps(manifest).encode())
    to = tmp_path / "landing"
    land(run, 0, to)
    assert "secret.csv" not in _files(to)


@pytest.mark.parametrize("batch", [-1, 3, 100])
def test_a_batch_out_of_range_is_exit_2(tmp_path: Path, batch: int) -> None:
    with pytest.raises(DataChaosError, match=f"from 0 to 2, got {batch}"):
        land(_run(tmp_path), batch, tmp_path / "landing")


@pytest.mark.parametrize("target", [".", "sub", "sub/deeper"])
def test_a_landing_inside_the_input_is_refused(tmp_path: Path, target: str) -> None:
    run = _run(tmp_path)
    before = _files(run)
    with pytest.raises(DataChaosError, match="must not overlap"):
        land(run, 0, run / target)
    assert _files(run) == before


def test_a_landing_that_holds_the_input_is_refused(tmp_path: Path) -> None:
    with pytest.raises(DataChaosError, match="must not overlap"):
        land(_run(tmp_path), 0, tmp_path)


def test_all_formats_land_but_not_combined_files(tmp_path: Path) -> None:
    run = _run(tmp_path, formats=("csv", "jsonl"), combined=True)
    to = tmp_path / "landing"
    land(run, 0, to)
    assert (to / "orders" / "batch_0000.jsonl").exists()
    assert (to / "orders" / "batch_0000.csv").exists()
    assert not (to / "orders.csv").exists()
    assert not (to / "orders.jsonl").exists()
    assert "chaos-manifest.json" not in _files(to)
    assert "chaos-ledger.jsonl" not in _files(to)


def test_each_batch_lands_with_its_own_schema_after_a_drift(tmp_path: Path) -> None:
    run = tmp_path / "run"
    engine.run(RunConfig(seed=5, dataset=SMALL, scenarios=(DROP,)), run)
    to = tmp_path / "landing"
    land(run, 2, to)
    assert b"status" in (to / "orders" / "batch_0000.csv").read_bytes().splitlines()[0]
    assert b"status" not in (to / "orders" / "batch_0001.csv").read_bytes().splitlines()[0]
    assert b'"status"' in (to / "schemas" / "orders" / "batch_0000.json").read_bytes()
    assert b'"status"' not in (to / "schemas" / "orders" / "batch_0001.json").read_bytes()


def test_the_input_is_still_valid_afterwards(tmp_path: Path) -> None:
    run = _run(tmp_path)
    land(run, 2, tmp_path / "landing")
    assert validate_dir(run).ok


def test_a_failed_copy_leaves_no_temp_file_and_no_partial_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = _run(tmp_path)
    to = tmp_path / "landing"

    def fail(source: object, target: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "rename", fail)
    with pytest.raises(DataChaosError, match="disk full"):
        land(run, 0, to)
    assert _files(to) == []
    assert not [path for path in to.rglob("*") if path.name.endswith(".tmp")]


def test_a_leftover_temp_file_is_exit_2_and_kept(tmp_path: Path) -> None:
    to = tmp_path / "landing"
    (to / "orders").mkdir(parents=True)
    temp = to / "orders" / ".batch_0000.csv.tmp"
    temp.write_bytes(b"old")
    with pytest.raises(DataChaosError, match="already exists"):
        land(_run(tmp_path), 0, to)
    assert temp.read_bytes() == b"old"


def test_not_datachaos_output_is_refused(tmp_path: Path) -> None:
    (tmp_path / "plain").mkdir()
    with pytest.raises(DataChaosError, match="has no chaos-manifest.json"):
        land(tmp_path / "plain", 0, tmp_path / "landing")


def test_cli_text(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    to = tmp_path / "landing"
    assert cli.main(["land", str(_run(tmp_path)), "--upto-batch", "0", "--to", str(to)]) == 0
    assert capsys.readouterr().out == (
        f"landed batches up to 0 in {to}: 6 files copied, 0 already there\n"
    )


def test_cli_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    to = tmp_path / "landing"
    args = ["land", str(_run(tmp_path)), "--upto-batch", "1", "--to", str(to), "--output", "json"]
    assert cli.main(args) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["schema_version"] == "1.0"
    assert document["status"] == "ok"
    assert (document["copied"], document["skipped"]) == (12, 0)


def test_cli_conflict_is_exit_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    to = tmp_path / "landing"
    (to / "orders").mkdir(parents=True)
    (to / "orders" / "batch_0000.csv").write_bytes(b"mine")
    args = ["land", str(_run(tmp_path)), "--upto-batch", "0", "--to", str(to)]
    assert cli.main(args) == 2
    err = capsys.readouterr().err
    assert err.startswith("datachaos: error: ")
    assert err.count("\n") == 1


def test_cli_requires_its_flags(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as info:
        cli.main(["land", "x"])
    assert info.value.code == 2
    assert "required" in capsys.readouterr().err


def test_help_says_it_never_overwrites(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        cli.main(["land", "--help"])
    assert "Never deletes or overwrites" in " ".join(capsys.readouterr().out.split())
