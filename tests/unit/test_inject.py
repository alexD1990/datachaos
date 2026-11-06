from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

from datachaos import __version__, cli, engine
from datachaos.config import DatasetConfig, OutputConfig, RunConfig, ScenarioConfig
from datachaos.errors import DataChaosError
from datachaos.inject import inject
from datachaos.io.reader import read_json
from datachaos.validate import validate_dir

REPO = Path(__file__).resolve().parents[2]
SMALL = DatasetConfig(as_of="2026-01-08T00:00:00Z", customers=20, orders=100, batches=3)
FIRST = ScenarioConfig("null-critical-column", "orders", {"column": "amount", "count": 3})
ADDED = ScenarioConfig("invalid-enum", "orders", {"column": "status", "count": 3})


def _input(tmp_path: Path, *scenarios: ScenarioConfig) -> Path:
    path = tmp_path / "input"
    engine.run(RunConfig(seed=5, dataset=SMALL, scenarios=scenarios), path)
    return path


def _manifest(path: Path) -> dict:  # type: ignore[type-arg]
    return read_json(path / "chaos-manifest.json")


def _tree(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_the_scenario_is_added_last_and_the_result_validates(tmp_path: Path) -> None:
    source = _input(tmp_path, FIRST)
    out = tmp_path / "out"
    manifest = inject(source, out, ADDED)
    assert [record["name"] for record in manifest.scenarios] == [
        "null-critical-column",
        "invalid-enum",
    ]
    assert manifest.scenarios[1]["index"] == 1
    result = validate_dir(out, reproduce=True)
    assert result.ok, result.problems


def test_the_result_equals_a_run_of_the_extended_config(tmp_path: Path) -> None:
    source = _input(tmp_path, FIRST)
    manifest = inject(source, tmp_path / "out", ADDED)
    direct = engine.run(
        RunConfig(seed=5, dataset=SMALL, scenarios=(FIRST, ADDED)), tmp_path / "direct"
    )
    assert manifest.content_sha256 == direct.content_sha256
    assert manifest.run.run_id == direct.run.run_id
    assert (tmp_path / "out" / "chaos-ledger.jsonl").read_bytes() == (
        tmp_path / "direct" / "chaos-ledger.jsonl"
    ).read_bytes()


def test_earlier_ledger_lines_are_unchanged(tmp_path: Path) -> None:
    source = _input(tmp_path, FIRST)
    inject(source, tmp_path / "out", ADDED)
    before = (source / "chaos-ledger.jsonl").read_bytes().splitlines()
    after = (tmp_path / "out" / "chaos-ledger.jsonl").read_bytes().splitlines()
    assert after[: len(before)] == before
    assert len(after) == len(before) + 3


def test_the_manifest_records_which_run_it_builds_on(tmp_path: Path) -> None:
    source = _input(tmp_path, FIRST)
    inject(source, tmp_path / "out", ADDED)
    original = _manifest(source)
    manifest = _manifest(tmp_path / "out")
    assert manifest["run"]["based_on"] == {
        "run_id": original["run"]["run_id"],
        "content_sha256": original["content_sha256"],
    }
    schema = read_json(REPO / "schema" / "chaos-manifest-1.0.schema.json")
    jsonschema.Draft202012Validator(schema).validate(manifest)
    assert "based_on" not in original["run"]


def test_the_schema_requires_both_fields_of_based_on(tmp_path: Path) -> None:
    source = _input(tmp_path)
    inject(source, tmp_path / "out", ADDED)
    manifest = _manifest(tmp_path / "out")
    del manifest["run"]["based_on"]["run_id"]
    schema = read_json(REPO / "schema" / "chaos-manifest-1.0.schema.json")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(schema).validate(manifest)


def test_inject_can_be_chained(tmp_path: Path) -> None:
    source = _input(tmp_path)
    inject(source, tmp_path / "one", FIRST)
    inject(tmp_path / "one", tmp_path / "two", ADDED)
    assert validate_dir(tmp_path / "two", reproduce=True).ok
    assert (
        _manifest(tmp_path / "two")["run"]["based_on"]["run_id"]
        == (_manifest(tmp_path / "one")["run"]["run_id"])
    )


def test_the_input_is_not_touched(tmp_path: Path) -> None:
    source = _input(tmp_path, FIRST)
    before = _tree(source)
    inject(source, tmp_path / "out", ADDED)
    assert _tree(source) == before


def test_output_settings_follow_the_input(tmp_path: Path) -> None:
    source = tmp_path / "input"
    settings = OutputConfig(formats=("csv", "jsonl"), combined=True, emit_row_ids=True)
    engine.run(RunConfig(seed=5, dataset=SMALL, output=settings), source)
    inject(source, tmp_path / "out", ADDED)
    assert (tmp_path / "out" / "data" / "orders.jsonl").exists()
    assert b"_chaos_rid" in (tmp_path / "out" / "data" / "orders.csv").read_bytes()


def test_a_changed_file_is_refused_and_nothing_is_written(tmp_path: Path) -> None:
    source = _input(tmp_path, FIRST)
    path = source / "data" / "orders" / "batch_0000.csv"
    path.write_bytes(path.read_bytes() + b"x")
    with pytest.raises(
        DataChaosError, match="does not match its manifest .*batch_0000.csv.*datachaos validate"
    ):
        inject(source, tmp_path / "out", ADDED)
    assert not (tmp_path / "out").exists()


def test_other_regenerated_content_is_refused_with_both_versions(tmp_path: Path) -> None:
    source = _input(tmp_path, FIRST)
    path = source / "chaos-manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["run"]["config"]["seed"] = 6
    manifest["tool_version"] = "0.0.1"
    path.write_bytes(json.dumps(manifest).encode())
    with pytest.raises(DataChaosError) as info:
        inject(source, tmp_path / "out", ADDED)
    message = str(info.value)
    assert "gives other content" in message
    assert "written by DataChaos 0.0.1" in message
    assert f"this is {__version__}" in message
    assert not (tmp_path / "out").exists()


def test_a_generator_that_has_changed_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _input(tmp_path, FIRST)
    real = engine._build_dataset

    def changed(config: RunConfig):  # type: ignore[no-untyped-def]
        return real(RunConfig(seed=config.seed + 1, dataset=config.dataset))

    monkeypatch.setattr(engine, "_build_dataset", changed)
    with pytest.raises(DataChaosError, match="gives other content"):
        inject(source, tmp_path / "out", ADDED)
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("name", [".", "sub", "sub/deeper"])
def test_out_inside_the_input_is_refused(tmp_path: Path, name: str) -> None:
    source = _input(tmp_path)
    with pytest.raises(DataChaosError, match="must be outside --input"):
        inject(source, source / name, ADDED)


def test_an_invalid_scenario_writes_nothing(tmp_path: Path) -> None:
    source = _input(tmp_path, FIRST)
    bad = ScenarioConfig("invalid-enum", "orders", {"column": "nope", "count": 1})
    with pytest.raises(DataChaosError, match=r"scenarios\[1\]\.params\.column"):
        inject(source, tmp_path / "out", bad)
    assert not (tmp_path / "out").exists()


def test_exclusivity_and_drift_conflicts_apply_to_earlier_scenarios(tmp_path: Path) -> None:
    source = _input(tmp_path, FIRST)
    drift = ScenarioConfig(
        "schema-drift", "orders", {"operation": "drop_column", "column": "amount"}
    )
    with pytest.raises(DataChaosError, match="cannot be combined"):
        inject(source, tmp_path / "out", drift)


def test_not_datachaos_output_is_refused(tmp_path: Path) -> None:
    with pytest.raises(DataChaosError, match="has no chaos-manifest.json"):
        inject(tmp_path, tmp_path.parent / "elsewhere-out", ADDED)


def test_an_existing_non_empty_out_is_refused(tmp_path: Path) -> None:
    source = _input(tmp_path)
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "keep.txt").write_bytes(b"keep")
    with pytest.raises(DataChaosError, match="is not empty"):
        inject(source, tmp_path / "out", ADDED)
    assert (tmp_path / "out" / "keep.txt").read_bytes() == b"keep"


def test_nothing_is_left_next_to_the_output(tmp_path: Path) -> None:
    source = _input(tmp_path)
    inject(source, tmp_path / "out", ADDED)
    assert sorted(path.name for path in tmp_path.iterdir()) == ["input", "out"]


def test_cli_text_and_params(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source = _input(tmp_path, FIRST)
    args = ["inject", "--input", str(source), "--out", str(tmp_path / "out")]
    args += ["--scenario", "invalid-enum", "--table", "orders"]
    args += ["--param", "column=status", "--param", "count=2"]
    assert cli.main(args) == 0
    text = capsys.readouterr().out
    original = _manifest(source)["run"]["run_id"]
    assert f"  based on run {original}\n" in text
    assert "scenario 1: invalid-enum on orders, 2 ledger lines" in text


def test_cli_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source = _input(tmp_path, FIRST)
    args = ["inject", "--input", str(source), "--out", str(tmp_path / "out"), "--output", "json"]
    args += ["--scenario", "invalid-enum", "--table", "orders"]
    args += ["--param", "column=status", "--param", "count=2"]
    assert cli.main(args) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["based_on"] == _manifest(tmp_path / "out")["run"]["based_on"]
    assert document["schema_version"] == "1.0"


def test_cli_refusal_is_exit_2_with_one_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = _input(tmp_path)
    path = source / "data" / "orders" / "batch_0000.csv"
    path.write_bytes(path.read_bytes() + b"x")
    args = ["inject", "--input", str(source), "--out", str(tmp_path / "out")]
    args += ["--scenario", "invalid-enum", "--table", "orders", "--param", "column=status"]
    assert cli.main(args) == 2
    err = capsys.readouterr().err
    assert err.startswith("datachaos: error: ")
    assert err.count("\n") == 1
