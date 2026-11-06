from __future__ import annotations

import json
from pathlib import Path

import pytest

from datachaos import __version__, cli, engine
from datachaos.config import load_config
from datachaos.dbtcheck import check, merge_status, read_run_results, render_check
from datachaos.errors import DataChaosError

DUPLICATE = "test.shop.assert_unique"
MODEL = "model.shop.stg_orders"
CONFIG = """\
version: 1
seed: 1
dataset: {as_of: "2026-01-08T00:00:00Z", customers: 5, orders: 20, batches: 2}
scenarios:
  - {name: duplicate-business-key, table: orders, params: {count: 1}}
expectations:
  - scenario: duplicate-business-key
    failing_tests: ["test.shop.assert_unique*"]
"""
BASELINE = """\
version: 1
seed: 1
dataset: {as_of: "2026-01-08T00:00:00Z", customers: 5, orders: 20, batches: 2}
"""


@pytest.fixture
def run(tmp_path: Path) -> tuple[Path, Path]:
    config = tmp_path / "chaos.yml"
    config.write_bytes(CONFIG.encode())
    out = tmp_path / "out"
    engine.run(load_config(config), out)
    return out, config


@pytest.fixture
def baseline(tmp_path: Path) -> tuple[Path, Path]:
    config = tmp_path / "base.yml"
    config.write_bytes(BASELINE.encode())
    out = tmp_path / "base"
    engine.run(load_config(config), out)
    return out, config


def _results(tmp_path: Path, name: str, **statuses: str) -> Path:
    """Node names use __ for the dots that keyword arguments cannot hold."""
    path = tmp_path / f"{name}.json"
    results = [
        {"unique_id": node.replace("__", "."), "status": status}
        for node, status in statuses.items()
    ]
    path.write_bytes(json.dumps({"metadata": {}, "results": results}).encode())
    return path


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        (["success"], "success"),
        (["pass"], "pass"),
        (["success", "error"], "error"),
        (["error", "success"], "error"),
        (["fail", "pass"], "fail"),
        (["pass", "fail"], "fail"),
        (["fail", "error"], "error"),
        (["success", "skipped"], "skipped"),
        (["skipped", "error"], "error"),
        (["pass", "warn"], "warn"),
        (["warn", "pass"], "warn"),
        (["skipped", "success"], "skipped"),
    ],
)
def test_merge_status(statuses: list[str], expected: str) -> None:
    assert merge_status(statuses) == expected


def test_an_expected_failure_that_happened_is_ok(tmp_path: Path, run: tuple[Path, Path]) -> None:
    results = _results(tmp_path, "r", test__shop__assert_unique="fail")
    result = check(run[0], [results], run[1])
    assert result.ok
    assert result.expectations[0].detected
    assert result.unexpected == ()


@pytest.mark.parametrize("status", ["pass", "success", "skipped", "warn"])
def test_an_expected_failure_that_did_not_happen_is_not_ok(
    tmp_path: Path, run: tuple[Path, Path], status: str
) -> None:
    results = _results(tmp_path, "r", test__shop__assert_unique=status)
    result = check(run[0], [results], run[1])
    assert not result.ok
    assert not result.expectations[0].detected


def test_a_glob_can_match_a_model_with_status_error(tmp_path: Path) -> None:
    content = CONFIG.replace("test.shop.assert_unique*", "model.shop.stg_orders")
    config = tmp_path / "chaos.yml"
    config.write_bytes(content.encode())
    out = tmp_path / "out"
    engine.run(load_config(config), out)
    assert check(out, [_results(tmp_path, "r", model__shop__stg_orders="error")], config).ok


def test_one_of_several_matches_failing_is_enough(tmp_path: Path, run: tuple[Path, Path]) -> None:
    results = _results(
        tmp_path, "r", test__shop__assert_unique_a="pass", test__shop__assert_unique_b="fail"
    )
    assert check(run[0], [results], run[1]).ok


def test_a_model_error_in_dbt_run_counts_even_if_later_tests_pass(
    tmp_path: Path, run: tuple[Path, Path]
) -> None:
    first = _results(tmp_path, "run0", test__shop__assert_unique="pass")
    second = _results(tmp_path, "run1", test__shop__assert_unique="error")
    third = _results(tmp_path, "test", test__shop__assert_unique="pass")
    assert check(run[0], [first, second, third], run[1]).ok


def test_results_from_several_files_are_merged(tmp_path: Path, run: tuple[Path, Path]) -> None:
    run_file = _results(tmp_path, "run", model__shop__stg_orders="error")
    test_file = _results(tmp_path, "test", test__shop__assert_unique="fail")
    result = check(run[0], [run_file, test_file], run[1])
    assert result.nodes == 2
    assert result.ok
    assert [node.unique_id for node in result.unexpected] == [MODEL]
    assert read_run_results([run_file, test_file]) == {MODEL: ["error"], DUPLICATE: ["fail"]}


def test_a_glob_without_any_match_is_a_config_error(tmp_path: Path, run: tuple[Path, Path]) -> None:
    with pytest.raises(DataChaosError, match="matches no node in the dbt results"):
        check(run[0], [_results(tmp_path, "r", test__shop__other="pass")], run[1])


def test_a_glob_matching_only_in_a_later_file_is_fine(
    tmp_path: Path, run: tuple[Path, Path]
) -> None:
    first = _results(tmp_path, "a", model__shop__x="success")
    second = _results(tmp_path, "b", test__shop__assert_unique="fail")
    assert check(run[0], [first, second], run[1]).ok


def test_other_failures_are_unexpected_but_do_not_change_the_result(
    tmp_path: Path, run: tuple[Path, Path]
) -> None:
    results = _results(
        tmp_path,
        "r",
        test__shop__assert_unique="fail",
        test__shop__other="fail",
        model__shop__broken="error",
        model__shop__fine="success",
        model__shop__after="skipped",
    )
    result = check(run[0], [results], run[1])
    assert result.ok
    assert sorted(node.unique_id for node in result.unexpected) == [
        "model.shop.broken",
        "test.shop.other",
    ]


def test_a_baseline_needs_every_node_to_pass(tmp_path: Path, baseline: tuple[Path, Path]) -> None:
    good = _results(tmp_path, "good", test__shop__a="pass", model__shop__b="success")
    assert check(baseline[0], [good], baseline[1]).ok
    bad = _results(tmp_path, "bad", test__shop__a="fail", model__shop__b="success")
    result = check(baseline[0], [bad], baseline[1])
    assert not result.ok
    assert [node.unique_id for node in result.unexpected] == ["test.shop.a"]


def test_every_pattern_of_an_expectation_must_be_detected(tmp_path: Path) -> None:
    content = CONFIG.replace(
        '["test.shop.assert_unique*"]', '["test.shop.assert_unique*", "test.shop.second*"]'
    )
    config = tmp_path / "chaos.yml"
    config.write_bytes(content.encode())
    out = tmp_path / "out"
    engine.run(load_config(config), out)
    both = _results(tmp_path, "b", test__shop__assert_unique="fail", test__shop__second="fail")
    assert check(out, [both], config).ok
    one = _results(tmp_path, "o", test__shop__assert_unique="fail", test__shop__second="pass")
    result = check(out, [one], config)
    assert not result.ok
    assert [item.detected for item in result.expectations] == [True, False]


def test_a_run_with_scenarios_but_no_expectations_is_no_baseline(tmp_path: Path) -> None:
    head, _, _ = CONFIG.partition("expectations:")
    config = tmp_path / "chaos.yml"
    config.write_bytes(head.encode())
    out = tmp_path / "out"
    engine.run(load_config(config), out)
    result = check(out, [_results(tmp_path, "r", test__shop__a="fail")], config)
    assert not result.is_baseline
    assert result.ok
    assert [node.unique_id for node in result.unexpected] == ["test.shop.a"]


def test_another_config_than_the_run_is_refused(
    tmp_path: Path, run: tuple[Path, Path], baseline: tuple[Path, Path]
) -> None:
    results = _results(tmp_path, "r", test__shop__assert_unique="fail")
    with pytest.raises(DataChaosError, match="is not the config of the run"):
        check(run[0], [results], baseline[1])


@pytest.mark.parametrize(
    "content",
    [b"{}", b'{"results": 3}', b'{"results": [{}]}', b'{"results": [{"unique_id": "a"}]}', b"[]"],
)
def test_files_that_are_not_run_results_are_refused(
    tmp_path: Path, run: tuple[Path, Path], content: bytes
) -> None:
    path = tmp_path / "bad.json"
    path.write_bytes(content)
    with pytest.raises(DataChaosError):
        check(run[0], [path], run[1])


def test_a_missing_file_is_refused(tmp_path: Path, run: tuple[Path, Path]) -> None:
    with pytest.raises(DataChaosError, match="cannot read"):
        check(run[0], [tmp_path / "missing.json"], run[1])


def test_not_datachaos_output_is_refused(tmp_path: Path, run: tuple[Path, Path]) -> None:
    results = _results(tmp_path, "r", test__shop__assert_unique="fail")
    (tmp_path / "plain").mkdir()
    with pytest.raises(DataChaosError, match="has no chaos-manifest.json"):
        check(tmp_path / "plain", [results], run[1])


def test_globs_are_case_sensitive(tmp_path: Path, run: tuple[Path, Path]) -> None:
    results = _results(tmp_path, "r", TEST__shop__ASSERT_UNIQUE="fail")
    with pytest.raises(DataChaosError, match="matches no node"):
        check(run[0], [results], run[1])


def test_render_shows_the_matrix(tmp_path: Path, run: tuple[Path, Path]) -> None:
    results = _results(tmp_path, "r", test__shop__assert_unique="pass", model__shop__x="error")
    text = render_check(check(run[0], [results], run[1]))
    assert "NOT DETECTED (status pass)" in text
    assert "unexpected: model.shop.x: error" in text
    assert "failed: 2 nodes in 1 run_results files" in text


def _args(run: tuple[Path, Path], *paths: Path, output: str = "text") -> list[str]:
    args = ["check-dbt", "--run", str(run[0]), "--config", str(run[1]), "--output", output]
    for path in paths:
        args += ["--run-results", str(path)]
    return args


def test_cli_ok_is_exit_0(
    tmp_path: Path, run: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    results = _results(tmp_path, "r", test__shop__assert_unique="fail")
    assert cli.main(_args(run, results)) == 0
    assert "detected (status fail)" in capsys.readouterr().out


def test_cli_missed_is_exit_1(
    tmp_path: Path, run: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    results = _results(tmp_path, "r", test__shop__assert_unique="pass")
    assert cli.main(_args(run, results)) == 1
    assert "NOT DETECTED" in capsys.readouterr().out


def test_cli_config_error_is_exit_2_with_one_line(
    tmp_path: Path, run: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    results = _results(tmp_path, "r", test__shop__other="pass")
    assert cli.main(_args(run, results)) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("datachaos: error: ")
    assert captured.err.count("\n") == 1


def test_cli_takes_run_results_several_times_and_gives_json(
    tmp_path: Path, run: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    first = _results(tmp_path, "a", model__shop__x="error")
    second = _results(tmp_path, "b", test__shop__assert_unique="fail")
    assert cli.main(_args(run, first, second, output="json")) == 0
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert captured.err == ""
    assert list(document) == [
        "schema_version",
        "tool_version",
        "status",
        "directory",
        "run_results",
        "baseline",
        "nodes",
        "expectations",
        "unexpected",
    ]
    assert document["schema_version"] == "1.0"
    assert document["tool_version"] == __version__
    assert document["run_results"] == [str(first), str(second)]
    assert document["expectations"] == [
        {
            "scenario": "duplicate-business-key",
            "failing_test": "test.shop.assert_unique*",
            "detected": True,
            "nodes": [{"unique_id": DUPLICATE, "status": "fail"}],
        }
    ]
    assert document["unexpected"] == [{"unique_id": "model.shop.x", "status": "error"}]


def test_cli_requires_run_results(
    run: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as info:
        cli.main(["check-dbt", "--run", str(run[0]), "--config", str(run[1])])
    assert info.value.code == 2
    assert "--run-results" in capsys.readouterr().err


def test_help_explains_merging(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        cli.main(["check-dbt", "--help"])
    text = " ".join(capsys.readouterr().out.split())
    assert "status fail or error in any of them" in text
