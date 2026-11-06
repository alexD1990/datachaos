"""The whole loop against real dbt, including broken copies of the project as negative controls.

Skipped without dbt, except in the CI job with DATACHAOS_REQUIRE_DBT=1, where that is a failure.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.dbt

PROJECT = Path(__file__).resolve().parents[2] / "examples" / "dbt_shop"
EVENT_WATERMARK = "assert_daily_orders_event_watermark_matches_full"
INGEST_WATERMARK = "assert_daily_orders_ingest_watermark_matches_full"
BY_INGESTION = "assert_order_status_by_ingestion_matches_event_time"
BY_EVENT_TIME = "assert_order_status_by_event_time_matches_event_time"


@pytest.fixture(scope="module", autouse=True)
def _needs_dbt() -> None:
    if shutil.which("dbt", path=str(Path(sys.executable).parent)):
        return
    if os.environ.get("DATACHAOS_REQUIRE_DBT") == "1":
        pytest.fail("dbt is required here, but not installed")
    pytest.skip("dbt is not installed")


def _demo(project: Path, work: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(project / "run_demo.py"), "--work", str(work), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


@pytest.fixture(scope="module")
def demo(tmp_path_factory: pytest.TempPathFactory) -> tuple[subprocess.CompletedProcess[str], Path]:
    work = tmp_path_factory.mktemp("demo") / "work"
    return _demo(PROJECT, work), work


def _nodes(work: Path, config: str, result: str = "test") -> dict[str, dict[str, Any]]:
    document = json.loads((work / config / "results" / f"{result}.json").read_bytes())
    return {item["unique_id"].split(".")[2]: item for item in document["results"]}


def _statuses(work: Path, config: str) -> dict[str, str]:
    return {name: item["status"] for name, item in _nodes(work, config).items()}


def test_the_demo_passes_for_every_config(
    demo: tuple[subprocess.CompletedProcess[str], Path],
) -> None:
    result, work = demo
    assert result.returncode == 0, result.stdout + result.stderr
    summary = json.loads((work / "summary.json").read_bytes())
    assert len(summary) == len(list((PROJECT / "configs").glob("*.yml")))
    assert all(outcome["passed"] for outcome in summary), summary
    assert all(outcome["unexpected"] == [] for outcome in summary)


def test_baseline_is_green_in_every_run_and_test(
    demo: tuple[subprocess.CompletedProcess[str], Path],
) -> None:
    _, work = demo
    for path in sorted((work / "baseline" / "results").glob("*.json")):
        for item in json.loads(path.read_bytes())["results"]:
            assert item["status"] in ("success", "pass"), (path.name, item["unique_id"])


@pytest.mark.parametrize(
    ("config", "vulnerable", "robust"),
    [
        ("historical_update", EVENT_WATERMARK, INGEST_WATERMARK),
        ("late_orders", EVENT_WATERMARK, INGEST_WATERMARK),
        ("out_of_order_events", BY_INGESTION, BY_EVENT_TIME),
    ],
)
def test_the_vulnerable_model_is_caught_and_the_robust_one_is_not(
    demo: tuple[subprocess.CompletedProcess[str], Path],
    config: str,
    vulnerable: str,
    robust: str,
) -> None:
    _, work = demo
    statuses = _statuses(work, config)
    assert statuses[vulnerable] == "fail"
    assert statuses[robust] == "pass"


def test_late_customers_pass_relationships_at_the_end_but_lose_orders_for_good(
    demo: tuple[subprocess.CompletedProcess[str], Path],
) -> None:
    _, work = demo
    statuses = _statuses(work, "late_customers")
    assert statuses["assert_fct_orders_incremental_matches_full"] == "fail"
    relationships = [name for name in statuses if name.startswith("relationships_")]
    assert len(relationships) == 2
    assert all(statuses[name] == "pass" for name in relationships)


def test_change_type_fails_the_cast_in_staging_from_its_batch_on(
    demo: tuple[subprocess.CompletedProcess[str], Path],
) -> None:
    _, work = demo
    config = "schema_drift_change_type"
    assert _nodes(work, config, "run_batch_0002")["stg_orders"]["status"] == "success"
    failed = _nodes(work, config, "run_batch_0003")
    assert failed["stg_orders"]["status"] == "error"
    assert "Conversion Error" in failed["stg_orders"]["message"]
    assert failed["fct_orders"]["status"] == "skipped"
    # The tests run on the table from before, which is why check-dbt needs the runs as well.
    assert _statuses(work, config)["not_null_stg_orders_amount"] == "pass"


def test_a_rename_needs_no_model_to_fail(
    demo: tuple[subprocess.CompletedProcess[str], Path],
) -> None:
    _, work = demo
    config = "schema_drift_rename"
    for batch in range(5):
        for item in _nodes(work, config, f"run_batch_{batch:04d}").values():
            assert item["status"] == "success"
    assert _statuses(work, config)["not_null_stg_orders_source_system"] == "fail"


def _broken_copy(tmp_path: Path, edit: Callable[[Path], None]) -> Path:
    copy = tmp_path / "dbt_shop"
    shutil.copytree(PROJECT, copy, ignore=shutil.ignore_patterns("target", "logs", "*.duckdb"))
    edit(copy)
    return copy


def _replace(path: Path, old: str, new: str) -> None:
    text = path.read_bytes().decode()
    assert old in text
    path.write_bytes(text.replace(old, new).encode())


def test_control_a_robust_model_that_only_adds_new_rows_fails_the_demo(tmp_path: Path) -> None:
    def only_new_rows(project: Path) -> None:
        _replace(
            project / "models" / "marts" / "daily_orders_ingest_watermark.sql",
            "where cast(order_timestamp as date) in (select order_date from touched_days)",
            "where ingested_at > (select max(last_ingested_at) from {{ this }})",
        )

    project = _broken_copy(tmp_path, only_new_rows)
    work = tmp_path / "work"
    result = _demo(project, work, "--only", "historical_update")
    assert result.returncode == 1, result.stdout + result.stderr
    assert _statuses(work, "historical_update")[INGEST_WATERMARK] == "fail"


def test_control_counting_orders_alone_does_not_catch_a_historical_update(tmp_path: Path) -> None:
    """Why the daily models also hold sum(amount) and orders per status."""

    def count_only(project: Path) -> None:
        macro = project / "macros" / "daily_orders.sql"
        text = macro.read_bytes().decode()
        start = text.index("{% macro daily_order_metrics() %}")
        end = text.index("{% endmacro %}", start)
        text = (
            text[:start]
            + "{% macro daily_order_metrics() %}\n    count(*) as orders\n"
            + text[end:]
        )
        for line in (
            "        amount_sum,\n",
            "        orders_created,\n",
            "        orders_paid,\n",
            "        orders_shipped,\n",
            "        orders_delivered,\n",
        ):
            text = text.replace(line, "")
        text = text.replace("        orders,\n        orders_cancelled\n", "        orders\n")
        macro.write_bytes(text.encode())

    project = _broken_copy(tmp_path, count_only)
    work = tmp_path / "work"
    result = _demo(project, work, "--only", "historical_update")
    assert result.returncode == 1, result.stdout + result.stderr
    assert "not caught" in result.stdout
    assert _statuses(work, "historical_update")[EVENT_WATERMARK] == "pass"


def test_control_a_robust_order_status_that_sorts_on_ingestion_fails_the_demo(
    tmp_path: Path,
) -> None:
    def on_ingestion(project: Path) -> None:
        _replace(
            project / "models" / "marts" / "order_status_by_event_time.sql",
            "order by event_time desc",
            "order by ingested_at desc",
        )

    project = _broken_copy(tmp_path, on_ingestion)
    work = tmp_path / "work"
    result = _demo(project, work, "--only", "out_of_order_events")
    assert result.returncode == 1, result.stdout + result.stderr
    assert _statuses(work, "out_of_order_events")[BY_EVENT_TIME] == "fail"
