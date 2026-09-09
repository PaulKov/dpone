from __future__ import annotations

import importlib.util
import sys
from argparse import Namespace
from pathlib import Path


def _load_suite_module():
    path = Path("tools/mssql_benchmark_suite.py")
    spec = importlib.util.spec_from_file_location("dpone_tools_mssql_benchmark_suite", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_benchmark_suite_builds_independent_export_and_load_worker_matrix(tmp_path: Path) -> None:
    module = _load_suite_module()
    args = Namespace(
        rows="1000",
        partitions="4",
        workers=4,
        export_workers="2,4",
        load_workers="1,3",
        batch_size=500,
        bcp_path="/opt/homebrew/bin/bcp",
        optimizer_profile="high_throughput_safe",
        output_dir=str(tmp_path),
        slo_pg_mssql_rps=None,
        slo_mssql_clickhouse_rps=None,
        clickhouse_bulk_mode="http",
        clickhouse_client_command=None,
        clickhouse_client_host=None,
        clickhouse_client_port=None,
        clickhouse_http_host="127.0.0.1",
        clickhouse_http_port=8123,
        continue_on_fail=False,
        stress_script="tools/mssql_stress.py",
    )

    scenarios = list(module.iter_scenarios(args, tmp_path))

    assert [scenario.name for scenario in scenarios] == [
        "rows1000_p4_ew2_lw1",
        "rows1000_p4_ew2_lw3",
        "rows1000_p4_ew4_lw1",
        "rows1000_p4_ew4_lw3",
    ]
    command = module.build_command(args, scenarios[0])
    assert "--export-workers" in command
    assert command[command.index("--export-workers") + 1] == "2"
    assert "--load-workers" in command
    assert command[command.index("--load-workers") + 1] == "1"
    assert "--optimizer-profile" in command
    assert command[command.index("--optimizer-profile") + 1] == "high_throughput_safe"


def test_benchmark_suite_defaults_to_one_consistent_postgres_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_suite_module()
    monkeypatch.setattr(sys, "argv", ["mssql_benchmark_suite.py", "--output-dir", str(tmp_path)])

    args = module.parse_args()
    scenarios = module.iter_scenarios(args, tmp_path)

    assert scenarios
    assert {(item.partitions, item.export_workers, item.load_workers) for item in scenarios} == {(1, 1, 1)}
    for scenario in scenarios:
        command = module.build_command(args, scenario)
        assert "--partition-column" not in command
        assert "--num-partitions" not in command
        assert "--export-workers" not in command
        assert "--load-workers" not in command


def test_benchmark_suite_certifies_scenario_output(tmp_path: Path) -> None:
    module = _load_suite_module()
    output_file = tmp_path / "scenario.json"
    output_file.write_text(
        """
        {
          "rows": 1000,
          "phase_metrics": [
            {"name": "mssql_to_clickhouse.source_export", "rows": 1000, "seconds": 1.0, "rows_per_second": 1000.0},
            {"name": "mssql_to_clickhouse.target_load_finalize", "rows": 1000, "seconds": 1.0, "rows_per_second": 1000.0}
          ],
          "mssql_count": 1000,
          "clickhouse_count": 1000
        }
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    scenario = module.BenchmarkScenario(
        name="rows1000_p1_ew1_lw1",
        rows=1000,
        partitions=1,
        export_workers=1,
        load_workers=1,
        output_file=str(output_file),
    )
    args = Namespace(
        slo_pg_mssql_rps=None,
        slo_mssql_clickhouse_rps=400.0,
    )

    certification = module.certify_scenario_output(args, scenario)

    assert certification["passed"] is True
    assert certification["bottleneck_phase"] == "mssql_to_clickhouse.source_export"
    assert certification["rows_per_second"]["mssql_to_clickhouse"] == 500.0


def test_benchmark_suite_renders_markdown_evidence(tmp_path: Path) -> None:
    module = _load_suite_module()
    result = module.ScenarioResult(
        name="rows10000_p4_ew2_lw2",
        rows=10_000,
        partitions=4,
        export_workers=2,
        load_workers=2,
        returncode=0,
        seconds=3.25,
        output_file=str(tmp_path / "rows10000.json"),
        certification={
            "passed": True,
            "rows_per_second": {"postgres_to_mssql": 35_000.0, "mssql_to_clickhouse": 21_000.0},
            "bottleneck_phase": "postgres_to_mssql.target_load_finalize",
        },
    )

    markdown = module.render_markdown_summary(
        {
            "failed": False,
            "certification_passed": True,
            "results": [module.asdict(result)],
        },
        generated_at="2026-06-09T18:00:00+00:00",
    )

    assert markdown.startswith("# Native transfer benchmark certification")
    assert "| `rows10000_p4_ew2_lw2` | 10,000 | 4 | 2 | 2 | passed | passed |" in markdown
    assert "35,000.00" in markdown
    assert "postgres_to_mssql.target_load_finalize" in markdown


def test_live_certification_workflow_runs_native_benchmark_suite() -> None:
    workflow = Path(".github/workflows/live-certification.yml").read_text(encoding="utf-8")

    assert "native_benchmark_rows" in workflow
    assert "tools/mssql_benchmark_suite.py" in workflow
    assert "--markdown-output" in workflow
    assert "postgres_mssql_native_benchmark_summary.md" in workflow
