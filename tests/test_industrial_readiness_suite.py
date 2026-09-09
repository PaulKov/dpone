from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.industrial_readiness import IndustrialReadinessService


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_industrial_readiness_passes_when_all_domains_and_required_cases_are_green(tmp_path: Path) -> None:
    artifacts = {
        "local_matrix": _write_json(
            tmp_path / "matrix.json",
            {
                "passed": True,
                "cases": [
                    {
                        "source": "postgres",
                        "sink": "mssql",
                        "strategy": "incremental_merge",
                        "row_count": 10000,
                        "changed_pct": 20,
                        "deleted_pct": 5,
                        "wide_columns": 120,
                    },
                    {
                        "source": "mssql",
                        "sink": "clickhouse",
                        "strategy": "partition_replace",
                        "row_count": 10000,
                        "changed_pct": 20,
                        "deleted_pct": 5,
                        "wide_columns": 120,
                    },
                ],
            },
        ),
        "correctness": _write_json(tmp_path / "correctness.json", {"passed": True, "hash_mismatches": 0}),
        "reliability": _write_json(tmp_path / "reliability.json", {"passed": True, "resume_failures": 0}),
        "performance_lab": _write_json(tmp_path / "performance.json", {"passed": True, "regressions": []}),
        "ux": _write_json(tmp_path / "ux.json", {"passed": True, "commands": ["doctor", "plan", "run"]}),
        "governance": _write_json(tmp_path / "governance.json", {"passed": True, "violations": []}),
        "schema_evolution": _write_json(tmp_path / "schema_evolution.json", {"passed": True, "changes": []}),
    }

    report = IndustrialReadinessService().evaluate(
        output_dir=tmp_path / "industrial",
        release="v0.5.1",
        artifacts=artifacts,
        required_matrix_cases=(
            "postgres:mssql:incremental_merge",
            "mssql:clickhouse:partition_replace",
        ),
    )

    assert report.passed is True
    assert report.level == "industrial_ready"
    assert report.score == 100.0
    assert report.blockers == ()
    assert report.matrix.total_cases == 2
    assert report.matrix.min_row_count == 10000
    assert report.matrix.max_column_count == 120
    assert (tmp_path / "industrial" / "industrial_readiness.json").exists()
    assert (tmp_path / "industrial" / "industrial_readiness.md").exists()


def test_industrial_readiness_blocks_missing_domains_and_missing_matrix_cases(tmp_path: Path) -> None:
    matrix = _write_json(
        tmp_path / "matrix.json",
        {
            "passed": True,
            "cases": [{"source": "postgres", "sink": "mssql", "strategy": "incremental_merge", "row_count": 10000}],
        },
    )
    correctness = _write_json(tmp_path / "correctness.json", {"passed": False, "blockers": ["hash.mismatch"]})

    report = IndustrialReadinessService().evaluate(
        output_dir=tmp_path / "industrial",
        release="v0.5.1",
        artifacts={"local_matrix": matrix, "correctness": correctness},
        required_domains=("local_matrix", "correctness", "governance"),
        required_matrix_cases=(
            "postgres:mssql:incremental_merge",
            "mssql:clickhouse:partition_replace",
        ),
    )

    assert report.passed is False
    assert report.level == "blocked"
    assert report.blockers == (
        "correctness.not_passed",
        "governance.missing",
        "local_matrix.case_missing:mssql:clickhouse:partition_replace",
    )
    assert "Fix industrial readiness blockers" in report.to_markdown()
