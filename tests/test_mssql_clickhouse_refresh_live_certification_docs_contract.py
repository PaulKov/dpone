from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_mssql_clickhouse_refresh_live_certification_docs_are_self_service() -> None:
    required_fragments = {
        "docs/route-refresh-execute.md": (
            "MSSQL -> ClickHouse refresh executor live certification",
            "DPONE_RUN_REFRESH_EXECUTOR_LIVE=1",
            "test_mssql_clickhouse_refresh_executor_live_integration.py",
            "--executor mssql_clickhouse",
        ),
        "docs/developer-route-refresh-execute.md": (
            "MSSQL -> ClickHouse refresh executor live certification",
            "test_mssql_clickhouse_refresh_executor_live_integration.py",
            "idempotency replay",
            "typed hash",
        ),
        "docs/ci-cd.md": (
            "DPONE_RUN_REFRESH_EXECUTOR_LIVE=1",
            "test_mssql_clickhouse_refresh_executor_live_integration.py",
            "route_refresh_execution.json",
        ),
        "docs/source-sink/mssql-to-clickhouse.md": (
            "MSSQL -> ClickHouse refresh executor live certification",
            "route-refresh-execute",
            "mssql_clickhouse",
        ),
    }

    for relative_path, fragments in required_fragments.items():
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        missing = [fragment for fragment in fragments if fragment not in text]
        assert missing == [], f"{relative_path} missing {missing}"


def test_live_certification_workflow_runs_refresh_executor_gate() -> None:
    workflow = (ROOT / ".github/workflows/live-certification.yml").read_text(encoding="utf-8")

    assert "test_mssql_clickhouse_refresh_executor_live_integration.py" in workflow
    assert "DPONE_RUN_REFRESH_EXECUTOR_LIVE" in workflow
    assert "route-refresh-execute" in workflow
