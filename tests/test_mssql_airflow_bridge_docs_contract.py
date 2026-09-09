from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_mssql_three_part_names_are_documented() -> None:
    text = " ".join((DOCS / "mssql.md").read_text(encoding="utf-8").split())

    assert "Three-part table names" in text
    assert "database: analytics_staging" in text
    assert "schema: clickhouse" in text
    assert "name: v_dim_example" in text
    assert "analytics_staging.clickhouse" in text
    assert "PostgreSQL and ClickHouse table names remain two-part" in text


def test_runtime_only_airflow_connection_bridge_is_documented() -> None:
    docs = {
        "connections": (DOCS / "connections.md").read_text(encoding="utf-8"),
        "credentials": (DOCS / "getting-started" / "credentials-quickstart.md").read_text(encoding="utf-8"),
        "airflow": (DOCS / "gitops-airflow-runner-pack.md").read_text(encoding="utf-8"),
        "runtime_image": (DOCS / "runtime-image.md").read_text(encoding="utf-8"),
        "developer": (DOCS / "developer-gitops-airflow-runner-pack.md").read_text(encoding="utf-8"),
        "architecture": (DOCS / "architecture.md").read_text(encoding="utf-8"),
    }
    joined = "\n".join(docs.values())

    assert "--airflow-connection-bridge" in joined
    assert "--airflow-connection-secret" in joined
    assert "--airflow-runtime-mode" in joined
    assert "AIRFLOW_CONN_MSSQL_DWH" in joined
    assert "runtime-only" in joined
    assert "connection_bridge" in joined
    assert "GitOpsAirflowConnectionBridgeBuilder" in joined
    assert "airflow_pod_doctor_connection_bridge" in joined
    assert "--target airflow-runtime" in docs["runtime_image"]
