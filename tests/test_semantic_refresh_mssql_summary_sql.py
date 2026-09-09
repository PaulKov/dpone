"""SQL-shape regressions for durable MSSQL workflow summaries."""

from __future__ import annotations

from dpone.adapters.semantic_refresh_mssql_workflow_summary import (
    MssqlSemanticRefreshWorkflowSummaryState,
)


class _Cursor:
    def __init__(self, rows: list[tuple[object, ...]] | None = None) -> None:
        self.sql = ""
        self.rows = [] if rows is None else rows

    def execute(self, sql: str, *_parameters: object) -> _Cursor:
        self.sql = sql
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


def test_workflow_summary_checkpoint_join_avoids_reserved_identifier_alias() -> None:
    cursor = _Cursor()
    state = MssqlSemanticRefreshWorkflowSummaryState(lambda: None)  # type: ignore[arg-type,return-value]

    assert state._assert_journals(cursor, "workflow-1", []) == ()  # type: ignore[arg-type]

    assert " AS checkpoint " not in cursor.sql
    assert "checkpoint_state.checkpoint_sha256" in cursor.sql
    assert "AS checkpoint_state" in cursor.sql


def test_terminal_summary_accepts_uppercase_uuid_from_sql_server_driver() -> None:
    operation_id = "sha256:" + "1" * 64
    operation_plan = "sha256:" + "2" * 64
    attempt = "sha256:" + "3" * 64
    artifact = "sha256:" + "4" * 64
    clickhouse_receipt = "sha256:" + "5" * 64
    terminal_receipt = "sha256:" + "6" * 64
    cursor = _Cursor(
        [
            (
                operation_id,
                operation_plan,
                attempt,
                "COMPLETE",
                artifact,
                clickhouse_receipt,
                terminal_receipt,
                1,
                1,
                "DWH.mart.events",
                "sha256:" + "7" * 64,
                "0198F11C-6956-74F2-984B-4CFCB1653B87",
                "NOT_REQUIRED_EMPTY_SCOPE",
                "sha256:" + "8" * 64,
                "sha256:" + "9" * 64,
                1,
            )
        ]
    )
    state = MssqlSemanticRefreshWorkflowSummaryState(lambda: None)  # type: ignore[arg-type,return-value]
    publications = [
        {
            "operation_id": operation_id,
            "operation_plan_sha256": operation_plan,
            "attempt_binding_sha256": attempt,
            "artifact_manifest_sha256": artifact,
            "clickhouse_terminal_receipt_sha256": clickhouse_receipt,
            "terminal_receipt_sha256": terminal_receipt,
            "target_generation": 1,
            "scope_revision": 1,
        }
    ]

    assert state._assert_terminal_journals(cursor, "workflow-1", publications) == (  # type: ignore[arg-type]
        "DWH.mart.events",
    )
