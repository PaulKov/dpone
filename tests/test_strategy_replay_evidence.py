from __future__ import annotations

import json
from pathlib import Path

from dpone.strategy_intelligence.replay import ReplayExecutionRequest, ReplayExecutionService
from dpone.strategy_intelligence.replay_adapters import ReplayAdapterRegistry, ReplayBackendResult


def test_replay_execution_writes_json_and_markdown_evidence(tmp_path: Path) -> None:
    service = ReplayExecutionService(
        artifact_dir=tmp_path,
        adapter_registry=ReplayAdapterRegistry(backend=_EvidenceBackend()),
    )

    result = service.execute(
        ReplayExecutionRequest(
            action="resync",
            run_id="01JREPLAYEVIDENCE000000001",
            source_type="postgres",
            sink_type="mssql",
            strategy_mode="incremental_merge",
            partitions=("2026-06-05",),
            yes=True,
        )
    )

    json_path = tmp_path / "resync_01JREPLAYEVIDENCE000000001_evidence.json"
    md_path = tmp_path / "resync_01JREPLAYEVIDENCE000000001_evidence.md"

    assert json_path.exists()
    assert md_path.exists()
    evidence = json.loads(json_path.read_text(encoding="utf-8"))

    assert evidence["schema_version"] == "dpone.replay.evidence.v1"
    assert evidence["action"] == "resync"
    assert evidence["service"] == "mssql"
    assert evidence["run_id"] == "01JREPLAYEVIDENCE000000001"
    assert evidence["artifact_path"] == str(result.artifact_path)
    assert evidence["runbook"] == "docs/testing/replay-integration.md"
    assert evidence["commands"] == ["dpone resync --run-id 01JREPLAYEVIDENCE000000001 --partitions 2026-06-05 --yes"]
    assert evidence["operations"] == [
        "validate_staging:incremental_merge",
        "execute_finalizer:mssql:incremental_merge",
        "reconcile:mssql:incremental_merge",
        "commit_state:01JREPLAYEVIDENCE000000001",
    ]
    assert evidence["row_count_checks"] == [{"name": "staging_rows", "value": 10000}]
    assert evidence["status_checks"] == [
        {"name": "staging_exists", "status": "passed"},
        {"name": "finalizer", "status": "passed"},
        {"name": "reconciliation", "status": "passed"},
        {"name": "state_commit", "status": "passed"},
    ]

    md = md_path.read_text(encoding="utf-8")
    assert "# Replay evidence: resync 01JREPLAYEVIDENCE000000001" in md
    assert "[Replay integration runbook](../../docs/testing/replay-integration.md)" in md
    assert "| staging_rows | 10000 |" in md
    assert "| state_commit | passed |" in md


class _EvidenceBackend:
    def validate_staging(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        del request
        return ReplayBackendResult(
            True,
            "staging exists",
            details={
                "status_check": {"name": "staging_exists", "status": "passed"},
                "row_count_check": {"name": "staging_rows", "value": 10000},
            },
        )

    def execute_finalizer(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        del request
        return ReplayBackendResult(
            True,
            "finalizer executed",
            details={"status_check": {"name": "finalizer", "status": "passed"}},
        )

    def produce_replay_events(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        del request
        return ReplayBackendResult(False, "not used")

    def reconcile(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        del request
        return ReplayBackendResult(
            True,
            "reconciliation passed",
            details={"status_check": {"name": "reconciliation", "status": "passed"}},
        )

    def commit_state(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        del request
        return ReplayBackendResult(
            True,
            "state committed",
            details={"status_check": {"name": "state_commit", "status": "passed"}},
        )
