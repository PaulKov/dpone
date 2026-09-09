from __future__ import annotations

import hashlib
import inspect
import json

import pytest

from dpone.contracts.semantic_refresh_core import SemanticRefreshContractError
from dpone.contracts.semantic_refresh_failure_summary import (
    SemanticRefreshFailedWorkflowSummary,
)
from dpone.services.semantic_refresh_mssql_failure import (
    SemanticRefreshMssqlFailureTerminalizationService,
)


def _summary(outcome: str = "COMMITTED_WITH_IMAGES") -> dict[str, object]:
    operation_id = "sha256:" + "5" * 64
    unsigned: dict[str, object] = {
        "schema": "dpone.semantic-refresh-failed-workflow-summary.v1",
        "workflow_id": "failed-workflow",
        "workflow_plan_sha256": "sha256:" + "1" * 64,
        "workflow_execution_binding_sha256": "sha256:" + "2" * 64,
        "expected_operation_ids": [operation_id],
        "models": [
            {
                "operation_id": operation_id,
                "attempt_binding_sha256": "sha256:" + "3" * 64,
                "mssql_outcome": outcome,
                "mssql_evidence_sha256": "sha256:" + "4" * 64,
            }
        ],
        "status": "FAILED_PRE_COMMIT",
    }
    raw = json.dumps(unsigned, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return {**unsigned, "terminal_summary_sha256": "sha256:" + hashlib.sha256(raw).hexdigest()}


def test_failure_summary_is_canonical_and_closed() -> None:
    summary = _summary()
    assert SemanticRefreshFailedWorkflowSummary.from_mapping(summary).to_dict() == summary

    with pytest.raises(SemanticRefreshContractError, match="fields"):
        SemanticRefreshFailedWorkflowSummary.from_mapping({**summary, "airflow_state": "failed"})


def test_commit_unknown_can_never_authorize_replacement() -> None:
    with pytest.raises(SemanticRefreshContractError, match="unresolved MSSQL outcome"):
        SemanticRefreshFailedWorkflowSummary.from_mapping(_summary("COMMIT_UNKNOWN"))


def test_failure_summary_digest_detects_per_model_outcome_drift() -> None:
    summary = _summary()
    changed = dict(summary)
    changed["models"] = [
        {
            **summary["models"][0],  # type: ignore[index]
            "mssql_outcome": "ROLLED_BACK",
        }
    ]
    with pytest.raises(SemanticRefreshContractError, match="differs"):
        SemanticRefreshFailedWorkflowSummary.from_mapping(changed)


def test_failure_terminalization_public_api_accepts_no_caller_outcome_or_digest() -> None:
    parameters = inspect.signature(SemanticRefreshMssqlFailureTerminalizationService.terminalize).parameters
    assert tuple(parameters) == ("self", "workflow_id")
