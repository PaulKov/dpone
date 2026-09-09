"""Exact decision and replay validation for MSSQL workflow failure."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql_failure import MssqlFailureContext, MssqlFailureDecision


def assert_decision_identity(
    context: MssqlFailureContext,
    decision: MssqlFailureDecision,
    *,
    error_type: type[RuntimeError],
) -> None:
    """Require decision identity and journal closure to match locked state."""

    if (
        decision.workflow_plan_sha256,
        decision.workflow_execution_binding_sha256,
    ) != (
        context.workflow_plan_sha256,
        context.workflow_execution_binding_sha256,
    ):
        raise error_type("failure decision identity differs from execution")
    if tuple((item.operation_id, item.attempt_binding_sha256) for item in decision.models) != tuple(
        (item.operation_id, item.attempt_binding_sha256) for item in context.journals
    ):
        raise error_type("failure decision journal closure differs")


def assert_replay(
    context: MssqlFailureContext,
    decision: MssqlFailureDecision,
    *,
    error_type: type[RuntimeError],
) -> None:
    """Require an existing terminal decision to replay byte-exactly."""

    durable = tuple(
        (
            item.operation_id,
            item.attempt_binding_sha256,
            item.persisted_outcome,
            item.persisted_evidence_sha256,
        )
        for item in context.journals
    )
    expected = tuple(
        (
            item.operation_id,
            item.attempt_binding_sha256,
            item.mssql_outcome,
            item.mssql_evidence_sha256,
        )
        for item in decision.models
    )
    if (
        context.terminal_summary_sha256 != decision.terminal_summary_sha256
        or context.terminal_summary_json != decision.terminal_summary_json
        or durable != expected
    ):
        raise error_type("failure decision replay differs")
