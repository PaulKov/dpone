"""Pure bounded progression policy for the durable workspace witness.

This slice does not perform SQL or enable runtime handover. The application
executor must supply a fresh protected readback for each decision, validated
latest desired content, and locally verified replica agreement. It must recheck
both authorities after executing an action before reporting convergence.
"""

from __future__ import annotations

from enum import StrEnum

from dpone.contracts.airflow_desired_state import AirflowDesiredDeployment
from dpone.contracts.dbt_workspace_channel import WorkspaceHandoverError
from dpone.ports.dbt_workspace_handover import WorkspaceChannelReadback


class WorkspaceHandoverAction(StrEnum):
    """One next bounded operation; none authorizes a force-clear or cancellation."""

    OBSERVE_LATEST = "OBSERVE_LATEST"
    CLAIM_LATEST = "CLAIM_LATEST"
    BEGIN_RETIREMENT = "BEGIN_RETIREMENT"
    FINALIZE_RETIREMENT = "FINALIZE_RETIREMENT"
    PREPARE_SUCCESSOR = "PREPARE_SUCCESSOR"
    REPLICATE_SUCCESSOR = "REPLICATE_SUCCESSOR"
    COMPLETE = "COMPLETE"
    REPLICATE_CURRENT = "REPLICATE_CURRENT"
    CONVERGED = "CONVERGED"


def plan_workspace_handover(
    readback: WorkspaceChannelReadback, *, latest: AirflowDesiredDeployment | None, local_matches: bool
) -> WorkspaceHandoverAction:
    """Finish exact pending claim before comparing newest remote desired content.

    ``local_matches`` means verified pointer/artifacts agree with the pending
    successor when present, otherwise the current snapshot. It never proves ACTIVE.
    WAITING_ATTEMPTS/COMMIT_UNKNOWN refusal belongs to protected finalization; this
    pure planner cannot convert a requested finalization into a success receipt.
    """
    if not isinstance(readback, WorkspaceChannelReadback) or type(local_matches) is not bool:
        raise WorkspaceHandoverError("handover_inputs")
    readback.__post_init__()
    if readback.pending is not None:
        if readback.pending_occurrence is not None:
            return WorkspaceHandoverAction.COMPLETE if local_matches else WorkspaceHandoverAction.REPLICATE_SUCCESSOR
        if readback.current is not None:
            if readback.current.lifecycle.state == "ACTIVE":
                return WorkspaceHandoverAction.BEGIN_RETIREMENT
            if readback.current.lifecycle.state == "RETIRING":
                return WorkspaceHandoverAction.FINALIZE_RETIREMENT
        return WorkspaceHandoverAction.PREPARE_SUCCESSOR
    if latest is None:
        if readback.current is not None and not local_matches:
            return WorkspaceHandoverAction.REPLICATE_CURRENT
        return WorkspaceHandoverAction.OBSERVE_LATEST
    if not isinstance(latest, AirflowDesiredDeployment):
        raise WorkspaceHandoverError("latest_desired")
    latest.__post_init__()
    channel = readback.channel
    if (latest.environment, latest.source.project, latest.source.ref, latest.promotion.registry_scope_id) != (
        channel.environment,
        channel.source_project,
        channel.source_ref,
        channel.registry_scope_id,
    ):
        raise WorkspaceHandoverError("latest_channel")
    current = readback.current
    if current is None or latest.source.occurrence_id != current.request.activation_id:
        return WorkspaceHandoverAction.CLAIM_LATEST
    if latest.sha256 != current.snapshot.desired_state_sha256:
        raise WorkspaceHandoverError("current_desired_changed")
    return WorkspaceHandoverAction.CONVERGED if local_matches else WorkspaceHandoverAction.REPLICATE_CURRENT
