"""Ports for exact dbt workspace task-attempt fencing."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from dpone.contracts.dbt_workspace_control import (
    AirflowAttemptCorrelation,
    AirflowRunIdentity,
    DbtExecutionPack,
    DbtWorkspaceAttemptReceipt,
    DbtWorkspaceAttemptRequest,
    DbtWorkspaceAttemptTerminalState,
)


class DbtWorkspaceAttemptRequestFactoryPort(Protocol):
    """Build an attempt from authenticated activation, pack and parsed manifest."""

    def build(
        self,
        *,
        pack: DbtExecutionPack,
        manifest: Mapping[str, object],
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
    ) -> DbtWorkspaceAttemptRequest: ...


class DbtWorkspaceAttemptAdmissionPort(Protocol):
    """Durably admit and terminalize one exact task attempt."""

    def admit(self, request: DbtWorkspaceAttemptRequest) -> DbtWorkspaceAttemptReceipt: ...

    def terminalize(
        self,
        request: DbtWorkspaceAttemptRequest,
        *,
        state: DbtWorkspaceAttemptTerminalState,
    ) -> DbtWorkspaceAttemptReceipt: ...


__all__ = [
    "DbtWorkspaceAttemptAdmissionPort",
    "DbtWorkspaceAttemptReceipt",
    "DbtWorkspaceAttemptRequest",
    "DbtWorkspaceAttemptRequestFactoryPort",
    "DbtWorkspaceAttemptTerminalState",
]
