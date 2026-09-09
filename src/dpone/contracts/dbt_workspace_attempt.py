"""Exact task-attempt fencing inside one active dbt workspace occurrence."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest
from dpone.contracts.credential_env import is_valid_connection_ref
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationError, DbtWorkspaceGuardEpoch

_TOKEN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_STATES = frozenset({"RUNNING", "SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"})
DbtWorkspaceAttemptTerminalState = Literal["SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"]
DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV = "DPONE_DBT_WORKSPACE_AUTHORITY_CONNECTION_REF"


def require_workspace_authority_connection_ref(value: object) -> str:
    """Require a logical projected control-plane binding, never target credentials."""

    if not isinstance(value, str) or not is_valid_connection_ref(value):
        raise DbtWorkspaceActivationError("workspace_authority_connection_ref")
    return value


@dataclass(frozen=True, slots=True)
class DbtWorkspaceAttemptRequest:
    """One immutable Airflow task attempt and its complete write subset."""

    activation_id: str
    attempt_id: str
    workflow_id: str
    write_subjects: tuple[str, ...]
    request_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.activation_id, str) or not self.activation_id:
            raise DbtWorkspaceActivationError("attempt_activation")
        if not is_canonical_sha256_digest(self.attempt_id):
            raise DbtWorkspaceActivationError("attempt_id")
        if _TOKEN.fullmatch(self.workflow_id) is None:
            raise DbtWorkspaceActivationError("attempt_workflow")
        if (
            not isinstance(self.write_subjects, tuple)
            or not self.write_subjects
            or tuple(sorted(self.write_subjects)) != self.write_subjects
            or len(set(self.write_subjects)) != len(self.write_subjects)
            or any(not is_canonical_sha256_digest(item) for item in self.write_subjects)
        ):
            raise DbtWorkspaceActivationError("attempt_write_closure")
        if not is_canonical_sha256_digest(self.request_sha256) or self.request_sha256 != canonical_fingerprint(
            self._unsigned()
        ):
            raise DbtWorkspaceActivationError("attempt_subject")

    @classmethod
    def build(
        cls,
        *,
        activation_id: str,
        attempt_id: str,
        workflow_id: str,
        write_subjects: tuple[str, ...],
    ) -> DbtWorkspaceAttemptRequest:
        unsigned = _request_dict(
            activation_id=activation_id,
            attempt_id=attempt_id,
            workflow_id=workflow_id,
            write_subjects=write_subjects,
        )
        return cls(
            activation_id=activation_id,
            attempt_id=attempt_id,
            workflow_id=workflow_id,
            write_subjects=write_subjects,
            request_sha256=canonical_fingerprint(unsigned),
        )

    def _unsigned(self) -> dict[str, object]:
        return _request_dict(
            activation_id=self.activation_id,
            attempt_id=self.attempt_id,
            workflow_id=self.workflow_id,
            write_subjects=self.write_subjects,
        )


@dataclass(frozen=True, slots=True)
class DbtWorkspaceAttemptReceipt:
    """Durable readback binding an attempt to the activation's exact epochs."""

    activation_id: str
    attempt_id: str
    request_sha256: str
    state: str
    guard_epochs: tuple[DbtWorkspaceGuardEpoch, ...]
    receipt_sha256: str

    def __post_init__(self) -> None:
        if not self.activation_id or not is_canonical_sha256_digest(self.attempt_id):
            raise DbtWorkspaceActivationError("attempt_receipt_identity")
        if not is_canonical_sha256_digest(self.request_sha256) or self.state not in _STATES:
            raise DbtWorkspaceActivationError("attempt_receipt_state")
        if (
            not isinstance(self.guard_epochs, tuple)
            or not self.guard_epochs
            or tuple(sorted(self.guard_epochs)) != self.guard_epochs
            or len({item.guard_id for item in self.guard_epochs}) != len(self.guard_epochs)
        ):
            raise DbtWorkspaceActivationError("attempt_receipt_guards")
        if not is_canonical_sha256_digest(self.receipt_sha256) or self.receipt_sha256 != canonical_fingerprint(
            self._unsigned()
        ):
            raise DbtWorkspaceActivationError("attempt_receipt_subject")

    @classmethod
    def build(
        cls,
        *,
        request: DbtWorkspaceAttemptRequest,
        state: str,
        guard_epochs: tuple[DbtWorkspaceGuardEpoch, ...],
    ) -> DbtWorkspaceAttemptReceipt:
        unsigned = _receipt_dict(request=request, state=state, guard_epochs=guard_epochs)
        return cls(
            activation_id=request.activation_id,
            attempt_id=request.attempt_id,
            request_sha256=request.request_sha256,
            state=state,
            guard_epochs=guard_epochs,
            receipt_sha256=canonical_fingerprint(unsigned),
        )

    def _unsigned(self) -> dict[str, object]:
        return {
            "schema": "dpone.dbt-workspace-attempt-receipt.v1",
            "activation_id": self.activation_id,
            "attempt_id": self.attempt_id,
            "request_sha256": self.request_sha256,
            "state": self.state,
            "guard_epochs": [item.to_dict() for item in self.guard_epochs],
        }


def require_attempt_receipt(
    receipt: DbtWorkspaceAttemptReceipt,
    request: DbtWorkspaceAttemptRequest,
    *,
    state: str,
) -> DbtWorkspaceAttemptReceipt:
    """Reject a stale, foreign, partially fenced or differently terminal attempt."""

    receipt.__post_init__()
    if (
        receipt.activation_id != request.activation_id
        or receipt.attempt_id != request.attempt_id
        or receipt.request_sha256 != request.request_sha256
        or receipt.state != state
    ):
        raise DbtWorkspaceActivationError("attempt_occurrence_mismatch")
    return receipt


def _request_dict(
    *, activation_id: str, attempt_id: str, workflow_id: str, write_subjects: tuple[str, ...]
) -> dict[str, object]:
    return {
        "schema": "dpone.dbt-workspace-attempt-request.v1",
        "activation_id": activation_id,
        "attempt_id": attempt_id,
        "workflow_id": workflow_id,
        "write_subjects": list(write_subjects),
    }


def _receipt_dict(
    *, request: DbtWorkspaceAttemptRequest, state: str, guard_epochs: tuple[DbtWorkspaceGuardEpoch, ...]
) -> dict[str, object]:
    return {
        "schema": "dpone.dbt-workspace-attempt-receipt.v1",
        "activation_id": request.activation_id,
        "attempt_id": request.attempt_id,
        "request_sha256": request.request_sha256,
        "state": state,
        "guard_epochs": [item.to_dict() for item in guard_epochs],
    }


__all__ = [
    "DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV",
    "DbtWorkspaceAttemptReceipt",
    "DbtWorkspaceAttemptRequest",
    "DbtWorkspaceAttemptTerminalState",
    "require_attempt_receipt",
    "require_workspace_authority_connection_ref",
]
