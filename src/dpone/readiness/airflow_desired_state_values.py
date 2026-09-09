"""Scalar parsing for Airflow desired-state cache pointers."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol

from dpone.ports.airflow_desired_state import DesiredStateReconcilePortError
from dpone.runtime.deployment_cache import DeploymentCacheError
from dpone.services.airflow_desired_state_reconcile import DesiredStateReconcileError

if TYPE_CHECKING:
    from dpone.contracts.airflow_desired_state_reconcile import DesiredStateReconcileEvidence


class DesiredStateReconcileEvidenceStore(Protocol):
    """Minimal durable status sink required by the reconcile reporter."""

    def commit_status(self, payload: bytes) -> None: ...


def required_text(value: Mapping[str, object], field: str) -> str:
    """Read one required current-pointer field."""

    candidate = value.get(field)
    if not isinstance(candidate, str) or not candidate:
        raise DeploymentCacheError(
            "DPONE_CURRENT_POINTER_INVALID",
            "current pointer is incomplete",
        )
    return candidate


def optional_text(value: object) -> str | None:
    """Read one optional non-empty text value."""

    return value if isinstance(value, str) and value else None


def cache_reconcile_error(exc: DeploymentCacheError) -> DesiredStateReconcileError:
    """Project a cache failure onto the stable reconcile error contract."""

    return DesiredStateReconcileError(
        exc.code,
        "desired-state reconcile lock or cache guard failed",
        state_may_have_changed=exc.code
        in {"DPONE_CACHE_PROMOTION_WRITE_FAILED", "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED"},
    )


def commit_running_status(store: DesiredStateReconcileEvidenceStore) -> None:
    """Publish a fail-closed marker before a reconcile cycle mutates state."""

    body = _error_status(
        "DPONE_AIRFLOW_DESIRED_STATE_RECONCILE_IN_PROGRESS",
        "Airflow desired-state reconcile is in progress.",
        state_may_have_changed=False,
    )
    try:
        store.commit_status(body)
    except (OSError, ValueError, DeploymentCacheError, DesiredStateReconcilePortError) as exc:
        raise DesiredStateReconcileError(
            "DPONE_AIRFLOW_DESIRED_STATE_STATUS_FAILED",
            "reconcile could not mark its cycle in progress",
        ) from exc


def commit_success_status(
    store: DesiredStateReconcileEvidenceStore,
    evidence: DesiredStateReconcileEvidence,
) -> None:
    """Commit successful reconcile evidence or replace it with a failure marker."""

    try:
        store.commit_status(evidence.to_json_bytes())
    except (OSError, ValueError, DeploymentCacheError, DesiredStateReconcilePortError) as exc:
        error = DesiredStateReconcileError(
            "DPONE_AIRFLOW_DESIRED_STATE_STATUS_FAILED",
            "reconcile completed but its latest status could not be committed",
            state_may_have_changed=evidence.activated,
        )
        commit_failure_status(store, error)
        raise error from exc


def commit_failure_status(
    store: DesiredStateReconcileEvidenceStore,
    error: DesiredStateReconcileError,
) -> None:
    """Publish credential-free failure evidence for the latest cycle."""

    body = _error_status(
        error.code,
        "Airflow desired-state operation failed.",
        state_may_have_changed=error.state_may_have_changed,
    )
    try:
        store.commit_status(body)
    except (OSError, ValueError, DesiredStateReconcilePortError) as exc:
        raise DesiredStateReconcileError(
            "DPONE_AIRFLOW_DESIRED_STATE_STATUS_FAILED",
            "failed reconcile status could not be committed",
            state_may_have_changed=error.state_may_have_changed,
        ) from exc


def _error_status(code: str, message: str, *, state_may_have_changed: bool) -> bytes:
    return json.dumps(
        {
            "schema": "dpone.error.v1",
            "passed": False,
            "errors": [{"code": code, "message": message}],
            "state_may_have_changed": state_may_have_changed,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
