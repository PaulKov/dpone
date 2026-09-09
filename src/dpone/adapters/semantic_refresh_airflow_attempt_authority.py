"""Worker-time Airflow/Kubernetes attempt authority for atomic MSSQL admission."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from importlib import import_module
from typing import Any
from uuid import UUID

from dpone.ports.semantic_refresh_mssql_worker_admission import (
    MssqlTrustedAttemptCoordinate,
    MssqlWorkerAdmissionCoordinates,
)


class SemanticRefreshAirflowAttemptAuthorityError(RuntimeError):
    """Raised when the actual scheduler/pod attempt cannot be proven."""


class AirflowKubernetesWorkerAttemptAuthority:
    """Read one actual Airflow task attempt and a Downward-API pod UID.

    Construction is parse-inert. The Airflow SDK context and the bounded,
    no-follow pod identity file are read only when the dbt worker admits its
    actual logical DagRun.
    """

    def __init__(
        self,
        *,
        pod_uid_reader: Callable[[], bytes],
        expected_task_id: str = "semantic_refresh__dbt_build_test",
    ) -> None:
        if not callable(pod_uid_reader):
            raise TypeError("pod_uid_reader must be callable")
        if not isinstance(expected_task_id, str) or not expected_task_id.strip():
            raise ValueError("expected_task_id must be non-empty")
        self._pod_uid_reader = pod_uid_reader
        self._expected_task_id = expected_task_id

    def load_attempts(
        self,
        *,
        plan_bundle_sha256: str,
        workflow_execution_id: str,
        operation_ids: tuple[str, ...],
    ) -> MssqlWorkerAdmissionCoordinates:
        """Return one canonical operation closure bound to the current worker."""

        _digest(plan_bundle_sha256, "plan_bundle_sha256")
        if not isinstance(workflow_execution_id, str) or not workflow_execution_id.strip():
            raise ValueError("workflow_execution_id must be non-empty")
        if (
            not operation_ids
            or operation_ids != tuple(sorted(set(operation_ids)))
            or any(_is_digest(operation_id) is False for operation_id in operation_ids)
        ):
            raise ValueError("operation_ids must be a canonical non-empty digest closure")
        context = _airflow_context()
        dag_run = _text(_attribute_or_key(context.get("dag_run"), "run_id"), "run_id")
        task_instance = context.get("task_instance") or context.get("ti")
        task_id = _text(_attribute_or_key(task_instance, "task_id"), "task_id")
        try_number = _positive_int(_attribute_or_key(task_instance, "try_number"), "try_number")
        if dag_run != workflow_execution_id or task_id != self._expected_task_id:
            raise SemanticRefreshAirflowAttemptAuthorityError(
                "actual Airflow DagRun/task differs from semantic-refresh worker authority"
            )
        pod_uid = _pod_uid(self._pod_uid_reader())
        return MssqlWorkerAdmissionCoordinates(
            tuple(
                MssqlTrustedAttemptCoordinate(
                    operation_id=operation_id,
                    task_id=task_id,
                    try_number=try_number,
                    pod_uid=pod_uid,
                )
                for operation_id in operation_ids
            )
        )


def _airflow_context() -> Mapping[str, Any]:
    try:
        value = import_module("airflow.sdk").get_current_context()
    except (AttributeError, ModuleNotFoundError, RuntimeError) as exc:
        raise SemanticRefreshAirflowAttemptAuthorityError("actual Airflow worker context is unavailable") from exc
    if not isinstance(value, Mapping):
        raise SemanticRefreshAirflowAttemptAuthorityError("actual Airflow worker context is invalid")
    return value


def _attribute_or_key(value: object, field_name: str) -> object:
    if isinstance(value, Mapping):
        result = value.get(field_name)
    else:
        result = getattr(value, field_name, None)
    if result is None:
        raise SemanticRefreshAirflowAttemptAuthorityError(f"actual Airflow {field_name} is unavailable")
    return result


def _pod_uid(raw: bytes) -> str:
    try:
        value = raw.decode("ascii").strip()
        canonical = str(UUID(value))
    except (UnicodeDecodeError, ValueError, AttributeError) as exc:
        raise SemanticRefreshAirflowAttemptAuthorityError("Downward-API pod UID is invalid") from exc
    if canonical != value:
        raise SemanticRefreshAirflowAttemptAuthorityError("Downward-API pod UID is not canonical")
    return canonical


def _positive_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SemanticRefreshAirflowAttemptAuthorityError(f"actual Airflow {field_name} is invalid")
    return value


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SemanticRefreshAirflowAttemptAuthorityError(f"actual Airflow {field_name} is invalid")
    return value


def _digest(value: object, field_name: str) -> str:
    if not _is_digest(value):
        raise ValueError(f"{field_name} must be a canonical lowercase sha256 digest")
    return str(value)


def _is_digest(value: object) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 71
        and value.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in value[7:])
    )


__all__ = [
    "AirflowKubernetesWorkerAttemptAuthority",
    "SemanticRefreshAirflowAttemptAuthorityError",
]
