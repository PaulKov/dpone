"""Destructive retention authority for the exact active Airflow cache."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dpone.ports.airflow_desired_state import DesiredStateCheckpointReader
from dpone.runtime.deployment_cache_audit import inspect_promotion_audit
from dpone.runtime.deployment_cache_common import (
    read_regular_json_object,
    regular_file_identity,
    resolve_relative_current_symlink,
)
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
from dpone.runtime.deployment_cache_retention_contracts import (
    AirflowLoaderAcknowledgement,
    DeploymentCacheRetentionApplyError,
)


class DeploymentCacheRetentionAuthority:
    """Validate parser acknowledgement and promotion audit before deletion."""

    def __init__(
        self,
        cache_root: Path,
        *,
        projection_validator: DeploymentCacheProjectionValidator,
        checkpoint_reader: DesiredStateCheckpointReader,
    ) -> None:
        self._cache_root = cache_root
        self._validator = projection_validator
        self._checkpoint_reader = checkpoint_reader

    def validate(self, ack: AirflowLoaderAcknowledgement, *, environment: str) -> None:
        pointer_path = self._cache_root / "current-pointer.json"
        pointer = read_regular_json_object(
            pointer_path,
            missing_code="DPONE_CURRENT_POINTER_NOT_FOUND",
            invalid_code="DPONE_CURRENT_POINTER_INVALID",
            label="current pointer",
            root=self._cache_root,
        )
        current_target = resolve_relative_current_symlink(self._cache_root)
        projection = self._validator.validate_current_details(current_target, environment=environment)
        index_identity = regular_file_identity(
            current_target / "airflow-index.json",
            root=self._cache_root,
            missing_code="DPONE_AIRFLOW_INDEX_NOT_FOUND",
            invalid_code="DPONE_AIRFLOW_INDEX_INVALID",
            label="current airflow index",
        )
        identity_matches = (
            ack.release_id == projection.release_id == pointer.get("release_id")
            and ack.deployment_id == projection.deployment_id == pointer.get("deployment_id")
            and ack.activation_id == pointer.get("activation_id")
            and ack.airflow_index_sha256 == index_identity.sha256
        )
        if (
            not identity_matches
            or ack.fatal
            or ack.error_codes
            or ack.skipped_dag_ids
            or ack.loaded_dag_ids != _expected_dag_ids(projection.airflow_index)
        ):
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_ACK_INVALID",
                "loader acknowledgement does not prove the exact active DAG inventory",
                path=pointer_path.as_posix(),
            )
        blocking_issue = next(
            (
                item
                for item in inspect_promotion_audit(
                    self._cache_root / "current-pointer-audit.jsonl",
                    expected_pointer=pointer,
                    environment=environment,
                )
                if item.severity == "error"
            ),
            None,
        )
        if blocking_issue is not None:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_PROMOTION_AUDIT_INVALID",
                "promotion audit does not prove the active deployment",
                path=blocking_issue.path,
                details={"cause_code": blocking_issue.code},
            )
        self._validate_desired_checkpoint(ack, environment=environment)

    def _validate_desired_checkpoint(self, ack: AirflowLoaderAcknowledgement, *, environment: str) -> None:
        path = self._cache_root / "status" / "desired-state-checkpoint.json"
        try:
            checkpoint = self._checkpoint_reader.read()
        except (OSError, ValueError) as exc:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_DESIRED_CHECKPOINT_INVALID",
                "desired-state checkpoint is unreadable or invalid",
                path=path.as_posix(),
            ) from exc
        if checkpoint is None:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_DESIRED_CHECKPOINT_REQUIRED",
                "destructive retention requires the current desired-state checkpoint",
                path=path.as_posix(),
            )
        if (
            checkpoint.environment != environment
            or checkpoint.release_id != ack.release_id
            or checkpoint.deployment_id != ack.deployment_id
            or checkpoint.activation_id != ack.activation_id
            or checkpoint.occurrence_id != ack.activation_id
            or checkpoint.airflow_index_sha256 != ack.airflow_index_sha256
            or checkpoint.expected_dag_ids != ack.loaded_dag_ids
        ):
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_DESIRED_CHECKPOINT_MISMATCH",
                "desired state, current deployment and loader acknowledgement do not converge",
                path=path.as_posix(),
            )


def _expected_dag_ids(index: Mapping[str, object]) -> tuple[str, ...]:
    specs = index.get("dag_specs")
    if not isinstance(specs, list):
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_ACK_INVALID",
            "active airflow index has no canonical DAG inventory",
        )
    dag_ids = tuple(
        sorted(
            str(item.get("id"))
            for item in specs
            if isinstance(item, Mapping) and isinstance(item.get("id"), str) and item.get("id")
        )
    )
    if len(dag_ids) != len(specs) or len(dag_ids) != len(set(dag_ids)):
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_ACK_INVALID",
            "active airflow index DAG inventory is ambiguous",
        )
    return dag_ids


__all__ = ["DeploymentCacheRetentionAuthority"]
