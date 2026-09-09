"""Pure plan-first policy for reproducible Airflow workload reruns."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_run_identity import (
    AirflowBundleRunIdentity,
    AirflowRunIdentity,
    AirflowRunIdentityError,
)
from dpone.readiness.airflow_bundle_identity import airflow_bundle_identity

RERUN_PLAN_SCHEMA = "dpone.airflow-rerun-plan.v1"
_SELECTIONS = frozenset({"original", "latest"})


def parse_airflow_run_identity(value: object) -> AirflowRunIdentity:
    """Parse the canonical identity at the rerun domain boundary."""

    return AirflowRunIdentity.from_mapping(value)


@dataclass(frozen=True, slots=True)
class AirflowRerunIssue:
    code: str
    message: str
    path: str
    severity: str

    def to_dict(self) -> dict[str, str]:
        return {
            "schema": "dpone.error.v1",
            "code": self.code,
            "stage": "airflow_rerun_plan",
            "severity": self.severity,
            "message": self.message,
            "path": self.path,
        }


@dataclass(frozen=True, slots=True)
class AirflowRerunPlan:
    source_attempt: Mapping[str, str]
    bundle_selection: str
    artifact_selection: str
    critical: bool
    resolved_identity: AirflowRunIdentity
    execution_mode: str
    run_on_latest_version: bool
    warnings: tuple[AirflowRerunIssue, ...] = ()
    blockers: tuple[AirflowRerunIssue, ...] = ()

    @property
    def passed(self) -> bool:
        return not self.blockers

    @property
    def semantic_fingerprint(self) -> str:
        encoded = json.dumps(self.to_dict(), ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": RERUN_PLAN_SCHEMA,
            "status": "ready" if self.passed else "blocked",
            "critical": self.critical,
            "source_attempt": dict(self.source_attempt),
            "selection": {
                "bundle": self.bundle_selection,
                "artifacts": self.artifact_selection,
            },
            "resolved": self.resolved_identity.to_dict(),
            "airflow_request": {
                "run_on_latest_version": self.run_on_latest_version,
                "execution_mode": self.execution_mode,
            },
            "retention_refs": {
                "release_ids": [self.resolved_identity.release_id],
                "deployment_ids": [self.resolved_identity.deployment_id],
            },
            "warnings": [warning.to_dict() for warning in self.warnings],
            "blockers": [blocker.to_dict() for blocker in self.blockers],
        }


class AirflowRerunPlanner:
    """Select bundle and dpone artifacts independently without external I/O."""

    def plan(
        self,
        *,
        original_identity: AirflowRunIdentity,
        source_attempt: Mapping[str, object],
        current_index: Mapping[str, Any],
        bundle_selection: str = "original",
        artifact_selection: str = "original",
        critical: bool = False,
        original_artifacts_available: bool = True,
        original_artifacts_error_code: str = "DPONE_DEPLOYMENT_EXPIRED",
    ) -> AirflowRerunPlan:
        _require_selection("bundle", bundle_selection)
        _require_selection("artifacts", artifact_selection)
        latest_identity = _latest_identity(original_identity, current_index)
        artifact_identity = original_identity if artifact_selection == "original" else latest_identity
        selected_bundle = (
            original_identity.airflow_bundle if bundle_selection == "original" else latest_identity.airflow_bundle
        )
        resolved_identity = _with_bundle(artifact_identity, selected_bundle)
        warnings: list[AirflowRerunIssue] = []
        blockers: list[AirflowRerunIssue] = []
        if artifact_selection == "original" and not original_artifacts_available:
            blockers.append(
                _issue(
                    code=original_artifacts_error_code,
                    message=_original_artifacts_unavailable_message(original_artifacts_error_code),
                    path="resolved.deployment_id",
                    severity="error",
                )
            )
        reproducibility_issue = _bundle_reproducibility_issue(
            selected_bundle,
            bundle_selection=bundle_selection,
            critical=critical,
        )
        if reproducibility_issue is not None:
            if reproducibility_issue.severity == "error":
                blockers.append(reproducibility_issue)
            else:
                warnings.append(reproducibility_issue)
        if critical and not _has_complete_deployment_identity(resolved_identity):
            blockers.append(
                _issue(
                    code="DPONE_RERUN_NOT_REPRODUCIBLE",
                    message="Critical rerun requires complete content-addressed deployment identity",
                    path="resolved",
                    severity="error",
                )
            )
        return AirflowRerunPlan(
            source_attempt=_attempt(source_attempt),
            bundle_selection=bundle_selection,
            artifact_selection=artifact_selection,
            critical=critical,
            resolved_identity=resolved_identity,
            execution_mode=(
                "clear_existing_run"
                if bundle_selection == "original" and artifact_selection == "original"
                else "create_pinned_rerun"
            ),
            run_on_latest_version=bundle_selection == "latest",
            warnings=tuple(warnings),
            blockers=tuple(blockers),
        )


def _latest_identity(original: AirflowRunIdentity, index: Mapping[str, Any]) -> AirflowRunIdentity:
    if index.get("schema") != "dpone.airflow-deployment-index.v1":
        raise ValueError("DPONE_AIRFLOW_INDEX_SCHEMA_INVALID: current index schema is invalid")
    if original.dag_spec is None:
        dag_spec: dict[str, str] | None = None
    else:
        dag_spec = _indexed_artifact(index.get("dag_specs"), original.dag_spec.id, section="dag_specs")
    workload_pack = _indexed_artifact(
        index.get("workload_packs"),
        original.workload_pack.id,
        section="workload_packs",
    )
    return AirflowRunIdentity.from_mapping(
        {
            "schema": "dpone.airflow-run-identity.v1",
            "release_id": index.get("release_id"),
            "deployment_id": index.get("deployment_id"),
            "dag_spec": dag_spec,
            "workload_pack": workload_pack,
            "runtime_image_digest": index.get("runtime_image_digest"),
            "binding_set_ref": index.get("binding_set_ref"),
            "connection_registry_ref": index.get("connection_registry_ref"),
            "credential_runtime_ref": index.get("credential_runtime_ref"),
            "airflow_bundle": _bundle_from_ref(index.get("airflow_bundle_ref")),
        }
    )


def _indexed_artifact(value: object, logical_id: str, *, section: str) -> dict[str, str]:
    if not isinstance(value, list):
        raise ValueError(f"DPONE_AIRFLOW_INDEX_FIELD_INVALID: {section} must be a list")
    for item in value:
        if isinstance(item, Mapping) and item.get("id") == logical_id:
            return {"id": logical_id, "sha256": str(item.get("sha256") or "")}
    raise ValueError(f"DPONE_CACHE_REF_NOT_FOUND: {section} does not list {logical_id}")


def _bundle_from_ref(value: object) -> dict[str, Any] | None:
    identity = airflow_bundle_identity(value)
    if identity is None:
        return None
    return {**identity.to_dict(), "snapshot_ref": None}


def _with_bundle(
    identity: AirflowRunIdentity,
    bundle: AirflowBundleRunIdentity | None,
) -> AirflowRunIdentity:
    payload = identity.to_dict()
    payload["airflow_bundle"] = bundle.to_dict() if bundle is not None else None
    return AirflowRunIdentity.from_mapping(payload)


def _bundle_reproducibility_issue(
    bundle: AirflowBundleRunIdentity | None,
    *,
    bundle_selection: str,
    critical: bool,
) -> AirflowRerunIssue | None:
    reproducible = bundle is not None and (bundle.versioned or bundle.snapshot_ref is not None)
    if reproducible:
        return None
    blocked = critical or bundle_selection == "original"
    return _issue(
        code="DPONE_RERUN_NOT_REPRODUCIBLE",
        message=("Selected Airflow DAG Bundle is not versioned and has no content-addressed snapshot"),
        path="resolved.airflow_bundle",
        severity="error" if blocked else "warning",
    )


def _has_complete_deployment_identity(identity: AirflowRunIdentity) -> bool:
    return all(
        value is not None
        for value in (
            identity.runtime_image_digest,
            identity.binding_set_ref,
            identity.connection_registry_ref,
            identity.credential_runtime_ref,
        )
    )


def _original_artifacts_unavailable_message(code: str) -> str:
    messages = {
        "DPONE_RELEASE_NOT_FOUND": "Original dpone release is incomplete or cannot be read",
        "DPONE_RELEASE_EXPIRED": "Original dpone release is no longer retained",
        "DPONE_DEPLOYMENT_NOT_FOUND": "Original dpone deployment cannot be found",
        "DPONE_DEPLOYMENT_EXPIRED": "Original dpone deployment is no longer retained",
        "DPONE_DEPLOYMENT_INCOMPLETE": "Original dpone deployment is incomplete",
    }
    return messages.get(code, "Original dpone artifacts are unavailable for a reproducible rerun")


def _require_selection(field: str, value: str) -> None:
    if value not in _SELECTIONS:
        raise ValueError(f"DPONE_RERUN_SELECTION_INVALID: {field} must be original or latest")


def _attempt(value: Mapping[str, object]) -> dict[str, str]:
    return {
        key: str(value[key]) for key in ("dag_id", "task_id", "run_id") if key in value and value[key] not in (None, "")
    }


def _issue(*, code: str, message: str, path: str, severity: str) -> AirflowRerunIssue:
    return AirflowRerunIssue(code=code, message=message, path=path, severity=severity)


__all__ = [
    "RERUN_PLAN_SCHEMA",
    "AirflowRunIdentityError",
    "AirflowRerunIssue",
    "AirflowRerunPlan",
    "AirflowRerunPlanner",
    "parse_airflow_run_identity",
]
