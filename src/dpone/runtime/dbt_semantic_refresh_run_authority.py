"""Run-neutral projection identity and protected dbt worker admission port."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol

_PROJECTION_IDENTITY_FIELDS = {
    "dag_projection_sha256",
    "deployment_id",
    "package_artifacts_sha256",
    "plan_bundle_sha256",
    "pre_release_bundle_sha256",
    "release_id",
    "template_pack_fingerprint",
    "topology_sha256",
    "workflow_plan_sha256",
}
WORKER_PACK_IDENTITY_SCHEMA = "dpone.dbt-semantic-refresh-worker-pack-identity.v1"
ProofStatus = Literal["PROVEN", "NONCONFORMANT", "UNVERIFIED"]


@dataclass(frozen=True, slots=True)
class SemanticRefreshDbtProofRecheckResult:
    """Exact runtime proof comparison result; only ``PROVEN`` may execute."""

    status: ProofStatus
    can_execute: bool
    preflight_sha256: str
    blockers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SemanticRefreshDbtProofObservation:
    """Current compiled SQL/catalog/lifecycle proof closure."""

    observed_selected_unique_ids: tuple[str, ...]
    observed_proof_digests: tuple[str, ...]
    proof_statuses: tuple[str, ...]

    def __post_init__(self) -> None:
        if not _valid_unique_strings(self.observed_selected_unique_ids):
            raise ValueError("observed model selection is not canonical")
        if not _valid_digests(self.observed_proof_digests):
            raise ValueError("observed proof digest closure is invalid")
        if (
            not self.proof_statuses
            or len(self.proof_statuses) != len(self.observed_proof_digests)
            or any(status not in {"PROVEN", "NONCONFORMANT", "UNVERIFIED"} for status in self.proof_statuses)
        ):
            raise ValueError("observed proof status closure is invalid")


class SemanticRefreshDbtProofObservationPort(Protocol):
    """Structural result returned by a concrete proof rechecker."""

    @property
    def observed_selected_unique_ids(self) -> tuple[str, ...]: ...

    @property
    def observed_proof_digests(self) -> tuple[str, ...]: ...

    @property
    def proof_statuses(self) -> tuple[str, ...]: ...


@dataclass(frozen=True, slots=True)
class SemanticRefreshDbtStaticProjectionIdentity:
    """Exact authenticated sidecar identity presented to worker admission."""

    dag_projection_sha256: str
    release_id: str
    deployment_id: str
    plan_bundle_sha256: str
    workflow_plan_sha256: str
    topology_sha256: str
    pre_release_bundle_sha256: str
    package_artifacts_sha256: str
    template_pack_fingerprint: str

    def __post_init__(self) -> None:
        for field_name in _PROJECTION_IDENTITY_FIELDS:
            _require_digest(getattr(self, field_name), field_name)

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, object],
    ) -> SemanticRefreshDbtStaticProjectionIdentity:
        """Parse one closed sidecar projection without accepting defaults."""

        if not isinstance(value, Mapping) or set(value) != _PROJECTION_IDENTITY_FIELDS:
            raise ValueError("semantic-refresh static projection identity fields are not closed")
        return cls(**{field: _require_digest(value[field], field) for field in _PROJECTION_IDENTITY_FIELDS})

    def to_mapping(self) -> dict[str, str]:
        """Return the canonical JSON-native projection identity."""

        return {field: getattr(self, field) for field in sorted(_PROJECTION_IDENTITY_FIELDS)}


@dataclass(frozen=True, slots=True)
class SemanticRefreshDbtAdmittedRun:
    """Protected run-admission result consumed by the dbt worker gate."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    plan_bundle_sha256: str
    pre_release_bundle_sha256: str
    package_artifacts_sha256: str
    model_unique_ids: tuple[str, ...]
    protected_state_receipt_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.workflow_execution_id, str) or not self.workflow_execution_id.strip():
            raise ValueError("workflow_execution_id must be non-empty")
        for field_name in (
            "workflow_execution_binding_sha256",
            "plan_bundle_sha256",
            "pre_release_bundle_sha256",
            "package_artifacts_sha256",
            "protected_state_receipt_sha256",
        ):
            _require_digest(getattr(self, field_name), field_name)
        _model_ids(self.model_unique_ids)


class SemanticRefreshDbtRunAdmissionPort(Protocol):
    """Compile, register, and persist one actual logical DagRun authority."""

    def admit(
        self,
        *,
        plan_bundle: Mapping[str, object],
        workflow_execution_id: str,
        projection_identity: SemanticRefreshDbtStaticProjectionIdentity,
    ) -> SemanticRefreshDbtAdmittedRun: ...


class SemanticRefreshDbtImmutableProofRecheckPort(Protocol):
    """Re-prove current compiled SQL, catalog closure, and lifecycle authority."""

    def recheck_sources(
        self,
        *,
        manifest: Mapping[str, Any],
        plan_bundle: Mapping[str, object],
        projection_identity: SemanticRefreshDbtStaticProjectionIdentity,
        selected_model_unique_ids: tuple[str, ...],
    ) -> None: ...

    def recheck(
        self,
        *,
        manifest: Mapping[str, Any],
        plan_bundle: Mapping[str, object],
        projection_identity: SemanticRefreshDbtStaticProjectionIdentity,
        selected_model_unique_ids: tuple[str, ...],
    ) -> SemanticRefreshDbtProofObservationPort: ...


class SemanticRefreshDbtProofComparator:
    """Compare current selection and proof identities with the immutable plan."""

    def verify(
        self,
        *,
        expected_selected_unique_ids: tuple[str, ...],
        observed_selected_unique_ids: tuple[str, ...],
        expected_proof_digests: tuple[str, ...],
        observed_proof_digests: tuple[str, ...],
        proof_statuses: tuple[str, ...],
    ) -> SemanticRefreshDbtProofRecheckResult:
        """Fail closed on drift, incomplete authority, or a non-proven producer."""

        blockers: list[str] = []
        nonconformant = False
        if not _valid_unique_strings(expected_selected_unique_ids) or not _valid_unique_strings(
            observed_selected_unique_ids
        ):
            blockers.append("selection metadata is invalid")
        elif tuple(sorted(expected_selected_unique_ids)) != tuple(sorted(observed_selected_unique_ids)):
            blockers.append("exact selected graph differs from the immutable plan")
            nonconformant = True
        if not _valid_digests(expected_proof_digests) or not _valid_digests(observed_proof_digests):
            blockers.append("proof digest metadata is invalid")
        elif expected_proof_digests != observed_proof_digests:
            blockers.append("runtime proof digests differ from the immutable plan")
            nonconformant = True
        if not isinstance(proof_statuses, tuple) or not proof_statuses:
            blockers.append("proof status metadata is unavailable")
        elif any(status not in {"PROVEN", "NONCONFORMANT", "UNVERIFIED"} for status in proof_statuses):
            blockers.append("proof status metadata is invalid")
        elif any(status != "PROVEN" for status in proof_statuses):
            blockers.append("every selection, definition, dependency and lifecycle proof must be PROVEN")
            nonconformant = any(status == "NONCONFORMANT" for status in proof_statuses)
        status: ProofStatus = "PROVEN"
        if blockers:
            status = "NONCONFORMANT" if nonconformant else "UNVERIFIED"
        payload = {
            "expected_selected_unique_ids": list(expected_selected_unique_ids),
            "observed_selected_unique_ids": list(observed_selected_unique_ids),
            "expected_proof_digests": list(expected_proof_digests),
            "observed_proof_digests": list(observed_proof_digests),
            "proof_statuses": list(proof_statuses),
            "status": status,
            "blockers": blockers,
        }
        return SemanticRefreshDbtProofRecheckResult(
            status,
            status == "PROVEN",
            _canonical_fingerprint(payload),
            tuple(blockers),
        )


def semantic_refresh_expected_proof_digests(
    plan_bundle: Mapping[str, object],
    *,
    selected_model_unique_ids: tuple[str, ...],
) -> tuple[str, ...]:
    """Read the immutable per-model proof closure from a canonical PlanBundle."""

    operations = plan_bundle.get("operation_plans")
    if not isinstance(operations, Sequence) or isinstance(operations, str | bytes):
        raise ValueError("semantic-refresh operation proof closure is absent")
    by_model: dict[str, Mapping[str, object]] = {}
    for value in operations:
        if not isinstance(value, Mapping):
            raise ValueError("semantic-refresh operation proof is invalid")
        model_id = value.get("model_unique_id")
        if not isinstance(model_id, str) or not model_id.startswith("model.") or model_id in by_model:
            raise ValueError("semantic-refresh operation proof identity is invalid")
        by_model[model_id] = value
    if tuple(sorted(by_model)) != selected_model_unique_ids:
        raise ValueError("semantic-refresh operation proof model closure differs")
    fields = (
        "model_definition_proof_sha256",
        "read_dependency_proof_sha256",
        "mutation_closure_sha256",
        "sqlserver_lifecycle_policy_sha256",
    )
    result = tuple(
        str(by_model[model_id].get(field_name) or "") for model_id in selected_model_unique_ids for field_name in fields
    )
    if not _valid_digests(result):
        raise ValueError("semantic-refresh immutable operation proof digest is invalid")
    return result


def validate_static_projection_identity(
    identity: SemanticRefreshDbtStaticProjectionIdentity,
    *,
    plan_bundle: Mapping[str, object],
    topology_sha256: str,
) -> None:
    """Bind authenticated sidecar identity to the exact deployment plan."""

    release = _mapping(plan_bundle.get("release_deployment_authority"), "release deployment authority")
    workflow = _mapping(plan_bundle.get("workflow_plan"), "workflow plan")
    expected = (
        plan_bundle.get("plan_bundle_sha256"),
        plan_bundle.get("pre_release_bundle_sha256"),
        plan_bundle.get("package_artifacts_sha256"),
        release.get("release_id"),
        release.get("deployment_id"),
        workflow.get("workflow_plan_sha256"),
        topology_sha256,
    )
    actual = (
        identity.plan_bundle_sha256,
        identity.pre_release_bundle_sha256,
        identity.package_artifacts_sha256,
        identity.release_id,
        identity.deployment_id,
        identity.workflow_plan_sha256,
        identity.topology_sha256,
    )
    if actual != expected:
        raise ValueError("semantic-refresh static projection differs from the exact deployment plan")


def semantic_refresh_worker_pack_fingerprint(
    *,
    projection_identity: SemanticRefreshDbtStaticProjectionIdentity,
    run_execution_bundle_sha256: str,
    activation_authority_receipt_sha256: str,
    authority_store_ref: str,
    run_guard_closure_sha256: str,
) -> str:
    """Bind one run pack to its sidecar, run, deployment receipt, and guards."""

    if not isinstance(projection_identity, SemanticRefreshDbtStaticProjectionIdentity):
        raise TypeError("projection_identity must be canonical and typed")
    if not isinstance(authority_store_ref, str) or not authority_store_ref.strip() or len(authority_store_ref) > 1024:
        raise ValueError("authority_store_ref must be bounded non-empty text")
    payload = {
        "activation_authority_receipt_sha256": _require_digest(
            activation_authority_receipt_sha256,
            "activation_authority_receipt_sha256",
        ),
        "authority_store_ref": authority_store_ref,
        "projection_identity": projection_identity.to_mapping(),
        "run_execution_bundle_sha256": _require_digest(
            run_execution_bundle_sha256,
            "run_execution_bundle_sha256",
        ),
        "run_guard_closure_sha256": _require_digest(
            run_guard_closure_sha256,
            "run_guard_closure_sha256",
        ),
        "schema": WORKER_PACK_IDENTITY_SCHEMA,
    }
    canonical = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


def _mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"semantic-refresh {field_name} must be an object")
    return value


def _model_ids(value: object) -> tuple[str, ...]:
    if (
        not isinstance(value, tuple)
        or not value
        or value != tuple(sorted(set(value)))
        or any(not isinstance(item, str) or not item.startswith("model.") for item in value)
    ):
        raise ValueError("semantic-refresh model closure is not canonical")
    return value


def _require_digest(value: object, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"semantic-refresh {field_name} must be a canonical digest")
    return value


def _valid_unique_strings(values: object) -> bool:
    return (
        isinstance(values, tuple)
        and bool(values)
        and all(isinstance(value, str) and bool(value) for value in values)
        and len(values) == len(set(values))
    )


def _valid_digests(values: object) -> bool:
    return isinstance(values, tuple) and bool(values) and all(_is_digest(value) for value in values)


def _is_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and value.startswith("sha256:")
        and len(value) == 71
        and all(character in "0123456789abcdef" for character in value[7:])
    )


def _canonical_fingerprint(value: Mapping[str, object]) -> str:
    raw = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


__all__ = [
    "WORKER_PACK_IDENTITY_SCHEMA",
    "SemanticRefreshDbtAdmittedRun",
    "SemanticRefreshDbtImmutableProofRecheckPort",
    "SemanticRefreshDbtProofComparator",
    "SemanticRefreshDbtProofObservation",
    "SemanticRefreshDbtProofObservationPort",
    "SemanticRefreshDbtProofRecheckResult",
    "SemanticRefreshDbtRunAdmissionPort",
    "SemanticRefreshDbtStaticProjectionIdentity",
    "semantic_refresh_expected_proof_digests",
    "semantic_refresh_worker_pack_fingerprint",
    "validate_static_projection_identity",
]
