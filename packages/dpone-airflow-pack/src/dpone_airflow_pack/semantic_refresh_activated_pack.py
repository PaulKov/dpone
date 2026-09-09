"""Authenticate one protected activated semantic-refresh Airflow pack."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from dpone_airflow_pack.pack_identity import verify_pack_fingerprint
from dpone_airflow_pack.semantic_refresh_projection import (
    SemanticRefreshProjectionError,
    SemanticRefreshTaskProjection,
    build_semantic_refresh_task_projection,
)

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_PACK_FIELDS = {
    "activation",
    "dbt_execution_pack",
    "executable",
    "kind",
    "pack_fingerprint",
    "pack_identity",
    "producer",
    "runtime_command",
    "runtime_payload_ids",
    "schema_version",
    "semantic_refresh",
    "workload",
}
_SEMANTIC_FIELDS = {
    "activation_authority",
    "execution_binding",
    "mode",
    "operation_ids_by_model",
    "package_artifacts_sha256",
    "plan_bundle_sha256",
    "pre_release_bundle_sha256",
    "run_execution_bundle_sha256",
    "task_projection",
    "topology",
    "topology_sha256",
    "workflow_execution_binding_sha256",
    "workflow_execution_id",
    "workflow_plan_sha256",
}
_ACTIVATION_FIELDS = {
    "activation_authority_receipt_sha256",
    "authority_store_ref",
    "baseline_receipts",
    "deployment_id",
    "persisted_at",
    "plan_bundle_sha256",
    "release_id",
    "route_certification_receipt_sha256",
    "runtime_assurance_receipts",
    "schema",
}
_LOCAL_AUTHORITY_SCHEMA = "dpone.semantic-refresh-activated-pack-local-authority.v1"
_LOCAL_AUTHORITY_FIELDS = {
    "activation_authority_receipt_sha256",
    "authority_record_sha256",
    "authority_store_ref",
    "pack_fingerprint",
    "package_artifacts_sha256",
    "plan_bundle_sha256",
    "pre_release_bundle_sha256",
    "run_execution_bundle_sha256",
    "schema",
    "workflow_execution_binding_sha256",
    "workflow_execution_id",
    "workflow_plan_sha256",
}


@dataclass(frozen=True, slots=True)
class SemanticRefreshActivatedPackIdentity:
    """Exact external authority coordinates for one activated pack."""

    pack_fingerprint: str
    activation_authority_receipt_sha256: str
    authority_store_ref: str
    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    workflow_plan_sha256: str
    plan_bundle_sha256: str
    pre_release_bundle_sha256: str
    package_artifacts_sha256: str
    run_execution_bundle_sha256: str


class SemanticRefreshActivatedPackAuthority(Protocol):
    """Authenticate pack identity against protected deployment/run authority."""

    def assert_authorized(self, identity: SemanticRefreshActivatedPackIdentity) -> None:
        """Raise unless the exact fingerprint is protected for this logical DagRun."""


@dataclass(frozen=True, slots=True)
class LocalSemanticRefreshActivatedPackAuthority:
    """Parse-safe exact authority loaded from a provenance-verified deployment index."""

    identity: SemanticRefreshActivatedPackIdentity
    authority_record_sha256: str

    @classmethod
    def build(
        cls,
        identity: SemanticRefreshActivatedPackIdentity,
    ) -> LocalSemanticRefreshActivatedPackAuthority:
        """Build the inner record; the outer deployment index owns file provenance."""

        unsigned = _local_authority_unsigned(identity)
        return cls(identity=identity, authority_record_sha256=_canonical_digest(unsigned))

    @classmethod
    def from_mapping(cls, value: object) -> LocalSemanticRefreshActivatedPackAuthority:
        """Decode one exact local record without filesystem, database, or network I/O."""

        raw = _mapping(value, "local_authority")
        if set(raw) != _LOCAL_AUTHORITY_FIELDS or raw.get("schema") != _LOCAL_AUTHORITY_SCHEMA:
            raise SemanticRefreshProjectionError("activated local authority fields are not closed")
        identity = SemanticRefreshActivatedPackIdentity(
            pack_fingerprint=_digest(raw.get("pack_fingerprint"), "pack_fingerprint"),
            activation_authority_receipt_sha256=_digest(
                raw.get("activation_authority_receipt_sha256"),
                "activation_authority_receipt_sha256",
            ),
            authority_store_ref=_text(raw.get("authority_store_ref"), "authority_store_ref"),
            workflow_execution_id=_text(raw.get("workflow_execution_id"), "workflow_execution_id"),
            workflow_execution_binding_sha256=_digest(
                raw.get("workflow_execution_binding_sha256"),
                "workflow_execution_binding_sha256",
            ),
            workflow_plan_sha256=_digest(raw.get("workflow_plan_sha256"), "workflow_plan_sha256"),
            plan_bundle_sha256=_digest(raw.get("plan_bundle_sha256"), "plan_bundle_sha256"),
            pre_release_bundle_sha256=_digest(
                raw.get("pre_release_bundle_sha256"),
                "pre_release_bundle_sha256",
            ),
            package_artifacts_sha256=_digest(
                raw.get("package_artifacts_sha256"),
                "package_artifacts_sha256",
            ),
            run_execution_bundle_sha256=_digest(
                raw.get("run_execution_bundle_sha256"),
                "run_execution_bundle_sha256",
            ),
        )
        supplied = _digest(raw.get("authority_record_sha256"), "authority_record_sha256")
        if _canonical_digest(_local_authority_unsigned(identity)) != supplied:
            raise SemanticRefreshProjectionError("activated local authority digest differs")
        return cls(identity=identity, authority_record_sha256=supplied)

    def to_mapping(self) -> dict[str, str]:
        """Return the closed JSON-native record embedded by the deployment controller."""

        return {
            **_local_authority_unsigned(self.identity),
            "authority_record_sha256": self.authority_record_sha256,
        }

    def assert_authorized(self, identity: SemanticRefreshActivatedPackIdentity) -> None:
        """Compare the complete run/release/package identity without external I/O."""

        if identity != self.identity:
            raise SemanticRefreshProjectionError("activated pack differs from local deployment authority")


@dataclass(frozen=True, slots=True)
class AuthenticatedSemanticRefreshActivatedPack:
    """Rebuilt task graph plus exact workflow-plan authority."""

    projection: SemanticRefreshTaskProjection
    workflow_plan_sha256: str
    dbt_execution_pack: Mapping[str, object]
    project_config_overlay: Mapping[str, object]
    profile_sha256: str
    topology_sha256: str
    pre_release_bundle_sha256: str
    package_artifacts_sha256: str


def authenticate_semantic_refresh_activated_pack(
    payload: Mapping[str, object],
    *,
    authority: SemanticRefreshActivatedPackAuthority,
) -> AuthenticatedSemanticRefreshActivatedPack:
    """Verify local closure, then require exact protected external authority."""

    authenticated, identity = validate_semantic_refresh_activated_pack(payload)
    authority.assert_authorized(identity)
    return authenticated


def validate_semantic_refresh_activated_pack(
    payload: Mapping[str, object],
) -> tuple[AuthenticatedSemanticRefreshActivatedPack, SemanticRefreshActivatedPackIdentity]:
    """Self-validate a closed pack and return its external authority identity."""

    if not isinstance(payload, Mapping) or set(payload) != _PACK_FIELDS:
        raise SemanticRefreshProjectionError("activated semantic-refresh pack fields are not closed")
    try:
        pack_fingerprint = str(verify_pack_fingerprint(payload))
    except (TypeError, ValueError) as exc:
        raise SemanticRefreshProjectionError("activated semantic-refresh pack fingerprint differs") from exc
    if (
        payload.get("activation") != "PROTECTED_RUN_AUTHORITY_BOUND"
        or payload.get("executable") is not True
        or payload.get("kind") != "gitops.airflow_pack"
        or payload.get("schema_version") != "3"
        or payload.get("producer") != "dpone semantic-refresh activate"
    ):
        raise SemanticRefreshProjectionError("activated semantic-refresh pack mode is invalid")
    semantic = _mapping(payload.get("semantic_refresh"), "semantic_refresh")
    if set(semantic) != _SEMANTIC_FIELDS or semantic.get("mode") != "semantic_refresh_v2_activated":
        raise SemanticRefreshProjectionError("activated semantic-refresh authority fields are not closed")
    activation = _activation_authority(semantic.get("activation_authority"))
    plan_bundle_sha256 = _digest(semantic.get("plan_bundle_sha256"), "plan_bundle_sha256")
    if activation["plan_bundle_sha256"] != plan_bundle_sha256:
        raise SemanticRefreshProjectionError("activation receipt and plan bundle differ")
    identity = SemanticRefreshActivatedPackIdentity(
        pack_fingerprint=_digest(pack_fingerprint, "pack_fingerprint"),
        activation_authority_receipt_sha256=_digest(
            activation["activation_authority_receipt_sha256"],
            "activation_authority_receipt_sha256",
        ),
        authority_store_ref=_text(activation["authority_store_ref"], "authority_store_ref"),
        workflow_execution_id=_text(semantic.get("workflow_execution_id"), "workflow_execution_id"),
        workflow_execution_binding_sha256=_digest(
            semantic.get("workflow_execution_binding_sha256"),
            "workflow_execution_binding_sha256",
        ),
        workflow_plan_sha256=_digest(semantic.get("workflow_plan_sha256"), "workflow_plan_sha256"),
        plan_bundle_sha256=plan_bundle_sha256,
        pre_release_bundle_sha256=_digest(
            semantic.get("pre_release_bundle_sha256"),
            "pre_release_bundle_sha256",
        ),
        package_artifacts_sha256=_digest(
            semantic.get("package_artifacts_sha256"),
            "package_artifacts_sha256",
        ),
        run_execution_bundle_sha256=_digest(
            semantic.get("run_execution_bundle_sha256"),
            "run_execution_bundle_sha256",
        ),
    )
    topology = _mapping(semantic.get("topology"), "topology")
    if topology.get("topology_sha256") != semantic.get("topology_sha256"):
        raise SemanticRefreshProjectionError("activated topology identity differs")
    execution_binding = _mapping(semantic.get("execution_binding"), "execution_binding")
    operation_ids = _mapping(semantic.get("operation_ids_by_model"), "operation_ids_by_model")
    projection = build_semantic_refresh_task_projection(
        execution_binding=execution_binding,
        topology=topology,
        operation_ids_by_model={str(key): str(value) for key, value in operation_ids.items()},
    )
    if (
        projection.workflow_execution_id != identity.workflow_execution_id
        or projection.workflow_execution_binding_sha256 != identity.workflow_execution_binding_sha256
        or execution_binding.get("workflow_plan_sha256") != identity.workflow_plan_sha256
        or projection.to_mapping() != semantic.get("task_projection")
    ):
        raise SemanticRefreshProjectionError("activated task projection differs from protected pack")
    return (
        AuthenticatedSemanticRefreshActivatedPack(
            projection=projection,
            workflow_plan_sha256=identity.workflow_plan_sha256,
            dbt_execution_pack=_mapping(payload.get("dbt_execution_pack"), "dbt_execution_pack"),
            project_config_overlay=_mapping(
                topology.get("project_config_overlay"),
                "project_config_overlay",
            ),
            profile_sha256=_digest(topology.get("profile_sha256"), "profile_sha256"),
            topology_sha256=_digest(topology.get("topology_sha256"), "topology_sha256"),
            pre_release_bundle_sha256=identity.pre_release_bundle_sha256,
            package_artifacts_sha256=identity.package_artifacts_sha256,
        ),
        identity,
    )


def _activation_authority(value: object) -> Mapping[str, object]:
    activation = _mapping(value, "activation_authority")
    if (
        set(activation) != _ACTIVATION_FIELDS
        or activation.get("schema") != "dpone.dbt-semantic-refresh-activation-authority-receipt.v1"
    ):
        raise SemanticRefreshProjectionError("activation authority receipt fields are not closed")
    supplied = _digest(
        activation.get("activation_authority_receipt_sha256"),
        "activation_authority_receipt_sha256",
    )
    unsigned = {key: raw for key, raw in activation.items() if key != "activation_authority_receipt_sha256"}
    if _canonical_digest(unsigned) != supplied:
        raise SemanticRefreshProjectionError("activation authority receipt digest differs")
    return activation


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise SemanticRefreshProjectionError(f"activated {field} must be an object")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SemanticRefreshProjectionError(f"activated {field} must be non-empty text")
    return value


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise SemanticRefreshProjectionError(f"activated {field} must be a canonical digest")
    return value


def _canonical_digest(value: Mapping[str, object]) -> str:
    try:
        raw = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    except (TypeError, ValueError) as exc:
        raise SemanticRefreshProjectionError("activated authority is not canonical JSON") from exc
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _local_authority_unsigned(identity: SemanticRefreshActivatedPackIdentity) -> dict[str, str]:
    return {
        "activation_authority_receipt_sha256": identity.activation_authority_receipt_sha256,
        "authority_store_ref": identity.authority_store_ref,
        "pack_fingerprint": identity.pack_fingerprint,
        "package_artifacts_sha256": identity.package_artifacts_sha256,
        "plan_bundle_sha256": identity.plan_bundle_sha256,
        "pre_release_bundle_sha256": identity.pre_release_bundle_sha256,
        "run_execution_bundle_sha256": identity.run_execution_bundle_sha256,
        "schema": _LOCAL_AUTHORITY_SCHEMA,
        "workflow_execution_binding_sha256": identity.workflow_execution_binding_sha256,
        "workflow_execution_id": identity.workflow_execution_id,
        "workflow_plan_sha256": identity.workflow_plan_sha256,
    }


__all__ = [
    "AuthenticatedSemanticRefreshActivatedPack",
    "LocalSemanticRefreshActivatedPackAuthority",
    "SemanticRefreshActivatedPackAuthority",
    "SemanticRefreshActivatedPackIdentity",
    "authenticate_semantic_refresh_activated_pack",
    "validate_semantic_refresh_activated_pack",
]
