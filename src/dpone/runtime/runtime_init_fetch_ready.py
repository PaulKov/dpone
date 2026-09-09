"""Ready-manifest contract separating verified runtime state from public evidence."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.runtime_init_fetch_plan import RuntimeInitFetchPlan


import json
from collections.abc import Mapping
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_attestation_decision import attestation_decision_sha256
from dpone.runtime.runtime_init_fetch_ready_models import (
    ATTESTATION_REQUIREMENT_OPTIONAL,
    ATTESTATION_REQUIREMENT_REQUIRED,
    ATTESTATION_REQUIREMENTS,
    RUNTIME_FETCH_READY_NAME,
    RUNTIME_FETCH_READY_SCHEMA,
    RUNTIME_FETCH_READY_SCHEMA_V2,
    ReadyArtifact,
    RuntimeFetchReady,
)


def effective_attestation_requirement(
    plan: RuntimeInitFetchPlan,
    *,
    trusted_attestation_required: bool = False,
) -> str:
    """Select the effective requirement after applying trusted init policy."""

    required = (
        trusted_attestation_required
        or plan.trust_tier == "production"
        or plan.verify["attestations"] == "required_for_prod"
    )
    return ATTESTATION_REQUIREMENT_REQUIRED if required else ATTESTATION_REQUIREMENT_OPTIONAL


def ready_manifest_bytes(ready: RuntimeFetchReady) -> bytes:
    return (json.dumps(ready.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n").encode(
        "utf-8"
    )


def parse_ready_manifest(
    payload: bytes,
    *,
    plan: RuntimeInitFetchPlan,
    plan_sha256: str,
    attestation_required: bool | None = None,
    deployment_attestation_required: bool | None = None,
) -> RuntimeFetchReady:
    """Validate the final record; init may additionally assert its trusted decision."""

    try:
        raw = json.loads(payload.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise _ready_error("runtime ready manifest is invalid JSON") from exc
    schema = raw.get("schema") if isinstance(raw, Mapping) else None
    if schema not in {RUNTIME_FETCH_READY_SCHEMA, RUNTIME_FETCH_READY_SCHEMA_V2}:
        raise _ready_error("runtime ready manifest schema is invalid")
    try:
        _require_exact_fields(
            raw,
            {
                "schema",
                "plan_sha256",
                "release_id",
                "deployment_id",
                "runtime_image_digest",
                "trust_tier",
                "artifact_registry_ref",
                "registry_config_sha256",
                "trust_policy_sha256",
                "workload_id",
                "pack_fingerprint",
                "artifacts",
                "verification",
            },
            "runtime ready manifest",
        )
        artifacts = _mapping(raw.get("artifacts"), "artifacts")
        verification = _mapping(raw.get("verification"), "verification")
        _require_exact_fields(
            artifacts,
            {"release", "deployment", "workload_pack"},
            "artifacts",
        )
        verification_fields = {
            "checksums",
            "effective_attestation_requirement",
            "attestations",
            "attestation_decision_sha256",
            "runtime_payload_sha256",
        }
        if schema == RUNTIME_FETCH_READY_SCHEMA_V2:
            verification_fields.add("artifact_attestation")
        _require_exact_fields(verification, verification_fields, "verification")
        artifact_attestation = (
            _mapping(verification.get("artifact_attestation"), "verification.artifact_attestation")
            if schema == RUNTIME_FETCH_READY_SCHEMA_V2
            else {}
        )
        if artifact_attestation:
            _require_exact_fields(
                artifact_attestation,
                {
                    "subject_kind",
                    "backend",
                    "attestation_id",
                    "verification_sha256",
                    "observed_claims",
                    "unobserved_claims",
                },
                "verification.artifact_attestation",
            )
        ready = RuntimeFetchReady(
            plan_sha256=_digest(raw.get("plan_sha256"), "plan_sha256"),
            release_id=_digest(raw.get("release_id"), "release_id"),
            deployment_id=_digest(raw.get("deployment_id"), "deployment_id"),
            runtime_image_digest=_digest(raw.get("runtime_image_digest"), "runtime_image_digest"),
            trust_tier=_text(raw.get("trust_tier"), "trust_tier"),
            artifact_registry_ref=_text(raw.get("artifact_registry_ref"), "artifact_registry_ref"),
            registry_config_sha256=_digest(
                raw.get("registry_config_sha256"),
                "registry_config_sha256",
            ),
            trust_policy_sha256=_optional_digest(
                raw.get("trust_policy_sha256"),
                "trust_policy_sha256",
            ),
            workload_id=_text(raw.get("workload_id"), "workload_id"),
            pack_fingerprint=_digest(raw.get("pack_fingerprint"), "pack_fingerprint"),
            release=_ready_artifact(artifacts.get("release"), "release"),
            deployment=_ready_artifact(artifacts.get("deployment"), "deployment"),
            workload_pack=_ready_artifact(artifacts.get("workload_pack"), "workload_pack"),
            checksum_status=_text(verification.get("checksums"), "verification.checksums"),
            effective_attestation_requirement=_text(
                verification.get("effective_attestation_requirement"),
                "verification.effective_attestation_requirement",
            ),
            attestation_status=_text(
                verification.get("attestations"),
                "verification.attestations",
            ),
            attestation_decision_sha256=_digest(
                verification.get("attestation_decision_sha256"),
                "verification.attestation_decision_sha256",
            ),
            runtime_payload_sha256=_digest(
                verification.get("runtime_payload_sha256"),
                "verification.runtime_payload_sha256",
            ),
            artifact_attestation_subject_kind=_optional_text(
                artifact_attestation.get("subject_kind"),
                "verification.artifact_attestation.subject_kind",
            ),
            artifact_attestation_backend=_optional_text(
                artifact_attestation.get("backend"),
                "verification.artifact_attestation.backend",
            ),
            artifact_attestation_id=_optional_digest(
                artifact_attestation.get("attestation_id"),
                "verification.artifact_attestation.attestation_id",
            ),
            artifact_attestation_verification_sha256=_optional_digest(
                artifact_attestation.get("verification_sha256"),
                "verification.artifact_attestation.verification_sha256",
            ),
            artifact_attestation_observed_claims=_text_tuple(
                artifact_attestation.get("observed_claims", ()),
                "verification.artifact_attestation.observed_claims",
            ),
            artifact_attestation_unobserved_claims=_text_tuple(
                artifact_attestation.get("unobserved_claims", ()),
                "verification.artifact_attestation.unobserved_claims",
            ),
            schema=str(schema),
        )
    except (TypeError, ValueError) as exc:
        raise _ready_error(str(exc)) from exc
    _require_ready_matches_plan(
        ready,
        plan=plan,
        plan_sha256=plan_sha256,
        attestation_required=attestation_required,
        deployment_attestation_required=deployment_attestation_required,
    )
    return ready


def _require_ready_matches_plan(
    ready: RuntimeFetchReady,
    *,
    plan: RuntimeInitFetchPlan,
    plan_sha256: str,
    attestation_required: bool | None,
    deployment_attestation_required: bool | None,
) -> None:
    expected = (
        (ready.plan_sha256, plan_sha256, "plan hash"),
        (ready.release_id, plan.release_id, "release identity"),
        (ready.deployment_id, plan.deployment_id, "deployment identity"),
        (ready.runtime_image_digest, plan.runtime_image_digest, "runtime image"),
        (ready.trust_tier, plan.trust_tier, "trust tier"),
        (ready.artifact_registry_ref, plan.artifact_registry_ref, "registry"),
        (ready.registry_config_sha256, plan.registry_config_ref["sha256"], "registry configuration"),
        (ready.workload_id, plan.workload_pack.id, "workload"),
        (ready.pack_fingerprint, plan.workload_pack.pack_fingerprint, "pack fingerprint"),
    )
    if any(actual != wanted for actual, wanted, _ in expected):
        raise _ready_error("runtime ready manifest does not match the pinned plan")
    expected_trust_sha = plan.trust_policy_ref["sha256"] if plan.trust_policy_ref is not None else None
    if ready.trust_policy_sha256 != expected_trust_sha:
        raise _ready_error("runtime ready trust policy does not match the pinned plan")
    _require_ready_artifact(ready.release, plan.release.to_dict())
    _require_ready_artifact(ready.deployment, plan.deployment.to_dict())
    _require_ready_artifact(ready.workload_pack, plan.workload_pack.to_dict())
    if ready.checksum_status != "passed":
        raise _ready_error("runtime ready checksum decision is not passed")
    _require_attestation_decision(
        ready,
        plan=plan,
        plan_sha256=plan_sha256,
        attestation_required=attestation_required,
        deployment_attestation_required=deployment_attestation_required,
    )


def _require_attestation_decision(
    ready: RuntimeFetchReady,
    *,
    plan: RuntimeInitFetchPlan,
    plan_sha256: str,
    attestation_required: bool | None,
    deployment_attestation_required: bool | None,
) -> None:
    requirement = ready.effective_attestation_requirement
    if requirement not in ATTESTATION_REQUIREMENTS:
        raise _ready_error("runtime ready effective attestation requirement is invalid")
    plan_minimum = effective_attestation_requirement(plan)
    if plan_minimum == ATTESTATION_REQUIREMENT_REQUIRED and requirement != plan_minimum:
        raise _ready_error("runtime ready effective attestation requirement downgrades the pinned plan")
    if attestation_required is not None:
        expected = ATTESTATION_REQUIREMENT_REQUIRED if attestation_required else ATTESTATION_REQUIREMENT_OPTIONAL
        if requirement != expected:
            raise _ready_error("runtime ready effective attestation requirement does not match trusted init policy")
    expected_status = "passed" if requirement == ATTESTATION_REQUIREMENT_REQUIRED else "not_required"
    if ready.attestation_status != expected_status:
        raise _ready_error("runtime ready attestation decision is invalid")
    if deployment_attestation_required is True and ready.schema != RUNTIME_FETCH_READY_SCHEMA_V2:
        raise _ready_error("runtime ready deployment attestation evidence is missing")
    if deployment_attestation_required is False and requirement == ATTESTATION_REQUIREMENT_REQUIRED:
        if ready.schema != RUNTIME_FETCH_READY_SCHEMA:
            raise _ready_error("runtime ready attestation authority does not match trusted init policy")
    if ready.schema == RUNTIME_FETCH_READY_SCHEMA_V2:
        if requirement != ATTESTATION_REQUIREMENT_REQUIRED or any(
            value is None
            for value in (
                ready.artifact_attestation_subject_kind,
                ready.artifact_attestation_backend,
                ready.artifact_attestation_id,
                ready.artifact_attestation_verification_sha256,
            )
        ):
            raise _ready_error("runtime ready artifact attestation evidence is incomplete")
        _require_observation_partition(ready)
    elif ready.schema != RUNTIME_FETCH_READY_SCHEMA:
        raise _ready_error("runtime ready schema is invalid for release attestation")
    expected_digest = attestation_decision_sha256(
        plan_sha256=plan_sha256,
        trust_policy_sha256=ready.trust_policy_sha256,
        effective_requirement=requirement,
        attestation_status=ready.attestation_status,
        artifact_attestation_subject_kind=ready.artifact_attestation_subject_kind,
        artifact_attestation_backend=ready.artifact_attestation_backend,
        artifact_attestation_id=ready.artifact_attestation_id,
        artifact_attestation_verification_sha256=ready.artifact_attestation_verification_sha256,
        artifact_attestation_observed_claims=ready.artifact_attestation_observed_claims,
        artifact_attestation_unobserved_claims=ready.artifact_attestation_unobserved_claims,
    )
    if ready.attestation_decision_sha256 != expected_digest:
        raise _ready_error("runtime ready attestation decision binding is invalid")


_DEPLOYMENT_SUBJECT_CLAIMS = frozenset(
    {
        "release_id",
        "deployment_id",
        "environment",
        "artifact_registry_ref",
        "registry_scope_id",
        "release_set_sha256",
        "deployment_sha256",
        "airflow_index_sha256",
        "runtime_image_digest",
    }
)


def _require_observation_partition(ready: RuntimeFetchReady) -> None:
    observed = set(ready.artifact_attestation_observed_claims)
    unobserved = set(ready.artifact_attestation_unobserved_claims)
    if (
        ready.artifact_attestation_subject_kind != "airflow_deployment"
        or ready.artifact_attestation_backend != "cosign_public_key"
        or observed & unobserved
        or observed | unobserved != _DEPLOYMENT_SUBJECT_CLAIMS
    ):
        raise _ready_error("runtime ready artifact attestation observation evidence is invalid")


def _require_ready_artifact(ready: ReadyArtifact, expected: Mapping[str, Any]) -> None:
    if (
        ready.artifact_ref != expected["artifact_ref"]
        or ready.sha256 != expected["sha256"]
        or ready.bytes != expected["bytes"]
    ):
        raise _ready_error("runtime ready artifact does not match the pinned plan")
    if (
        not ready.locator.startswith("payload/")
        or ready.locator.startswith("/")
        or "\\" in ready.locator
        or ".." in ready.locator.split("/")
    ):
        raise _ready_error("runtime ready artifact locator is unsafe")


def _ready_artifact(value: object, field: str) -> ReadyArtifact:
    item = _mapping(value, field)
    if set(item) != {"artifact_ref", "locator", "sha256", "bytes"}:
        raise ValueError(f"{field} contains unknown or missing fields")
    size = item["bytes"]
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise ValueError(f"{field}.bytes must be a positive integer")
    return ReadyArtifact(
        artifact_ref=_text(item["artifact_ref"], f"{field}.artifact_ref"),
        locator=_text(item["locator"], f"{field}.locator"),
        sha256=_digest(item["sha256"], f"{field}.sha256"),
        bytes=size,
    )


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return value


def _require_exact_fields(
    value: Mapping[str, Any],
    expected: set[str],
    field: str,
) -> None:
    if set(value) != expected:
        raise ValueError(f"{field} contains unknown or missing fields")


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _digest(value: object, field: str) -> str:
    if not is_canonical_sha256_digest(value):
        raise ValueError(f"{field} must be a canonical sha256 digest")
    return str(value)


def _optional_digest(value: object, field: str) -> str | None:
    return None if value is None else _digest(value, field)


def _optional_text(value: object, field: str) -> str | None:
    return None if value is None else _text(value, field)


def _text_tuple(value: object, field: str) -> tuple[str, ...]:
    if value == ():
        return ()
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise ValueError(f"{field} must be a list of non-empty strings")
    result = tuple(value)
    if len(set(result)) != len(result):
        raise ValueError(f"{field} must not contain duplicates")
    return result


def _unique_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("runtime ready manifest contains duplicate JSON keys")
        result[key] = value
    return result


def _ready_error(message: str) -> InitFetchError:
    return InitFetchError("DPONE_RUNTIME_FETCH_READY_INVALID", message)


__all__ = [
    "ATTESTATION_REQUIREMENT_OPTIONAL",
    "ATTESTATION_REQUIREMENT_REQUIRED",
    "attestation_decision_sha256",
    "effective_attestation_requirement",
    "parse_ready_manifest",
    "ReadyArtifact",
    "ready_manifest_bytes",
    "RUNTIME_FETCH_READY_NAME",
    "RUNTIME_FETCH_READY_SCHEMA",
    "RUNTIME_FETCH_READY_SCHEMA_V2",
    "RuntimeFetchReady",
]
