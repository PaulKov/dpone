"""Validate fetched release, deployment, and workload receipts."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.runtime_init_fetch_plan import RuntimeInitFetchPlan


import json
import math
from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.pack_identity import (
    PackIdentityError,
    parse_pack_json,
    verify_pack_fingerprint,
)

from dpone.contracts.airflow_deployment import (
    canonical_fingerprint,
    deployment_id,
    release_id,
    requires_release_set_v2_for_runtime_payloads,
)
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan import plan_matches_declared_runtime_image


def validate_runtime_receipts(
    plan: RuntimeInitFetchPlan,
    payloads: Mapping[str, bytes],
) -> tuple[Mapping[str, Any], str]:
    """Verify that fetched receipts reproduce the exact pinned execution plan."""

    expected_refs = {artifact.artifact_ref for artifact in plan.artifacts}
    if set(payloads) != expected_refs:
        raise _integrity_error("runtime artifact payload set does not match the pinned plan")
    release = _json_object(payloads[plan.release.artifact_ref], "release-set")
    deployment = _json_object(payloads[plan.deployment.artifact_ref], "deployment-set")
    runtime_connections = {
        "binding_set": _json_object(
            payloads[plan.binding_set.artifact_ref],
            "binding-set",
        ),
        "connection_registry": _json_object(
            payloads[plan.connection_registry.artifact_ref],
            "connection-registry",
        ),
        "credential_runtime": _json_object(
            payloads[plan.credential_runtime.artifact_ref],
            "credential-runtime",
        ),
    }
    pack_bytes = payloads[plan.workload_pack.artifact_ref]
    try:
        pack = parse_pack_json(pack_bytes)
        verified_pack_fingerprint = verify_pack_fingerprint(pack)
    except PackIdentityError as exc:
        raise _integrity_error("workload pack identity is invalid") from exc
    if (
        release.get("schema")
        not in {
            "dpone.release-set.v1",
            "dpone.release-set.v2",
            "dpone.release-set.v3",
        }
        or release_id(release) != plan.release_id
    ):
        raise _integrity_error("release-set identity does not match the pinned plan")
    if release.get("schema") == "dpone.release-set.v3":
        from dpone.contracts.release_composition_policy import validate_composition_metadata

        try:
            validate_composition_metadata(release)
        except (ValueError, TypeError, KeyError) as exc:
            raise _integrity_error("composition ownership or authority is invalid") from exc
    if release.get("release_id") != plan.release_id:
        raise _integrity_error("release-set declared identity does not match the pinned plan")
    if requires_release_set_v2_for_runtime_payloads(
        release_schema=release.get("schema"),
        has_runtime_payloads=_release_has_runtime_payloads(release),
        trust_tier=plan.trust_tier,
    ):
        raise InitFetchError(
            "DPONE_DBT_PRODUCTION_RELEASE_SCHEMA_REQUIRED",
            "production dbt runtime payloads require release-set.v2",
        )
    if (
        deployment.get("schema")
        not in {
            "dpone.deployment-set.v2",
            "dpone.deployment-set.v3",
        }
        or deployment_id(deployment) != plan.deployment_id
    ):
        raise _integrity_error("deployment-set identity does not match the pinned plan")
    if deployment.get("deployment_id") != plan.deployment_id:
        raise _integrity_error("deployment-set declared identity does not match the pinned plan")
    _validate_deployment_mirror(plan, deployment)
    _validate_runtime_connection_receipts(
        plan,
        deployment=deployment,
        runtime_connections=runtime_connections,
    )
    _validate_release_membership(plan, release)
    if verified_pack_fingerprint != plan.workload_pack.pack_fingerprint:
        raise _integrity_error("workload pack fingerprint does not match the pinned plan")
    workload = pack.get("workload")
    if (
        not isinstance(workload, Mapping)
        or (workload.get("workload_id") or workload.get("id")) != plan.workload_pack.id
    ):
        raise _integrity_error("workload pack identity does not match the pinned plan")
    return pack, verified_pack_fingerprint


def _release_has_runtime_payloads(release: Mapping[str, Any]) -> bool:
    """Return the release-wide runtime-payload authority fact."""

    artifacts = release.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise _integrity_error("release-set artifact inventory is invalid")
    runtime_payloads = artifacts.get("runtime_payloads")
    if runtime_payloads is None:
        return False
    if not isinstance(runtime_payloads, list):
        raise _integrity_error("release-set runtime payload inventory is invalid")
    return bool(runtime_payloads)


def _validate_deployment_mirror(
    plan: RuntimeInitFetchPlan,
    deployment: Mapping[str, Any],
) -> None:
    delivery = deployment.get("runtime_artifact_delivery")
    workloads = deployment.get("workloads")
    if (
        deployment.get("release_ref") != plan.release_id
        or not plan_matches_declared_runtime_image(plan, deployment)
        or not isinstance(delivery, Mapping)
        or delivery.get("mode") != "init_fetch"
        or delivery.get("artifact_registry_ref") != plan.artifact_registry_ref
        or delivery.get("trust_tier") != plan.trust_tier
        or delivery.get("registry_config_ref") != dict(plan.registry_config_ref)
        or delivery.get("trust_policy_ref")
        != (dict(plan.trust_policy_ref) if plan.trust_policy_ref is not None else None)
        or delivery.get("identity") != dict(plan.identity)
        or delivery.get("verify") != dict(plan.verify)
        or deployment.get("binding_set") != plan.binding_set.to_dict()
        or deployment.get("connection_registry") != plan.connection_registry.to_dict()
        or deployment.get("credential_runtime") != plan.credential_runtime.to_dict()
        or not isinstance(workloads, list)
    ):
        raise _integrity_error("deployment-set runtime projection does not match the pinned plan")
    expected_workload = {
        "id": plan.workload_pack.id,
        "sha256": plan.workload_pack.sha256,
        "pack_fingerprint": plan.workload_pack.pack_fingerprint,
    }
    if expected_workload not in workloads:
        raise _integrity_error("deployment-set workload inventory does not contain the pinned pack")


def _validate_runtime_connection_receipts(
    plan: RuntimeInitFetchPlan,
    *,
    deployment: Mapping[str, Any],
    runtime_connections: Mapping[str, Mapping[str, Any]],
) -> None:
    contracts = (
        (
            "binding_set",
            "binding_set_ref",
            "dpone.binding-set.v1",
        ),
        (
            "connection_registry",
            "connection_registry_ref",
            "dpone.connection-registry.v1",
        ),
        (
            "credential_runtime",
            "credential_runtime_ref",
            "dpone.credential-runtime.v1",
        ),
    )
    for artifact_field, fingerprint_field, schema in contracts:
        payload = runtime_connections[artifact_field]
        if (
            payload.get("schema") != schema
            or payload.get("environment") != plan.environment
            or deployment.get(fingerprint_field) != canonical_fingerprint(payload)
        ):
            raise _integrity_error("runtime connection receipt does not match the pinned deployment")


def _validate_release_membership(
    plan: RuntimeInitFetchPlan,
    release: Mapping[str, Any],
) -> None:
    artifacts = release.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise _integrity_error("release-set artifact inventory is invalid")
    packs = artifacts.get("workload_packs")
    if not isinstance(packs, list):
        raise _integrity_error("release-set workload inventory is invalid")
    release_prefix = f"cache://releases/{plan.release_id.replace(':', '-')}/"
    expected_path = plan.workload_pack.artifact_ref.removeprefix(release_prefix)
    for item in packs:
        if not isinstance(item, Mapping):
            continue
        locator = item.get("path", item.get("artifact_ref"))
        if (
            item.get("id") == plan.workload_pack.id
            and item.get("sha256") == plan.workload_pack.sha256
            and locator == expected_path
        ):
            break
    else:
        raise _integrity_error("release-set does not contain the pinned workload pack")
    _validate_runtime_payload_membership(plan, artifacts)


def _validate_runtime_payload_membership(
    plan: RuntimeInitFetchPlan,
    artifacts: Mapping[str, Any],
) -> None:
    if not plan.runtime_payloads:
        return
    release_payloads = artifacts.get("runtime_payloads")
    if not isinstance(release_payloads, list):
        raise _integrity_error("release-set runtime payload inventory is invalid")
    release_prefix = f"cache://releases/{plan.release_id.replace(':', '-')}/"
    inventory = {
        (
            item.get("id"),
            item.get("kind"),
            item.get("path", item.get("artifact_ref")),
            item.get("sha256"),
            item.get("bytes"),
            item.get("media_type"),
        )
        for item in release_payloads
        if isinstance(item, Mapping)
    }
    for payload in plan.runtime_payloads:
        expected = (
            payload.id,
            payload.kind,
            payload.artifact_ref.removeprefix(release_prefix),
            payload.sha256,
            payload.bytes,
            payload.media_type,
        )
        if expected not in inventory:
            raise _integrity_error("release-set does not contain the pinned runtime payload")


def _json_object(payload: bytes, label: str) -> Mapping[str, Any]:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
            parse_float=_parse_finite_float,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise _integrity_error(f"{label} is not valid canonical JSON") from exc
    if not isinstance(value, Mapping):
        raise _integrity_error(f"{label} must be a JSON object")
    return value


def _unique_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value}")


def _parse_finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("non-finite JSON number")
    return parsed


def _integrity_error(message: str) -> InitFetchError:
    return InitFetchError("DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED", message)


__all__ = ["validate_runtime_receipts"]
