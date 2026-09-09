"""Pod-side launch envelope persistence and extraction."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.deployment_identity import (
    AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
    deployment_identity_error,
    serialize_deployment_identity,
)
from dpone_airflow_pack.launch_pin_codes import PIN_INVALID, PIN_MISSING
from dpone_airflow_pack.launch_pin_envelope_env import base_container_env_map, merge_base_container_env
from dpone_airflow_pack.outcome_identity import is_sha256, run_identity_error
from dpone_airflow_pack.run_identity import AIRFLOW_RUN_IDENTITY_ENV, serialize_run_identity
from dpone_airflow_pack.strict_json import loads_strict_json_object

AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV = "DPONE_AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256"
AIRFLOW_ENVELOPE_SHA256_ENV = "DPONE_AIRFLOW_ENVELOPE_SHA256"
AIRFLOW_RUNTIME_LAUNCH_ENVELOPE_ANNOTATION = "dpone.airflow/runtime-launch-envelope.v1"
AIRFLOW_LAUNCH_PIN_RETAIN_ANNOTATION = "dpone.airflow/retain-for-outcome-gate"


def envelope_sha256(
    *,
    run_identity: Mapping[str, Any] | None,
    deployment_identity: Mapping[str, Any] | None,
    expected_runtime_evidence_sha256: str | None,
) -> str:
    """Canonical digest of the three launch-envelope identity fields."""

    body = {
        "deployment_identity": (
            {key: str(deployment_identity[key]) for key in ("schema", "release_id", "deployment_id", "activation_id")}
            if isinstance(deployment_identity, Mapping)
            else None
        ),
        "expected_runtime_evidence_sha256": expected_runtime_evidence_sha256,
        "run_identity": dict(run_identity) if isinstance(run_identity, Mapping) else None,
    }
    encoded = json.dumps(body, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def ensure_launch_envelope_on_pod(
    pod: Any,
    *,
    run_identity: Mapping[str, Any] | None,
    deployment_identity: Mapping[str, Any] | None,
    expected_runtime_evidence_sha256: str | None,
) -> Any:
    """Persist the full launch envelope on the pod request (annotation + base env)."""

    digest = envelope_sha256(
        run_identity=run_identity,
        deployment_identity=deployment_identity,
        expected_runtime_evidence_sha256=expected_runtime_evidence_sha256,
    )
    envelope = {
        "run_identity": dict(run_identity) if isinstance(run_identity, Mapping) else None,
        "deployment_identity": (
            {key: str(deployment_identity[key]) for key in ("schema", "release_id", "deployment_id", "activation_id")}
            if isinstance(deployment_identity, Mapping)
            else None
        ),
        "expected_runtime_evidence_sha256": expected_runtime_evidence_sha256,
        "envelope_sha256": digest,
    }
    _set_pod_annotation(
        pod,
        AIRFLOW_RUNTIME_LAUNCH_ENVELOPE_ANNOTATION,
        json.dumps(envelope, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
    )
    # Marker for platform retention sweeper: keep_pod pods retained until gate
    # cleanup or bounded creation-age sweep if the gate never starts.
    _set_pod_annotation(pod, AIRFLOW_LAUNCH_PIN_RETAIN_ANNOTATION, "true")
    env: dict[str, str] = {}
    if isinstance(run_identity, Mapping):
        env[AIRFLOW_RUN_IDENTITY_ENV] = serialize_run_identity(run_identity)
    if isinstance(deployment_identity, Mapping):
        env[AIRFLOW_DEPLOYMENT_IDENTITY_ENV] = serialize_deployment_identity(deployment_identity)
    env[AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV] = (
        "" if expected_runtime_evidence_sha256 is None else str(expected_runtime_evidence_sha256)
    )
    env[AIRFLOW_ENVELOPE_SHA256_ENV] = digest
    merge_base_container_env(pod, env)
    return pod


def launch_envelope_from_pod(pod: Any) -> dict[str, Any]:
    """Extract the authoritative launch envelope from the selected pod only.

    Dual-channel contract: when the annotation is present, identity and digest
    env keys are required and must agree with it. Kubernetes reconciliation may
    omit the optional evidence env key only when the annotation authoritatively
    records null evidence. Disagreement or any other partial env is
    ``PIN_INVALID`` (tamper / partial mutation).
    """

    annotated = _envelope_from_annotation(pod)
    if annotated is not None:
        validated_annotated = _with_envelope_digest(annotated)
        # Annotation present ⇒ env channel is mandatory. The optional evidence
        # key may disappear when its injected value was the empty-string/null
        # representation, but only the authoritative annotated null permits it.
        try:
            env_envelope = _envelope_from_env(
                pod,
                required=True,
                allow_missing_null_evidence=annotated["expected_runtime_evidence_sha256"] is None,
            )
        except RuntimeError as exc:
            detail = str(exc)
            if detail.startswith(PIN_MISSING):
                raise RuntimeError(
                    f"{PIN_INVALID}: launch envelope annotation present but base-container env "
                    f"channel is incomplete: {detail}"
                ) from exc
            raise
        if env_envelope is None:
            raise RuntimeError(
                f"{PIN_INVALID}: launch envelope annotation present but base-container env channel is incomplete"
            )
        validated_env = _with_envelope_digest(env_envelope)
        if _envelope_identity_fields(validated_annotated) != _envelope_identity_fields(validated_env):
            raise RuntimeError(f"{PIN_INVALID}: launch envelope annotation disagrees with base-container env")
        return validated_annotated
    env_envelope = _envelope_from_env(pod, required=False)
    if env_envelope is not None:
        return _with_envelope_digest(env_envelope)
    raise RuntimeError(f"{PIN_MISSING}: selected pod has no launch envelope annotation or base-container env")


def _envelope_identity_fields(envelope: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "run_identity": envelope.get("run_identity"),
        "deployment_identity": envelope.get("deployment_identity"),
        "expected_runtime_evidence_sha256": envelope.get("expected_runtime_evidence_sha256"),
    }


def _with_envelope_digest(envelope: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(envelope)
    digest = envelope_sha256(
        run_identity=result.get("run_identity") if isinstance(result.get("run_identity"), Mapping) else None,
        deployment_identity=(
            result.get("deployment_identity") if isinstance(result.get("deployment_identity"), Mapping) else None
        ),
        expected_runtime_evidence_sha256=(
            result.get("expected_runtime_evidence_sha256")
            if isinstance(result.get("expected_runtime_evidence_sha256"), str)
            else None
        ),
    )
    annotated_digest = result.get("envelope_sha256")
    if annotated_digest is not None and annotated_digest != digest:
        raise RuntimeError(f"{PIN_INVALID}: launch envelope envelope_sha256 disagrees with envelope fields")
    result["envelope_sha256"] = digest
    return result


def _envelope_from_env(
    pod: Any,
    *,
    required: bool,
    allow_missing_null_evidence: bool = False,
) -> dict[str, Any] | None:
    env = base_container_env_map(pod)
    if not env:
        if required:
            raise RuntimeError(f"{PIN_MISSING}: selected pod has no launch envelope annotation or base-container env")
        return None
    required_names = [
        AIRFLOW_RUN_IDENTITY_ENV,
        AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
        AIRFLOW_ENVELOPE_SHA256_ENV,
    ]
    if not allow_missing_null_evidence:
        required_names.append(AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV)
    missing = [name for name in required_names if name not in env]
    if missing:
        if required:
            raise RuntimeError(f"{PIN_MISSING}: selected pod base-container env is missing {', '.join(missing)}")
        return None
    run_raw = env.get(AIRFLOW_RUN_IDENTITY_ENV) or ""
    deployment_raw = env.get(AIRFLOW_DEPLOYMENT_IDENTITY_ENV) or ""
    evidence_raw = env.get(AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV)
    digest_raw = str(env.get(AIRFLOW_ENVELOPE_SHA256_ENV) or "").strip()
    try:
        run_identity = loads_strict_json_object(run_raw) if run_raw.strip() else None
    except (TypeError, ValueError, RecursionError) as exc:
        raise RuntimeError(f"{PIN_INVALID}: selected pod run_identity env is malformed") from exc
    try:
        deployment_identity = loads_strict_json_object(deployment_raw) if deployment_raw.strip() else None
    except (TypeError, ValueError, RecursionError) as exc:
        raise RuntimeError(f"{PIN_INVALID}: selected pod deployment_identity env is malformed") from exc
    if isinstance(run_identity, Mapping) and run_identity_error(run_identity):
        raise RuntimeError(
            f"{PIN_INVALID}: selected pod run_identity env is invalid: {run_identity_error(run_identity)}"
        )
    if isinstance(deployment_identity, Mapping) and deployment_identity_error(deployment_identity):
        raise RuntimeError(
            f"{PIN_INVALID}: selected pod deployment_identity env is invalid: "
            f"{deployment_identity_error(deployment_identity)}"
        )
    if evidence_raw is None or str(evidence_raw).strip() == "":
        evidence: str | None = None
    else:
        evidence = str(evidence_raw)
        if not is_sha256(evidence):
            raise RuntimeError(f"{PIN_INVALID}: selected pod expected_runtime_evidence_sha256 env is malformed")
    if not is_sha256(digest_raw):
        raise RuntimeError(f"{PIN_INVALID}: selected pod envelope_sha256 env is malformed")
    return {
        "run_identity": dict(run_identity) if isinstance(run_identity, Mapping) else None,
        "deployment_identity": (
            {key: str(deployment_identity[key]) for key in ("schema", "release_id", "deployment_id", "activation_id")}
            if isinstance(deployment_identity, Mapping)
            else None
        ),
        "expected_runtime_evidence_sha256": evidence,
        "envelope_sha256": digest_raw,
    }


def pod_metadata_fields(pod: Any, *, require_uid: bool = True) -> tuple[str, str, str]:
    """Return ``(namespace, name, uid)`` from a pod object or mapping."""

    metadata = getattr(pod, "metadata", None)
    if metadata is None and isinstance(pod, Mapping):
        metadata = pod.get("metadata")
    if isinstance(metadata, Mapping):
        namespace = str(metadata.get("namespace") or "")
        name = str(metadata.get("name") or "")
        uid = str(metadata.get("uid") or "")
    else:
        namespace = str(getattr(metadata, "namespace", "") or "")
        name = str(getattr(metadata, "name", "") or "")
        uid = str(getattr(metadata, "uid", "") or "")
    if not name:
        raise RuntimeError(f"{PIN_INVALID}: selected pod is missing metadata.name")
    if require_uid and not uid:
        raise RuntimeError(f"{PIN_INVALID}: selected pod is missing metadata.uid")
    return namespace, name, uid


def pod_coordinates(pod: Any) -> tuple[str, str, str]:
    """Return selected pod namespace/name/uid or raise PIN_INVALID."""

    namespace, name, uid = pod_metadata_fields(pod, require_uid=True)
    if not namespace:
        raise RuntimeError(f"{PIN_INVALID}: selected pod is missing metadata.namespace")
    return namespace, name, uid


def _envelope_from_annotation(pod: Any) -> dict[str, Any] | None:
    metadata = getattr(pod, "metadata", None)
    if metadata is None and isinstance(pod, Mapping):
        metadata = pod.get("metadata")
    annotations = None
    if isinstance(metadata, Mapping):
        annotations = metadata.get("annotations")
    else:
        annotations = getattr(metadata, "annotations", None)
    if not isinstance(annotations, Mapping):
        return None
    raw = annotations.get(AIRFLOW_RUNTIME_LAUNCH_ENVELOPE_ANNOTATION)
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        raise RuntimeError(f"{PIN_INVALID}: launch envelope annotation must be a JSON object string")
    try:
        payload = loads_strict_json_object(raw)
    except (TypeError, ValueError, RecursionError) as exc:
        raise RuntimeError(f"{PIN_INVALID}: launch envelope annotation is malformed") from exc
    for field in ("run_identity", "deployment_identity", "expected_runtime_evidence_sha256", "envelope_sha256"):
        if field not in payload:
            raise RuntimeError(f"{PIN_INVALID}: launch envelope annotation missing {field}")
    run_identity = payload.get("run_identity")
    deployment_identity = payload.get("deployment_identity")
    evidence = payload.get("expected_runtime_evidence_sha256")
    digest = payload.get("envelope_sha256")
    if run_identity is not None and (not isinstance(run_identity, Mapping) or run_identity_error(run_identity)):
        raise RuntimeError(f"{PIN_INVALID}: launch envelope annotation run_identity is invalid")
    if deployment_identity is not None and (
        not isinstance(deployment_identity, Mapping) or deployment_identity_error(deployment_identity)
    ):
        raise RuntimeError(f"{PIN_INVALID}: launch envelope annotation deployment_identity is invalid")
    if evidence is not None and not is_sha256(evidence):
        raise RuntimeError(f"{PIN_INVALID}: launch envelope annotation evidence digest is invalid")
    if not is_sha256(digest):
        raise RuntimeError(f"{PIN_INVALID}: launch envelope annotation envelope_sha256 is invalid")
    return {
        "run_identity": dict(run_identity) if isinstance(run_identity, Mapping) else None,
        "deployment_identity": (
            {key: str(deployment_identity[key]) for key in ("schema", "release_id", "deployment_id", "activation_id")}
            if isinstance(deployment_identity, Mapping)
            else None
        ),
        "expected_runtime_evidence_sha256": evidence if isinstance(evidence, str) else None,
        "envelope_sha256": str(digest),
    }


def _set_pod_annotation(pod: Any, key: str, value: str) -> None:
    if isinstance(pod, dict):
        metadata = pod.setdefault("metadata", {})
        if not isinstance(metadata, dict):
            raise RuntimeError(f"{PIN_INVALID}: pod metadata must be a mutable mapping")
        annotations = metadata.setdefault("annotations", {})
        if not isinstance(annotations, dict):
            raise RuntimeError(f"{PIN_INVALID}: pod metadata.annotations must be a mutable mapping")
        annotations[key] = value
        return
    metadata = getattr(pod, "metadata", None)
    if metadata is None:
        raise RuntimeError(f"{PIN_INVALID}: selected pod is missing metadata")
    annotations = getattr(metadata, "annotations", None)
    if annotations is None:
        metadata.annotations = {key: value}
        return
    if isinstance(annotations, dict):
        annotations[key] = value
        return
    if isinstance(annotations, Mapping):
        metadata.annotations = {**dict(annotations), key: value}
        return
    raise RuntimeError(f"{PIN_INVALID}: pod metadata.annotations must be a mutable mapping")


__all__ = [
    "AIRFLOW_ENVELOPE_SHA256_ENV",
    "AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV",
    "AIRFLOW_LAUNCH_PIN_RETAIN_ANNOTATION",
    "AIRFLOW_RUNTIME_LAUNCH_ENVELOPE_ANNOTATION",
    "ensure_launch_envelope_on_pod",
    "envelope_sha256",
    "launch_envelope_from_pod",
    "pod_coordinates",
    "pod_metadata_fields",
]
