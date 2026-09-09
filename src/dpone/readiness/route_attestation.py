"""Composition root for local signed route-attestation verification."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.readiness.route_attestation_files import (
    RouteAttestationFileError,
    json_bytes,
    read_bounded_file,
    read_strict_json_mapping,
    write_create_only,
)
from dpone.services.route_attestation_verification import (
    RouteAttestationExpectedSubject,
    RouteAttestationSignatureVerifier,
    RouteAttestationVerification,
    RouteAttestationVerificationService,
    certified_route_id_for_key,
)

if TYPE_CHECKING:
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan

_ATTESTATION_MAX_BYTES = 256 * 1024
_POLICY_MAX_BYTES = 256 * 1024
_BUNDLE_MAX_BYTES = 4 * 1024 * 1024
_CERTIFICATION_MAX_BYTES = 4 * 1024 * 1024
_TRUST_ROOT_MAX_BYTES = 4 * 1024 * 1024


class RouteAttestationReadinessError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def verify_route_attestation_files(
    *,
    attestation_path: str | Path,
    sigstore_bundle_path: str | Path,
    certification_bundle_path: str | Path,
    policy_path: str | Path,
    expected: RouteAttestationExpectedSubject,
    signature_verifier: RouteAttestationSignatureVerifier | None = None,
) -> RouteAttestationVerification:
    """Read bounded immutable files and verify one exact runtime subject."""

    attestation = read_bounded_file(
        attestation_path,
        max_bytes=_ATTESTATION_MAX_BYTES,
        label="route attestation",
    )
    sigstore_bundle = read_bounded_file(
        sigstore_bundle_path,
        max_bytes=_BUNDLE_MAX_BYTES,
        label="Sigstore bundle",
    )
    certification_bundle = read_bounded_file(
        certification_bundle_path,
        max_bytes=_CERTIFICATION_MAX_BYTES,
        label="route-certification bundle",
    )
    policy, _ = read_strict_json_mapping(
        policy_path,
        max_bytes=_POLICY_MAX_BYTES,
        label="route-attestation policy",
    )
    trusted_root_path = _trusted_root_path(policy, policy_path=Path(policy_path))
    trusted_root = read_bounded_file(
        trusted_root_path,
        max_bytes=_TRUST_ROOT_MAX_BYTES,
        label="Sigstore trusted root",
    )
    return RouteAttestationVerificationService(
        signature_verifier=signature_verifier or _default_signature_verifier()
    ).verify(
        attestation=attestation,
        sigstore_bundle=sigstore_bundle,
        certification_bundle=certification_bundle,
        policy=policy,
        trusted_root=trusted_root,
        expected=expected,
    )


def expected_subject_for_safe_sample(
    plan: SafeSampleExecutionPlan,
    pipeline_source: Mapping[str, Any],
) -> RouteAttestationExpectedSubject:
    """Derive the expected subject from pinned runtime facts, never plan proof."""

    context = plan.deployment_context
    target = plan.temporary_target_plan
    if context is None or target is None:
        raise _subject_error("Live safe-sample plan has no pinned deployment or process.")
    route_id = _route_id_for_pipeline(pipeline_source, process_name=target.process)
    image = str(context.runtime_image_digest or "")
    if not all((context.release_id, context.deployment_id, image)):
        raise _subject_error("Live safe-sample deployment subject is incomplete.")
    return RouteAttestationExpectedSubject(
        route_id=route_id,
        release_id=context.release_id,
        deployment_id=context.deployment_id,
        environment=_environment(context.environment or plan.environment),
        runtime_image_digest=image,
        authorization_profile="safe_sample_production",
    )


def expected_subject_for_deployment(
    *,
    attestation: Mapping[str, Any],
    deployment: Mapping[str, Any],
) -> RouteAttestationExpectedSubject:
    """Build standalone preflight expectations from deployment plus route ID."""

    claims = attestation.get("claims")
    route = claims.get("route") if isinstance(claims, Mapping) else None
    route_id = str(route.get("route_id") or "") if isinstance(route, Mapping) else ""
    release_id = str(deployment.get("release_ref") or "")
    deployment_id = str(deployment.get("deployment_id") or "")
    image = str(deployment.get("runtime_image_digest") or "")
    environment = _environment(deployment.get("environment"))
    if not all((route_id, release_id, deployment_id, image, environment)):
        raise _subject_error("Standalone route-attestation subject is incomplete.")
    return RouteAttestationExpectedSubject(
        route_id=route_id,
        release_id=release_id,
        deployment_id=deployment_id,
        environment=environment,
        runtime_image_digest=image,
        authorization_profile="safe_sample_production",
    )


def _route_id_for_pipeline(pipeline: Mapping[str, Any], *, process_name: str) -> str:
    processes = pipeline.get("processes")
    if not isinstance(processes, list):
        raise _subject_error("Pipeline processes are missing.")
    process = next(
        (item for item in processes if isinstance(item, Mapping) and str(item.get("name") or "") == process_name),
        None,
    )
    if not isinstance(process, Mapping):
        raise _subject_error("Pinned safe-sample process is missing.")
    source = process.get("source")
    sink = process.get("sink")
    if not isinstance(source, Mapping) or not isinstance(sink, Mapping):
        raise _subject_error("Safe-sample route source or sink is missing.")
    strategy = sink.get("strategy")
    strategy_value = strategy.get("mode") if isinstance(strategy, Mapping) else strategy
    key = (
        canonical_endpoint_type(str(source.get("type") or "")),
        canonical_endpoint_type(str(sink.get("type") or "")),
        _normalized(strategy_value),
    )
    route_id = certified_route_id_for_key(key)
    if route_id is None:
        raise _subject_error("Safe-sample route is not present in the certified route catalog.")
    return route_id


def _default_signature_verifier() -> RouteAttestationSignatureVerifier:
    adapter = importlib.import_module("dpone.adapters.cosign_route_attestation")
    return adapter.CosignRouteAttestationSignatureVerifier()


def _trusted_root_path(policy: Mapping[str, Any], *, policy_path: Path) -> Path:
    trusted_root = policy.get("trusted_root")
    raw = trusted_root.get("path") if isinstance(trusted_root, Mapping) else None
    text = str(raw or "").strip()
    if not text:
        raise RouteAttestationReadinessError(
            "DPONE_ROUTE_ATTESTATION_POLICY_INVALID",
            "Route-attestation policy has no trusted-root path.",
        )
    path = Path(text)
    return path if path.is_absolute() else policy_path.parent / path


def _environment(value: object) -> str:
    normalized = _normalized(value)
    if normalized in {"prod", "production"}:
        return "production"
    if normalized in {"dev", "development", "local"}:
        return "development"
    return normalized


def _normalized(value: object) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _subject_error(message: str) -> RouteAttestationReadinessError:
    return RouteAttestationReadinessError("DPONE_ROUTE_ATTESTATION_SUBJECT_INVALID", message)


__all__ = [
    "RouteAttestationExpectedSubject",
    "RouteAttestationFileError",
    "RouteAttestationReadinessError",
    "RouteAttestationSignatureVerifier",
    "RouteAttestationVerification",
    "expected_subject_for_deployment",
    "expected_subject_for_safe_sample",
    "json_bytes",
    "read_strict_json_mapping",
    "verify_route_attestation_files",
    "write_create_only",
]
