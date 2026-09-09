"""Thin platform CLI handlers for route-attestation build and verification."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.readiness.route_attestation import (
    RouteAttestationFileError,
    expected_subject_for_deployment,
    json_bytes,
    read_strict_json_mapping,
    verify_route_attestation_files,
    write_create_only,
)
from dpone.services.route_attestation_builder import RouteAttestationBuilder, RouteAttestationBuildError

from .command_helpers import _emit, _route_attestation_exit_code

_MAX_CONTROL_BYTES = 4 * 1024 * 1024


def cmd_route_attestation_build(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        certification, certification_bytes = read_strict_json_mapping(
            args.route_certification_bundle,
            max_bytes=_MAX_CONTROL_BYTES,
            label="route-certification bundle",
        )
        deployment, _ = read_strict_json_mapping(
            args.deployment_set,
            max_bytes=_MAX_CONTROL_BYTES,
            label="deployment-set",
        )
        artifact = RouteAttestationBuilder().build(
            route_id=args.route_id,
            certification_bundle=certification,
            certification_bundle_bytes=certification_bytes,
            deployment=deployment,
            authorization_profile=args.authorization_profile,
            issued_at=args.issued_at,
            not_before=args.not_before,
            expires_at=args.expires_at,
        )
        write_create_only(args.output, artifact.to_bytes())
    except (RouteAttestationBuildError, RouteAttestationFileError) as exc:
        error = _error(exc.code, _safe_message(exc))
        _emit(error, _error_markdown(error), args.format)
        return 2
    except FileExistsError:
        error = _error(
            "DPONE_ROUTE_ATTESTATION_OUTPUT_EXISTS",
            "Route-attestation output already exists; use a new immutable path.",
        )
        _emit(error, _error_markdown(error), args.format)
        return 2
    payload = artifact.to_dict()
    _emit(payload, _build_markdown(payload), args.format)
    return 0


def cmd_route_attestation_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        attestation, _ = read_strict_json_mapping(
            args.attestation,
            max_bytes=256 * 1024,
            label="route attestation",
        )
        deployment, _ = read_strict_json_mapping(
            args.deployment_set,
            max_bytes=_MAX_CONTROL_BYTES,
            label="deployment-set",
        )
        expected = expected_subject_for_deployment(attestation=attestation, deployment=deployment)
        verification = verify_route_attestation_files(
            attestation_path=args.attestation,
            sigstore_bundle_path=args.sigstore_bundle,
            certification_bundle_path=args.route_certification_bundle,
            policy_path=args.policy,
            expected=expected,
            signature_verifier=getattr(args, "signature_verifier", None),
        )
        output = Path(args.output_dir) / "route-attestation-verification.json"
        write_create_only(output, json_bytes(verification.to_dict()))
    except (RouteAttestationBuildError, RouteAttestationFileError, ValueError) as exc:
        code = getattr(exc, "code", "DPONE_ROUTE_ATTESTATION_INPUT_INVALID")
        error = _error(str(code), _safe_message(exc))
        _emit(error, _error_markdown(error), args.format)
        return 2
    except FileExistsError:
        error = _error(
            "DPONE_ROUTE_ATTESTATION_OUTPUT_EXISTS",
            "Route-attestation verification receipt already exists; use a new run directory.",
        )
        _emit(error, _error_markdown(error), args.format)
        return 2
    payload = verification.to_dict()
    _emit(payload, _verify_markdown(payload), args.format)
    return 0 if verification.is_verified else _route_attestation_exit_code(verification.code)


def _build_markdown(payload: dict[str, object]) -> str:
    claims = payload.get("claims")
    route = claims.get("route") if isinstance(claims, dict) else {}
    subject = claims.get("subject") if isinstance(claims, dict) else {}
    return (
        "# dpone Route Attestation Build\n\n"
        f"- attestation_id: `{payload.get('attestation_id')}`\n"
        f"- route_id: `{route.get('route_id') if isinstance(route, dict) else None}`\n"
        f"- deployment_id: `{subject.get('deployment_id') if isinstance(subject, dict) else None}`\n"
        "- next: sign the exact output bytes with your approved external cosign workflow\n"
    )


def _verify_markdown(payload: dict[str, object]) -> str:
    return (
        "# dpone Route Attestation Verification\n\n"
        f"- decision: `{payload.get('decision')}`\n"
        f"- code: `{payload.get('code')}`\n"
        f"- attestation_id: `{payload.get('attestation_id')}`\n"
        f"- release_id: `{payload.get('release_id')}`\n"
        f"- deployment_id: `{payload.get('deployment_id')}`\n"
    )


def _error(code: str, message: str) -> dict[str, object]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "route_attestation",
        "severity": "error",
        "message": message,
        "fixes": [],
    }


def _error_markdown(error: dict[str, object]) -> str:
    return f"# dpone Route Attestation\n\n- error: `{error.get('code')}`\n- message: {error.get('message')}\n"


def _safe_message(exc: Exception) -> str:
    message = " ".join(str(exc).split()) or exc.__class__.__name__
    return message[:500]


__all__ = ["cmd_route_attestation_build", "cmd_route_attestation_verify"]
