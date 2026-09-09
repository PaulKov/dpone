"""CLI composition roots for immutable Airflow artifact delivery."""

from __future__ import annotations

import argparse
import logging
from typing import TypedDict

from dpone.commands.airflow_self_service_output import emit_self_service_result
from dpone.readiness.airflow_artifact_delivery import (
    DEFAULT_MAX_OBJECT_BYTES,
    DEFAULT_MAX_TOTAL_BYTES,
    ArtifactRegistryOptions,
    materialize_command_result,
    publish_command_result,
)


def register_artifact_publish_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("publish", help="Publish one pinned release/deployment to an immutable registry")
    _add_common_arguments(parser)
    parser.add_argument(
        "--artifact-attestation-bundle",
        help="Portable offline attestation bundle for the exact release-set.json",
    )
    parser.add_argument(
        "--expected-registry-scope-id",
        help="Optional protected sha256 identity for the exact endpoint-bound registry authority",
    )
    parser.add_argument(
        "--publication-mode",
        choices=["compatible", "exact"],
        default="compatible",
        help="Compatible v1 receipt or exact v2 remote-readback commitment",
    )
    return parser


def register_cache_materialize_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cache-materialize",
        help="Download one pinned release/deployment without activating current",
    )
    _add_common_arguments(parser)
    parser.add_argument(
        "--trust-policy-path",
        default="/etc/dpone/artifact-trust/policy.json",
        help="Read-only mounted production artifact trust policy",
    )
    parser.add_argument(
        "--trust-key-root",
        default="/etc/dpone/artifact-trust",
        help="Read-only directory containing policy-allowlisted public keys",
    )
    return parser


def cmd_airflow_artifact_publish(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = publish_command_result(
        **_result_arguments(args),
        attestation_bundle_path=args.artifact_attestation_bundle,
        expected_registry_scope_id=args.expected_registry_scope_id,
        publication_mode=args.publication_mode,
    )
    emit_self_service_result(result, args.format, command="airflow_artifact_publish")
    return result.exit_code if result.exit_code is not None else (0 if result.passed else 1)


def cmd_airflow_cache_materialize(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = materialize_command_result(
        **_result_arguments(args),
        trust_policy_path=args.trust_policy_path,
        trust_key_root=args.trust_key_root,
    )
    emit_self_service_result(result, args.format, command="airflow_cache_materialize")
    return result.exit_code if result.exit_code is not None else (0 if result.passed else 1)


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cache-root", default=".dpone-cache", help="Local dpone cache root")
    parser.add_argument("--release-id", required=True, help="Exact canonical release digest")
    parser.add_argument("--deployment-id", required=True, help="Exact canonical deployment digest")
    parser.add_argument("--environment", required=True, help="Deployment environment")
    parser.add_argument("--artifact-registry-ref", required=True, help="Deployment-owned logical registry ref")
    parser.add_argument("--registry-uri", required=True, help="Object-storage registry root URI")
    access = parser.add_mutually_exclusive_group(required=True)
    access.add_argument("--local-registry-root", help="Local object-store emulator root for deterministic proof")
    access.add_argument("--identity-mode", choices=["workload_identity"], help="Platform workload identity")
    access.add_argument("--connection-id", help="Logical dpone credential reference")
    parser.add_argument(
        "--connection-type",
        choices=["airflow", "env", "vault"],
        default=None,
        help="Credential resolver used with --connection-id",
    )
    parser.add_argument("--max-object-bytes", type=int, default=DEFAULT_MAX_OBJECT_BYTES)
    parser.add_argument("--max-total-bytes", type=int, default=DEFAULT_MAX_TOTAL_BYTES)
    parser.add_argument("--format", choices=["text", "json"], default="text")


class _ResultArguments(TypedDict):
    cache_root: str
    release_id: str
    deployment_id: str
    environment: str
    artifact_registry_ref: str
    max_object_bytes: int
    max_total_bytes: int
    registry_options: ArtifactRegistryOptions


def _result_arguments(args: argparse.Namespace) -> _ResultArguments:
    return {
        "cache_root": args.cache_root,
        "release_id": args.release_id,
        "deployment_id": args.deployment_id,
        "environment": args.environment,
        "artifact_registry_ref": args.artifact_registry_ref,
        "max_object_bytes": args.max_object_bytes,
        "max_total_bytes": args.max_total_bytes,
        "registry_options": ArtifactRegistryOptions(
            registry_uri=args.registry_uri,
            local_registry_root=args.local_registry_root,
            identity_mode=args.identity_mode,
            connection_type=getattr(args, "connection_type", None),
            connection_id=getattr(args, "connection_id", None),
        ),
    }


__all__ = [
    "cmd_airflow_artifact_publish",
    "cmd_airflow_cache_materialize",
    "register_artifact_publish_parser",
    "register_cache_materialize_parser",
]
