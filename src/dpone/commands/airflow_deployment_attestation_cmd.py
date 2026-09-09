"""CLI contract for protected Airflow artifact signing and publication."""

from __future__ import annotations

import argparse
import logging

from dpone.commands.airflow_self_service_output import emit_self_service_result
from dpone.readiness.airflow_artifact_delivery import ArtifactRegistryOptions
from dpone.readiness.airflow_deployment_attestation import (
    prepare_artifact_attestation,
    publish_artifact_attestation,
)
from dpone.readiness.airflow_deployment_trust_policy import (
    render_airflow_deployment_trust_policy,
)


def artifact_attestation_group():
    from dpone.commands.func_command import CommandGroup, FuncCommand

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser(
            "artifact-attestation",
            help="Prepare or publish signed immutable Airflow deployment evidence",
        )

    return CommandGroup(
        name="artifact-attestation",
        help="Airflow artifact-attestation lifecycle",
        build_parser=build,
        subcommands=[
            FuncCommand(
                "prepare",
                register_prepare_parser,
                cmd_prepare,
                _requires_app_context=False,
            ),
            FuncCommand(
                "publish",
                register_publish_parser,
                cmd_publish,
                _requires_app_context=False,
            ),
            FuncCommand(
                "policy-render",
                register_policy_render_parser,
                cmd_policy_render,
                _requires_app_context=False,
            ),
        ],
        subdest="airflow_artifact_attestation_cmd",
    )


def register_prepare_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("prepare", help="Build the canonical statement for external cosign")
    parser.add_argument("--cache-root", required=True, help="Materialized exact deployment cache root")
    parser.add_argument("--release-id", required=True, help="Canonical sha256 release identity")
    parser.add_argument("--deployment-id", required=True, help="Canonical sha256 deployment identity")
    parser.add_argument("--environment", required=True, help="Deployment environment claim")
    parser.add_argument("--artifact-registry-ref", required=True, help="Trusted logical registry reference")
    parser.add_argument("--registry-scope-id", required=True, help="Endpoint-bound registry authority digest")
    parser.add_argument("--publication-evidence", required=True, help="Exact publish v2 evidence JSON")
    parser.add_argument("--source-project", required=True, help="Source Git project path")
    parser.add_argument("--source-ref", required=True, help="Protected source Git ref")
    parser.add_argument("--source-git-sha", required=True, help="Exact source commit SHA")
    parser.add_argument(
        "--issued-at",
        required=True,
        help="Stable RFC 3339 pipeline creation time, for example CI_PIPELINE_CREATED_AT",
    )
    parser.add_argument("--output", required=True, help="Immutable canonical statement output path")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Result output format")
    return parser


def register_publish_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("publish", help="Verify and publish one signed statement package")
    parser.add_argument("--cache-root", required=True, help="Materialized exact deployment cache root")
    parser.add_argument("--publication-evidence", required=True, help="Exact publish v2 evidence JSON")
    parser.add_argument("--statement", required=True, help="Canonical statement signed by cosign")
    parser.add_argument("--sigstore-bundle", required=True, help="Cosign v3 bundle for the statement")
    parser.add_argument("--trust-policy-path", required=True, help="Canonical deployment trust-policy path")
    parser.add_argument("--trust-key-root", required=True, help="Directory containing allowlisted public keys")
    parser.add_argument("--registry-uri", required=True, help="Allowlisted artifact registry root URI")
    access = parser.add_mutually_exclusive_group(required=True)
    access.add_argument("--local-registry-root", help="Local registry root for tests and development")
    access.add_argument("--identity-mode", choices=["workload_identity"], help="Cloud workload identity")
    access.add_argument("--connection-id", help="Credential provider connection identifier")
    parser.add_argument(
        "--connection-type",
        choices=["airflow", "env", "vault"],
        help="Required credential provider when --connection-id is used",
    )
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Result output format")
    return parser


def register_policy_render_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "policy-render",
        help="Validate readable JSON and write canonical deployment trust-policy bytes",
    )
    parser.add_argument("--input", required=True, help="Human-readable policy JSON input")
    parser.add_argument("--output", required=True, help="Immutable canonical policy output path")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Result output format")
    return parser


def cmd_prepare(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = prepare_artifact_attestation(
        cache_root=args.cache_root,
        release_id=args.release_id,
        deployment_id=args.deployment_id,
        environment=args.environment,
        artifact_registry_ref=args.artifact_registry_ref,
        registry_scope_id=args.registry_scope_id,
        publication_evidence_path=args.publication_evidence,
        source_project=args.source_project,
        source_ref=args.source_ref,
        source_git_sha=args.source_git_sha,
        issued_at=args.issued_at,
        output_path=args.output,
    )
    emit_self_service_result(result, args.format, command="airflow_artifact_attestation_prepare")
    return result.exit_code or 0


def cmd_publish(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = publish_artifact_attestation(
        cache_root=args.cache_root,
        publication_evidence_path=args.publication_evidence,
        statement_path=args.statement,
        sigstore_bundle_path=args.sigstore_bundle,
        trust_policy_path=args.trust_policy_path,
        trust_key_root=args.trust_key_root,
        registry_options=ArtifactRegistryOptions(
            registry_uri=args.registry_uri,
            local_registry_root=args.local_registry_root,
            identity_mode=args.identity_mode,
            connection_type=args.connection_type,
            connection_id=args.connection_id,
        ),
    )
    emit_self_service_result(result, args.format, command="airflow_artifact_attestation_publish")
    return result.exit_code or 0


def cmd_policy_render(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = render_airflow_deployment_trust_policy(
        input_path=args.input,
        output_path=args.output,
    )
    emit_self_service_result(
        result,
        args.format,
        command="airflow_deployment_trust_policy_render",
    )
    return result.exit_code or 0


__all__ = ["artifact_attestation_group"]
