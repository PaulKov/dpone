"""CLI adapter for building one immutable Airflow deployment projection."""

from __future__ import annotations

import argparse
import logging

from dpone.commands.airflow_self_service_output import emit_self_service_result
from dpone.contracts.composition_supervisor import CompositionSupervisorProjection
from dpone.readiness.airflow_deployment_errors import deployment_projection_error
from dpone.readiness.airflow_deployment_projection import AirflowDeploymentProjectionError
from dpone.readiness.airflow_self_service_deployment import build_deployment_result
from dpone.readiness.airflow_self_service_models import SelfServiceResult


def register_build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    """Register the strict environment deployment build command."""

    parser = subparsers.add_parser("build", help="Materialize an environment Airflow deployment projection")
    parser.add_argument("--release-id", required=True, help="Environment-neutral release-set digest")
    parser.add_argument("--environment", required=True, help="Environment name, for example dev or prod")
    parser.add_argument(
        "--trust-tier",
        choices=["production", "non_production"],
        help="Explicit deployment trust tier required by strict v2; never inferred from the environment name",
    )
    parser.add_argument(
        "--runtime-image-ref",
        help="Exact OCI runtime image ref ending in @sha256:<digest>; required by strict v2",
    )
    parser.add_argument("--runtime-image-digest", required=True, help="Pinned dpone runtime image digest")
    parser.add_argument(
        "--runtime-image-dbt-ref",
        help="Exact OCI dpone-dbt runtime image ref ending in @sha256:<digest>; optional dual-digest flavor",
    )
    parser.add_argument(
        "--runtime-image-dbt-digest",
        help="Pinned dpone-dbt runtime image digest selected for dbt__* workloads",
    )
    parser.add_argument(
        "--artifact-registry-ref",
        required=True,
        help="Deployment-owned bounded logical artifact registry reference",
    )
    parser.add_argument("--registry-config-map-name", help="Artifact registry ConfigMap name required by strict v2")
    parser.add_argument(
        "--registry-config-map-key",
        help="Artifact registry ConfigMap key; defaults to registry.json when the ConfigMap is selected",
    )
    parser.add_argument(
        "--registry-config-sha256",
        help="SHA-256 of the exact artifact registry ConfigMap file bytes required by strict v2",
    )
    parser.add_argument("--trust-policy-config-map-name", help="Production artifact trust-policy ConfigMap name")
    parser.add_argument(
        "--trust-policy-config-map-key",
        help="Artifact trust-policy ConfigMap key; defaults to policy.json when the ConfigMap is selected",
    )
    parser.add_argument(
        "--trust-policy-sha256",
        help="SHA-256 of the exact artifact trust-policy ConfigMap file bytes",
    )
    parser.add_argument(
        "--airflow-bundle-ref",
        help="Airflow DAG Bundle reference required by strict deployment projection policy",
    )
    parser.add_argument(
        "--dev-evidence-pvc-claim",
        help="Non-production RWX PVC used only for exact dbt promotion evidence",
    )
    parser.add_argument(
        "--dev-evidence-worker-queue",
        help="Dedicated Airflow worker queue that mounts the dev evidence PVC",
    )
    parser.add_argument(
        "--composition-supervisor-pvc",
        help="Administrator-provisioned ReadWriteMany PVC for v3 composition supervision",
    )
    parser.add_argument(
        "--composition-child-uid-start",
        type=int,
        help="First reserved numeric child UID for v3 composition supervision",
    )
    parser.add_argument(
        "--composition-child-gid-start",
        type=int,
        help="First reserved numeric child GID for v3 composition supervision",
    )
    parser.add_argument(
        "--composition-child-identity-count",
        type=int,
        help="Reserved child UID/GID range size; must be at least 1000000",
    )
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def cmd_airflow_build(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    """Build one strict deployment projection and emit its public result."""

    del ctx, logger
    supervisor, supervisor_error = _composition_supervisor_from_args(args)
    if supervisor_error is not None:
        result = supervisor_error
    else:
        result = _build_deployment_result(args, supervisor)
    emit_self_service_result(result, args.format, command="airflow_build")
    if result.exit_code is not None:
        return result.exit_code
    return 0 if result.passed else 1


def _build_deployment_result(
    args: argparse.Namespace,
    supervisor: CompositionSupervisorProjection | None,
) -> SelfServiceResult:
    return build_deployment_result(
        root=".",
        release_id=args.release_id,
        environment=args.environment,
        trust_tier=args.trust_tier,
        runtime_image_ref=args.runtime_image_ref,
        runtime_image_digest=args.runtime_image_digest,
        runtime_image_dbt_ref=args.runtime_image_dbt_ref,
        runtime_image_dbt_digest=args.runtime_image_dbt_digest,
        artifact_registry_ref=args.artifact_registry_ref,
        registry_config_ref=_registry_config_ref(args),
        trust_policy_ref=_trust_policy_ref(args),
        airflow_bundle_ref=args.airflow_bundle_ref,
        dev_evidence_pvc_claim=args.dev_evidence_pvc_claim,
        dev_evidence_worker_queue=args.dev_evidence_worker_queue,
        composition_supervisor=supervisor.to_dict() if supervisor is not None else None,
    )


def _composition_supervisor_from_args(
    args: argparse.Namespace,
) -> tuple[CompositionSupervisorProjection | None, SelfServiceResult | None]:
    values = {
        "schema": "dpone.composition-supervisor.v1",
        "persistent_volume_claim": args.composition_supervisor_pvc,
        "child_uid_start": args.composition_child_uid_start,
        "child_gid_start": args.composition_child_gid_start,
        "child_identity_count": args.composition_child_identity_count,
    }
    configured = [value is not None for field, value in values.items() if field != "schema"]
    if not any(configured):
        return None, None
    if not all(configured):
        return None, _supervisor_configuration_error(
            args,
            code="DPONE_COMPOSITION_SUPERVISOR_GROUP_INCOMPLETE",
            message="composition supervisor options must be provided together",
        )
    try:
        return CompositionSupervisorProjection.from_mapping(values), None
    except ValueError:
        return None, _supervisor_configuration_error(
            args,
            code="DPONE_COMPOSITION_SUPERVISOR_INVALID",
            message="composition supervisor deployment capability is invalid",
        )


def _supervisor_configuration_error(
    args: argparse.Namespace,
    *,
    code: str,
    message: str,
) -> SelfServiceResult:
    error = AirflowDeploymentProjectionError(code, message)
    return SelfServiceResult(
        passed=False,
        errors=(
            deployment_projection_error(
                error,
                release_id=args.release_id,
                environment=args.environment,
            ),
        ),
        details={"release_id": args.release_id, "environment": args.environment},
        exit_code=2,
    )


def _registry_config_ref(args: argparse.Namespace) -> dict[str, object] | None:
    return _optional_config_map_ref(
        name=args.registry_config_map_name,
        key=args.registry_config_map_key,
        sha256=args.registry_config_sha256,
        default_key="registry.json",
    )


def _trust_policy_ref(args: argparse.Namespace) -> dict[str, object] | None:
    return _optional_config_map_ref(
        name=args.trust_policy_config_map_name,
        key=args.trust_policy_config_map_key,
        sha256=args.trust_policy_sha256,
        default_key="policy.json",
    )


def _optional_config_map_ref(
    *,
    name: object,
    key: object,
    sha256: object,
    default_key: str,
) -> dict[str, object] | None:
    if name is None and key is None and sha256 is None:
        return None
    return {
        "kind": "kubernetes_config_map",
        "name": name,
        "key": key if key is not None else default_key,
        "sha256": sha256,
    }


__all__ = ["cmd_airflow_build", "register_build_parser"]
