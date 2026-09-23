"""CLI adapter for building one immutable Airflow deployment projection."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from dpone.commands.airflow_self_service_output import emit_self_service_result
from dpone.readiness.airflow_desired_state_authority import AUTHORITY_FILE_ENV, load_airflow_desired_state_authority
from dpone.readiness.airflow_runtime_authority_input import (
    immutable_runtime_authority_payload_from_file,
)
from dpone.readiness.airflow_self_service_deployment import build_deployment_result


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
    parser.add_argument(
        "--registry-credentials-secret-name",
        help="Kubernetes Secret containing one runtime artifact registry Airflow Connection",
    )
    parser.add_argument(
        "--registry-credentials-secret-key",
        help="Exact AIRFLOW_CONN_* key; derived from the connection id when omitted",
    )
    parser.add_argument(
        "--registry-credentials-connection-id",
        help="Logical connection id used only by runtime init-fetch",
    )
    parser.add_argument(
        "--registry-credentials-connection-type",
        choices=["airflow", "env", "vault"],
        help="Credential provider; follows the standard dpone connection_type vocabulary",
    )
    parser.add_argument(
        "--registry-credentials-projection-mode",
        choices=["k8s_secret", "env"],
        help="How the selected credential variable is projected into the init container",
    )
    parser.add_argument(
        "--registry-credentials-env-name",
        help="Exact environment variable exposed only to runtime init-fetch",
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
    authority_source = parser.add_mutually_exclusive_group()
    authority_source.add_argument(
        "--runtime-authority-secret-name",
        help="Kubernetes Secret containing confidential runtime-authority configuration for a protected development release",
    )
    parser.add_argument(
        "--runtime-authority-secret-key",
        help="Secret key to project read-only; defaults to authority.json when the Secret is selected",
    )
    authority_source.add_argument(
        "--runtime-authority-payload-file",
        help="Safe-to-persist non-secret authority payload file (1..4096 exact binary bytes); mutually exclusive with Secret mode",
    )
    parser.add_argument(
        "--runtime-authority-payload-sha256",
        help="Required canonical SHA-256 of exact payload file bytes; provides integrity, not confidentiality",
    )
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def cmd_airflow_build(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    """Build one strict deployment projection and emit its public result."""

    del ctx, logger
    try:
        runtime_authority_ref = _optional_runtime_authority_ref(
            secret_name=getattr(args, "runtime_authority_secret_name", None),
            secret_key=getattr(args, "runtime_authority_secret_key", None),
            payload_file=getattr(args, "runtime_authority_payload_file", None),
            payload_sha256=getattr(args, "runtime_authority_payload_sha256", None),
        )
    except ValueError:
        print(
            "DPONE_DEPLOYMENT_RUNTIME_AUTHORITY_INVALID: runtime authority source options or payload integrity are invalid",
            file=sys.stderr,
        )
        return 2
    try:
        registry_credentials = _optional_registry_credentials(
            secret_name=getattr(args, "registry_credentials_secret_name", None),
            secret_key=getattr(args, "registry_credentials_secret_key", None),
            connection_id=getattr(args, "registry_credentials_connection_id", None),
            connection_type=getattr(args, "registry_credentials_connection_type", None),
            projection_mode=getattr(args, "registry_credentials_projection_mode", None),
            env_name=getattr(args, "registry_credentials_env_name", None),
        )
    except ValueError:
        print(
            "DPONE_DEPLOYMENT_REGISTRY_CREDENTIALS_INVALID: registry credential Secret options are invalid",
            file=sys.stderr,
        )
        return 2
    try:
        desired_state_authority = load_airflow_desired_state_authority() if os.environ.get(AUTHORITY_FILE_ENV) else None
    except (OSError, ValueError):
        print(
            "DPONE_RUNTIME_CREDENTIAL_PROJECTION_INVALID: protected desired-state authority is invalid", file=sys.stderr
        )
        return 2
    result = build_deployment_result(
        root=".",
        release_id=args.release_id,
        environment=args.environment,
        trust_tier=args.trust_tier,
        runtime_image_ref=args.runtime_image_ref,
        runtime_image_digest=args.runtime_image_digest,
        runtime_image_dbt_ref=args.runtime_image_dbt_ref,
        runtime_image_dbt_digest=args.runtime_image_dbt_digest,
        artifact_registry_ref=args.artifact_registry_ref,
        registry_config_ref=_optional_config_map_ref(
            name=args.registry_config_map_name,
            key=args.registry_config_map_key,
            sha256=args.registry_config_sha256,
            default_key="registry.json",
        ),
        trust_policy_ref=_optional_config_map_ref(
            name=args.trust_policy_config_map_name,
            key=args.trust_policy_config_map_key,
            sha256=args.trust_policy_sha256,
            default_key="policy.json",
        ),
        airflow_bundle_ref=args.airflow_bundle_ref,
        dev_evidence_pvc_claim=args.dev_evidence_pvc_claim,
        dev_evidence_worker_queue=args.dev_evidence_worker_queue,
        runtime_authority_ref=runtime_authority_ref,
        registry_credentials=registry_credentials,
        desired_state_authority=desired_state_authority,
    )
    emit_self_service_result(result, args.format, command="airflow_build")
    if result.exit_code is not None:
        return result.exit_code
    return 0 if result.passed else 1


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


def _optional_secret_ref(*, name: object, key: object) -> dict[str, object] | None:
    if name is None and key is None:
        return None
    return {
        "kind": "kubernetes_secret",
        "name": name,
        "key": key if key is not None else "authority.json",
    }


def _optional_registry_credentials(
    *,
    secret_name: object,
    secret_key: object,
    connection_id: object,
    connection_type: object,
    projection_mode: object,
    env_name: object,
) -> dict[str, object] | None:
    values = (secret_name, secret_key, connection_id, connection_type, projection_mode, env_name)
    if all(value is None for value in values):
        return None
    if not isinstance(connection_id, str):
        raise ValueError("registry credential connection id is required")
    selected_connection_type = connection_type if isinstance(connection_type, str) else "airflow"
    selected_projection_mode = (
        projection_mode if isinstance(projection_mode, str) else ("k8s_secret" if secret_name is not None else "env")
    )
    canonical_key = "AIRFLOW_CONN_" + "".join(
        character if character.isalnum() else "_" for character in connection_id.upper()
    )
    selected_env_name = env_name if isinstance(env_name, str) else canonical_key
    if selected_connection_type == "airflow" and selected_env_name != canonical_key:
        raise ValueError("registry credential environment name must match the Airflow connection id")
    projection: dict[str, object] = {
        "mode": selected_projection_mode,
        "env_name": selected_env_name,
    }
    if selected_projection_mode == "k8s_secret":
        if not isinstance(secret_name, str):
            raise ValueError("registry credential Secret name is required for k8s_secret projection")
        selected_secret_key = secret_key if isinstance(secret_key, str) else selected_env_name
        if selected_secret_key != selected_env_name:
            raise ValueError("registry credential Secret key must match the projected environment name")
        projection["secret_ref"] = {
            "name": secret_name,
            "key": selected_secret_key,
        }
    elif secret_name is not None or secret_key is not None:
        raise ValueError("registry credential Secret fields are not allowed for env projection")
    return {
        "connection_type": selected_connection_type,
        "connection_id": connection_id,
        "projection": projection,
    }


def _optional_runtime_authority_ref(
    *,
    secret_name: object,
    secret_key: object,
    payload_file: object,
    payload_sha256: object,
) -> dict[str, object] | None:
    secret_selected = secret_name is not None or secret_key is not None
    payload_selected = payload_file is not None or payload_sha256 is not None
    if secret_selected and payload_selected:
        raise ValueError("runtime authority sources are mutually exclusive")
    if secret_selected:
        return _optional_secret_ref(name=secret_name, key=secret_key)
    if not payload_selected:
        return None
    if not isinstance(payload_file, str) or not isinstance(payload_sha256, str):
        raise ValueError("runtime authority payload file and digest are required together")
    payload = immutable_runtime_authority_payload_from_file(
        payload_file,
        expected_sha256=payload_sha256,
    )
    return {"kind": "immutable_payload", **payload.to_dict()}


__all__ = ["cmd_airflow_build", "register_build_parser"]
