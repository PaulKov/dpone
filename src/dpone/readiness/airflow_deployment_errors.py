"""Structured errors for Airflow deployment projection failures."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.readiness.airflow_deployment_projection import AirflowDeploymentProjectionError


from typing import Any

from dpone.readiness.error_contract import dpone_error, manual_fix


def deployment_projection_error(
    exc: AirflowDeploymentProjectionError,
    *,
    release_id: str,
    environment: str,
) -> dict[str, Any]:
    return dpone_error(
        exc.code,
        str(exc),
        stage="airflow_build",
        path=exc.path,
        entity=_entity_for_code(exc.code, release_id=release_id, environment=environment),
        fixes=_fixes_for_code(exc.code),
    )


def _entity_for_code(code: str, *, release_id: str, environment: str) -> dict[str, str]:
    if code.startswith("DPONE_RELEASE_"):
        return {"kind": "release", "id": release_id}
    if code.startswith("DPONE_BINDING_SET_"):
        return {"kind": "binding_set", "id": environment}
    if code.startswith("DPONE_CONNECTION_REGISTRY_"):
        return {"kind": "connection_registry", "id": environment}
    if code.startswith("DPONE_CREDENTIAL_RUNTIME_"):
        return {"kind": "credential_runtime", "id": environment}
    return {"kind": "deployment", "id": environment}


def _fixes_for_code(code: str) -> list[dict[str, str]]:
    if code == "DPONE_RELEASE_NOT_FOUND":
        return [manual_fix("materialize_release_set")]
    if code.startswith("DPONE_RELEASE_ARTIFACT_"):
        return [manual_fix("rebuild_release_set")]
    if code.startswith("DPONE_BINDING_SET_"):
        return [manual_fix("create_binding_set")]
    if code.startswith("DPONE_CONNECTION_REGISTRY_"):
        return [manual_fix("create_connection_registry")]
    if code.startswith("DPONE_CREDENTIAL_RUNTIME_"):
        return [manual_fix("create_credential_runtime")]
    if code == "DPONE_DEPLOYMENT_ARTIFACT_REGISTRY_UNPINNED":
        return [manual_fix("pin_artifact_registry_ref")]
    if code == "DPONE_DEPLOYMENT_ARTIFACT_REGISTRY_INVALID":
        return [manual_fix("use_logical_artifact_registry_ref")]
    if code == "DPONE_DEPLOYMENT_TRUST_POLICY_REQUIRED":
        return [manual_fix("configure_artifact_trust_policy")]
    if code == "DPONE_DEPLOYMENT_RUNTIME_IMAGE_INVALID":
        return [manual_fix("pin_matching_runtime_image")]
    if code == "DPONE_DEPLOYMENT_CONFIG_REF_INVALID":
        return [manual_fix("provide_complete_config_map_reference")]
    if code == "DPONE_DEPLOYMENT_TRUST_TIER_INVALID":
        return [manual_fix("choose_supported_trust_tier")]
    if code == "DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED":
        return [
            manual_fix(
                "regenerate_strict_v2_deployment",
                command="dpone airflow build --help",
            )
        ]
    if code == "DPONE_DEPLOYMENT_DIGEST_INVALID":
        return [manual_fix("provide_sha256_digest")]
    if code == "DPONE_DEPLOYMENT_CACHE_LOCK_FAILED":
        return [manual_fix("check_cache_root_permissions")]
    if code == "DPONE_DBT_PRODUCTION_RELEASE_SCHEMA_REQUIRED":
        return [
            manual_fix(
                "build_release_set_v2",
                command=(
                    "dbt parse --project-dir path/to/dbt-project && "
                    "dpone dbt compile path/to/dbt-project --cache-root .dpone-cache "
                    "--output-dir .dpone/gitops/airflow-v2"
                ),
            )
        ]
    return []


def deployment_projection_exit_code(code: str) -> int | None:
    """Classify handled caller-remediable projection configuration blockers."""

    return 2 if code == "DPONE_DBT_PRODUCTION_RELEASE_SCHEMA_REQUIRED" else None


__all__ = ["deployment_projection_error", "deployment_projection_exit_code"]
