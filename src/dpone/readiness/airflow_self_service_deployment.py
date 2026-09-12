"""Self-service facade for environment Airflow deployment projection."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.readiness.airflow_deployment_artifacts import bytes_descriptor, digest_dir
from dpone.readiness.airflow_deployment_errors import deployment_projection_error, deployment_projection_exit_code
from dpone.readiness.airflow_deployment_projection import (
    AirflowDeploymentProjectionError,
    AirflowDeploymentProjectionService,
)
from dpone.readiness.airflow_self_service_models import SelfServiceResult

_STRICT_V2_MIGRATION_CODE = "DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED"


def build_deployment_result(
    *,
    root: str | Path,
    release_id: str,
    environment: str,
    trust_tier: str | None,
    runtime_image_ref: str | None,
    runtime_image_digest: str,
    runtime_image_dbt_ref: str | None = None,
    runtime_image_dbt_digest: str | None = None,
    artifact_registry_ref: str,
    registry_config_ref: Mapping[str, Any] | None,
    trust_policy_ref: Mapping[str, Any] | None = None,
    airflow_bundle_ref: str | None = None,
    dev_evidence_pvc_claim: str | None = None,
    dev_evidence_worker_queue: str | None = None,
    composition_supervisor: Mapping[str, object] | None = None,
) -> SelfServiceResult:
    """Materialize an environment deployment projection without promoting current."""

    input_error = _deployment_build_input_error(
        release_id=release_id,
        environment=environment,
        trust_tier=trust_tier,
        runtime_image_ref=runtime_image_ref,
        registry_config_ref=registry_config_ref,
        trust_policy_ref=trust_policy_ref,
    )
    if input_error is not None:
        return input_error
    try:
        assert trust_tier is not None
        assert runtime_image_ref is not None
        assert registry_config_ref is not None
        projection = AirflowDeploymentProjectionService(root=root).materialize(
            release_id=release_id,
            environment=environment,
            trust_tier=trust_tier,
            runtime_image_ref=runtime_image_ref,
            runtime_image_digest=runtime_image_digest,
            runtime_image_dbt_ref=runtime_image_dbt_ref,
            runtime_image_dbt_digest=runtime_image_dbt_digest,
            artifact_registry_ref=artifact_registry_ref,
            registry_config_ref=registry_config_ref,
            trust_policy_ref=trust_policy_ref,
            airflow_bundle_ref=airflow_bundle_ref,
            dev_evidence_pvc_claim=dev_evidence_pvc_claim,
            dev_evidence_worker_queue=dev_evidence_worker_queue,
            composition_supervisor=composition_supervisor,
        )
    except AirflowDeploymentProjectionError as exc:
        return SelfServiceResult(
            passed=False,
            errors=(deployment_projection_error(exc, release_id=release_id, environment=environment),),
            details={
                "release_id": release_id,
                "environment": environment,
            },
            exit_code=deployment_projection_exit_code(exc.code),
        )
    details = projection.to_dict()
    details["deployment_dir"] = _project_relative(root, projection.deployment_dir)
    index_bytes = (projection.deployment_dir / "airflow-index.json").read_bytes()
    details["airflow_index_artifact"] = bytes_descriptor(
        artifact_ref=(
            f"cache://deployments/{projection.deployment['environment']}/"
            f"{digest_dir(str(projection.deployment['deployment_id']))}/airflow-index.json"
        ),
        payload=index_bytes,
    )
    return SelfServiceResult(passed=True, details=details)


def _deployment_build_input_error(
    *,
    release_id: str,
    environment: str,
    trust_tier: str | None,
    runtime_image_ref: str | None,
    registry_config_ref: Mapping[str, Any] | None,
    trust_policy_ref: Mapping[str, Any] | None,
) -> SelfServiceResult | None:
    if trust_tier is not None and trust_tier not in {"production", "non_production"}:
        return _configuration_error(
            "DPONE_DEPLOYMENT_TRUST_TIER_INVALID",
            "trust_tier must be production or non_production",
            release_id=release_id,
            environment=environment,
        )
    for reference in (registry_config_ref, trust_policy_ref):
        if reference is not None and not _complete_config_map_ref(reference):
            return _configuration_error(
                "DPONE_DEPLOYMENT_CONFIG_REF_INVALID",
                "ConfigMap references require name, key, and canonical sha256 together",
                release_id=release_id,
                environment=environment,
            )
    if trust_tier == "production" and trust_policy_ref is None:
        return _configuration_error(
            "DPONE_DEPLOYMENT_TRUST_POLICY_REQUIRED",
            "production deployment requires a complete trust-policy ConfigMap reference",
            release_id=release_id,
            environment=environment,
        )
    missing = [
        name
        for name, value in (
            ("--trust-tier", trust_tier),
            ("--runtime-image-ref", runtime_image_ref),
            ("--registry-config-map-name/--registry-config-sha256", registry_config_ref),
        )
        if value is None
    ]
    if missing:
        return _configuration_error(
            _STRICT_V2_MIGRATION_CODE,
            "Legacy airflow build input cannot produce an executable deployment; "
            "regenerate with the strict v2 arguments: " + ", ".join(missing),
            release_id=release_id,
            environment=environment,
        )
    return None


def _complete_config_map_ref(reference: Mapping[str, Any]) -> bool:
    return all(
        isinstance(reference.get(field), str) and bool(str(reference[field]).strip())
        for field in ("name", "key", "sha256")
    )


def _configuration_error(
    code: str,
    message: str,
    *,
    release_id: str,
    environment: str,
) -> SelfServiceResult:
    error = AirflowDeploymentProjectionError(code, message)
    return SelfServiceResult(
        passed=False,
        errors=(deployment_projection_error(error, release_id=release_id, environment=environment),),
        details={"release_id": release_id, "environment": environment},
        exit_code=2,
    )


def _project_relative(root: str | Path, path: Path) -> str:
    project_root = Path(root).resolve(strict=False)
    return path.resolve(strict=False).relative_to(project_root).as_posix()


__all__ = ["build_deployment_result"]
