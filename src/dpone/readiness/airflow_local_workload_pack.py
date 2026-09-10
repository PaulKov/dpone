"""Build deterministic local Airflow workload packs for preview and sample flows."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.authoring_folder import AuthoringSourceDependency


from pathlib import Path
from typing import Any

from dpone.contracts.airflow_resources import manifest_airflow_resources
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.workload_catalog_models import GitOpsConfigProvenance, GitOpsWorkloadDefinition
from dpone.manifest.confined_files import project_relative_path, sha256_confined_file
from dpone.readiness.airflow_authoring_dependency_integrity import (
    AuthoringDependencyIntegrityError,
    PrimaryAuthoringSourceIntegrityError,
    source_file_provenance,
    verify_authoring_dependency_parity,
)


class LocalWorkloadPackBuildError(ValueError):
    """Stable, data-safe failure from local authoring pack materialization."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def build_local_airflow_workload_pack(
    *,
    root: Path,
    source_path: Path,
    pipeline_payload: dict[str, Any],
    pipeline_id: str,
    runtime_image: str,
    runtime_image_digest: str,
) -> dict[str, Any]:
    """Return one scheduler-static advisory pack without external I/O."""

    manifest_path = _repo_relative(root, source_path)
    domain = _domain(pipeline_payload)
    resources = manifest_airflow_resources(pipeline_payload)
    workload = GitOpsWorkloadDefinition(
        workload_id=pipeline_id,
        manifest=manifest_path,
        domain=domain,
        catalog_path=f"domains/{domain or 'default'}.yaml",
        effective_config={
            "image": runtime_image,
            "image_digest": runtime_image_digest,
            "namespace": "airflow-dev",
            "runner_policy": "advisory",
            "airflow": {
                "service_account_name": "dpone-runtime",
                **({"resources": resources} if resources is not None else {}),
            },
        },
        provenance={"manifest": GitOpsConfigProvenance("authoring", manifest_path, "pipeline")},
    )
    return (
        AirflowCompactPackBuilder()
        .build(
            workload=workload,
            output_path=f"packs/{pipeline_id}.airflow-pack.json",
            repo_root=root,
            mode="plan",
            runner_policy="advisory",
        )
        .to_jsonable()
    )


def build_verified_local_airflow_workload_pack(
    *,
    root: Path,
    source_path: Path,
    pipeline_payload: dict[str, Any],
    pipeline_id: str,
    runtime_image: str,
    runtime_image_digest: str,
    expected_authoring_dependencies: tuple[AuthoringSourceDependency, ...],
    expected_primary_path: str | None = None,
    expected_primary_sha256: str | None = None,
) -> dict[str, Any]:
    """Build one pack and require exact compiler/dependency-resolver parity."""

    recipe_closure = any(
        str(getattr(item, "kind", "")) in {"recipe", "profile", "component"} for item in expected_authoring_dependencies
    )
    try:
        _verify_primary_source_pin(
            root=root,
            source_path=source_path,
            expected_path=expected_primary_path,
            expected_sha256=expected_primary_sha256,
        )
        pack = build_local_airflow_workload_pack(
            root=root,
            source_path=source_path,
            pipeline_payload=pipeline_payload,
            pipeline_id=pipeline_id,
            runtime_image=runtime_image,
            runtime_image_digest=runtime_image_digest,
        )
        _verify_primary_source_pin(
            root=root,
            source_path=source_path,
            expected_path=expected_primary_path,
            expected_sha256=expected_primary_sha256,
        )
        if pack.get("blockers"):
            raise LocalWorkloadPackBuildError(
                "DPONE_AUTHORING_PACK_BUILD_FAILED",
                "Authoring dependencies could not be materialized into a blocker-free workload pack.",
            )
        verify_authoring_dependency_parity(
            expected=expected_authoring_dependencies,
            pack=pack,
            expected_primary_path=expected_primary_path,
            expected_primary_sha256=expected_primary_sha256,
        )
        return pack
    except PrimaryAuthoringSourceIntegrityError as exc:
        raise LocalWorkloadPackBuildError(
            "DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD",
            "The primary authoring source changed during preview; rerun check and preview.",
        ) from exc
    except AuthoringDependencyIntegrityError as exc:
        raise LocalWorkloadPackBuildError(
            (
                "DPONE_RECIPE_SOURCE_CHANGED_DURING_BUILD"
                if recipe_closure
                else "DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD"
            ),
            "Authoring source dependencies changed during preview; rerun check and preview.",
        ) from exc
    except LocalWorkloadPackBuildError:
        raise
    except OSError as exc:
        raise LocalWorkloadPackBuildError(
            "DPONE_AUTHORING_DEPENDENCY_READ_FAILED",
            "An authoring dependency could not be read safely during preview.",
        ) from exc
    except ValueError as exc:
        code = str(getattr(exc, "code", ""))
        recipe_drift = recipe_closure and (
            "recipe_digest_mismatch" in str(exc).lower() or code == "DPONE_RECIPE_DIGEST_MISMATCH"
        )
        if recipe_drift:
            code = "DPONE_RECIPE_SOURCE_CHANGED_DURING_BUILD"
        elif not code.startswith("DPONE_AUTHORING_FOLDER_"):
            code = "DPONE_AUTHORING_PACK_BUILD_FAILED"
        raise LocalWorkloadPackBuildError(
            code,
            "Authoring dependencies could not be materialized into a workload pack.",
        ) from exc


def _verify_primary_source_pin(
    *,
    root: Path,
    source_path: Path,
    expected_path: str | None,
    expected_sha256: str | None,
) -> None:
    if expected_path is None and expected_sha256 is None:
        return
    if expected_path is None or expected_sha256 is None:
        raise ValueError("Primary source path and digest must be provided together.")
    actual_path = project_relative_path(root, source_path)
    actual_sha256 = sha256_confined_file(root, actual_path)
    if actual_path != expected_path or _digest(actual_sha256) != _digest(expected_sha256):
        raise PrimaryAuthoringSourceIntegrityError(
            "Primary authoring source changed while the workload pack was being built."
        )


def _digest(value: str) -> str:
    return "sha256:" + value.removeprefix("sha256:").lower()


def _domain(payload: dict[str, Any]) -> str | None:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict) and isinstance(metadata.get("domain"), str):
        return _safe_id(metadata["domain"])
    return None


def _safe_id(value: str) -> str:
    return "_".join(part for part in value.strip().replace("-", "_").split("_") if part)


def _repo_relative(root: Path, path: Path) -> str:
    try:
        return path.resolve(strict=False).relative_to(root.resolve(strict=False)).as_posix()
    except ValueError:
        return path.as_posix()


__all__ = [
    "LocalWorkloadPackBuildError",
    "build_local_airflow_workload_pack",
    "build_verified_local_airflow_workload_pack",
    "source_file_provenance",
]
