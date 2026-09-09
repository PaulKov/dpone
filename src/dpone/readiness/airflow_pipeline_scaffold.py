"""Idempotent multi-file scaffolding for one self-service pipeline."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from dpone.manifest.pipeline_identity import PipelineId
from dpone.manifest.project_config import ProjectLayout
from dpone.manifest.project_root import ProjectRootIdentity
from dpone.manifest.recipe_catalog import ExternalRecipeScaffoldResolution, RecipeCatalogService
from dpone.manifest.recipe_models import RecipeResolutionError
from dpone.readiness.airflow_pipeline_scaffold_membership import apply_pipeline_scaffold_with_membership
from dpone.readiness.airflow_pipeline_scaffold_paths import (
    pipeline_path as scaffold_pipeline_path,
)
from dpone.readiness.airflow_pipeline_scaffold_paths import (
    test_paths as scaffold_test_paths,
)
from dpone.readiness.airflow_pipeline_scaffold_preflight import (
    DomainFirstScaffoldGuard,
    preflight_domain_first_scaffold,
)
from dpone.readiness.airflow_recipe_errors import recipe_error, recipe_error_exit_code
from dpone.readiness.airflow_recipe_templates import recipe_domain_payload, recipe_pipeline_payload
from dpone.readiness.airflow_scaffold_apply import ScaffoldFile
from dpone.readiness.airflow_self_service_models import SelfServiceResult
from dpone.readiness.airflow_self_service_templates import (
    PREVIEW_START_DATE,
    RECIPE_DEFAULTS,
    domain_payload,
    folder_fragment_payload,
    pipeline_payload,
    pipeline_process_payload,
    test_fixture_text,
    test_payload,
)
from dpone.readiness.capability_discovery_service import CapabilityDiscoveryError
from dpone.readiness.pipeline_init_overlays import PipelineInitOverlayError, resolve_builtin_defaults

SUPPORTED_AUTHORING_MODES = frozenset({"classic", "flow", "folder"})


class AirflowPipelineScaffolder:
    """Plan and atomically apply the author-owned pipeline source bundle."""

    def __init__(
        self,
        root: Path,
        *,
        recipe_resolver: Callable[[str], str],
        root_identity: ProjectRootIdentity | None = None,
    ) -> None:
        self._root = root
        self._recipe_resolver = recipe_resolver
        self._root_identity = root_identity

    def scaffold(
        self,
        *,
        pipeline_id: PipelineId,
        recipe: str,
        airflow: bool,
        authoring_mode: str,
        profile: str | None = None,
        answers: Path | None = None,
        airflow_source: str = "explicit",
        layout: ProjectLayout | None = None,
        project_config_sha256: str | None = None,
        domain: str | None = None,
        from_locator: str | None = None,
        to_locator: str | None = None,
        unique_key: str | None = None,
    ) -> SelfServiceResult:
        safe_id = str(pipeline_id)
        effective_layout = layout or ProjectLayout()
        if recipe in RECIPE_DEFAULTS:
            try:
                recipe = self._recipe_resolver(recipe)
            except CapabilityDiscoveryError as exc:
                return _failed(
                    exc.code,
                    str(exc),
                    recipe_ref=recipe,
                    entity={"kind": "pipeline", "id": safe_id},
                )
        elif "@" not in recipe:
            return _failed(
                "DPONE_RECIPE_NOT_FOUND",
                f"Unknown built-in recipe: {recipe}",
                recipe_ref=recipe,
                entity={"kind": "pipeline", "id": safe_id},
            )
        if authoring_mode not in SUPPORTED_AUTHORING_MODES:
            return _failed(
                "DPONE_AUTHORING_MODE_UNSUPPORTED",
                "Authoring mode must be classic, flow, or folder.",
                recipe_ref=recipe,
            )

        external_resolution: ExternalRecipeScaffoldResolution | None = None
        if recipe not in RECIPE_DEFAULTS:
            if any(value is not None for value in (from_locator, to_locator, unique_key)):
                return _failed(
                    "DPONE_RECIPE_OPTION_UNSUPPORTED",
                    "External recipes accept parameter overrides through --answers.",
                    recipe_ref=recipe,
                )
            if authoring_mode != "flow":
                return _failed(
                    "DPONE_RECIPE_AUTHORING_MODE_UNSUPPORTED",
                    "External recipes require --authoring flow.",
                    recipe_ref=recipe,
                )
            try:
                external_resolution = RecipeCatalogService(self._root).resolve_for_scaffold(
                    recipe_ref=recipe,
                    profile_ref=profile,
                    answers_path=answers,
                    pipeline_id=safe_id,
                )
            except RecipeResolutionError as exc:
                return _failed(
                    exc.code,
                    str(exc),
                    recipe_ref=recipe,
                )
            if domain is not None and domain != external_resolution.domain:
                return _failed(
                    "DPONE_PIPELINE_DOMAIN_MISMATCH",
                    "External recipe domain cannot be overridden during scaffolding.",
                    recipe_ref=recipe,
                    entity={"kind": "pipeline", "id": safe_id},
                )

        effective_preflight_domain = external_resolution.domain if external_resolution is not None else domain
        preflight = preflight_domain_first_scaffold(
            self._root,
            layout=effective_layout,
            domain=effective_preflight_domain,
            pipeline_id=safe_id,
            project_config_sha256=project_config_sha256,
        )
        if preflight.failure is not None:
            return preflight.failure
        guard = preflight.guard

        if external_resolution is not None:
            return self._scaffold_external(
                pipeline_id=safe_id,
                recipe=recipe,
                airflow=airflow,
                authoring_mode=authoring_mode,
                airflow_source=airflow_source,
                layout=effective_layout,
                resolution=external_resolution,
                guard=guard,
            )
        if profile is not None:
            return _failed(
                "DPONE_RECIPE_OPTION_UNSUPPORTED",
                "Built-in recipes do not accept --profile.",
                recipe_ref=recipe,
            )
        try:
            resolution = resolve_builtin_defaults(
                RECIPE_DEFAULTS[recipe],
                root=self._root,
                answers=answers,
                domain=domain,
                from_locator=from_locator,
                to_locator=to_locator,
                unique_key=unique_key,
            )
        except PipelineInitOverlayError as exc:
            return _failed(
                exc.code,
                str(exc),
                recipe_ref=recipe,
                entity={"kind": "pipeline", "id": safe_id},
            )
        defaults = dict(resolution.values)
        if guard is not None:
            guard = guard.with_consumed_files(resolution.consumed_files)
        effective_domain = str(defaults["domain"])
        pipeline_path = scaffold_pipeline_path(effective_layout, effective_domain, safe_id)
        process = pipeline_process_payload(safe_id, defaults)
        if not RecipeCatalogService.supports_starter_test(process):
            return _failed(
                "DPONE_RECIPE_HERMETIC_TEST_UNSUPPORTED",
                "Recipe behavior is outside the executable starter-test contract.",
                recipe_ref=recipe,
            )
        starter_test_key = RecipeCatalogService.starter_test_unique_key(process)
        test_path, fixture_path, pipeline_reference, fixture_reference = scaffold_test_paths(
            effective_layout,
            pipeline_path,
            safe_id,
        )
        files = [
            ScaffoldFile.yaml(
                pipeline_path,
                pipeline_payload(
                    safe_id,
                    recipe,
                    defaults,
                    authoring_mode=authoring_mode,
                    source_path=pipeline_path.as_posix(),
                    airflow=airflow,
                ),
            ),
            ScaffoldFile.yaml(
                test_path,
                test_payload(
                    safe_id,
                    pipeline_path,
                    process_name=safe_id,
                    pipeline_reference=pipeline_reference,
                    fixture_reference=fixture_reference,
                ),
            ),
            ScaffoldFile(
                fixture_path,
                test_fixture_text(unique_key=starter_test_key),
            ),
        ]
        if not effective_layout.is_domain_first:
            files.insert(
                1,
                ScaffoldFile.yaml(
                    Path("domains") / f"{defaults['domain']}.yaml",
                    domain_payload(safe_id, defaults, airflow=airflow),
                ),
            )
        if authoring_mode == "folder":
            files.append(
                ScaffoldFile.yaml(
                    pipeline_path.parent / "steps/load.yaml",
                    folder_fragment_payload(safe_id, defaults),
                )
            )
        return apply_pipeline_scaffold_with_membership(
            root=self._root,
            root_identity=self._root_identity,
            layout=effective_layout,
            domain=effective_domain,
            pipeline_id=safe_id,
            pipeline_path=pipeline_path,
            airflow=airflow,
            files=tuple(files),
            guard=guard,
            details={
                "pipeline_id": safe_id,
                "pipeline_path": pipeline_path.as_posix(),
                "recipe": recipe,
                "authoring_mode": authoring_mode,
                "airflow_enabled": airflow,
                "airflow_source": airflow_source,
            },
        )

    def _scaffold_external(
        self,
        *,
        pipeline_id: str,
        recipe: str,
        airflow: bool,
        authoring_mode: str,
        airflow_source: str,
        layout: ProjectLayout,
        resolution: ExternalRecipeScaffoldResolution,
        guard: DomainFirstScaffoldGuard | None,
    ) -> SelfServiceResult:
        if guard is not None:
            guard = guard.with_consumed_files(resolution.consumed_files)
        effective_domain = resolution.domain
        pipeline_path = scaffold_pipeline_path(layout, effective_domain, pipeline_id)
        if any(not RecipeCatalogService.supports_starter_test(item) for item in resolution.processes):
            return _failed(
                "DPONE_RECIPE_HERMETIC_TEST_UNSUPPORTED",
                "At least one recipe process is outside the executable starter-test contract.",
                recipe_ref=recipe,
            )
        process = resolution.processes[0]
        starter_test_key = RecipeCatalogService.starter_test_unique_key(process)
        process_name = str(process.get("name") or pipeline_id)
        test_path, fixture_path, pipeline_reference, fixture_reference = scaffold_test_paths(
            layout,
            pipeline_path,
            pipeline_id,
        )
        files: list[ScaffoldFile] = [
            ScaffoldFile.yaml(
                pipeline_path,
                recipe_pipeline_payload(
                    pipeline_id,
                    domain=effective_domain,
                    recipe_block=resolution.recipe_block,
                    source_path=pipeline_path.as_posix(),
                    airflow=airflow,
                ),
            ),
            ScaffoldFile.yaml(
                test_path,
                test_payload(
                    pipeline_id,
                    pipeline_path,
                    process_name=process_name,
                    pipeline_reference=pipeline_reference,
                    fixture_reference=fixture_reference,
                ),
            ),
            ScaffoldFile(
                fixture_path,
                test_fixture_text(unique_key=starter_test_key),
            ),
        ]
        if not layout.is_domain_first:
            files.insert(
                1,
                ScaffoldFile.yaml(
                    Path("domains") / f"{resolution.domain}.yaml",
                    recipe_domain_payload(
                        pipeline_id,
                        domain=resolution.domain,
                        connection_refs=resolution.connection_refs,
                        airflow=airflow,
                        start_date=PREVIEW_START_DATE,
                    ),
                ),
            )
        return apply_pipeline_scaffold_with_membership(
            root=self._root,
            root_identity=self._root_identity,
            layout=layout,
            domain=effective_domain,
            pipeline_id=pipeline_id,
            pipeline_path=pipeline_path,
            airflow=airflow,
            files=tuple(files),
            guard=guard,
            details={
                "pipeline_id": pipeline_id,
                "pipeline_path": pipeline_path.as_posix(),
                "recipe": recipe,
                "authoring_mode": authoring_mode,
                "airflow_enabled": airflow,
                "airflow_source": airflow_source,
                "recipe_resolution": resolution.provenance.to_jsonable(),
            },
        )


def safe_pipeline_id(value: str) -> str:
    """Preserve the legacy inferred-ID projection for compatibility callers."""

    normalized = "".join(char if char.isalnum() or char in {"_", "-"} else "_" for char in value.strip())
    return normalized.strip("_") or "pipeline"


def _failed(
    code: str,
    message: str,
    *,
    recipe_ref: str,
    entity: dict[str, str] | None = None,
) -> SelfServiceResult:
    return SelfServiceResult(
        passed=False,
        errors=(recipe_error(code, message, stage="init_pipeline", recipe_ref=recipe_ref, entity=entity),),
        exit_code=recipe_error_exit_code(code),
    )


__all__ = ["AirflowPipelineScaffolder", "SUPPORTED_AUTHORING_MODES", "safe_pipeline_id"]
