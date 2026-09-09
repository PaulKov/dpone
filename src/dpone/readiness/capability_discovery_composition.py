"""Composition root for the pure self-service capability projection."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.contracts.capability_discovery import (
    CapabilityIssue,
    RecipeDiscoveryEntry,
    RouteCertificationVariant,
    built_in_connector_declarations,
)
from dpone.manifest.recipe_catalog_operations import RecipeCatalogOperations
from dpone.manifest.recipe_models import RecipeResolutionError
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.readiness.airflow_self_service_templates import RECIPE_DEFAULTS
from dpone.readiness.capability_discovery_service import (
    CapabilityDiscoveryService,
    normalize_route_ref,
)
from dpone.readiness.capability_evidence import read_certification_matrix
from dpone.readiness.capability_project_config import (
    CapabilityEvidenceConfigError,
    load_capability_evidence_settings,
)


def build_capability_discovery_service(
    *,
    root: Path,
    recipe_operations: RecipeCatalogOperations | None = None,
    certification_matrix_path: Path | None = None,
    expected_commit: str | None = None,
    certification_evidence_dirs: Sequence[Path] = (),
    certification_max_age_hours: int = 168,
    clock: Callable[[], datetime] | None = None,
) -> CapabilityDiscoveryService:
    """Load bounded authorities once and inject them into the pure projector."""

    project_root = root.resolve(strict=False)
    operations = recipe_operations or RecipeCatalogOperations(
        project_root,
        built_in_recipes=RECIPE_DEFAULTS,
    )
    variants: tuple[RouteCertificationVariant, ...] = ()
    issues: tuple[CapabilityIssue, ...] = ()
    if certification_matrix_path is None:
        try:
            settings = load_capability_evidence_settings(project_root)
        except CapabilityEvidenceConfigError:
            settings = None
            issues = (_evidence_config_issue(),)
        if settings is not None:
            certification_matrix_path = settings.matrix_path
            expected_commit = settings.expected_commit
            certification_evidence_dirs = settings.evidence_dirs
            certification_max_age_hours = settings.max_age_hours
    if certification_matrix_path is not None:
        variants, evidence_issues = read_certification_matrix(
            project_root,
            certification_matrix_path,
            expected_commit=expected_commit,
            evidence_dirs=certification_evidence_dirs,
            max_age_hours=certification_max_age_hours,
            now=(clock or _utc_now)(),
        )
        issues = (*issues, *evidence_issues)
    try:
        recipes = _recipe_entries(operations)
    except RecipeResolutionError as exc:
        recipes = _built_in_recipe_entries()
        issues = (*issues, _recipe_catalog_issue(exc.code))
    return CapabilityDiscoveryService(
        connectors=built_in_connector_declarations(),
        route_profiles=RouteProfileCatalog.default().profiles(),
        recipes=recipes,
        certification_variants=variants,
        issues=issues,
    )


def _recipe_entries(
    operations: RecipeCatalogOperations,
) -> tuple[RecipeDiscoveryEntry, ...]:
    entries = []
    for item in operations.list_recipes():
        ref = str(item["ref"])
        built_in = RECIPE_DEFAULTS.get(ref)
        route_id = _built_in_route_id(built_in) or _catalog_route_id(item)
        scaffoldable = built_in is not None or item.get("scaffoldable") is True
        entries.append(
            RecipeDiscoveryEntry(
                ref=ref,
                origin=str(item["origin"]),
                status=str(item["status"]),
                route_id=route_id,
                scaffoldable=scaffoldable,
                default_for_route=built_in is not None,
                reason_codes=() if route_id is not None else ("route_metadata_unavailable",),
            )
        )
    return tuple(entries)


def _built_in_recipe_entries() -> tuple[RecipeDiscoveryEntry, ...]:
    return tuple(
        RecipeDiscoveryEntry(
            ref=ref,
            origin="built_in",
            status="stable",
            route_id=_built_in_route_id(raw),
            scaffoldable=True,
            default_for_route=True,
        )
        for ref, raw in sorted(RECIPE_DEFAULTS.items())
    )


def _built_in_route_id(raw: Mapping[str, Any] | None) -> str | None:
    if raw is None:
        return None
    return normalize_route_ref(":".join(str(raw.get(key) or "") for key in ("source_type", "sink_type", "strategy")))


def _catalog_route_id(raw: Mapping[str, Any]) -> str | None:
    route = raw.get("route")
    if not isinstance(route, Mapping):
        return None
    return normalize_route_ref(":".join(str(route.get(key) or "") for key in ("source_type", "sink_type", "strategy")))


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _evidence_config_issue() -> CapabilityIssue:
    return CapabilityIssue(
        code="DPONE_CAPABILITY_EVIDENCE_CONFIG_INVALID",
        entity_kind="project_config",
        entity_id="certification_evidence",
        message=(
            "Invalid dpone.yaml field capability_discovery.certification_evidence. "
            "Expected project-relative matrix_path/evidence_dirs, a non-empty expected_commit, "
            "and max_age_hours in 1..8760. Correct or remove the section, then run "
            "`dpone connectors list` again; route certification remains UNVERIFIED."
        ),
    )


def _recipe_catalog_issue(code: str) -> CapabilityIssue:
    return CapabilityIssue(
        code=code,
        entity_kind="recipe_catalog",
        entity_id="configured_recipes",
        message=(
            "The configured recipe catalog is invalid; only built-in recipes "
            "are available until the catalog is corrected."
        ),
    )


__all__ = ["build_capability_discovery_service"]
