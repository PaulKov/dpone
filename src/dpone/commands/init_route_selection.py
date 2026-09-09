"""Route-to-recipe selection policy for the beginner init facade."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dpone.manifest.errors import ManifestConfigurationError
from dpone.readiness.airflow_recipe_errors import recipe_error, recipe_error_exit_code
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error
from dpone.readiness.capability_discovery_composition import build_capability_discovery_service
from dpone.readiness.capability_discovery_service import CapabilityDiscoveryError
from dpone.readiness.error_contract import error_docs_url, manual_fix

_MAX_PICKER_ROUTES = 20


def resolve_pipeline_recipe(
    args: argparse.Namespace,
    *,
    default_recipe: str,
) -> str | SelfServiceResult:
    """Resolve one explicit/default recipe before any scaffold writes."""

    recipe = str(getattr(args, "recipe", "") or "").strip()
    route = str(getattr(args, "route", "") or "").strip()
    if recipe and route:
        parser = getattr(args, "_init_parser", None)
        if isinstance(parser, argparse.ArgumentParser):
            parser.error("--recipe and --route are mutually exclusive")
        raise ManifestConfigurationError("--recipe and --route are mutually exclusive")
    if recipe:
        try:
            return build_capability_discovery_service(root=Path.cwd()).resolve_scaffold_recipe(recipe)
        except CapabilityDiscoveryError as exc:
            return _capability_error(
                exc,
                entity_kind="recipe",
                entity_id=recipe,
                pipeline_id=str(getattr(args, "name", "") or "unknown"),
            )
    if not route and sys.stdin.isatty():
        selected = _interactive_route(default_recipe)
        if isinstance(selected, SelfServiceResult):
            return selected
        route = selected
    if not route:
        try:
            return build_capability_discovery_service(root=Path.cwd()).resolve_scaffold_recipe(default_recipe)
        except CapabilityDiscoveryError as exc:
            return _capability_error(
                exc,
                entity_kind="recipe",
                entity_id=default_recipe,
                pipeline_id=str(getattr(args, "name", "") or "unknown"),
            )
    try:
        return build_capability_discovery_service(root=Path.cwd()).resolve_beginner_recipe(route)
    except CapabilityDiscoveryError as exc:
        return _capability_error(exc, entity_kind="route", entity_id=route)


def _interactive_route(default_recipe: str) -> str | SelfServiceResult:
    service = build_capability_discovery_service(root=Path.cwd())
    routes = tuple(
        sorted(
            (
                item
                for item in service.snapshot().routes
                if item.support.status != "not_supported" and item.beginner.recipe_available
            ),
            key=lambda item: (default_recipe not in item.beginner.recipe_refs, item.id),
        )
    )[:_MAX_PICKER_ROUTES]
    if not routes:
        return _selection_error(
            "DPONE_ROUTE_NOT_SCAFFOLDABLE",
            "No beginner routes are available in the current capability snapshot.",
        )
    sys.stderr.write("Choose a scaffoldable route:\n")
    for index, item in enumerate(routes, start=1):
        sys.stderr.write(
            f"  {index}. {item.id} (support={item.support.status}, evidence={item.certification.evidence_status})\n"
        )
    sys.stderr.write("Route [1]: ")
    answer = sys.stdin.readline().strip()
    if not answer:
        return routes[0].id
    try:
        selected_index = int(answer)
    except ValueError:
        return _selection_error(
            "DPONE_ROUTE_SELECTION_INVALID",
            "Route selection must be one of the displayed numbers.",
        )
    if not 1 <= selected_index <= len(routes):
        return _selection_error(
            "DPONE_ROUTE_SELECTION_INVALID",
            "Route selection is outside the displayed range.",
        )
    return routes[selected_index - 1].id


def _selection_error(code: str, message: str) -> SelfServiceResult:
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                code,
                message,
                stage="init_pipeline",
                entity={"kind": "route", "id": "interactive_selection"},
                fixes=[_route_fix(code)],
                docs_url=error_docs_url(code),
            ),
        ),
        exit_code=2,
    )


def _capability_error(
    error: CapabilityDiscoveryError,
    *,
    entity_kind: str,
    entity_id: str,
    pipeline_id: str = "unknown",
) -> SelfServiceResult:
    if entity_kind == "recipe" and error.code == "DPONE_RECIPE_NOT_FOUND":
        return SelfServiceResult(
            passed=False,
            errors=(
                recipe_error(
                    error.code,
                    str(error),
                    stage="init_pipeline",
                    recipe_ref=entity_id,
                    entity={"kind": "pipeline", "id": pipeline_id},
                ),
            ),
            exit_code=recipe_error_exit_code(error.code),
        )
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                error.code,
                str(error),
                stage="init_pipeline",
                entity={"kind": entity_kind, "id": entity_id},
                fixes=[_route_fix(error.code)],
                docs_url=error_docs_url(error.code),
            ),
        ),
        exit_code=2,
    )


def _route_fix(code: str) -> dict[str, str]:
    if code == "DPONE_ROUTE_SELECTION_INVALID":
        return manual_fix(
            "choose_displayed_route",
            command="dpone init pipeline --help",
        )
    return manual_fix(
        "inspect_scaffoldable_routes",
        command="dpone recipe list",
    )


__all__ = ["resolve_pipeline_recipe"]
