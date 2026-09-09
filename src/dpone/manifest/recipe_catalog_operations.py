"""Read-only inspection and validation for declarative recipe catalogs."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import ConfinedFileError, project_relative_path, read_confined_file
from dpone.manifest.recipe_catalog import RecipeCatalog, RecipeCatalogService
from dpone.manifest.recipe_models import RecipeArtifactPin, RecipeResolutionError, validate_exact_ref
from dpone.manifest.recipe_parameters import (
    RecipeContractError,
    validate_recipe_component_contracts,
    validate_recipe_profile_contract,
)

_SCHEMA_KIND = {
    "dpone.recipe.v1": "recipe",
    "dpone.profile.v1": "profile",
    "dpone.component.v1": "component",
}


class RecipeCatalogOperations:
    """Serve list/show/pin/validate without mutating project files."""

    def __init__(self, root: Path, *, built_in_recipes: Mapping[str, Mapping[str, Any]]) -> None:
        self._root = root.resolve(strict=False)
        self._built_in_recipes = built_in_recipes
        self._catalog_service = RecipeCatalogService(self._root)

    def list_recipes(self) -> tuple[dict[str, Any], ...]:
        items: list[dict[str, Any]] = [
            {"ref": ref, "origin": "built_in", "status": "stable"} for ref in sorted(self._built_in_recipes)
        ]
        try:
            catalog = self._catalog_service.load_catalog()
        except RecipeResolutionError as exc:
            if exc.code == "DPONE_RECIPE_CATALOG_NOT_CONFIGURED":
                return tuple(items)
            raise
        for entry in catalog.entries:
            if entry.kind != "recipe":
                continue
            artifact = self._catalog_service.load_artifact(entry.pin, kind="recipe")
            route = _external_recipe_route(
                artifact.payload,
                catalog_service=self._catalog_service,
            )
            items.append(
                {
                    "ref": entry.pin.ref,
                    "origin": catalog.catalog_id,
                    "status": artifact.payload["status"],
                    "owner": artifact.payload["owner"],
                    "scaffoldable": True,
                    "route": route,
                }
            )
        return tuple(sorted(items, key=lambda item: str(item["ref"])))

    def show_recipe(self, ref: str) -> dict[str, Any]:
        if ref in self._built_in_recipes:
            defaults = self._built_in_recipes[ref]
            return {
                "ref": ref,
                "origin": "built_in",
                "status": "stable",
                "domain": defaults["domain"],
                "parameters": [],
                "allowed_profiles": [],
                "route": {key: str(defaults[key]) for key in ("source_type", "sink_type", "strategy")},
            }
        validate_exact_ref(ref)
        catalog = self._catalog_service.load_catalog()
        pin = catalog.recipe_pin(ref)
        payload = self._catalog_service.load_artifact(pin, kind="recipe").payload
        properties = payload["parameter_schema"]["properties"]
        route = _external_recipe_route(
            payload,
            catalog_service=self._catalog_service,
        )
        return {
            "ref": ref,
            "origin": catalog.catalog_id,
            "status": payload["status"],
            "owner": payload["owner"],
            "description": payload.get("description", ""),
            "domain": payload["domain"],
            "parameters": [
                {
                    "name": name,
                    "type": definition["type"],
                    "has_default": "default" in definition,
                }
                for name, definition in sorted(properties.items())
            ],
            "allowed_profiles": [item["ref"] for item in payload["profiles"]],
            "route": route,
        }

    def pin_artifact(self, path: Path) -> dict[str, str]:
        relative = _relative_path(self._root, path)
        try:
            content = read_confined_file(self._root, relative, max_bytes=BoundedYamlLimits().max_bytes)
            payload = load_bounded_yaml(content)
        except (BoundedYamlError, ConfinedFileError) as exc:
            raise RecipeResolutionError(
                "DPONE_RECIPE_ARTIFACT_INVALID",
                "Recipe artifact could not be read safely.",
            ) from exc
        if not isinstance(payload, Mapping) or payload.get("schema") not in _SCHEMA_KIND:
            raise RecipeResolutionError(
                "DPONE_RECIPE_ARTIFACT_INVALID",
                "Artifact schema must be recipe, profile, or component v1.",
            )
        kind = _SCHEMA_KIND[str(payload["schema"])]
        ref = f"{payload.get('id')}@{payload.get('version')}"
        pin = RecipeArtifactPin(
            ref=ref,
            artifact_ref=relative,
            sha256="sha256:" + hashlib.sha256(content).hexdigest(),
        )
        self._catalog_service.load_artifact(pin, kind=kind)
        return {"kind": kind, **pin.to_jsonable()}

    def validate_catalog(self, *, catalog: RecipeCatalog | None = None) -> dict[str, Any]:
        catalog = catalog or self._catalog_service.load_catalog()
        cache: dict[tuple[str, str, str, str], Mapping[str, Any]] = {}

        def load_payload(pin: RecipeArtifactPin, kind: str) -> Mapping[str, Any]:
            key = kind, pin.ref, pin.artifact_ref, pin.sha256
            if key not in cache:
                cache[key] = self._catalog_service.load_artifact(pin, kind=kind).payload
            return cache[key]

        return validate_recipe_catalog(catalog, load_payload=load_payload)


def validate_recipe_catalog(
    catalog: RecipeCatalog,
    *,
    load_payload: Callable[[RecipeArtifactPin, str], Mapping[str, Any]],
) -> dict[str, Any]:
    """Validate one catalog with storage-independent exact artifact loading."""

    validated: set[tuple[str, str, str]] = set()
    for entry in catalog.entries:
        payload = load_payload(entry.pin, entry.kind)
        validated.add((entry.kind, entry.pin.ref, entry.pin.sha256))
        if entry.kind == "recipe":
            _validate_recipe_closure(payload, validated=validated, load_payload=load_payload)
    return {
        "catalog_id": catalog.catalog_id,
        "status": "valid",
        "validated_artifacts": len(validated),
    }


def _validate_recipe_closure(
    payload: Mapping[str, Any],
    *,
    validated: set[tuple[str, str, str]],
    load_payload: Callable[[RecipeArtifactPin, str], Mapping[str, Any]],
) -> None:
    raw_pins = [*payload.get("profiles", []), *payload.get("components", [])]
    profile_count = len(payload.get("profiles", []))
    properties = set(payload["parameter_schema"]["properties"])
    components: list[Mapping[str, Any]] = []
    for index, raw_pin in enumerate(raw_pins):
        kind = "profile" if index < profile_count else "component"
        pin = RecipeArtifactPin.from_mapping(raw_pin, label=kind)
        artifact_payload = load_payload(pin, kind)
        validated.add((kind, pin.ref, pin.sha256))
        if kind == "profile":
            try:
                validate_recipe_profile_contract(payload, artifact_payload)
            except RecipeContractError as exc:
                raise RecipeResolutionError(exc.code, str(exc)) from exc
        else:
            components.append(artifact_payload)
    try:
        validate_recipe_component_contracts(components, parameter_names=tuple(sorted(properties)))
    except RecipeContractError as exc:
        raise RecipeResolutionError(exc.code, str(exc)) from exc


def _relative_path(root: Path, path: Path) -> str:
    try:
        return project_relative_path(root, path)
    except ConfinedFileError as exc:
        raise RecipeResolutionError(
            "DPONE_RECIPE_ARTIFACT_INVALID",
            "Recipe artifact path must stay inside the project root.",
        ) from exc


def _external_recipe_route(
    recipe: Mapping[str, Any],
    *,
    catalog_service: RecipeCatalogService,
) -> dict[str, str] | None:
    parameters = _parameter_defaults(recipe)
    selected_profile = recipe.get("default_profile_ref")
    for raw_pin in recipe.get("profiles", []):
        pin = RecipeArtifactPin.from_mapping(raw_pin, label="profile")
        if pin.ref == selected_profile:
            profile = catalog_service.load_artifact(pin, kind="profile").payload
            parameters.update(dict(profile.get("values") or {}))
            break

    routes: set[tuple[str, str, str]] = set()
    for raw_pin in recipe.get("components", []):
        pin = RecipeArtifactPin.from_mapping(raw_pin, label="component")
        component = catalog_service.load_artifact(pin, kind="component").payload
        for process in component.get("processes", []):
            if not isinstance(process, Mapping):
                return None
            source = _resolved_text(_nested(process, "source", "type"), parameters)
            sink = _resolved_text(_nested(process, "sink", "type"), parameters)
            strategy = _resolved_text(_nested(process, "sink", "strategy", "mode"), parameters)
            if not source or not sink or not strategy:
                return None
            routes.add((source, sink, strategy))
    if len(routes) != 1:
        return None
    source, sink, strategy = next(iter(routes))
    return {
        "source_type": source,
        "sink_type": sink,
        "strategy": strategy,
    }


def _parameter_defaults(recipe: Mapping[str, Any]) -> dict[str, Any]:
    schema = recipe.get("parameter_schema")
    properties = schema.get("properties") if isinstance(schema, Mapping) else None
    if not isinstance(properties, Mapping):
        return {}
    return {
        str(name): definition["default"]
        for name, definition in properties.items()
        if isinstance(definition, Mapping) and "default" in definition
    }


def _nested(value: Mapping[str, Any], *path: str) -> Any:
    current: Any = value
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _resolved_text(value: Any, parameters: Mapping[str, Any]) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, Mapping) and set(value) == {"$param"}:
        parameter = value.get("$param")
        resolved = parameters.get(parameter) if isinstance(parameter, str) else None
        return resolved.strip() if isinstance(resolved, str) and resolved.strip() else None
    return None


__all__ = ["RecipeCatalogOperations", "validate_recipe_catalog"]
