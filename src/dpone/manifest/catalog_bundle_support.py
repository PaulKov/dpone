"""Manifest-layer support for signed recipe catalog materialization."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import ConfinedFileError, project_relative_path, read_confined_file
from dpone.manifest.recipe_catalog import RecipeCatalogService, recipe_catalog_from_payload
from dpone.manifest.recipe_catalog_operations import RecipeCatalogOperations, validate_recipe_catalog
from dpone.manifest.recipe_models import RecipeArtifactPin, RecipeResolutionError
from dpone.manifest.recipe_resolver import recipe_artifact_from_bytes


class RecipeBundleSupportError(ValueError):
    """A recipe bundle could not be validated through the canonical manifest layer."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class RecipeBundleFile:
    logical_id: str
    source_ref: str
    content: bytes


def collect_recipe_bundle_files(root: Path, source: str, *, max_bytes: int) -> tuple[RecipeBundleFile, ...]:
    source_bytes = read_project_file(root, source, max_bytes=max_bytes)
    if _configured_catalog_path(root, max_bytes=max_bytes) != source:
        raise RecipeBundleSupportError("content_invalid", "bundle source is not the configured recipe catalog")
    try:
        operations = RecipeCatalogOperations(root, built_in_recipes={})
        operations.validate_catalog()
        service = RecipeCatalogService(root)
        catalog = service.load_catalog()
        result = [RecipeBundleFile("catalog", source, source_bytes)]
        seen: set[tuple[str, str]] = set()
        pending = [(entry.kind, entry.pin) for entry in catalog.entries]
        while pending:
            kind, pin = pending.pop(0)
            if (kind, pin.ref) in seen:
                continue
            seen.add((kind, pin.ref))
            loaded = service.load_artifact(pin, kind=kind)
            result.append(
                RecipeBundleFile(
                    f"{kind}/{pin.ref}",
                    pin.artifact_ref,
                    read_project_file(root, pin.artifact_ref, max_bytes=max_bytes),
                )
            )
            if kind == "recipe":
                pending.extend(_closure_pins(loaded.payload))
    except (RecipeResolutionError, ConfinedFileError, BoundedYamlError, OSError) as exc:
        raise RecipeBundleSupportError("content_invalid", "recipe catalog validation failed") from exc
    if source_bytes != read_project_file(root, source, max_bytes=max_bytes):
        raise RecipeBundleSupportError("source_changed", "recipe catalog changed during collection")
    return tuple(result)


def validate_recipe_bundle(
    entrypoint: bytes,
    artifacts: Mapping[str, bytes],
    *,
    max_bytes: int,
    max_tokens: int = 20_000,
    max_nodes: int = 10_000,
) -> None:
    catalog_payload = parse_bounded_mapping(
        entrypoint,
        max_bytes=max_bytes,
        max_tokens=max_tokens,
        max_nodes=max_nodes,
    )
    catalog_id = catalog_payload.get("catalog_id")
    raw_entries = catalog_payload.get("artifacts")
    if not isinstance(catalog_id, str) or not isinstance(raw_entries, list):
        raise RecipeBundleSupportError("content_invalid", "recipe catalog is invalid")
    catalog = recipe_catalog_from_payload(catalog_payload, trusted_catalog_ids=frozenset({catalog_id}))
    if artifacts.get("catalog") != entrypoint:
        raise RecipeBundleSupportError("content_invalid", "recipe catalog entrypoint identity is invalid")
    used = {"catalog"}
    pin_identities: dict[str, tuple[str, str]] = {}
    cache: dict[tuple[str, str, str, str], Mapping[str, Any]] = {}

    def load_payload(pin: RecipeArtifactPin, kind: str) -> Mapping[str, Any]:
        logical_id = f"{kind}/{pin.ref}"
        identity = pin.artifact_ref, pin.sha256
        if logical_id in pin_identities and pin_identities[logical_id] != identity:
            raise RecipeResolutionError("DPONE_RECIPE_ARTIFACT_INVALID", "Recipe pin identity is inconsistent.")
        pin_identities[logical_id] = identity
        key = kind, pin.ref, pin.artifact_ref, pin.sha256
        if key not in cache:
            content = artifacts.get(logical_id)
            if content is None:
                raise RecipeResolutionError("DPONE_RECIPE_ARTIFACT_INVALID", "Recipe closure artifact is missing.")
            cache[key] = recipe_artifact_from_bytes(content, pin=pin, kind=kind).payload
        used.add(logical_id)
        return cache[key]

    try:
        validate_recipe_catalog(catalog, load_payload=load_payload)
    except RecipeResolutionError as exc:
        raise RecipeBundleSupportError("content_invalid", "recipe closure validation failed") from exc
    if set(artifacts) != used:
        raise RecipeBundleSupportError("content_invalid", "recipe bundle contains unreferenced artifacts")


def parse_bounded_mapping(
    content: bytes,
    *,
    max_bytes: int,
    max_tokens: int = 20_000,
    max_nodes: int = 10_000,
) -> dict[str, Any]:
    try:
        value = load_bounded_yaml(
            content,
            limits=BoundedYamlLimits(
                max_bytes=max_bytes,
                max_tokens=max_tokens,
                max_nodes=max_nodes,
            ),
        )
    except BoundedYamlError as exc:
        raise RecipeBundleSupportError("content_invalid", "bounded YAML is invalid") from exc
    if not isinstance(value, Mapping):
        raise RecipeBundleSupportError("content_invalid", "bounded YAML must be an object")
    return dict(value)


def parse_bounded_json_mapping(
    content: bytes,
    *,
    max_bytes: int,
    max_nodes: int,
    max_depth: int = 32,
) -> dict[str, Any]:
    """Parse one canonical JSON control with duplicate and structure bounds."""

    if len(content) > max_bytes:
        raise RecipeBundleSupportError("content_invalid", "bounded JSON exceeds its byte limit")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise RecipeBundleSupportError("content_invalid", "bounded JSON keys must be unique")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise RecipeBundleSupportError("content_invalid", f"bounded JSON constant is invalid: {value}")

    try:
        value = json.loads(
            content.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecipeBundleSupportError("content_invalid", "bounded JSON is invalid") from exc
    if not isinstance(value, Mapping):
        raise RecipeBundleSupportError("content_invalid", "bounded JSON must be an object")
    _validate_json_shape(value, max_nodes=max_nodes, max_depth=max_depth)
    return dict(value)


def _validate_json_shape(value: object, *, max_nodes: int, max_depth: int) -> None:
    nodes = 0
    pending = [(value, 1)]
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > max_nodes:
            raise RecipeBundleSupportError("content_invalid", "bounded JSON exceeds its node limit")
        if depth > max_depth:
            raise RecipeBundleSupportError("content_invalid", "bounded JSON exceeds its depth limit")
        if isinstance(item, Mapping):
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)


def read_project_file(root: Path, relative: str, *, max_bytes: int) -> bytes:
    try:
        return read_confined_file(root, relative, max_bytes=max_bytes)
    except ConfinedFileError as exc:
        raise RecipeBundleSupportError("path_unsafe", "project file could not be read safely") from exc


def safe_project_relative(root: Path, value: str | Path) -> str:
    try:
        return project_relative_path(root, Path(value))
    except ConfinedFileError as exc:
        raise RecipeBundleSupportError("path_unsafe", "project path is unsafe") from exc


def _configured_catalog_path(root: Path, *, max_bytes: int) -> str:
    config = parse_bounded_mapping(read_project_file(root, "dpone.yaml", max_bytes=max_bytes), max_bytes=max_bytes)
    authoring = config.get("authoring")
    recipe = authoring.get("recipe_catalog") if isinstance(authoring, Mapping) else None
    path = recipe.get("path") if isinstance(recipe, Mapping) else None
    if not isinstance(path, str):
        raise RecipeBundleSupportError("content_invalid", "recipe catalog is not configured")
    return safe_project_relative(root, path)


def _closure_pins(payload: Mapping[str, Any]) -> list[tuple[str, RecipeArtifactPin]]:
    return [
        (kind, RecipeArtifactPin.from_mapping(raw, label=kind))
        for kind, values in (("profile", payload.get("profiles", [])), ("component", payload.get("components", [])))
        for raw in values
    ]


__all__ = [
    "RecipeBundleFile",
    "RecipeBundleSupportError",
    "collect_recipe_bundle_files",
    "parse_bounded_json_mapping",
    "parse_bounded_mapping",
    "read_project_file",
    "safe_project_relative",
    "validate_recipe_bundle",
]
