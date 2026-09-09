"""Content-pinned, data-only recipe resolution for the build/runtime plane."""

from __future__ import annotations

import copy
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.manifest.authoring_folder import AuthoringSourceDependency
from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import ConfinedFileError, read_confined_file
from dpone.manifest.recipe_models import (
    RecipeArtifactPin,
    RecipeContext,
    RecipeResolutionError,
    RecipeResolutionProvenance,
    ResolvedRecipe,
    split_exact_ref,
    validate_exact_ref,
    validate_logical_id,
)
from dpone.manifest.recipe_parameters import (
    MAX_PROCESSES,
    RecipeContractError,
    RecipeParameterError,
    effective_recipe_parameters,
    expand_recipe_components,
    reject_executable_recipe_content,
    validate_recipe_parameter_schema,
)

MAX_ARTIFACT_BYTES = 1024 * 1024
MAX_CLOSURE_BYTES = 8 * 1024 * 1024
MAX_COMPONENTS = 32
_LIMITS = BoundedYamlLimits(max_bytes=MAX_ARTIFACT_BYTES)
_STATUSES = {"experimental", "stable", "deprecated"}
_RECIPE_KEYS = {
    "schema",
    "id",
    "version",
    "owner",
    "status",
    "description",
    "domain",
    "parameter_schema",
    "override_allowlist",
    "default_profile_ref",
    "profiles",
    "components",
}
_PROFILE_KEYS = {"schema", "id", "version", "owner", "status", "values", "locked_parameters"}
_COMPONENT_KEYS = {"schema", "id", "version", "owner", "status", "processes"}
_BLOCK_KEYS = {"catalog_id", "ref", "artifact_ref", "sha256", "profile_ref", "parameters"}


@dataclass(frozen=True, slots=True)
class LoadedRecipeArtifact:
    """Verified bytes, parsed payload, and exact dependency pin."""

    payload: Mapping[str, Any]
    dependency: AuthoringSourceDependency
    byte_count: int


class RecipeArtifactLoader:
    """Read and validate exact recipe artifacts beneath one project root."""

    def __init__(self, project_root: Path) -> None:
        self._root = project_root.resolve(strict=True)

    def load(self, pin: RecipeArtifactPin, *, kind: str) -> LoadedRecipeArtifact:
        try:
            content = read_confined_file(self._root, pin.artifact_ref, max_bytes=MAX_ARTIFACT_BYTES)
        except ConfinedFileError as exc:
            raise RecipeResolutionError(
                "DPONE_RECIPE_ARTIFACT_INVALID",
                f"Pinned {kind} artifact could not be read safely.",
            ) from exc
        return recipe_artifact_from_bytes(content, pin=pin, kind=kind)


def recipe_artifact_from_bytes(
    content: bytes,
    *,
    pin: RecipeArtifactPin,
    kind: str,
) -> LoadedRecipeArtifact:
    """Validate exact artifact bytes without choosing their storage adapter."""

    if len(content) > MAX_ARTIFACT_BYTES:
        raise RecipeResolutionError(
            "DPONE_RECIPE_ARTIFACT_INVALID",
            f"Pinned {kind} artifact exceeds its byte limit.",
        )
    actual = "sha256:" + hashlib.sha256(content).hexdigest()
    if actual != pin.sha256:
        raise RecipeResolutionError(
            "DPONE_RECIPE_DIGEST_MISMATCH",
            f"Pinned {kind} artifact digest does not match its bytes.",
        )
    try:
        payload = load_bounded_yaml(content, limits=_LIMITS)
    except BoundedYamlError as exc:
        raise RecipeResolutionError(
            "DPONE_RECIPE_ARTIFACT_INVALID",
            f"Pinned {kind} artifact is not bounded declarative YAML.",
        ) from exc
    if not isinstance(payload, Mapping):
        raise RecipeResolutionError(
            "DPONE_RECIPE_ARTIFACT_INVALID",
            f"Pinned {kind} artifact must be a YAML object.",
        )
    normalized = copy.deepcopy(dict(payload))
    _validate_artifact_payload(normalized, pin=pin, kind=kind)
    return LoadedRecipeArtifact(
        payload=normalized,
        dependency=AuthoringSourceDependency(kind=kind, path=pin.artifact_ref, sha256=pin.sha256),
        byte_count=len(content),
    )


class BoundedLocalRecipeSourceResolver:
    """Resolve only the exact local closure pinned by a primary source."""

    def resolve(
        self,
        recipe_block: Mapping[str, object],
        *,
        source_path: Path,
        project_root: Path | None,
        context: RecipeContext,
    ) -> ResolvedRecipe:
        block = _validate_recipe_block(recipe_block)
        loader = RecipeArtifactLoader(project_root or _infer_project_root(source_path))
        recipe_pin = RecipeArtifactPin.from_mapping(
            {key: block[key] for key in ("ref", "artifact_ref", "sha256")},
            label="recipe",
        )
        recipe = loader.load(recipe_pin, kind="recipe")
        recipe_payload = recipe.payload
        if recipe_payload["domain"] != context.domain:
            raise RecipeResolutionError(
                "DPONE_RECIPE_ARTIFACT_INVALID",
                "Primary source domain must match the pinned recipe domain.",
            )

        profile_pin = _selected_profile_pin(block.get("profile_ref"), recipe_payload)
        profile_payload: Mapping[str, Any] = {}
        dependencies = [recipe.dependency]
        deprecated_refs = [f"recipe:{recipe_pin.ref}"] if recipe_payload["status"] == "deprecated" else []
        closure_bytes = recipe.byte_count
        if profile_pin is not None:
            profile = loader.load(profile_pin, kind="profile")
            profile_payload = profile.payload
            dependencies.append(profile.dependency)
            if profile.payload["status"] == "deprecated":
                deprecated_refs.append(f"profile:{profile_pin.ref}")
            closure_bytes += profile.byte_count

        component_pins = _pins(recipe_payload.get("components"), label="component", limit=MAX_COMPONENTS)
        components: list[Mapping[str, Any]] = []
        for pin in component_pins:
            component = loader.load(pin, kind="component")
            components.append(component.payload)
            dependencies.append(component.dependency)
            if component.payload["status"] == "deprecated":
                deprecated_refs.append(f"component:{pin.ref}")
            closure_bytes += component.byte_count
            if closure_bytes > MAX_CLOSURE_BYTES:
                raise RecipeResolutionError(
                    "DPONE_RECIPE_EXPANSION_LIMIT_EXCEEDED",
                    "Pinned recipe closure exceeds its total byte limit.",
                )

        try:
            parameters = effective_recipe_parameters(
                recipe_payload.get("parameter_schema"),
                profile_values=profile_payload.get("values"),
                locked_parameters=profile_payload.get("locked_parameters"),
                answers=block.get("parameters"),
                override_allowlist=recipe_payload.get("override_allowlist"),
            )
        except RecipeParameterError as exc:
            raise RecipeResolutionError(exc.code, str(exc)) from exc
        try:
            processes = expand_recipe_components(
                components,
                parameters=parameters,
                context={"pipeline_id": context.pipeline_id, "domain": context.domain},
            )
        except RecipeContractError as exc:
            raise RecipeResolutionError(exc.code, str(exc)) from exc
        ordered_dependencies = tuple(sorted(dependencies, key=lambda item: (item.kind, item.path, item.sha256)))
        provenance = RecipeResolutionProvenance(
            catalog_id=validate_logical_id(block.get("catalog_id"), label="catalog_id"),
            recipe_ref=recipe_pin.ref,
            recipe_sha256=recipe_pin.sha256,
            domain=context.domain,
            profile_ref=profile_pin.ref if profile_pin else None,
            component_refs=tuple(pin.ref for pin in component_pins),
            deprecated_refs=tuple(sorted(deprecated_refs)),
            status=str(recipe_payload["status"]),
            owner=str(recipe_payload["owner"]),
            closure=ordered_dependencies,
        )
        return ResolvedRecipe(
            processes=processes,
            dependencies=ordered_dependencies,
            provenance=provenance,
        )


def _validate_recipe_block(raw: Mapping[str, object]) -> dict[str, object]:
    if not set(raw) <= _BLOCK_KEYS or not {"catalog_id", "ref", "artifact_ref", "sha256"} <= set(raw):
        raise RecipeResolutionError(
            "DPONE_RECIPE_ARTIFACT_INVALID",
            "recipe block contains missing or unsupported fields.",
        )
    parameters = raw.get("parameters")
    if parameters is not None and not isinstance(parameters, Mapping):
        raise RecipeResolutionError(
            "DPONE_RECIPE_PARAMETER_SCHEMA_INVALID",
            "recipe.parameters must be a scalar mapping.",
        )
    return copy.deepcopy(dict(raw))


def _validate_artifact_payload(payload: Mapping[str, Any], *, pin: RecipeArtifactPin, kind: str) -> None:
    expected_keys = {"recipe": _RECIPE_KEYS, "profile": _PROFILE_KEYS, "component": _COMPONENT_KEYS}[kind]
    required = expected_keys - ({"default_profile_ref", "description"} if kind == "recipe" else set())
    if not required <= set(payload) or not set(payload) <= expected_keys:
        raise RecipeResolutionError(
            "DPONE_RECIPE_ARTIFACT_INVALID",
            f"Pinned {kind} contains missing or unsupported fields.",
        )
    if payload.get("schema") != f"dpone.{kind}.v1":
        raise RecipeResolutionError("DPONE_RECIPE_ARTIFACT_INVALID", f"Pinned {kind} schema is invalid.")
    expected_id, expected_version = split_exact_ref(pin.ref)
    if payload.get("id") != expected_id or payload.get("version") != expected_version:
        raise RecipeResolutionError(
            "DPONE_RECIPE_ARTIFACT_INVALID",
            f"Pinned {kind} id/version does not match its ref.",
        )
    validate_logical_id(payload.get("owner"), label=f"{kind} owner")
    if payload.get("status") not in _STATUSES:
        raise RecipeResolutionError("DPONE_RECIPE_ARTIFACT_INVALID", f"Pinned {kind} status is invalid.")
    if kind == "recipe":
        validate_logical_id(payload.get("domain"), label="recipe domain")
        try:
            validate_recipe_parameter_schema(payload.get("parameter_schema"))
        except RecipeParameterError as exc:
            raise RecipeResolutionError(exc.code, str(exc)) from exc
        profiles = _pins(payload.get("profiles"), label="profile", limit=32)
        if len({item.ref for item in profiles}) != len(profiles):
            raise RecipeResolutionError("DPONE_RECIPE_ARTIFACT_INVALID", "Profile refs must be unique.")
        default_ref = payload.get("default_profile_ref")
        if default_ref is not None and default_ref not in {item.ref for item in profiles}:
            raise RecipeResolutionError(
                "DPONE_RECIPE_ARTIFACT_INVALID",
                "default_profile_ref must match one exact profile pin.",
            )
        component_pins = _pins(payload.get("components"), label="component", limit=MAX_COMPONENTS)
        if not component_pins or len({item.ref for item in component_pins}) != len(component_pins):
            raise RecipeResolutionError(
                "DPONE_RECIPE_COMPONENT_INVALID",
                "Recipe components must be a non-empty unique ordered list.",
            )
    elif kind == "profile":
        if not isinstance(payload.get("values"), Mapping) or not isinstance(payload.get("locked_parameters"), list):
            raise RecipeResolutionError("DPONE_RECIPE_ARTIFACT_INVALID", "Profile values or locks are invalid.")
    else:
        processes = payload.get("processes")
        if not isinstance(processes, list) or not processes or len(processes) > MAX_PROCESSES:
            raise RecipeResolutionError("DPONE_RECIPE_COMPONENT_INVALID", "Component processes are invalid.")
        if any(
            not isinstance(process, Mapping) or not {"name", "source", "sink"} <= set(process) for process in processes
        ):
            raise RecipeResolutionError(
                "DPONE_RECIPE_COMPONENT_INVALID",
                "Every component process must declare name, source, and sink.",
            )
        try:
            reject_executable_recipe_content(processes)
        except RecipeContractError as exc:
            raise RecipeResolutionError(exc.code, str(exc)) from exc


def _selected_profile_pin(raw: object, recipe: Mapping[str, Any]) -> RecipeArtifactPin | None:
    profiles = _pins(recipe.get("profiles"), label="profile", limit=32)
    selected_ref = raw if raw is not None else recipe.get("default_profile_ref")
    if selected_ref is None:
        return None
    ref = validate_exact_ref(selected_ref, label="selected profile ref")
    for profile in profiles:
        if profile.ref == ref:
            return profile
    raise RecipeResolutionError(
        "DPONE_RECIPE_PROFILE_NOT_ALLOWED",
        "Selected profile is not present in the recipe profile pins.",
    )


def _pins(raw: object, *, label: str, limit: int) -> tuple[RecipeArtifactPin, ...]:
    if not isinstance(raw, list) or len(raw) > limit:
        raise RecipeResolutionError("DPONE_RECIPE_ARTIFACT_INVALID", f"{label} pins are invalid.")
    return tuple(RecipeArtifactPin.from_mapping(item, label=label) for item in raw)


def _infer_project_root(source_path: Path) -> Path:
    resolved = source_path.resolve(strict=False)
    for parent in (resolved.parent, *resolved.parents):
        if (parent / "dpone.yaml").is_file():
            return parent
    parts = resolved.parts
    try:
        index = parts.index("pipelines")
    except ValueError as exc:
        raise RecipeResolutionError(
            "DPONE_RECIPE_ARTIFACT_INVALID",
            "Recipe project root could not be inferred; pass project_root explicitly.",
        ) from exc
    return Path(*parts[:index])


__all__ = [
    "BoundedLocalRecipeSourceResolver",
    "LoadedRecipeArtifact",
    "RecipeArtifactLoader",
    "recipe_artifact_from_bytes",
]
