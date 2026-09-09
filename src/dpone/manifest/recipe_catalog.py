"""Build-plane discovery for one trusted local declarative recipe catalog."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Protocol

from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import (
    ConfinedFileError,
    project_relative_path,
    read_confined_file_snapshot,
)
from dpone.manifest.hermetic_test import hermetic_execution_plan, hermetic_execution_supported
from dpone.manifest.project_config import ProjectConfigError, ProjectConfigReader
from dpone.manifest.recipe_models import (
    RecipeArtifactPin,
    RecipeContext,
    RecipeResolutionError,
    RecipeResolutionProvenance,
    validate_artifact_ref,
    validate_exact_ref,
    validate_logical_id,
)
from dpone.manifest.recipe_resolver import (
    BoundedLocalRecipeSourceResolver,
    RecipeArtifactLoader,
)

MAX_CATALOG_ENTRIES = 1000
_LIMITS = BoundedYamlLimits()
_CATALOG_KEYS = {"schema", "catalog_id", "artifacts"}
_ENTRY_KEYS = {"kind", "ref", "artifact_ref", "sha256"}
_KINDS = {"recipe", "profile", "component"}


@dataclass(frozen=True, slots=True)
class ExternalRecipeScaffoldResolution:
    """Pinned source block and safe scaffold metadata."""

    recipe_block: Mapping[str, Any]
    domain: str
    connection_refs: tuple[str, ...]
    processes: tuple[Mapping[str, Any], ...]
    provenance: RecipeResolutionProvenance
    consumed_files: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class RecipeCatalogEntry:
    """One typed artifact pin from a validated catalog."""

    kind: str
    pin: RecipeArtifactPin

    def to_jsonable(self) -> dict[str, str]:
        return {"kind": self.kind, **self.pin.to_jsonable()}


@dataclass(frozen=True, slots=True)
class RecipeCatalog:
    """Validated, deterministic local catalog projection."""

    catalog_id: str
    entries: tuple[RecipeCatalogEntry, ...]
    source_path: str | None = None
    source_sha256: str | None = None

    def recipe_pin(self, ref: str) -> RecipeArtifactPin:
        matches = [entry.pin for entry in self.entries if entry.kind == "recipe" and entry.pin.ref == ref]
        if not matches:
            raise RecipeResolutionError("DPONE_RECIPE_NOT_FOUND", f"Recipe is not present in the catalog: {ref}")
        return matches[0]


class RecipeCatalogArtifact(Protocol):
    """Minimal artifact view exposed by the catalog facade."""

    @property
    def payload(self) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class _LoadedYaml:
    path: str
    payload: Any
    sha256: str


class RecipeCatalogService:
    """Discover and lock external recipes before atomic scaffolding."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve(strict=False)
        self._resolver = BoundedLocalRecipeSourceResolver()

    def resolve_for_scaffold(
        self,
        *,
        recipe_ref: str,
        profile_ref: str | None,
        answers_path: Path | None,
        pipeline_id: str,
    ) -> ExternalRecipeScaffoldResolution:
        validate_exact_ref(recipe_ref)
        catalog = self.load_catalog()
        recipe_pin = catalog.recipe_pin(recipe_ref)
        recipe = RecipeArtifactLoader(self._root).load(recipe_pin, kind="recipe")
        domain = str(recipe.payload["domain"])
        profile_pin = _select_profile(recipe.payload, profile_ref)
        parameters, answer_inputs = self._load_answers(answers_path)
        recipe_block: dict[str, Any] = {
            "catalog_id": catalog.catalog_id,
            **recipe_pin.to_jsonable(),
        }
        if profile_pin is not None:
            recipe_block["profile_ref"] = profile_pin.ref
        if parameters:
            recipe_block["parameters"] = parameters
        resolved = self._resolver.resolve(
            recipe_block,
            source_path=self._root / "pipelines" / pipeline_id / "pipeline.yaml",
            project_root=self._root,
            context=RecipeContext(pipeline_id=pipeline_id, domain=domain),
        )
        consumed_files = {item.path: item.sha256 for item in resolved.dependencies}
        if catalog.source_path is not None and catalog.source_sha256 is not None:
            consumed_files[catalog.source_path] = catalog.source_sha256
        consumed_files.update(answer_inputs)
        return ExternalRecipeScaffoldResolution(
            recipe_block=recipe_block,
            domain=domain,
            connection_refs=_connection_refs(resolved.processes),
            processes=resolved.processes,
            provenance=resolved.provenance,
            consumed_files=dict(sorted(consumed_files.items())),
        )

    @staticmethod
    def supports_starter_test(process: Mapping[str, Any]) -> bool:
        """Return whether the resolved recipe can emit an executable starter test."""

        return hermetic_execution_supported(process)

    @staticmethod
    def starter_test_unique_key(process: Mapping[str, Any]) -> tuple[str, ...]:
        """Return the normalized runtime-precedence key for a supported starter test."""

        return hermetic_execution_plan(process).unique_key

    def load_catalog(self) -> RecipeCatalog:
        config = self._load_project_config()
        authoring = config.get("authoring")
        recipe_config = authoring.get("recipe_catalog") if isinstance(authoring, Mapping) else None
        if not isinstance(recipe_config, Mapping):
            raise RecipeResolutionError(
                "DPONE_RECIPE_CATALOG_NOT_CONFIGURED",
                "External recipes require authoring.recipe_catalog in dpone.yaml.",
            )
        if set(recipe_config) != {"path", "trusted_catalog_ids"}:
            raise RecipeResolutionError(
                "DPONE_RECIPE_CATALOG_INVALID",
                "recipe_catalog requires exactly path and trusted_catalog_ids.",
            )
        catalog_path = validate_artifact_ref(recipe_config.get("path"), label="recipe catalog")
        trusted = recipe_config.get("trusted_catalog_ids")
        if not isinstance(trusted, list) or not trusted or any(not isinstance(item, str) for item in trusted):
            raise RecipeResolutionError(
                "DPONE_RECIPE_CATALOG_INVALID",
                "trusted_catalog_ids must be a non-empty list.",
            )
        loaded = self._read_yaml(catalog_path, code="DPONE_RECIPE_CATALOG_INVALID")
        catalog = recipe_catalog_from_payload(loaded.payload, trusted_catalog_ids=frozenset(trusted))
        return replace(
            catalog,
            source_path=loaded.path,
            source_sha256=loaded.sha256,
        )

    def load_artifact(self, pin: RecipeArtifactPin, *, kind: str) -> RecipeCatalogArtifact:
        """Load one exact catalog artifact through the shared confined reader."""

        return RecipeArtifactLoader(self._root).load(pin, kind=kind)

    def _load_project_config(self) -> Mapping[str, Any]:
        try:
            snapshot = ProjectConfigReader(self._root).read()
        except ProjectConfigError as exc:
            raise RecipeResolutionError(
                "DPONE_RECIPE_CATALOG_NOT_CONFIGURED",
                "dpone.yaml is missing or is not a dpone.project.v1 object.",
            ) from exc
        assert snapshot is not None
        return snapshot.payload

    def _load_answers(self, path: Path | None) -> tuple[Mapping[str, Any], Mapping[str, str]]:
        if path is None:
            return {}, {}
        relative = _project_relative(self._root, path)
        loaded = self._read_yaml(relative, code="DPONE_RECIPE_ANSWERS_UNSAFE")
        if not isinstance(loaded.payload, Mapping):
            raise RecipeResolutionError(
                "DPONE_RECIPE_ANSWERS_UNSAFE",
                "Recipe answers must be a bounded YAML object.",
            )
        return copy.deepcopy(dict(loaded.payload)), {loaded.path: loaded.sha256}

    def _read_yaml(self, relative_path: str, *, code: str) -> _LoadedYaml:
        try:
            snapshot = read_confined_file_snapshot(self._root, relative_path, max_bytes=_LIMITS.max_bytes)
            payload = load_bounded_yaml(snapshot.content, limits=_LIMITS)
            return _LoadedYaml(relative_path, payload, snapshot.sha256)
        except (BoundedYamlError, ConfinedFileError) as exc:
            raise RecipeResolutionError(code, "Configured recipe input could not be read safely.") from exc


def recipe_catalog_from_payload(
    payload: object,
    *,
    trusted_catalog_ids: frozenset[str],
) -> RecipeCatalog:
    """Validate one already parsed catalog through the canonical rules."""

    if not isinstance(payload, Mapping) or set(payload) != _CATALOG_KEYS:
        raise RecipeResolutionError(
            "DPONE_RECIPE_CATALOG_INVALID",
            "Recipe catalog must contain exactly schema, catalog_id, and artifacts.",
        )
    if payload.get("schema") != "dpone.recipe-catalog.v1":
        raise RecipeResolutionError("DPONE_RECIPE_CATALOG_INVALID", "Recipe catalog schema is unsupported.")
    catalog_id = validate_logical_id(payload.get("catalog_id"), label="catalog_id")
    if catalog_id not in trusted_catalog_ids:
        raise RecipeResolutionError(
            "DPONE_RECIPE_CATALOG_UNTRUSTED",
            "Recipe catalog id is not trusted by this project.",
        )
    raw_entries = payload.get("artifacts")
    if not isinstance(raw_entries, list) or len(raw_entries) > MAX_CATALOG_ENTRIES:
        raise RecipeResolutionError(
            "DPONE_RECIPE_CATALOG_INVALID",
            f"Recipe catalog supports at most {MAX_CATALOG_ENTRIES} entries.",
        )
    entries: list[RecipeCatalogEntry] = []
    identities: set[tuple[str, str]] = set()
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, Mapping) or set(raw_entry) != _ENTRY_KEYS:
            raise RecipeResolutionError("DPONE_RECIPE_CATALOG_INVALID", "Catalog entry fields are invalid.")
        kind = raw_entry.get("kind")
        if kind not in _KINDS:
            raise RecipeResolutionError("DPONE_RECIPE_CATALOG_INVALID", "Catalog artifact kind is invalid.")
        pin = RecipeArtifactPin.from_mapping(
            {key: raw_entry[key] for key in ("ref", "artifact_ref", "sha256")},
            label=str(kind),
        )
        identity = str(kind), pin.ref
        if identity in identities:
            raise RecipeResolutionError(
                "DPONE_RECIPE_CATALOG_INVALID",
                "Catalog kind/ref pairs must be unique.",
            )
        identities.add(identity)
        entries.append(RecipeCatalogEntry(kind=str(kind), pin=pin))
    return RecipeCatalog(
        catalog_id=catalog_id,
        entries=tuple(sorted(entries, key=lambda entry: (entry.kind, entry.pin.ref))),
    )


def _select_profile(recipe: Mapping[str, Any], requested_ref: str | None) -> RecipeArtifactPin | None:
    raw_profiles = recipe.get("profiles")
    if not isinstance(raw_profiles, list):
        raise RecipeResolutionError("DPONE_RECIPE_ARTIFACT_INVALID", "Recipe profiles is invalid.")
    profiles = tuple(RecipeArtifactPin.from_mapping(item, label="profile") for item in raw_profiles)
    if requested_ref is None:
        requested_ref = recipe.get("default_profile_ref")
        if requested_ref is None:
            return None
    validate_exact_ref(requested_ref, label="profile ref")
    for pin in profiles:
        if pin.ref == requested_ref:
            return pin
    raise RecipeResolutionError(
        "DPONE_RECIPE_PROFILE_NOT_ALLOWED",
        "Requested profile is not pinned by the selected recipe.",
    )


def _connection_refs(processes: tuple[Mapping[str, Any], ...]) -> tuple[str, ...]:
    refs: set[str] = set()
    for process in processes:
        for endpoint_name in ("source", "sink"):
            endpoint = process.get(endpoint_name)
            if isinstance(endpoint, Mapping) and isinstance(endpoint.get("connection_ref"), str):
                refs.add(endpoint["connection_ref"])
    return tuple(sorted(refs))


def _project_relative(root: Path, path: Path) -> str:
    try:
        return project_relative_path(root, path)
    except ConfinedFileError as exc:
        raise RecipeResolutionError(
            "DPONE_RECIPE_ANSWERS_UNSAFE",
            "Recipe answers path must stay inside the project root.",
        ) from exc


__all__ = [
    "RecipeCatalogService",
    "ExternalRecipeScaffoldResolution",
    "RecipeCatalog",
    "RecipeCatalogEntry",
    "recipe_catalog_from_payload",
]
