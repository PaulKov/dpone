"""Compile editable authoring sources into the canonical batch manifest IR."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.manifest.authoring_folder import (
    AuthoringSourceDependency,
    BoundedYamlFolderFragmentLoader,
    FolderAuthoringError,
    FolderFragmentLoader,
    collect_sql_file_dependencies,
)
from dpone.manifest.batch_compiler_impl import BatchManifestCompiler
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.pipeline_identity import PipelineId, PipelineIdError
from dpone.manifest.recipe_models import RecipeContext, RecipeResolutionError, RecipeSourceResolver
from dpone.manifest.recipe_resolver import BoundedLocalRecipeSourceResolver

_BATCH_KIND = "dpone.batch.v1"
_FLOW_KIND = "dpone.flow.v1"
_LEGACY_PIPELINE_KIND = "dpone.pipeline.v1"
_LEGACY_PROCESSES_ALIAS = "DPONE_LEGACY_SELF_SERVICE_BATCH_PROCESSES"
_LEGACY_PIPELINE_ALIAS = "DPONE_LEGACY_SELF_SERVICE_PIPELINE_V1"


class AuthoringCompilationError(ManifestConfigurationError):
    """A stable, credential-free authoring compilation failure."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class AuthoringCompilation:
    """Immutable summary shared by check, preview, and manifest loading."""

    authoring_mode: str
    source_kind: str
    canonical_kind: str
    canonical_manifest: Mapping[str, Any]
    processes: tuple[Mapping[str, Any], ...]
    source_fingerprint: str
    semantic_fingerprint: str
    deprecated_aliases: tuple[str, ...] = ()
    dependencies: tuple[AuthoringSourceDependency, ...] = ()
    recipe_provenance: Mapping[str, Any] | None = None
    pipeline_id: PipelineId | None = None


class AuthoringCompiler:
    """Normalize supported authoring modes through the existing batch compiler."""

    def __init__(
        self,
        *,
        batch_compiler: BatchManifestCompiler | None = None,
        folder_loader: FolderFragmentLoader | None = None,
        recipe_resolver: RecipeSourceResolver | None = None,
    ) -> None:
        self._batch_compiler = batch_compiler or BatchManifestCompiler()
        self._folder_loader = folder_loader
        self._recipe_resolver = recipe_resolver

    def compile(
        self,
        payload: Mapping[str, Any],
        *,
        source_path: Path,
        project_root: Path | None = None,
        include_content_dependencies: bool = True,
    ) -> AuthoringCompilation:
        source = copy.deepcopy(dict(payload))
        source_kind = str(source.get("kind") or source.get("schema") or "").strip()
        mode = self._validate_authority(source, source_kind=source_kind, source_path=source_path, root=project_root)
        pipeline_id = _pipeline_identity(source)
        dependencies: tuple[AuthoringSourceDependency, ...] = ()
        source_identity: Mapping[str, Any] = source
        recipe_provenance: Mapping[str, Any] | None = None
        recipe_deprecations: tuple[str, ...] = ()
        if "recipe" in source:
            if mode != "flow" or "processes" in source or "fragments" in source:
                raise AuthoringCompilationError(
                    "DPONE_AUTHORING_SOURCE_AMBIGUOUS",
                    "A recipe source must use flow authoring and cannot contain processes or fragments.",
                )
            if self._recipe_resolver is None:
                raise AuthoringCompilationError(
                    "DPONE_RECIPE_RESOLVER_UNAVAILABLE",
                    "Recipe authoring requires the configured bounded local resolver.",
                )
            raw_recipe = source.get("recipe")
            metadata = source.get("metadata")
            if not isinstance(raw_recipe, Mapping) or not isinstance(metadata, Mapping):
                raise AuthoringCompilationError(
                    "DPONE_RECIPE_ARTIFACT_INVALID",
                    "Recipe source requires recipe and metadata objects.",
                )
            recipe_pipeline_id = str(pipeline_id) if pipeline_id is not None else ""
            domain = str(metadata.get("domain") or "").strip()
            if not recipe_pipeline_id or not domain:
                raise AuthoringCompilationError(
                    "DPONE_RECIPE_ARTIFACT_INVALID",
                    "Recipe source metadata requires id and domain.",
                )
            try:
                resolved_recipe = self._recipe_resolver.resolve(
                    raw_recipe,
                    source_path=source_path,
                    project_root=project_root,
                    context=RecipeContext(pipeline_id=recipe_pipeline_id, domain=domain),
                )
            except RecipeResolutionError as exc:
                raise AuthoringCompilationError(exc.code, str(exc)) from exc
            dependencies = resolved_recipe.dependencies
            recipe_provenance = resolved_recipe.provenance.to_jsonable()
            recipe_deprecations = tuple(
                f"DPONE_RECIPE_ARTIFACT_DEPRECATED:{ref}" for ref in resolved_recipe.provenance.deprecated_refs
            )
            source_identity = {
                "root": copy.deepcopy(source),
                "dependencies": [item.to_jsonable() for item in dependencies],
            }
            source["processes"] = [copy.deepcopy(dict(process)) for process in resolved_recipe.processes]
            source.pop("recipe", None)
        if mode == "folder":
            if self._folder_loader is None:
                raise AuthoringCompilationError(
                    "DPONE_AUTHORING_FOLDER_LOADER_UNAVAILABLE",
                    "Folder authoring requires the configured bounded fragment loader.",
                )
            try:
                fragments = source.get("fragments")
                if "processes" in source or not isinstance(fragments, list) or not fragments:
                    raise FolderAuthoringError(
                        "DPONE_AUTHORING_FOLDER_FRAGMENTS_INVALID",
                        "Folder authoring requires a non-empty fragments list and no inline processes.",
                    )
                authoring = source.get("authoring")
                project_source = str(authoring.get("source") or "") if isinstance(authoring, Mapping) else ""
                folder = self._folder_loader.load(
                    source_path=source_path,
                    project_source=project_source,
                    fragment_refs=fragments,
                    project_root=project_root,
                )
            except FolderAuthoringError as exc:
                raise AuthoringCompilationError(exc.code, str(exc)) from exc
            dependencies = folder.dependencies
            source_identity = {
                "root": copy.deepcopy(source),
                "fragments": [
                    {"path": document.dependency.path, "payload": document.payload}
                    for document in folder.source_documents
                ],
            }
            source["processes"] = [copy.deepcopy(dict(process)) for process in folder.processes]
            source.pop("fragments", None)
        canonical, aliases = self._normalize(source, source_kind=source_kind, mode=mode)
        if project_root is not None and include_content_dependencies:
            try:
                sql_dependencies = collect_sql_file_dependencies(
                    canonical,
                    source_path=source_path,
                    project_root=project_root,
                )
            except (OSError, ValueError) as exc:
                raise AuthoringCompilationError(
                    "DPONE_AUTHORING_DEPENDENCY_READ_FAILED",
                    "A declared SQL dependency could not be read safely.",
                ) from exc
            dependencies = tuple(
                sorted(
                    set((*dependencies, *sql_dependencies)),
                    key=lambda item: (item.kind, item.path, item.sha256),
                )
            )
        source_content_dependencies = tuple(item for item in dependencies if item.kind == "sql_file")
        if source_content_dependencies:
            source_identity = {
                **copy.deepcopy(dict(source_identity)),
                "content_dependencies": [item.to_jsonable() for item in source_content_dependencies],
            }
        try:
            compiled = self._batch_compiler.compile(canonical, manifest_path=source_path)
        except ManifestConfigurationError:
            raise
        except Exception as exc:  # noqa: BLE001 - normalize all compiler failures at the public boundary.
            raise AuthoringCompilationError(
                "DPONE_AUTHORING_COMPILATION_FAILED",
                "Authoring source could not be compiled into the canonical manifest.",
            ) from exc
        processes = tuple(copy.deepcopy(process.raw_config) for process in compiled)
        semantic_payload = {
            "metadata": _semantic_metadata(canonical.get("metadata")),
            "processes": sorted(processes, key=_process_identity),
        }
        return AuthoringCompilation(
            authoring_mode=mode,
            source_kind=source_kind,
            canonical_kind=_BATCH_KIND,
            canonical_manifest=canonical,
            processes=processes,
            source_fingerprint=canonical_fingerprint(source_identity),
            semantic_fingerprint=canonical_fingerprint(semantic_payload),
            pipeline_id=pipeline_id,
            deprecated_aliases=tuple((*aliases, *recipe_deprecations)),
            dependencies=dependencies,
            recipe_provenance=recipe_provenance,
        )

    @staticmethod
    def _validate_authority(
        source: Mapping[str, Any],
        *,
        source_kind: str,
        source_path: Path,
        root: Path | None,
    ) -> str:
        authoring = source.get("authoring")
        if authoring is None and source_kind == _LEGACY_PIPELINE_KIND:
            return "flow"
        if authoring is None and source_kind == _BATCH_KIND:
            return "classic"
        if not isinstance(authoring, Mapping):
            raise AuthoringCompilationError(
                "DPONE_AUTHORING_SOURCE_INVALID",
                "authoring must be an object with mode and source.",
            )
        mode = str(authoring.get("mode") or "").strip()
        if mode not in {"classic", "flow", "folder"}:
            raise AuthoringCompilationError(
                "DPONE_AUTHORING_SOURCE_INVALID",
                "authoring.mode must be classic, flow, or folder.",
            )
        expected_kind = _BATCH_KIND if mode == "classic" else _FLOW_KIND
        if source_kind != expected_kind:
            raise AuthoringCompilationError(
                "DPONE_AUTHORING_MODE_KIND_MISMATCH",
                f"authoring.mode={mode} requires kind: {expected_kind}.",
            )
        declared = str(authoring.get("source") or "").strip().replace("\\", "/")
        if not declared:
            raise AuthoringCompilationError(
                "DPONE_AUTHORING_SOURCE_INVALID",
                "authoring.source must name the primary source file.",
            )
        _validate_declared_source(declared, source_path=source_path, root=root)
        return mode

    @staticmethod
    def _normalize(
        source: dict[str, Any],
        *,
        source_kind: str,
        mode: str,
    ) -> tuple[dict[str, Any], tuple[str, ...]]:
        has_processes = "processes" in source
        has_schemas = "schemas" in source
        if has_processes and has_schemas:
            raise AuthoringCompilationError(
                "DPONE_AUTHORING_SOURCE_AMBIGUOUS",
                "Authoring source cannot contain both processes and schemas.",
            )
        if source_kind == _BATCH_KIND and has_schemas:
            return source, ()
        if source_kind == _FLOW_KIND and has_processes:
            return _flow_to_batch(source), ()
        if source_kind == _BATCH_KIND and has_processes:
            return _flow_to_batch(source), (_LEGACY_PROCESSES_ALIAS,)
        if source_kind == _LEGACY_PIPELINE_KIND and has_processes:
            return _flow_to_batch(source), (_LEGACY_PIPELINE_ALIAS,)
        raise AuthoringCompilationError(
            "DPONE_AUTHORING_STRUCTURE_INVALID",
            "Classic authoring requires schemas; flow authoring requires processes.",
        )


def _flow_to_batch(source: Mapping[str, Any]) -> dict[str, Any]:
    raw_processes = source.get("processes")
    if not isinstance(raw_processes, list) or not raw_processes:
        raise AuthoringCompilationError(
            "DPONE_PIPELINE_PROCESS_MISSING",
            "processes must contain at least one process.",
        )
    schemas: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for index, raw_process in enumerate(raw_processes):
        if not isinstance(raw_process, Mapping):
            raise AuthoringCompilationError(
                "DPONE_PIPELINE_PROCESS_INVALID",
                f"processes[{index}] must be an object.",
            )
        process = copy.deepcopy(dict(raw_process))
        name = str(process.get("name") or "").strip()
        source_endpoint = process.get("source")
        table = source_endpoint.get("table") if isinstance(source_endpoint, Mapping) else None
        schema_name = str(table.get("schema") or "").strip() if isinstance(table, Mapping) else ""
        table_name = str(table.get("name") or "").strip() if isinstance(table, Mapping) else ""
        if not name or not schema_name or not table_name:
            raise AuthoringCompilationError(
                "DPONE_PIPELINE_PROCESS_INVALID",
                f"processes[{index}] requires name and source.table schema/name.",
            )
        schema = schemas.setdefault(schema_name, {"tables": []})
        schema["tables"].append(
            {
                "table": table_name,
                "id": name,
                "overrides": process,
            }
        )
    canonical: dict[str, Any] = {
        "kind": _BATCH_KIND,
        "defaults": {},
        "schemas": schemas,
    }
    for field in ("authoring", "metadata", "meta", "quality", "observability", "performance", "certification"):
        if field in source:
            canonical[field] = copy.deepcopy(source[field])
    return canonical


def _validate_declared_source(declared: str, *, source_path: Path, root: Path | None) -> None:
    declared_path = PurePosixPath(declared)
    if declared_path.is_absolute() or ".." in declared_path.parts:
        raise AuthoringCompilationError(
            "DPONE_AUTHORING_SOURCE_PATH_INVALID",
            "authoring.source must be a project-relative path without traversal.",
        )
    actual = source_path.resolve(strict=False)
    if root is not None:
        root_path = root.resolve(strict=False)
        try:
            actual_text = actual.relative_to(root_path).as_posix()
        except ValueError as exc:
            raise AuthoringCompilationError(
                "DPONE_AUTHORING_SOURCE_PATH_INVALID",
                "Primary authoring source is outside the project root.",
            ) from exc
    else:
        parts = declared_path.parts
        if len(actual.parts) < len(parts):
            raise AuthoringCompilationError(
                "DPONE_AUTHORING_SOURCE_PATH_MISMATCH",
                "authoring.source does not match the primary source file.",
            )
        inferred_root = actual.parents[len(parts) - 1]
        expected = inferred_root.joinpath(*parts).resolve(strict=False)
        if expected != actual:
            raise AuthoringCompilationError(
                "DPONE_AUTHORING_SOURCE_PATH_MISMATCH",
                "authoring.source does not match the primary source file.",
            )
        return
    if actual_text != declared:
        raise AuthoringCompilationError(
            "DPONE_AUTHORING_SOURCE_PATH_MISMATCH",
            "authoring.source does not match the primary source file.",
        )


def _semantic_metadata(raw: object) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        return {}
    metadata = {str(key): copy.deepcopy(value) for key, value in raw.items() if str(key) != "recipe"}
    tags = metadata.get("tags")
    if isinstance(tags, list) and all(isinstance(tag, str) for tag in tags):
        metadata["tags"] = sorted(set(tags))
    return metadata


def _pipeline_identity(source: Mapping[str, Any]) -> PipelineId | None:
    metadata = source.get("metadata")
    if not isinstance(metadata, Mapping) or "id" not in metadata:
        return None
    raw = metadata.get("id")
    try:
        return PipelineId.parse(raw if isinstance(raw, str) else "")
    except PipelineIdError as exc:
        raise AuthoringCompilationError("DPONE_PIPELINE_ID_INVALID", str(exc)) from exc


def _process_identity(process: Mapping[str, Any]) -> tuple[str, str, str]:
    source = process.get("source")
    table = source.get("table") if isinstance(source, Mapping) else None
    schema_name = str(table.get("schema") or "") if isinstance(table, Mapping) else ""
    table_name = str(table.get("name") or "") if isinstance(table, Mapping) else ""
    return str(process.get("name") or ""), schema_name, table_name


def default_authoring_compiler() -> AuthoringCompiler:
    """Compose the canonical compiler with its bounded filesystem adapter."""

    return AuthoringCompiler(
        folder_loader=BoundedYamlFolderFragmentLoader(),
        recipe_resolver=BoundedLocalRecipeSourceResolver(),
    )


def canonical_authoring_fingerprint(payload: Mapping[str, Any]) -> str:
    """Return the canonical fingerprint used by authoring plans and compilation."""

    return canonical_fingerprint(payload)


__all__ = [
    "AuthoringCompilation",
    "AuthoringCompilationError",
    "AuthoringCompiler",
    "AuthoringSourceDependency",
    "canonical_authoring_fingerprint",
    "default_authoring_compiler",
]
