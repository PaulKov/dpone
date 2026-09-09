"""Pure target-source rendering for authoring-mode migration."""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.manifest.authoring import AuthoringCompilation
from dpone.manifest.authoring_folder import FolderLoadResult
from dpone.manifest.authoring_migration_models import AuthoringMode

_FLOW_KIND = "dpone.flow.v1"
_BATCH_KIND = "dpone.batch.v1"
_FRAGMENT_KIND = "dpone.flow-fragment.v1"
_FRAGMENT_NAME = "processes.yaml"
_CONTROL_FIELDS = frozenset({"kind", "schema", "authoring", "schemas", "processes", "fragments", "recipe"})


@dataclass(frozen=True, slots=True)
class AuthoringMigrationCandidate:
    root_payload: dict[str, Any]
    fragment_payload: dict[str, Any] | None
    fragment_name: str | None
    retained_files: tuple[str, ...]


class AuthoringModeRenderer:
    """Render one candidate without interpreting runtime behavior."""

    def render(
        self,
        *,
        source_payload: Mapping[str, Any],
        compilation: AuthoringCompilation,
        source_ref: str,
        target_mode: AuthoringMode,
    ) -> AuthoringMigrationCandidate:
        retained = _retained_fragment_paths(source_payload, source_ref=source_ref)
        if target_mode == "classic":
            root = copy.deepcopy(dict(compilation.canonical_manifest))
            root["kind"] = _BATCH_KIND
            root["authoring"] = {"mode": "classic", "source": source_ref}
            root.pop("processes", None)
            root.pop("fragments", None)
            root.pop("recipe", None)
            return AuthoringMigrationCandidate(root, None, None, retained)

        root = _flow_root(source_payload, compilation=compilation, source_ref=source_ref)
        if target_mode == "flow":
            root["authoring"] = {"mode": "flow", "source": source_ref}
            root["processes"] = _processes(compilation)
            return AuthoringMigrationCandidate(root, None, None, retained)

        root["authoring"] = {"mode": "folder", "source": source_ref}
        root["fragments"] = [_FRAGMENT_NAME]
        fragment = {"kind": _FRAGMENT_KIND, "processes": _processes(compilation)}
        return AuthoringMigrationCandidate(root, fragment, _FRAGMENT_NAME, retained)


class CandidateFolderLoader:
    """In-memory folder-loader adapter used only for candidate verification."""

    def __init__(self, fragment_payload: Mapping[str, Any], *, fragment_name: str = _FRAGMENT_NAME) -> None:
        self._fragment_payload = copy.deepcopy(dict(fragment_payload))
        self._fragment_name = fragment_name

    def load(
        self,
        *,
        source_path: Path,
        project_source: str,
        fragment_refs: Sequence[str],
        project_root: Path | None,
    ) -> FolderLoadResult:
        del source_path, project_source, project_root
        if tuple(fragment_refs) != (self._fragment_name,):
            raise ValueError("Candidate folder source references an unexpected fragment set.")
        processes = self._fragment_payload.get("processes")
        if not isinstance(processes, list) or not processes:
            raise ValueError("Candidate folder fragment must contain processes.")
        return FolderLoadResult(
            processes=tuple(copy.deepcopy(process) for process in processes),
            dependencies=(),
            source_documents=(),
        )


def _flow_root(
    source_payload: Mapping[str, Any],
    *,
    compilation: AuthoringCompilation,
    source_ref: str,
) -> dict[str, Any]:
    root = {str(key): copy.deepcopy(value) for key, value in source_payload.items() if str(key) not in _CONTROL_FIELDS}
    canonical = compilation.canonical_manifest
    for key in ("metadata", "meta", "quality", "observability", "performance", "certification"):
        if key not in root and key in canonical:
            root[key] = copy.deepcopy(canonical[key])
    return {
        "kind": _FLOW_KIND,
        "authoring": {"mode": "flow", "source": source_ref},
        **root,
    }


def _processes(compilation: AuthoringCompilation) -> list[dict[str, Any]]:
    return [copy.deepcopy(dict(process)) for process in compilation.processes]


def _retained_fragment_paths(source_payload: Mapping[str, Any], *, source_ref: str) -> tuple[str, ...]:
    authoring = source_payload.get("authoring")
    if not isinstance(authoring, Mapping) or authoring.get("mode") != "folder":
        return ()
    fragments = source_payload.get("fragments")
    if not isinstance(fragments, list):
        return ()
    parent = Path(source_ref).parent
    return tuple(
        sorted((parent / fragment).as_posix() for fragment in fragments if isinstance(fragment, str) and fragment)
    )


__all__ = [
    "AuthoringMigrationCandidate",
    "AuthoringModeRenderer",
    "CandidateFolderLoader",
]
