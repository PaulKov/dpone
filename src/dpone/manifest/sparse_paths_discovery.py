from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

from dpone.dag.resolver import DependencyResolver
from dpone.manifest.sparse_paths_models import (
    SparsePathEntry,
    SparsePathIssue,
    SparsePathReport,
    SparsePathReportBuilder,
)
from dpone.manifest.sparse_paths_policy import ResolvedSparsePath, SparsePathPolicy, SparsePathValidationError

_BUILT_IN_CONVENTIONS = frozenset({"landing", "landing_raw", "landing_raw_v1"})
_YAML_SUFFIXES = frozenset({".yaml", ".yml"})


class ManifestSparsePathReader(Protocol):
    def read(self, path: Path) -> object: ...


class ManifestSparsePathDiscovery:
    """Discovers manifest-owned sparse checkout dependencies from raw YAML."""

    def __init__(self, *, reader: ManifestSparsePathReader, policy: SparsePathPolicy) -> None:
        self._reader = reader
        self._policy = policy
        self._visited_manifests: set[Path] = set()

    def discover(self, manifest_path: Path) -> SparsePathReport:
        resolved = manifest_path.resolve(strict=False)
        builder = SparsePathReportBuilder(
            manifest=self._policy.repo_relative(resolved),
            workload_root=self._policy.repo_relative(self._policy.workload_root, is_dir=False),
        )
        self._discover_manifest(resolved, builder, source="manifest")
        return builder.to_report()

    def _discover_manifest(self, manifest_path: Path, builder: SparsePathReportBuilder, *, source: str) -> None:
        resolved = manifest_path.resolve(strict=False)
        if resolved in self._visited_manifests:
            return
        self._visited_manifests.add(resolved)

        try:
            resolved_entry = self._policy.entry_for_resolved_path(
                resolved,
                kind="manifest" if source == "manifest" else "manifest_dependency",
                source=source,
                required=True,
                reason="requested manifest" if source == "manifest" else "file-backed depends_on manifest",
            )
        except SparsePathValidationError as exc:
            builder.add_blocker(code="invalid_path", message=str(exc), path=manifest_path, source=source)
            return

        entry = _entry(
            resolved_entry,
            kind="manifest" if source == "manifest" else "manifest_dependency",
            source=source,
            required=True,
            reason="requested manifest" if source == "manifest" else "file-backed depends_on manifest",
        )
        builder.add_entry(entry)
        if not entry.exists:
            if source == "manifest":
                builder.add_blocker(
                    code="manifest_not_found",
                    message="Root manifest path does not exist",
                    path=entry.path,
                    source=source,
                )
            else:
                builder.add_warning(
                    code="missing_path",
                    message="Manifest dependency path does not exist",
                    path=entry.path,
                    source=source,
                )
            return

        raw = self._read_mapping(resolved, builder=builder, source=source)
        if raw is None:
            return

        for value in _iter_convention_refs(raw):
            self._add_relative_file(
                value,
                base_dir=resolved.parent,
                builder=builder,
                kind="convention",
                source="convention",
                reason="custom convention YAML referenced by manifest",
                recurse=False,
            )

        for value in _iter_string_or_strings(raw.get("registry")):
            self._add_relative_file(
                value,
                base_dir=resolved.parent,
                builder=builder,
                kind="registry",
                source="registry",
                reason="registry YAML referenced by manifest",
                recurse=False,
            )
        for value in _iter_string_or_strings(raw.get("registries")):
            self._add_relative_file(
                value,
                base_dir=resolved.parent,
                builder=builder,
                kind="registry",
                source="registries",
                reason="registry YAML referenced by manifest",
                recurse=False,
            )

        for dep in _iter_depends_on_items(raw):
            self._add_dependency(dep, current_manifest=resolved, builder=builder)

    def _read_mapping(
        self,
        path: Path,
        *,
        builder: SparsePathReportBuilder,
        source: str,
    ) -> Mapping[str, Any] | None:
        try:
            payload = self._reader.read(path)
        except Exception as exc:
            add_issue = builder.add_blocker if source == "manifest" else builder.add_warning
            message = "Could not read or parse manifest YAML"
            if source != "manifest":
                message = f"{message}: {exc}"
            add_issue(
                code="manifest_read_failed", message=message, path=self._policy.repo_relative(path), source=source
            )
            return None
        if not isinstance(payload, Mapping):
            add_issue = builder.add_blocker if source == "manifest" else builder.add_warning
            add_issue(
                code="manifest_not_mapping",
                message="Manifest YAML root is not an object",
                path=self._policy.repo_relative(path),
                source=source,
            )
            return None
        return payload

    def _add_dependency(
        self,
        dep: object,
        *,
        current_manifest: Path,
        builder: SparsePathReportBuilder,
    ) -> None:
        if isinstance(dep, str):
            dep = {"path": dep}
        if not isinstance(dep, Mapping):
            builder.add_warning(
                code="depends_on_unsupported",
                message="depends_on item is not a string or object",
                path=self._policy.repo_relative(current_manifest),
                source="depends_on",
            )
            return

        group = dep.get("group")
        if group:
            builder.add_warning(
                code="group_dependency_unresolved",
                message="depends_on group dependencies do not encode a file path for sparse checkout",
                path=str(group),
                source="depends_on.group",
            )
            return

        raw_path = dep.get("path")
        if not raw_path:
            return
        path_part, _selector, is_local = DependencyResolver.split_selector(str(raw_path))
        if is_local:
            return
        self._add_relative_file(
            path_part,
            base_dir=current_manifest.parent,
            builder=builder,
            kind="manifest_dependency",
            source="depends_on.path",
            reason="file-backed depends_on manifest",
            recurse=True,
        )

    def _add_relative_file(
        self,
        raw_path: object,
        *,
        base_dir: Path,
        builder: SparsePathReportBuilder,
        kind: str,
        source: str,
        reason: str,
        recurse: bool,
    ) -> None:
        try:
            resolved_entry = self._policy.entry_for_user_path(
                raw_path,
                base_dir=base_dir,
                kind=kind,
                source=source,
                required=True,
                reason=reason,
            )
        except SparsePathValidationError as exc:
            builder.add_blocker(code="invalid_path", message=str(exc), path=str(raw_path), source=source)
            return

        entry = _entry(resolved_entry, kind=kind, source=source, required=True, reason=reason)
        builder.add_entry(entry)
        if not entry.exists:
            builder.add_warning(
                code="missing_path",
                message="Referenced path does not exist",
                path=entry.path,
                source=source,
            )
            return

        if recurse and Path(entry.path).suffix.lower() in _YAML_SUFFIXES:
            self._discover_manifest((self._policy.repo_root / entry.path).resolve(strict=False), builder, source=source)


def _iter_convention_refs(raw: Mapping[str, Any]) -> tuple[str, ...]:
    refs: list[str] = []
    for value in _iter_string_or_strings(raw.get("convention")):
        if value.strip().lower() not in _BUILT_IN_CONVENTIONS:
            refs.append(value)
    for value in _iter_string_or_strings(raw.get("conventions")):
        if value.strip().lower() not in _BUILT_IN_CONVENTIONS:
            refs.append(value)
    return tuple(refs)


def _iter_string_or_strings(value: object) -> tuple[str, ...]:
    if isinstance(value, str) and value.strip():
        return (value.strip(),)
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return tuple(str(item).strip() for item in value if isinstance(item, str) and item.strip())
    return ()


def _iter_depends_on_items(value: object) -> tuple[object, ...]:
    items: list[object] = []
    _collect_depends_on(value, items)
    return tuple(items)


def _collect_depends_on(value: object, items: list[object]) -> None:
    if isinstance(value, Mapping):
        if "depends_on" in value:
            raw = value.get("depends_on")
            if isinstance(raw, Sequence) and not isinstance(raw, str | bytes):
                items.extend(raw)
            elif raw is not None:
                items.append(raw)
        for child in value.values():
            _collect_depends_on(child, items)
        return
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        for child in value:
            _collect_depends_on(child, items)


def infer_workload_root(raw_manifest: str, repo_root: Path, raw_workload_root: object) -> Path:
    if raw_workload_root:
        text = str(raw_workload_root).strip()
        if text == ".":
            return repo_root
        SparsePathPolicy._validate_relative_text(text, source="--workload-root")  # noqa: SLF001
        return (repo_root / text).resolve(strict=False)

    try:
        SparsePathPolicy._validate_relative_text(raw_manifest, source="path")  # noqa: SLF001
    except SparsePathValidationError:
        return repo_root

    parts = Path(raw_manifest).parts
    if "manifests" not in parts:
        return repo_root
    index = parts.index("manifests")
    if index == 0:
        return repo_root
    return (repo_root / Path(*parts[:index])).resolve(strict=False)


def _entry(
    resolved: ResolvedSparsePath,
    *,
    kind: str,
    source: str,
    required: bool,
    reason: str,
) -> SparsePathEntry:
    return SparsePathEntry(
        path=resolved.path,
        kind=kind,
        source=source,
        required=required,
        exists=resolved.exists,
        is_dir=resolved.is_dir,
        reason=reason,
    )


__all__ = [
    "ManifestSparsePathDiscovery",
    "ManifestSparsePathReader",
    "ResolvedSparsePath",
    "SparsePathEntry",
    "SparsePathIssue",
    "SparsePathPolicy",
    "SparsePathReport",
    "SparsePathValidationError",
    "infer_workload_root",
]
