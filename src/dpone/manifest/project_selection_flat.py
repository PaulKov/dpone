"""Legacy flat-catalog project selection implementation."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.manifest.authoring import AuthoringCompilationError
from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import ConfinedFileError, project_relative_path, read_confined_file
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.project_config import ProjectConfigSnapshot
from dpone.manifest.project_selection_contracts import (
    LoadedProjectSelectionGraph,
    ProjectCheckedSource,
    ProjectSelectionDag,
)
from dpone.manifest.project_selection_projection import optional_text, selection_node, text_tuple
from dpone.manifest.selection import SelectionError, SelectionGraph, SelectionNode

if TYPE_CHECKING:
    from dpone.manifest.authoring import AuthoringCompiler
    from dpone.manifest.project_selection_contracts import ProjectDagParser, ProjectPipelinePathResolver

_MAX_PROJECT_WORKLOADS = 1000
_MAX_PROJECT_FILES = 1000
_MAX_PROJECT_EDGES = 5000
_MAX_FILE_BYTES = 1024 * 1024
_MAX_TOTAL_BYTES = 16 * 1024 * 1024


class FlatProjectSelectionLoader:
    """Compile one legacy flat project without leaking catalog policy into layout dispatch."""

    def __init__(
        self,
        *,
        root: Path,
        dag_parser: ProjectDagParser,
        pipeline_path_resolver: ProjectPipelinePathResolver,
        compiler: AuthoringCompiler,
    ) -> None:
        self._root = root
        self._dag_parser = dag_parser
        self._pipeline_path_resolver = pipeline_path_resolver
        self._compiler = compiler

    def load(
        self,
        target: str | Path,
        *,
        config_snapshot: ProjectConfigSnapshot | None,
    ) -> LoadedProjectSelectionGraph:
        pipeline = self._pipeline_target(target)
        if pipeline is None:
            return self._load_project(config_snapshot)
        checked = self._compile_source(_relative(self._root, pipeline), expected_id=None)
        node = selection_node(checked, catalog_domain=None, catalog_owner=None, tags=(), groups=())
        graph = SelectionGraph.build(nodes=(node,), edges=())
        declaration, issues = self._dag_parser(
            node.node_id,
            {
                "workloads": [node.node_id],
                "schedule": None,
                "start_date": "2026-01-01",
                "catchup": False,
            },
        )
        assert declaration is not None and not issues
        consumed = self._project_config_input(config_snapshot, required=False)
        self._record_checked_source(consumed, checked)
        return LoadedProjectSelectionGraph(
            graph,
            {node.node_id: checked},
            dict(sorted(consumed.items())),
            (ProjectSelectionDag(declaration, node.domain, (node.node_id,)),),
        )

    def _load_project(
        self,
        config_snapshot: ProjectConfigSnapshot | None,
    ) -> LoadedProjectSelectionGraph:
        domain_dir = self._root / "domains"
        if not domain_dir.is_dir():
            raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Project domains directory was not found.")
        domain_paths = sorted(path for path in domain_dir.iterdir() if path.suffix in {".yaml", ".yml"})
        if not domain_paths or len(domain_paths) > _MAX_PROJECT_FILES:
            raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Project domain catalog is empty or too large.")
        nodes: list[SelectionNode] = []
        edges: set[tuple[str, str]] = set()
        checked_sources: dict[str, ProjectCheckedSource] = {}
        consumed = self._project_config_input(config_snapshot, required=True)
        dags: list[ProjectSelectionDag] = []
        total_bytes = (self._root / "dpone.yaml").stat().st_size
        for domain_path in domain_paths:
            relative_domain = _relative(self._root, domain_path)
            payload, digest, byte_count = self._load_mapping(relative_domain)
            total_bytes += byte_count
            consumed[relative_domain] = digest
            domain, catalog_nodes, catalog_edges = _domain_entries(payload, path=relative_domain)
            if len(nodes) + len(catalog_nodes) > _MAX_PROJECT_WORKLOADS:
                raise SelectionError(
                    "DPONE_SELECTION_LIMIT_EXCEEDED",
                    "Project selection workload budget was exceeded.",
                )
            groups = _workflow_groups(payload)
            dags.extend(_domain_dags(payload, domain=domain, groups=groups, dag_parser=self._dag_parser))
            for workload_id, raw_workload in catalog_nodes:
                source_ref = _required_text(raw_workload, "authoring_source", code="DPONE_SELECTION_CATALOG_INVALID")
                checked = self._compile_source(source_ref, expected_id=workload_id)
                if workload_id in checked_sources:
                    raise SelectionError(
                        "DPONE_SELECTION_DUPLICATE_WORKLOAD",
                        f"Workload id is declared more than once: {workload_id}.",
                    )
                total_bytes += self._record_checked_source(consumed, checked)
                if total_bytes > _MAX_TOTAL_BYTES:
                    raise SelectionError(
                        "DPONE_SELECTION_LIMIT_EXCEEDED",
                        "Project selection input budget was exceeded.",
                    )
                catalog_owner = optional_text(raw_workload.get("owner"))
                tags = text_tuple(raw_workload.get("tags", ()), field="tags")
                member_groups = tuple(sorted(name for name, members in groups.items() if workload_id in members))
                node = selection_node(
                    checked,
                    catalog_domain=domain,
                    catalog_owner=catalog_owner,
                    tags=tags,
                    groups=member_groups,
                )
                nodes.append(node)
                checked_sources[workload_id] = checked
                edges.update((dependency, workload_id) for dependency in _dependency_ids(raw_workload))
            edges.update(catalog_edges)
        if len(edges) > _MAX_PROJECT_EDGES:
            raise SelectionError("DPONE_SELECTION_LIMIT_EXCEEDED", "Project selection edge budget was exceeded.")
        graph = SelectionGraph.build(nodes=tuple(nodes), edges=tuple(edges))
        _validate_dags(dags, known_ids=set(checked_sources))
        return LoadedProjectSelectionGraph(
            graph,
            checked_sources,
            dict(sorted(consumed.items())),
            tuple(sorted(dags, key=lambda item: item.declaration.dag_id)),
        )

    def _compile_source(self, relative_path: str, *, expected_id: str | None) -> ProjectCheckedSource:
        payload, digest, _ = self._load_mapping(relative_path)
        path = self._root / relative_path
        try:
            compilation = self._compiler.compile(payload, source_path=path, project_root=self._root)
        except (AuthoringCompilationError, ManifestConfigurationError, OSError, ValueError) as exc:
            raise SelectionError(
                "DPONE_SELECTION_CATALOG_INVALID",
                f"Workload authoring source could not be compiled: {relative_path}.",
            ) from exc
        workload_id = _metadata_id(payload)
        if expected_id is not None and workload_id != expected_id:
            raise SelectionError(
                "DPONE_SELECTION_CATALOG_INVALID",
                f"Domain workload id {expected_id} does not match authoring metadata id {workload_id}.",
            )
        return ProjectCheckedSource(workload_id, path, relative_path, payload, compilation, digest)

    def _load_mapping(self, relative_path: str) -> tuple[Mapping[str, Any], str, int]:
        try:
            safe_path = project_relative_path(self._root, Path(relative_path))
            content = read_confined_file(self._root, safe_path, max_bytes=_MAX_FILE_BYTES)
            payload = load_bounded_yaml(content, limits=BoundedYamlLimits(max_bytes=_MAX_FILE_BYTES))
            digest = "sha256:" + hashlib.sha256(content).hexdigest()
        except (ConfinedFileError, BoundedYamlError) as exc:
            raise SelectionError(
                "DPONE_SELECTION_CATALOG_INVALID",
                "Project selection input could not be read safely.",
            ) from exc
        if not isinstance(payload, Mapping):
            raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Project selection input must be a YAML object.")
        return payload, digest, len(content)

    def _project_config_input(
        self,
        snapshot: ProjectConfigSnapshot | None,
        *,
        required: bool,
    ) -> dict[str, str]:
        if snapshot is None:
            if required:
                raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Project dpone.yaml was not found.")
            return {}
        return {"dpone.yaml": snapshot.sha256}

    def _record_checked_source(
        self,
        consumed: dict[str, str],
        checked: ProjectCheckedSource,
    ) -> int:
        inputs = [(checked.source_label, checked.source_sha256)]
        inputs.extend((dependency.path, dependency.sha256) for dependency in checked.compilation.dependencies)
        total_bytes = 0
        for path, digest in inputs:
            existing = consumed.get(path)
            if existing is not None and existing != digest:
                raise SelectionError(
                    "DPONE_SELECTION_STATE_CHANGED",
                    "The same selection input was observed with different content.",
                )
            consumed[path] = digest
            try:
                total_bytes += (self._root / project_relative_path(self._root, Path(path))).stat().st_size
            except (ConfinedFileError, OSError) as exc:
                raise SelectionError(
                    "DPONE_SELECTION_CATALOG_INVALID",
                    "Project selection dependency could not be measured safely.",
                ) from exc
        return total_bytes

    def _pipeline_target(self, target: str | Path) -> Path | None:
        raw = Path(target)
        absolute = raw if raw.is_absolute() else self._root / raw
        if absolute.resolve(strict=False) == self._root:
            return None
        candidate = self._pipeline_path_resolver(self._root, target)
        _relative(self._root, candidate)
        if not candidate.is_file():
            raise SelectionError(
                "DPONE_SELECTION_CATALOG_INVALID",
                "Project selection target must be the configured project root or one pipeline source.",
            )
        return candidate


def _domain_entries(
    payload: Mapping[str, Any],
    *,
    path: str,
) -> tuple[str, tuple[tuple[str, Mapping[str, Any]], ...], tuple[tuple[str, str], ...]]:
    if payload.get("schema") != "dpone.domain-catalog.v1":
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", f"Domain catalog schema is invalid: {path}.")
    domain = _required_text(payload, "domain", code="DPONE_SELECTION_CATALOG_INVALID")
    raw_workloads = payload.get("workloads")
    if not isinstance(raw_workloads, Mapping):
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", f"Domain workloads must be a mapping: {path}.")
    entries: list[tuple[str, Mapping[str, Any]]] = []
    for raw_id, raw_workload in raw_workloads.items():
        workload_id = str(raw_id).strip()
        if not workload_id or not isinstance(raw_workload, Mapping):
            raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", f"Domain workload entry is invalid: {path}.")
        entries.append((workload_id, raw_workload))
    return domain, tuple(sorted(entries)), _dag_wiring_edges(payload)


def _validate_dags(dags: list[ProjectSelectionDag], *, known_ids: set[str]) -> None:
    dag_ids: set[str] = set()
    for dag in dags:
        if dag.declaration.dag_id in dag_ids:
            raise SelectionError(
                "DPONE_SELECTION_CATALOG_INVALID",
                f"DAG id is declared more than once: {dag.declaration.dag_id}.",
            )
        dag_ids.add(dag.declaration.dag_id)
        unknown = sorted(set(dag.workload_ids) - known_ids)
        if unknown:
            raise SelectionError(
                "DPONE_SELECTION_CATALOG_INVALID",
                f"DAG {dag.declaration.dag_id} references unknown workloads: {', '.join(unknown)}.",
            )


def _workflow_groups(payload: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    raw_groups = payload.get("workflow_groups") or payload.get("workload_groups") or payload.get("groups") or {}
    if not isinstance(raw_groups, Mapping):
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Workflow groups must be a mapping.")
    groups: dict[str, tuple[str, ...]] = {}
    for raw_name, raw_group in raw_groups.items():
        values: Any = raw_group
        if isinstance(raw_group, Mapping):
            values = raw_group.get("workloads") or raw_group.get("workload_ids") or ()
        groups[str(raw_name)] = text_tuple(values, field="workflow group")
    return groups


def _dag_wiring_edges(payload: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    raw_dags = payload.get("dags") or {}
    if not isinstance(raw_dags, Mapping):
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Domain dags must be a mapping.")
    edges: set[tuple[str, str]] = set()
    for raw_dag in raw_dags.values():
        if not isinstance(raw_dag, Mapping):
            raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Domain DAG entry must be an object.")
        wiring = raw_dag.get("wiring") or {}
        if not isinstance(wiring, Mapping):
            raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "DAG wiring must be an object.")
        dependencies = wiring.get("dependencies") or {}
        if not isinstance(dependencies, Mapping):
            raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "DAG wiring dependencies must be a mapping.")
        for downstream, upstreams in dependencies.items():
            for upstream in text_tuple(upstreams, field="DAG wiring dependencies"):
                edges.add((upstream, str(downstream)))
    return tuple(sorted(edges))


def _domain_dags(
    payload: Mapping[str, Any],
    *,
    domain: str,
    groups: Mapping[str, tuple[str, ...]],
    dag_parser: ProjectDagParser,
) -> tuple[ProjectSelectionDag, ...]:
    raw_dags = payload.get("dags") or {}
    if not isinstance(raw_dags, Mapping):
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Domain dags must be a mapping.")
    dags: list[ProjectSelectionDag] = []
    for raw_id, raw_dag in raw_dags.items():
        dag_id = str(raw_id).strip()
        declaration, issues = dag_parser(dag_id, raw_dag)
        if declaration is None or issues:
            message = "; ".join(issue.message for issue in issues[:5])
            raise SelectionError(
                "DPONE_SELECTION_CATALOG_INVALID",
                f"DAG declaration {dag_id} is invalid: {message}",
            )
        if declaration.workloads is not None:
            workload_ids = declaration.workloads
        else:
            assert declaration.group is not None
            if declaration.group not in groups:
                raise SelectionError(
                    "DPONE_SELECTION_CATALOG_INVALID",
                    f"DAG {dag_id} references unknown workflow group {declaration.group}.",
                )
            workload_ids = groups[declaration.group]
        dags.append(ProjectSelectionDag(declaration, domain, tuple(sorted(set(workload_ids)))))
    return tuple(dags)


def _dependency_ids(payload: Mapping[str, Any]) -> tuple[str, ...]:
    return text_tuple(payload.get("depends_on", ()), field="workload depends_on")


def _metadata_id(payload: Mapping[str, Any]) -> str:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Authoring metadata is required.")
    return _required_text(metadata, "id", code="DPONE_SELECTION_CATALOG_INVALID")


def _required_text(payload: Mapping[str, Any], key: str, *, code: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise SelectionError(code, f"{key} must be a non-empty string.")
    return value.strip()


def _relative(root: Path, path: Path) -> str:
    try:
        return path.resolve(strict=False).relative_to(root).as_posix()
    except ValueError as exc:
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Project target is outside the project root.") from exc


__all__ = ["FlatProjectSelectionLoader"]
