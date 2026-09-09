"""Build `gitops.airflow_dag_spec` artifacts from domain catalog ``dags:`` blocks.

The builder walks every domain document of a workload-set, validates the
authoring block (:func:`dpone.gitops.airflow_dag_spec.parse_dag_declaration`),
resolves workload membership (explicit list, ``workflow_groups`` reference, or
``dpone.batch.v1`` per-process expansion), merges the dependency layers
(:mod:`dpone.gitops.airflow_dag_spec_deps`) and emits one fully explicit
dag-spec per DAG for `.dpone/gitops/airflow/_dags/<dag_id>.dag-spec.json`.

Invariants enforced here:

- unknown workload id or workflow group -> build blocker;
- ``schedule.assets`` URIs must be produced by a declared or build-inferred
  workload outlet, or be marked ``external: true``;
- dependency cycles and invalid declarations block the build with an
  explanation instead of emitting a partial artifact.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue, GitOpsWorkloadCatalogReport


from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.gitops.airflow_asset_cross_dag import (
    cross_dag_build_warnings,
    cross_dag_cycle_blockers,
    enrich_cross_dag_specs,
)
from dpone.gitops.airflow_asset_graph import AssetGraphReport, build_asset_graph
from dpone.gitops.airflow_asset_partition_plan import asset_partition_plan_blockers
from dpone.gitops.airflow_dag_spec import (
    DAG_SPEC_ARTIFACT_DIR,
    DagSpecDeclaration,
    DagSpecNode,
    GitOpsAirflowDagSpec,
)
from dpone.gitops.airflow_dag_spec_build_models import GitOpsAirflowDagSpecBuildReport
from dpone.gitops.airflow_dag_spec_builder_support import (
    BATCH_MANIFEST_KIND,
    AirflowDagSpecInputError,
    DomainDocument,
    build_workload_nodes,
    dag_spec_blocker,
    load_yaml,
    mapping,
    materialized_outlet_uris,
    member_workload_ids,
    schedule_asset_blockers,
    visible_task_budget_issues,
)
from dpone.gitops.airflow_dag_spec_deps import (
    DagMembership,
    EdgeMergeResult,
    default_edge_providers,
    merge_dag_edges,
)
from dpone.gitops.domain_dag_spec_source import (
    load_dag_spec_source_entries,
    membership_gap_warnings,
    resolve_dag_spec_sources,
)
from dpone.gitops.workload_catalog import WorkloadCatalogResolver
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.manifest.project_config import ProjectConfigError, load_project_layout


class AirflowDagSpecBuildError(ValueError):
    """A known workload-set input problem prevents DAG-spec planning."""


@dataclass(frozen=True, slots=True)
class ResolvedDagSpecContext:
    """One resolved dag-spec membership/merge context for explain tooling."""

    dag_id: str
    repo_root: Path
    membership: DagMembership
    merge: EdgeMergeResult
    spec: GitOpsAirflowDagSpec | None
    domain: str | None
    source_path: str


class AirflowDagSpecBuilder:
    """Resolve ``dags:`` declarations into scheduler-static dag-spec artifacts."""

    def __init__(self, *, repo_root: Path, manifest_loader: ManifestLoaderRouter | None = None) -> None:
        self._repo_root = repo_root.resolve(strict=False)
        self._loader = manifest_loader or ManifestLoaderRouter()

    def build(
        self,
        *,
        workload_set: str,
        env: str = "dev",
        mssql_registry: object | None = None,
    ) -> GitOpsAirflowDagSpecBuildReport:
        from dpone.gitops.airflow_asset_uri import ResolvedMssqlAssetRegistry

        catalog = WorkloadCatalogResolver(repo_root=self._repo_root).resolve(workload_set, env=env)
        registry = mssql_registry if isinstance(mssql_registry, ResolvedMssqlAssetRegistry) else None
        asset_graph = build_asset_graph(
            catalog.workloads,
            repo_root=self._repo_root,
            manifest_loader=self._loader,
            env=env,
            mssql_registry=registry,
        )
        outlet_uris = materialized_outlet_uris(catalog, asset_graph)
        specs: list[GitOpsAirflowDagSpec] = []
        warnings: list[GitOpsWorkloadCatalogIssue] = [*catalog.warnings, *asset_graph.warnings]
        blockers: list[GitOpsWorkloadCatalogIssue] = [*catalog.blockers, *asset_graph.blockers]
        try:
            domain_documents = self._domain_documents(workload_set)
        except AirflowDagSpecInputError as exc:
            raise AirflowDagSpecBuildError(str(exc)) from exc
        source_resolution = resolve_dag_spec_sources(
            repo_root=self._repo_root,
            workload_set=workload_set,
            domain_documents=domain_documents,
        )
        blockers.extend(source_resolution.blockers)
        layout = None
        try:
            layout, _ = load_project_layout(self._repo_root)
        except (ProjectConfigError, OSError):
            layout = None
        warnings.extend(
            membership_gap_warnings(
                source_resolution.discovered_airflow_pipelines,
                {workload.workload_id for workload in catalog.workloads},
                layout=layout,
            )
        )
        for entry in source_resolution.entries:
            spec, spec_warnings, spec_blockers = self._build_spec(
                declaration=entry.declaration,
                domain=entry.domain,
                source_path=entry.source_path,
                catalog=catalog,
                groups=entry.groups,
                outlet_uris=outlet_uris,
                asset_graph=asset_graph,
            )
            warnings.extend(spec_warnings)
            blockers.extend(spec_blockers)
            if spec is not None:
                specs.append(spec)
        sorted_specs = tuple(sorted(specs, key=lambda spec: spec.dag_id))
        sorted_specs = enrich_cross_dag_specs(asset_graph=asset_graph, specs=sorted_specs, catalog=catalog)
        blockers.extend(asset_partition_plan_blockers(asset_graph=asset_graph, specs=sorted_specs))
        blockers.extend(cross_dag_cycle_blockers(asset_graph=asset_graph, specs=sorted_specs))
        warnings.extend(cross_dag_build_warnings(asset_graph=asset_graph, specs=sorted_specs, catalog=catalog))
        return GitOpsAirflowDagSpecBuildReport(
            workload_set=str(workload_set),
            env=env,
            specs=sorted_specs,
            warnings=tuple(warnings),
            blockers=tuple(blockers),
        )

    def build_and_write(
        self,
        *,
        workload_set: str,
        env: str = "dev",
        artifact_dir: str | Path = DAG_SPEC_ARTIFACT_DIR,
    ) -> GitOpsAirflowDagSpecBuildReport:
        """Compatibility facade over the confined DAG-spec artifact writer."""

        from dpone.gitops.airflow_dag_spec_artifacts import AirflowDagSpecArtifactWriter

        return AirflowDagSpecArtifactWriter(repo_root=self._repo_root).write(
            self.build(workload_set=workload_set, env=env),
            artifact_dir=artifact_dir,
        )

    def resolve_context(
        self,
        *,
        workload_set: str,
        dag_id: str,
        env: str = "dev",
    ) -> tuple[ResolvedDagSpecContext | None, tuple[GitOpsWorkloadCatalogIssue, ...]]:
        """Resolve one ``dags:`` entry into membership and merged edges."""

        catalog = WorkloadCatalogResolver(repo_root=self._repo_root).resolve(workload_set, env=env)
        asset_graph = build_asset_graph(
            catalog.workloads,
            repo_root=self._repo_root,
            manifest_loader=self._loader,
            env=env,
        )
        blockers: list[GitOpsWorkloadCatalogIssue] = [*catalog.blockers, *asset_graph.blockers]
        outlet_uris = materialized_outlet_uris(catalog, asset_graph)
        domain_documents = self._domain_documents(workload_set)
        source_entries, source_blockers = load_dag_spec_source_entries(
            repo_root=self._repo_root,
            workload_set=workload_set,
            domain_documents=domain_documents,
        )
        blockers.extend(source_blockers)
        for entry in source_entries:
            if entry.declaration.dag_id != dag_id:
                continue
            membership, _membership_warnings, membership_blockers = self._build_membership(
                declaration=entry.declaration,
                catalog=catalog,
                groups=entry.groups,
                outlet_uris=outlet_uris,
            )
            if membership is None:
                return None, (*blockers, *membership_blockers)
            merge = merge_dag_edges(
                membership,
                default_edge_providers(repo_root=self._repo_root, asset_graph=asset_graph),
            )
            spec = None
            if not merge.blockers:
                spec = GitOpsAirflowDagSpec(
                    declaration=entry.declaration,
                    domain=entry.domain,
                    source_path=entry.source_path,
                    nodes=membership.nodes,
                    edges=merge.edges,
                    topological_order=merge.topological_order,
                )
            return (
                ResolvedDagSpecContext(
                    dag_id=dag_id,
                    repo_root=self._repo_root,
                    membership=membership,
                    merge=merge,
                    spec=spec,
                    domain=entry.domain,
                    source_path=entry.source_path,
                ),
                (),
            )
        missing = dag_spec_blocker(
            dag_id, "dag_spec_not_found", f"No dags: entry named {dag_id!r} in workload-set {workload_set!r}"
        )
        return None, (*blockers, missing)

    def _build_spec(
        self,
        *,
        declaration: DagSpecDeclaration,
        domain: str | None,
        source_path: str,
        catalog: GitOpsWorkloadCatalogReport,
        groups: Mapping[str, tuple[str, ...]],
        outlet_uris: frozenset[str],
        asset_graph: AssetGraphReport,
    ) -> tuple[GitOpsAirflowDagSpec | None, list[GitOpsWorkloadCatalogIssue], list[GitOpsWorkloadCatalogIssue]]:
        membership, warnings, blockers = self._build_membership(
            declaration=declaration,
            catalog=catalog,
            groups=groups,
            outlet_uris=outlet_uris,
        )
        if membership is None:
            return None, warnings, blockers
        merge = merge_dag_edges(
            membership,
            default_edge_providers(repo_root=self._repo_root, asset_graph=asset_graph),
        )
        warnings.extend(merge.warnings)
        if merge.blockers:
            return None, warnings, [*merge.blockers]
        spec = GitOpsAirflowDagSpec(
            declaration=declaration,
            domain=domain,
            source_path=source_path,
            nodes=membership.nodes,
            edges=merge.edges,
            topological_order=merge.topological_order,
        )
        return spec, warnings, []

    def _build_membership(
        self,
        *,
        declaration: DagSpecDeclaration,
        catalog: GitOpsWorkloadCatalogReport,
        groups: Mapping[str, tuple[str, ...]],
        outlet_uris: frozenset[str],
    ) -> tuple[DagMembership | None, list[GitOpsWorkloadCatalogIssue], list[GitOpsWorkloadCatalogIssue]]:
        warnings: list[GitOpsWorkloadCatalogIssue] = []
        blockers: list[GitOpsWorkloadCatalogIssue] = list(schedule_asset_blockers(declaration, outlet_uris))
        workload_ids, membership_blockers = member_workload_ids(declaration, groups)
        blockers.extend(membership_blockers)
        nodes: list[DagSpecNode] = []
        dependencies: dict[str, tuple[Any, ...]] = {}
        manifest_index: dict[str, list[DagSpecNode]] = {}
        for workload_id in workload_ids:
            try:
                workload = catalog.by_id(workload_id)
            except KeyError:
                blockers.append(
                    dag_spec_blocker(
                        declaration.dag_id, "dag_spec_workload_unknown", f"Unknown workload id: {workload_id}"
                    )
                )
                continue
            workload_nodes, node_blockers = build_workload_nodes(
                loader=self._loader,
                repo_root=self._repo_root,
                declaration=declaration,
                workload=workload,
                dependencies=dependencies,
            )
            blockers.extend(node_blockers)
            nodes.extend(workload_nodes)
            manifest_index.setdefault(workload.manifest, []).extend(workload_nodes)
        task_warnings, task_blockers = visible_task_budget_issues(
            dag_id=declaration.dag_id,
            nodes=tuple(nodes),
            budget=declaration.wiring.visible_task_budget,
        )
        warnings.extend(task_warnings)
        blockers.extend(task_blockers)
        if blockers:
            return None, warnings, blockers
        membership = DagMembership(
            declaration=declaration,
            nodes=tuple(nodes),
            dependencies=dependencies,
            manifest_index={path: tuple(members) for path, members in manifest_index.items()},
            group_index=groups,
        )
        return membership, warnings, []

    def _domain_documents(self, workload_set: str) -> tuple[DomainDocument, ...]:
        root_path = self._resolve_input_path(workload_set)
        root_payload = mapping(load_yaml(root_path).get("gitops"))
        documents: list[DomainDocument] = []
        for include in root_payload.get("includes") or ():
            include_map = mapping(include)
            raw_path = include_map.get("path")
            if not raw_path:
                continue
            for path in sorted(root_path.parent.glob(str(raw_path))):
                resolved_path = path.resolve(strict=False)
                try:
                    relative_path = resolved_path.relative_to(self._repo_root).as_posix()
                except ValueError as exc:
                    raise AirflowDagSpecBuildError("dag_spec_domain_path_outside_repo") from exc
                documents.append(DomainDocument(path=relative_path, payload=load_yaml(path)))
        return tuple(documents)

    def _resolve_input_path(self, raw_path: str | Path) -> Path:
        path = Path(raw_path)
        return (path if path.is_absolute() else self._repo_root / path).resolve(strict=False)


__all__ = [
    "AirflowDagSpecBuildError",
    "AirflowDagSpecBuilder",
    "BATCH_MANIFEST_KIND",
    "GitOpsAirflowDagSpecBuildReport",
    "ResolvedDagSpecContext",
]
