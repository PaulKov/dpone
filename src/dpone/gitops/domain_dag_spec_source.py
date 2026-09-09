"""Resolve dag-spec authoring entries from discovery IR or legacy catalogs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dpone.gitops.airflow_dag_spec_builder_support import (
    DagSpecDeclaration,
    DomainDocument,
    dags_block,
    optional_str,
    parse_dag_declaration,
    workflow_groups,
)
from dpone.gitops.airflow_dag_spec_policy import GitOpsWorkloadCatalogIssue, dag_spec_issue
from dpone.gitops.workload_catalog_models import issue
from dpone.manifest.domain_catalog_membership import membership_catalog_snippet
from dpone.manifest.project_config import ProjectConfigError, ProjectLayout, load_project_layout
from dpone.manifest.project_discovery import ProjectDiscoveryService

MEMBERSHIP_MISSING_WARNING_CODE = "workload_catalog_membership_missing"


@dataclass(frozen=True, slots=True)
class DiscoveredAirflowPipeline:
    """One Airflow-eligible pipeline discovered outside the resolved workload set."""

    pipeline_id: str
    source_label: str
    domain: str


@dataclass(frozen=True, slots=True)
class DagSpecSourceEntry:
    """One validated DAG declaration ready for membership resolution."""

    declaration: DagSpecDeclaration
    domain: str | None
    source_path: str
    groups: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True, slots=True)
class DagSpecSourceResolution:
    """Dag-spec authoring entries plus the discovery membership audit surface.

    ``discovered_airflow_pipelines`` maps every discovered Airflow-eligible
    pipeline id to its discovery metadata; ``None`` means the resolution ran
    without domain-first discovery (legacy catalog fallback) and no membership
    audit is possible.
    """

    entries: tuple[DagSpecSourceEntry, ...]
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...]
    discovered_airflow_pipelines: Mapping[str, DiscoveredAirflowPipeline] | None = None


def load_dag_spec_source_entries(
    *,
    repo_root: Path,
    workload_set: str,
    domain_documents: tuple[DomainDocument, ...],
) -> tuple[tuple[DagSpecSourceEntry, ...], tuple[GitOpsWorkloadCatalogIssue, ...]]:
    """Compatibility projection of :func:`resolve_dag_spec_sources`."""

    resolution = resolve_dag_spec_sources(
        repo_root=repo_root,
        workload_set=workload_set,
        domain_documents=domain_documents,
    )
    return resolution.entries, resolution.blockers


def resolve_dag_spec_sources(
    *,
    repo_root: Path,
    workload_set: str,
    domain_documents: tuple[DomainDocument, ...],
) -> DagSpecSourceResolution:
    """Prefer domain-first discovery IR; fall back to catalog ``dags:`` blocks."""

    try:
        layout, _ = load_project_layout(repo_root)
    except (ProjectConfigError, OSError):
        return _catalog_documents_resolution(domain_documents)
    if not layout.is_domain_first:
        return _catalog_documents_resolution(domain_documents)
    snapshot = ProjectDiscoveryService(repo_root).discover(layout=layout)
    discovered = {
        workload.pipeline_id: DiscoveredAirflowPipeline(
            pipeline_id=workload.pipeline_id,
            source_label=workload.checked_source.source_label,
            domain=workload.domain,
        )
        for workload in snapshot.workloads
        if workload.airflow_enabled
    }
    blockers = tuple(
        dag_spec_issue(
            code=item.code,
            dag_id=item.path or item.domain or "*",
            message=item.message,
            producer="dpone gitops airflow dag-spec",
        )
        for item in snapshot.issues
    )
    if snapshot.domain_dags:
        # Keep valid IR entries even when discovery also reports blockers so
        # reconcile/explain can still resolve known DAGs; blockers remain fail-closed.
        entries = tuple(
            DagSpecSourceEntry(
                declaration=item.declaration,
                domain=item.domain,
                source_path=item.path,
                groups={},
            )
            for item in snapshot.domain_dags
        )
        return DagSpecSourceResolution(
            entries=entries,
            blockers=blockers,
            discovered_airflow_pipelines=discovered,
        )
    if blockers:
        return DagSpecSourceResolution(
            entries=(),
            blockers=blockers,
            discovered_airflow_pipelines=discovered,
        )
    # Empty discovery IR: fall back to workload-set domain catalog documents so
    # catalog-only dual-read/migration repos still reconcile before colocated cutover.
    return _catalog_documents_resolution(domain_documents, discovered=discovered)


def membership_gap_warnings(
    discovered_airflow_pipelines: Mapping[str, DiscoveredAirflowPipeline] | None,
    catalog_workload_ids: frozenset[str] | set[str],
    *,
    layout: ProjectLayout | None = None,
) -> tuple[GitOpsWorkloadCatalogIssue, ...]:
    """Warn about discovered Airflow-eligible pipelines missing catalog membership.

    A pipeline that exists on disk but is absent from the resolved workload set
    would otherwise disappear from reconcile/pack without any signal, while a
    DAG reference to it fails closed with ``dag_spec_workload_unknown``.
    """

    if not discovered_airflow_pipelines:
        return ()
    return tuple(
        issue(
            code=MEMBERSHIP_MISSING_WARNING_CODE,
            message=_membership_gap_message(pipeline, layout=layout),
            path=pipeline.source_label,
        )
        for pipeline in sorted(
            discovered_airflow_pipelines.values(),
            key=lambda item: item.pipeline_id,
        )
        if pipeline.pipeline_id not in catalog_workload_ids
    )


def _membership_gap_message(
    pipeline: DiscoveredAirflowPipeline,
    *,
    layout: ProjectLayout | None,
) -> str:
    if layout is not None and layout.is_domain_first:
        catalog_rel, snippet = membership_catalog_snippet(
            layout,
            domain=pipeline.domain,
            pipeline_id=pipeline.pipeline_id,
            pipeline_path=Path(pipeline.source_label),
        )
    else:
        catalog_rel = f".dpone/config/domains/{pipeline.domain}.yaml"
        snippet = f"  {pipeline.pipeline_id}:\n    manifest: ../../../{pipeline.source_label}"
    return (
        f"Discovered Airflow-eligible pipeline {pipeline.pipeline_id!r} is not part "
        f"of the resolved workload set of catalog {catalog_rel!r}; reconcile will "
        f"not pack it until membership is added under workloads:\n{snippet}"
    )


def _catalog_documents_resolution(
    domain_documents: tuple[DomainDocument, ...],
    *,
    discovered: Mapping[str, DiscoveredAirflowPipeline] | None = None,
) -> DagSpecSourceResolution:
    entries, blockers = _entries_from_catalog_documents(domain_documents)
    return DagSpecSourceResolution(
        entries=entries,
        blockers=blockers,
        discovered_airflow_pipelines=discovered,
    )


def _entries_from_catalog_documents(
    domain_documents: tuple[DomainDocument, ...],
) -> tuple[tuple[DagSpecSourceEntry, ...], tuple[GitOpsWorkloadCatalogIssue, ...]]:
    entries: list[DagSpecSourceEntry] = []
    blockers: list[GitOpsWorkloadCatalogIssue] = []
    for domain_doc in domain_documents:
        domain = optional_str(domain_doc.payload.get("domain"))
        groups = workflow_groups(domain_doc.payload)
        for dag_id, raw_entry in dags_block(domain_doc.payload).items():
            declaration, declaration_issues = parse_dag_declaration(dag_id, raw_entry)
            if declaration is None:
                blockers.extend(declaration_issues)
                continue
            entries.append(
                DagSpecSourceEntry(
                    declaration=declaration,
                    domain=domain,
                    source_path=domain_doc.path,
                    groups=groups,
                )
            )
    return tuple(entries), tuple(blockers)


__all__ = [
    "MEMBERSHIP_MISSING_WARNING_CODE",
    "DiscoveredAirflowPipeline",
    "DagSpecSourceEntry",
    "DagSpecSourceResolution",
    "load_dag_spec_source_entries",
    "membership_gap_warnings",
    "resolve_dag_spec_sources",
]
