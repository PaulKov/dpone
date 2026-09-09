"""Canonical pack outlet materialization for compact Airflow packs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from dpone.gitops.airflow_asset_graph import (
    INFERRED_OUTLET_PROVENANCE,
    build_asset_graph,
)
from dpone.gitops.airflow_asset_graph_lineage import declared_uris
from dpone.gitops.airflow_asset_uri import (
    MSSQL_ASSET_URI_INVALID,
    ResolvedMssqlAssetRegistry,
    canonicalize_declared_mssql_uri,
    resolve_explicit_mssql_uri,
    resolve_mssql_asset_registry,
)
from dpone.gitops.airflow_compact_pack_logical_outlets import (
    infer_logical_outlet_entries,
    logical_declared_outlet_entries,
)
from dpone.gitops.airflow_mssql_registry_snapshot import require_mssql_registry_env_match
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue, GitOpsWorkloadDefinition, issue
from dpone.manifest.loader import ManifestLoaderRouter

_OUTLET_SOURCE = "dpone gitops airflow compact-pack"
OutletBinding = Literal["physical", "logical"]


@dataclass(frozen=True, slots=True)
class InferredOutletReport:
    """Structured pack outlets with asset-graph warnings and blockers."""

    entries: tuple[dict[str, Any], ...]
    warnings: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...] = ()


def inferred_outlet_entries(
    workload: GitOpsWorkloadDefinition,
    *,
    repo_root: Path,
    manifest_loader: ManifestLoaderRouter | None = None,
    env: str = "dev",
    mssql_registry: ResolvedMssqlAssetRegistry | None = None,
    outlet_binding: OutletBinding = "physical",
) -> InferredOutletReport:
    """Return pack outlets (declared ∪ inferred) plus graph issues."""

    if outlet_binding == "logical":
        logical_report = infer_logical_outlet_entries(
            workload,
            repo_root=repo_root,
            manifest_loader=manifest_loader,
            issue_source=_OUTLET_SOURCE,
        )
        return InferredOutletReport(entries=logical_report.entries, blockers=logical_report.blockers)

    report = build_asset_graph(
        (workload,),
        repo_root=repo_root,
        manifest_loader=manifest_loader,
        env=env,
        mssql_registry=mssql_registry,
    )
    airflow = workload.effective_config.get("airflow")
    execution = airflow.get("execution") if isinstance(airflow, Mapping) else None
    raw_declared = declared_uris(execution.get("outlets") if isinstance(execution, Mapping) else None)
    canonical_declared = _canonical_declared_set(raw_declared, mssql_registry=mssql_registry)
    sinks = sorted({uri for node in report.nodes for uri in node.sink_uris})
    entries: list[dict[str, Any]] = []
    for uri in sinks:
        if uri in canonical_declared or uri in raw_declared:
            entries.append({"uri": uri})
        else:
            entries.append({"uri": uri, "provenance": INFERRED_OUTLET_PROVENANCE})
    return InferredOutletReport(
        entries=tuple(entries),
        warnings=report.warnings,
        blockers=report.blockers,
    )


def execution_policy_with_inferred_outlets(
    *,
    workload: GitOpsWorkloadDefinition,
    execution: dict[str, Any],
    repo_root: Path | None,
    env: str = "dev",
    mssql_registry: ResolvedMssqlAssetRegistry | None = None,
    outlet_binding: OutletBinding = "physical",
) -> tuple[dict[str, Any], tuple[GitOpsWorkloadCatalogIssue, ...], tuple[GitOpsWorkloadCatalogIssue, ...]]:
    """Return execution with outlets and graph issues for the selected binding mode."""

    if repo_root is None:
        return execution, (), ()
    if outlet_binding == "logical":
        outlet_report = inferred_outlet_entries(
            workload,
            repo_root=repo_root,
            outlet_binding="logical",
        )
        if outlet_report.blockers:
            declared_logical, _ = logical_declared_outlet_entries(
                execution.get("outlets"),
                issue_source=_OUTLET_SOURCE,
            )
            execution["outlets"] = list(declared_logical) if declared_logical else list(outlet_report.entries)
            return execution, outlet_report.warnings, outlet_report.blockers
        if outlet_report.entries:
            execution["outlets"] = list(outlet_report.entries)
        return execution, outlet_report.warnings, ()

    registry = mssql_registry or resolve_mssql_asset_registry(repo_root, env=env)
    registry_blockers = _registry_blockers(registry, env=env)
    if registry_blockers:
        return execution, (), registry_blockers
    outlet_report = inferred_outlet_entries(
        workload,
        repo_root=repo_root,
        env=env,
        mssql_registry=registry,
        outlet_binding="physical",
    )
    declared_by_uri, dedupe_blockers = canonical_declared_outlet_map(
        execution.get("outlets"),
        mssql_registry=registry,
    )
    blockers = (*outlet_report.blockers, *dedupe_blockers)
    if blockers:
        execution["outlets"] = list(declared_by_uri.values())
        return execution, outlet_report.warnings, blockers
    if outlet_report.entries:
        merged: list[Any] = []
        for entry in outlet_report.entries:
            uri = entry["uri"]
            if uri in declared_by_uri:
                merged.append(declared_by_uri[uri])
            else:
                merged.append(dict(entry))
        execution["outlets"] = merged
    elif declared_by_uri:
        execution["outlets"] = list(declared_by_uri.values())
    return execution, outlet_report.warnings, ()


def canonical_declared_outlet_map(
    raw_outlets: object,
    *,
    mssql_registry: ResolvedMssqlAssetRegistry | None = None,
) -> tuple[dict[str, Any], tuple[GitOpsWorkloadCatalogIssue, ...]]:
    if not isinstance(raw_outlets, list):
        return {}, ()
    result: dict[str, Any] = {}
    blockers: list[GitOpsWorkloadCatalogIssue] = []
    for item in raw_outlets:
        if isinstance(item, Mapping):
            uri = str(item.get("uri") or "").strip()
            if not uri:
                continue
            canonical = _canonicalize_outlet_uri(uri, mssql_registry=mssql_registry)
            if canonical is None:
                continue
            normalized = dict(item)
            normalized["uri"] = canonical
            prior = result.get(canonical)
            if prior is not None and prior != normalized:
                blockers.append(
                    issue(
                        code=MSSQL_ASSET_URI_INVALID,
                        message=(f"duplicate canonical outlet URI with conflicting metadata: {canonical}"),
                        path=canonical,
                        source=_OUTLET_SOURCE,
                    )
                )
                continue
            result[canonical] = normalized
            continue
        uri = str(item).strip()
        if not uri:
            continue
        canonical = _canonicalize_outlet_uri(uri, mssql_registry=mssql_registry)
        if canonical is None:
            continue
        prior = result.get(canonical)
        plain = {"uri": canonical}
        if prior is not None and prior != plain:
            blockers.append(
                issue(
                    code=MSSQL_ASSET_URI_INVALID,
                    message=(f"duplicate canonical outlet URI with conflicting metadata: {canonical}"),
                    path=canonical,
                    source=_OUTLET_SOURCE,
                )
            )
            continue
        if prior is None:
            result[canonical] = plain
    return result, tuple(blockers)


def _registry_blockers(
    registry: ResolvedMssqlAssetRegistry,
    *,
    env: str,
) -> tuple[GitOpsWorkloadCatalogIssue, ...]:
    mismatch = require_mssql_registry_env_match(registry, env=env)
    issues = list(registry.issues)
    if mismatch is not None:
        issues.append(mismatch)
    return tuple(
        issue(
            code=item.code,
            message=item.message,
            path=item.path or str(registry.path or "connection-registry"),
            source=_OUTLET_SOURCE,
        )
        for item in issues
    )


def _canonicalize_outlet_uri(
    uri: str,
    *,
    mssql_registry: ResolvedMssqlAssetRegistry | None,
) -> str | None:
    if not uri.lower().startswith("mssql://"):
        return uri
    if mssql_registry is not None:
        return resolve_explicit_mssql_uri(uri, registry=mssql_registry).uri
    return canonicalize_declared_mssql_uri(uri).uri


def _canonical_declared_set(
    raw_declared: tuple[str, ...],
    *,
    mssql_registry: ResolvedMssqlAssetRegistry | None,
) -> set[str]:
    canonical: set[str] = set()
    for uri in raw_declared:
        resolved = _canonicalize_outlet_uri(uri, mssql_registry=mssql_registry)
        if resolved is not None:
            canonical.add(resolved)
    return canonical


__all__ = [
    "InferredOutletReport",
    "OutletBinding",
    "canonical_declared_outlet_map",
    "execution_policy_with_inferred_outlets",
    "inferred_outlet_entries",
]
