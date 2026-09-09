"""Adapt legacy ``dpone_workloads/gitops/domains/*.yaml`` DAG blocks into IR."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from dpone.gitops.airflow_dag_spec import DagSpecDeclaration, parse_dag_declaration
from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import ConfinedFileError, read_confined_file
from dpone.manifest.domain_dag_models import DiscoveredDomainDag, domain_dag_fingerprint
from dpone.manifest.domain_identity import DomainId, DomainIdError
from dpone.manifest.project_discovery_issues import discovery_issue as _issue
from dpone.manifest.project_discovery_models import ProjectDiscoveryIssue

DEFAULT_LEGACY_DOMAIN_CATALOG_GLOB = "dpone_workloads/gitops/domains/*.yaml"
_LEGACY_LIMITS = BoundedYamlLimits(max_bytes=512 * 1024, max_tokens=40_000, max_depth=32, max_nodes=20_000)


def adapt_legacy_domain_catalogs(
    root: Path,
    *,
    catalog_glob: str = DEFAULT_LEGACY_DOMAIN_CATALOG_GLOB,
) -> tuple[list[DiscoveredDomainDag], list[ProjectDiscoveryIssue], dict[str, str]]:
    """Load legacy domain-catalog ``dags:`` entries into discovered DAG IR."""

    dags: list[DiscoveredDomainDag] = []
    issues: list[ProjectDiscoveryIssue] = []
    consumed: dict[str, str] = {}
    matches = sorted(root.glob(catalog_glob))
    for path in matches:
        try:
            relative = path.resolve(strict=False).relative_to(root.resolve(strict=False)).as_posix()
        except ValueError:
            issues.append(
                _issue(
                    "DPONE_LEGACY_CATALOG_PATH_INVALID",
                    "Legacy domain catalog path escaped the project root.",
                    path.as_posix(),
                )
            )
            continue
        try:
            content = read_confined_file(root, relative, max_bytes=_LEGACY_LIMITS.max_bytes)
        except ConfinedFileError:
            issues.append(
                _issue(
                    "DPONE_LEGACY_CATALOG_READ_FAILED",
                    "Legacy domain catalog could not be read safely.",
                    relative,
                )
            )
            continue
        digest = "sha256:" + hashlib.sha256(content).hexdigest()
        try:
            payload = load_bounded_yaml(content, limits=_LEGACY_LIMITS)
        except BoundedYamlError as exc:
            issues.append(
                _issue(
                    "DPONE_LEGACY_CATALOG_YAML_INVALID",
                    f"Legacy domain catalog YAML is invalid ({exc.code}).",
                    relative,
                )
            )
            continue
        if not isinstance(payload, Mapping):
            issues.append(
                _issue(
                    "DPONE_LEGACY_CATALOG_PAYLOAD_INVALID",
                    "Legacy domain catalog payload must be a mapping.",
                    relative,
                )
            )
            continue
        raw_domain = payload.get("domain")
        try:
            domain = str(DomainId.parse(raw_domain)) if isinstance(raw_domain, str) else None
        except DomainIdError:
            domain = None
        if domain is None:
            issues.append(
                _issue(
                    "DPONE_LEGACY_CATALOG_DOMAIN_INVALID",
                    "Legacy domain catalog domain must be a canonical domain id.",
                    relative,
                )
            )
            continue
        dags_block = payload.get("dags")
        if dags_block is None:
            consumed[relative] = digest
            continue
        if not isinstance(dags_block, Mapping):
            issues.append(
                _issue(
                    "DPONE_LEGACY_CATALOG_DAGS_INVALID",
                    "Legacy domain catalog dags must be a mapping.",
                    relative,
                    domain=domain,
                )
            )
            continue
        groups = _workflow_groups(payload)
        for dag_id, raw_entry in dags_block.items():
            if not isinstance(dag_id, str) or not isinstance(raw_entry, Mapping):
                issues.append(
                    _issue(
                        "DPONE_LEGACY_CATALOG_DAG_ENTRY_INVALID",
                        "Legacy dags entry must map a string dag_id to an object.",
                        relative,
                        domain=domain,
                    )
                )
                continue
            declaration, declaration_issues = parse_dag_declaration(dag_id, dict(raw_entry))
            for item in declaration_issues:
                issues.append(
                    _issue(
                        item.code,
                        item.message,
                        relative,
                        domain=domain,
                    )
                )
            if declaration is None:
                continue
            expanded = _expand_group_declaration(
                declaration,
                groups=groups,
                path=relative,
                domain=domain,
                issues=issues,
            )
            if expanded is None:
                continue
            fingerprint = domain_dag_fingerprint(expanded)
            dags.append(
                DiscoveredDomainDag(
                    dag_id=expanded.dag_id,
                    domain=domain,
                    path=relative,
                    source_sha256=digest,
                    declaration=expanded,
                    fingerprint=fingerprint,
                    pipelines=tuple(expanded.workloads or ()),
                    source="legacy_catalog",
                )
            )
        consumed[relative] = digest
    return dags, issues, consumed


def merge_discovered_domain_dags(
    colocated: list[DiscoveredDomainDag],
    legacy: list[DiscoveredDomainDag],
) -> tuple[list[DiscoveredDomainDag], list[ProjectDiscoveryIssue], list[ProjectDiscoveryIssue]]:
    """Merge colocated and legacy DAG IR using fingerprint conflict policy."""

    blockers: list[ProjectDiscoveryIssue] = []
    warnings: list[ProjectDiscoveryIssue] = []
    by_id: dict[str, DiscoveredDomainDag] = {}
    for item in colocated:
        prior = by_id.get(item.dag_id)
        if prior is not None:
            blockers.append(
                _issue(
                    "DPONE_DOMAIN_DAG_ID_DUPLICATE",
                    "Domain DAG id must be unique across the project.",
                    item.path,
                    domain=item.domain,
                )
            )
            continue
        by_id[item.dag_id] = replace(item, source="colocated")
    for item in legacy:
        prior = by_id.get(item.dag_id)
        if prior is None:
            by_id[item.dag_id] = replace(item, source="legacy_catalog")
            continue
        if prior.fingerprint == item.fingerprint:
            warnings.append(
                _issue(
                    "DPONE_DOMAIN_DAG_DUAL_READ_WARNING",
                    "Identical domain DAG declared in colocated and legacy catalog; using colocated source.",
                    prior.path,
                    domain=prior.domain,
                )
            )
            continue
        blockers.append(
            _issue(
                "DPONE_DOMAIN_DAG_DUAL_READ_CONFLICT",
                "Domain DAG fingerprint differs between colocated and legacy catalog sources.",
                prior.path,
                domain=prior.domain,
            )
        )
        # Fail closed for this dag_id: keep the blocker and omit both sources from IR
        # so reconcile cannot materialize a dag-spec under an active conflict.
        del by_id[item.dag_id]
    ordered = sorted(by_id.values(), key=lambda item: item.dag_id)
    return ordered, blockers, warnings


def validate_domain_dag_pipeline_refs(
    dags: list[DiscoveredDomainDag],
    *,
    pipeline_domains: Mapping[str, str],
) -> list[ProjectDiscoveryIssue]:
    """Fail closed on missing pipeline refs; enforce same-domain only for colocated DAGs.

    Legacy catalogs historically host multi-domain DAG entries (for example CRM
    OLAP under an interchange catalog). Keep existence checks for dual-read, but
    defer cross-domain ownership to colocated ``workloads/<domain>/dags`` files.
    """

    issues: list[ProjectDiscoveryIssue] = []
    for dag in dags:
        for pipeline_id in dag.pipelines:
            owner = pipeline_domains.get(pipeline_id)
            if owner is None:
                issues.append(
                    _issue(
                        "DPONE_DOMAIN_DAG_PIPELINE_MISSING",
                        f"Domain DAG references unknown pipeline id {pipeline_id!r}.",
                        dag.path,
                        domain=dag.domain,
                        pipeline_id=pipeline_id,
                    )
                )
            elif dag.source == "colocated" and owner != dag.domain:
                issues.append(
                    _issue(
                        "DPONE_DOMAIN_DAG_CROSS_DOMAIN_PIPELINE",
                        f"Domain DAG cannot reference pipeline {pipeline_id!r} owned by domain {owner!r}.",
                        dag.path,
                        domain=dag.domain,
                        pipeline_id=pipeline_id,
                    )
                )
    return issues


def _workflow_groups(payload: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    raw_groups = payload.get("workflow_groups") or payload.get("workload_groups") or payload.get("groups") or {}
    if not isinstance(raw_groups, Mapping):
        return {}
    groups: dict[str, tuple[str, ...]] = {}
    for name, raw_group in raw_groups.items():
        if isinstance(raw_group, Mapping):
            raw_ids = raw_group.get("workload_ids") or raw_group.get("workloads") or ()
        else:
            raw_ids = raw_group or ()
        if isinstance(raw_ids, list | tuple):
            groups[str(name)] = tuple(str(item) for item in raw_ids)
    return groups


def _expand_group_declaration(
    declaration: DagSpecDeclaration,
    *,
    groups: Mapping[str, tuple[str, ...]],
    path: str,
    domain: str,
    issues: list[ProjectDiscoveryIssue],
) -> DagSpecDeclaration | None:
    if declaration.group is None:
        return declaration
    members = groups.get(declaration.group)
    if members is None:
        issues.append(
            _issue(
                "DPONE_LEGACY_CATALOG_GROUP_UNKNOWN",
                f"Legacy DAG group {declaration.group!r} is not defined.",
                path,
                domain=domain,
            )
        )
        return None
    if not members:
        issues.append(
            _issue(
                "DPONE_LEGACY_CATALOG_GROUP_EMPTY",
                f"Legacy DAG group {declaration.group!r} has no workloads.",
                path,
                domain=domain,
            )
        )
        return None
    return replace(declaration, workloads=members, group=None)


__all__ = [
    "DEFAULT_LEGACY_DOMAIN_CATALOG_GLOB",
    "adapt_legacy_domain_catalogs",
    "merge_discovered_domain_dags",
    "validate_domain_dag_pipeline_refs",
]
