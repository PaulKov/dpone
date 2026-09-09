"""Lightweight GitOps workload group reader for Airflow DAG parse.

The module intentionally depends only on YAML parsing and plain files. It does
not import full dpone, source/sink connectors, Kubernetes clients, or database
drivers.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

DEFAULT_GITOPS_DOMAIN_DIR = Path("dpone_workloads/gitops/domains")


def load_gitops_domain_catalog(
    repo_root: str | Path,
    domain: str,
    *,
    domain_dir: str | Path = DEFAULT_GITOPS_DOMAIN_DIR,
) -> Mapping[str, Any]:
    """Load one GitOps domain YAML catalog."""

    catalog_path = Path(repo_root) / Path(domain_dir) / f"{domain}.yaml"
    if not catalog_path.exists():
        raise ValueError(f"GitOps domain catalog does not exist: {catalog_path}")
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise RuntimeError("PyYAML is required to read dpone GitOps domain catalogs") from exc
    payload = yaml.safe_load(catalog_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, Mapping):
        raise ValueError(f"GitOps domain catalog must be a YAML mapping: {catalog_path}")
    return payload


def load_gitops_workload_groups(
    repo_root: str | Path,
    domain: str,
    *,
    domain_dir: str | Path = DEFAULT_GITOPS_DOMAIN_DIR,
) -> dict[str, tuple[str, ...]]:
    """Return declared workflow groups for a GitOps domain."""

    catalog = load_gitops_domain_catalog(repo_root, domain, domain_dir=domain_dir)
    groups = _as_mapping(catalog.get("workflow_groups") or catalog.get("workload_groups") or catalog.get("groups"))
    return {str(group): _group_workload_ids(group, raw_group) for group, raw_group in groups.items()}


def workload_ids_from_gitops_domain(
    repo_root: str | Path,
    domain: str,
    *,
    group: str | None = None,
    workload_ids: Sequence[str] | None = None,
    exclude_workload_ids: Sequence[str] = (),
    domain_dir: str | Path = DEFAULT_GITOPS_DOMAIN_DIR,
) -> tuple[str, ...]:
    """Resolve workload ids from a GitOps domain.

    `workload_ids` is an optional narrowing filter for diagnostics and partial
    rollouts. Without the filter, the full domain/group order is returned.
    """

    catalog = load_gitops_domain_catalog(repo_root, domain, domain_dir=domain_dir)
    workloads = _as_mapping(catalog.get("workloads"))
    selected = _selected_workload_ids(catalog, group=group)
    known = {str(item) for item in workloads}
    unknown = tuple(workload_id for workload_id in selected if workload_id not in known)
    if unknown:
        raise ValueError(f"GitOps domain {domain!r} references unknown workload ids: {', '.join(unknown)}")

    narrowed = tuple(str(item) for item in workload_ids) if workload_ids is not None else selected
    missing = tuple(workload_id for workload_id in narrowed if workload_id not in selected)
    if missing:
        scope = f"{domain}.{group}" if group else domain
        raise ValueError(f"Requested workload ids are not part of GitOps domain scope {scope!r}: {', '.join(missing)}")

    excluded = {str(item) for item in exclude_workload_ids}
    return tuple(workload_id for workload_id in narrowed if workload_id not in excluded)


def _selected_workload_ids(catalog: Mapping[str, Any], *, group: str | None) -> tuple[str, ...]:
    if group is None:
        return tuple(str(item) for item in _as_mapping(catalog.get("workloads")))
    groups = _as_mapping(catalog.get("workflow_groups") or catalog.get("workload_groups") or catalog.get("groups"))
    if group not in groups:
        raise ValueError(f"GitOps workflow group {group!r} is not declared")
    return _group_workload_ids(group, groups[group])


def _group_workload_ids(group: object, raw_group: object) -> tuple[str, ...]:
    if isinstance(raw_group, Mapping):
        raw_ids = raw_group.get("workload_ids") or raw_group.get("workloads")
    else:
        raw_ids = raw_group
    if not isinstance(raw_ids, list | tuple) or not raw_ids:
        raise ValueError(f"GitOps workflow group {group!r} must declare a non-empty workload id list")
    return tuple(str(item) for item in raw_ids)


def _as_mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
