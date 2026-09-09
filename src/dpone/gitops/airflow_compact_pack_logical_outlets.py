"""Environment-neutral logical outlet materialization for compact packs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.gitops.airflow_asset_graph import INFERRED_OUTLET_PROVENANCE
from dpone.gitops.airflow_asset_uri import MSSQL_ASSET_URI_INVALID
from dpone.gitops.airflow_mssql_logical_asset import logical_asset_ref_from_sink_block
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue, GitOpsWorkloadDefinition, issue
from dpone.manifest.loader import ManifestLoaderRouter


@dataclass(frozen=True, slots=True)
class LogicalOutletReport:
    """Logical outlets and fail-closed declaration/inference blockers."""

    entries: tuple[dict[str, Any], ...]
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...] = ()


def infer_logical_outlet_entries(
    workload: GitOpsWorkloadDefinition,
    *,
    repo_root: Path,
    manifest_loader: ManifestLoaderRouter | None,
    issue_source: str,
) -> LogicalOutletReport:
    """Emit environment-neutral logical asset-ref outlets for dbt release packs."""

    loader = manifest_loader or ManifestLoaderRouter()
    blockers: list[GitOpsWorkloadCatalogIssue] = []
    entries: list[dict[str, Any]] = []
    try:
        manifest = loader.load((repo_root / workload.manifest).resolve(strict=False), metadata_only=True)
        processes = manifest.processes
    except Exception as exc:  # noqa: BLE001 - convert to structured blocker
        return LogicalOutletReport(
            entries=(),
            blockers=(
                issue(
                    code=MSSQL_ASSET_URI_INVALID,
                    message=f"logical outlet inference failed to load manifest: {exc}",
                    path=workload.manifest,
                    source=issue_source,
                ),
            ),
        )
    for process in processes:
        raw = process.raw_config if isinstance(process.raw_config, Mapping) else {}
        sink = raw.get("sink") if isinstance(raw.get("sink"), Mapping) else None
        if not isinstance(sink, Mapping):
            continue
        engine = canonical_endpoint_type(str(sink.get("type") or ""))
        if engine != "mssql":
            raw_table = sink.get("table")
            table: Mapping[str, Any] = raw_table if isinstance(raw_table, Mapping) else {}
            schema = str(table.get("schema") or "").strip()
            name = str(table.get("name") or "").strip()
            if engine and schema and name:
                entries.append({"uri": f"{engine}://{schema}/{name}", "provenance": INFERRED_OUTLET_PROVENANCE})
            continue
        asset_ref, ref_issues = logical_asset_ref_from_sink_block(sink, path=workload.manifest)
        if ref_issues:
            blockers.extend(
                issue(code=item.code, message=item.message, path=item.path or workload.manifest, source=issue_source)
                for item in ref_issues
            )
            continue
        if asset_ref is None:
            continue
        entries.append({"asset_ref": asset_ref.to_mapping(), "provenance": INFERRED_OUTLET_PROVENANCE})
    airflow = workload.effective_config.get("airflow")
    execution = airflow.get("execution") if isinstance(airflow, Mapping) else None
    declared, declared_blockers = logical_declared_outlet_entries(
        execution.get("outlets") if isinstance(execution, Mapping) else None,
        issue_source=issue_source,
    )
    blockers.extend(declared_blockers)
    merged, merge_blockers = _merge_logical_outlet_entries(entries, declared, issue_source=issue_source)
    blockers.extend(merge_blockers)
    return LogicalOutletReport(entries=merged, blockers=tuple(blockers))


def logical_declared_outlet_entries(
    raw_outlets: object,
    *,
    issue_source: str,
) -> tuple[list[dict[str, Any]], tuple[GitOpsWorkloadCatalogIssue, ...]]:
    """Normalize declared logical outlets and reject deployment-owned MSSQL URIs."""

    if not isinstance(raw_outlets, list):
        return [], ()
    entries: list[dict[str, Any]] = []
    blockers: list[GitOpsWorkloadCatalogIssue] = []
    for item in raw_outlets:
        if isinstance(item, Mapping) and isinstance(item.get("asset_ref"), Mapping):
            from dpone.gitops.airflow_mssql_logical_asset import parse_mssql_logical_asset_ref

            asset_ref, ref_issues = parse_mssql_logical_asset_ref(item.get("asset_ref"))
            if ref_issues:
                blockers.extend(
                    issue(
                        code=issue_item.code,
                        message=issue_item.message,
                        path=issue_item.path or "airflow.execution.outlets",
                        source=issue_source,
                    )
                    for issue_item in ref_issues
                )
                continue
            assert asset_ref is not None
            normalized = {key: value for key, value in item.items() if key != "uri"}
            normalized["asset_ref"] = asset_ref.to_mapping()
            entries.append(normalized)
            continue
        uri = str(item.get("uri") or "").strip() if isinstance(item, Mapping) else str(item).strip()
        if uri.lower().startswith("mssql://"):
            blockers.append(
                issue(
                    code=MSSQL_ASSET_URI_INVALID,
                    message=(
                        "environment-neutral dbt release packs must use logical mssql asset_ref; "
                        "physical mssql:// URIs are deployment-owned"
                    ),
                    path=uri,
                    source=issue_source,
                )
            )
            continue
        if uri:
            entries.append({"uri": uri} if not isinstance(item, Mapping) else dict(item))
    return entries, tuple(blockers)


def _merge_logical_outlet_entries(
    inferred: list[dict[str, Any]],
    declared: list[dict[str, Any]],
    *,
    issue_source: str,
) -> tuple[tuple[dict[str, Any], ...], tuple[GitOpsWorkloadCatalogIssue, ...]]:
    """Union inferred and declared logical outlets by canonical identity."""

    ordered: list[dict[str, Any]] = []
    by_identity: dict[str, dict[str, Any]] = {}
    blockers: list[GitOpsWorkloadCatalogIssue] = []
    for entry in (*inferred, *declared):
        identity = _logical_outlet_identity(entry)
        if identity is None:
            continue
        core = {key: value for key, value in entry.items() if key != "provenance"}
        prior = by_identity.get(identity)
        if prior is None:
            by_identity[identity] = entry
            ordered.append(entry)
            continue
        if "provenance" in prior and "provenance" not in entry:
            ordered[ordered.index(prior)] = entry
            by_identity[identity] = entry
            continue
        prior_core = {key: value for key, value in prior.items() if key != "provenance"}
        if prior_core != core:
            message = f"duplicate canonical logical outlet with conflicting metadata: {identity}"
            blockers.append(
                issue(
                    code=MSSQL_ASSET_URI_INVALID,
                    message=message,
                    path=identity,
                    source=issue_source,
                )
            )
            continue
        if "provenance" not in entry:
            ordered[ordered.index(prior)] = entry
            by_identity[identity] = entry
    return tuple(ordered), tuple(blockers)


def _logical_outlet_identity(entry: Mapping[str, Any]) -> str | None:
    """Map logical MSSQL to asset-ref digest and ordinary outlets to URI text."""

    asset_ref = entry.get("asset_ref")
    if isinstance(asset_ref, Mapping):
        from dpone_airflow_pack.mssql_asset_ref_codec import asset_ref_sha256

        return asset_ref_sha256(asset_ref)
    uri = str(entry.get("uri") or "").strip()
    return uri or None


__all__ = ["LogicalOutletReport", "infer_logical_outlet_entries", "logical_declared_outlet_entries"]
