"""Resolve compact Airflow runner contracts from GitOps ``airflow`` config.

Industry alignment
------------------

Large data-platform teams solve the same KPO isolation problem in three layers:

1. **Placement** (nodeSelector / tolerations / affinity) — environment values in
   Helm/Terraform, same pattern as Airflow official chart ``tolerations`` and
   Argo Workflows ``podSpecPatch``.
2. **Runner assets** — files required inside the execution pod (TLS roots, JDBC
   drivers, license files). Airbyte mounts connector-specific secrets; dlt keeps
   project-local ``.dlt`` assets; we embed declared repo paths into the compact
   pack inline archive because KPO pods do not inherit git-sync volumes.
3. **Connection path rewriting** — workspace-relative URIs expanded at reconcile
   time so runtime never depends on scheduler-only paths such as
   ``/opt/airflow/dags/...``.

dpone exposes these as a single ``airflow.runner`` block with legacy flat-key
compat for existing workload sets.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.gitops.airflow_pod_contract import GitOpsAirflowToleration
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue, issue

RUNNER_WORKSPACE_ROOT = "/workspace/repo"
_RUNNER_CONTRACT_SOURCE = "dpone gitops airflow runner"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowRunnerPlacement:
    node_selector: dict[str, str]
    tolerations: tuple[GitOpsAirflowToleration, ...]


@dataclass(frozen=True, slots=True)
class GitOpsAirflowRunnerEmbedAsset:
    path: str
    connection_id: str | None = None
    query_key: str | None = None


@dataclass(frozen=True, slots=True)
class GitOpsAirflowRunnerContract:
    workspace_root: str
    placement: GitOpsAirflowRunnerPlacement
    embed_paths: tuple[str, ...]
    connection_projection: dict[str, Any]
    warnings: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...] = ()

    @property
    def passed(self) -> bool:
        return not self.blockers


def resolve_airflow_runner_contract(
    airflow: Mapping[str, Any],
    *,
    repo_root: Path | None = None,
    workload_id: str = "",
) -> GitOpsAirflowRunnerContract:
    """Materialize placement, embed assets, and connection projection for one pack."""

    runner = _runner_section(airflow)
    workspace_root = _workspace_root(runner)
    placement = GitOpsAirflowRunnerPlacement(
        node_selector=_node_selector(runner, airflow),
        tolerations=_tolerations(runner, airflow),
    )
    embed_assets = _embed_assets(runner, airflow)
    embed_paths = tuple(dict.fromkeys(asset.path for asset in embed_assets))
    warnings: list[GitOpsWorkloadCatalogIssue] = []
    blockers: list[GitOpsWorkloadCatalogIssue] = []
    if repo_root is not None:
        _validate_embed_paths(
            repo_root=repo_root,
            embed_paths=embed_paths,
            workload_id=workload_id,
            blockers=blockers,
        )
    projection = _expand_connection_projection(
        airflow,
        embed_assets=embed_assets,
        workspace_root=workspace_root,
    )
    return GitOpsAirflowRunnerContract(
        workspace_root=workspace_root,
        placement=placement,
        embed_paths=embed_paths,
        connection_projection=projection,
        warnings=tuple(warnings),
        blockers=tuple(blockers),
    )


def runner_workspace_path(relative_path: str, *, workspace_root: str = RUNNER_WORKSPACE_ROOT) -> str:
    normalized = safe_relative_path(relative_path, source="runner.workspace_path").as_posix()
    return f"{workspace_root.rstrip('/')}/{normalized}"


def _runner_section(airflow: Mapping[str, Any]) -> Mapping[str, Any]:
    raw = airflow.get("runner")
    return raw if isinstance(raw, Mapping) else {}


def _workspace_root(runner: Mapping[str, Any]) -> str:
    text = str(runner.get("workspace_root") or RUNNER_WORKSPACE_ROOT).strip()
    return text or RUNNER_WORKSPACE_ROOT


def _node_selector(runner: Mapping[str, Any], airflow: Mapping[str, Any]) -> dict[str, str]:
    placement = runner.get("placement")
    if isinstance(placement, Mapping) and isinstance(placement.get("node_selector"), Mapping):
        raw = placement.get("node_selector")
    else:
        raw = runner.get("node_selector", airflow.get("node_selector", airflow.get("nodeSelector")))
    if not isinstance(raw, Mapping):
        return {}
    return {str(key): str(value) for key, value in raw.items() if str(key).strip() and str(value).strip()}


def _tolerations(runner: Mapping[str, Any], airflow: Mapping[str, Any]) -> tuple[GitOpsAirflowToleration, ...]:
    placement = runner.get("placement")
    if isinstance(placement, Mapping) and placement.get("tolerations") is not None:
        raw = placement.get("tolerations")
    else:
        raw = runner.get("tolerations", airflow.get("tolerations"))
    return _parse_tolerations(raw)


def _embed_assets(runner: Mapping[str, Any], airflow: Mapping[str, Any]) -> tuple[GitOpsAirflowRunnerEmbedAsset, ...]:
    assets: list[GitOpsAirflowRunnerEmbedAsset] = []
    raw_assets = runner.get("embed_assets")
    if isinstance(raw_assets, Sequence) and not isinstance(raw_assets, (str, bytes)):
        for item in raw_assets:
            parsed = _parse_embed_asset(item)
            if parsed is not None:
                assets.append(parsed)
    for path in _legacy_embed_paths(runner, airflow):
        assets.append(GitOpsAirflowRunnerEmbedAsset(path=path))
    deduped: dict[str, GitOpsAirflowRunnerEmbedAsset] = {}
    for asset in assets:
        existing = deduped.get(asset.path)
        if existing is None:
            deduped[asset.path] = asset
            continue
        if existing.connection_id is None and asset.connection_id is not None:
            deduped[asset.path] = asset
    return tuple(deduped[path] for path in sorted(deduped))


def _legacy_embed_paths(runner: Mapping[str, Any], airflow: Mapping[str, Any]) -> tuple[str, ...]:
    raw = runner.get("embed_paths", runner.get("runner_embed_paths", airflow.get("runner_embed_paths")))
    if raw is None:
        return ()
    if isinstance(raw, str):
        values = [raw]
    elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        values = [str(item) for item in raw]
    else:
        return ()
    paths: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        try:
            paths.append(safe_relative_path(text, source="runner.embed_paths").as_posix())
        except GitOpsPathValidationError:
            continue
    return tuple(dict.fromkeys(paths))


def _parse_embed_asset(raw: object) -> GitOpsAirflowRunnerEmbedAsset | None:
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        try:
            return GitOpsAirflowRunnerEmbedAsset(path=safe_relative_path(text, source="runner.embed_assets").as_posix())
        except GitOpsPathValidationError:
            return None
    if not isinstance(raw, Mapping):
        return None
    path_text = str(raw.get("path") or "").strip()
    if not path_text:
        return None
    try:
        path = safe_relative_path(path_text, source="runner.embed_assets.path").as_posix()
    except GitOpsPathValidationError:
        return None
    bind = raw.get("bind")
    if not isinstance(bind, Mapping):
        return GitOpsAirflowRunnerEmbedAsset(path=path)
    connection_id = str(bind.get("connection_id") or "").strip() or None
    query_key = str(bind.get("query_key") or "").strip() or None
    return GitOpsAirflowRunnerEmbedAsset(path=path, connection_id=connection_id, query_key=query_key)


def _parse_tolerations(raw: object) -> tuple[GitOpsAirflowToleration, ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return ()
    tolerations: list[GitOpsAirflowToleration] = []
    for item in raw:
        if isinstance(item, Mapping):
            key = str(item.get("key") or "").strip()
            value = str(item.get("value") or "").strip()
            effect = str(item.get("effect") or "").strip()
            operator = str(item.get("operator") or "Equal").strip() or "Equal"
            if not key or not effect or operator != "Equal":
                continue
            tolerations.append(GitOpsAirflowToleration(key=key, value=value, effect=effect))
            continue
        parsed = _parse_toleration_token(str(item or "").strip())
        if parsed is not None:
            tolerations.append(parsed)
    return tuple(tolerations)


def _parse_toleration_token(text: str) -> GitOpsAirflowToleration | None:
    if not text or "=" not in text or ":" not in text:
        return None
    key, remainder = text.split("=", 1)
    value, effect = remainder.rsplit(":", 1)
    key = key.strip()
    value = value.strip()
    effect = effect.strip()
    if not key or not effect:
        return None
    return GitOpsAirflowToleration(key=key, value=value, effect=effect)


def _expand_connection_projection(
    airflow: Mapping[str, Any],
    *,
    embed_assets: tuple[GitOpsAirflowRunnerEmbedAsset, ...],
    workspace_root: str,
) -> dict[str, Any]:
    projection = (
        dict(airflow.get("connection_projection") or {})
        if isinstance(airflow.get("connection_projection"), Mapping)
        else {}
    )
    query_overrides = (
        dict(projection.get("query_overrides") or {}) if isinstance(projection.get("query_overrides"), Mapping) else {}
    )
    workspace_overrides = (
        dict(projection.get("workspace_query_overrides") or {})
        if isinstance(projection.get("workspace_query_overrides"), Mapping)
        else {}
    )
    for connection_id, overrides in workspace_overrides.items():
        if not isinstance(overrides, Mapping):
            continue
        merged = (
            dict(query_overrides.get(str(connection_id)) or {})
            if isinstance(query_overrides.get(str(connection_id)), Mapping)
            else {}
        )
        for key, relative_path in overrides.items():
            merged[str(key)] = runner_workspace_path(str(relative_path), workspace_root=workspace_root)
        query_overrides[str(connection_id)] = merged
    for asset in embed_assets:
        if not asset.connection_id or not asset.query_key:
            continue
        merged = (
            dict(query_overrides.get(asset.connection_id) or {})
            if isinstance(query_overrides.get(asset.connection_id), Mapping)
            else {}
        )
        merged[asset.query_key] = runner_workspace_path(asset.path, workspace_root=workspace_root)
        query_overrides[asset.connection_id] = merged
    result = dict(projection)
    result["query_overrides"] = query_overrides
    return result


def _validate_embed_paths(
    *,
    repo_root: Path,
    embed_paths: tuple[str, ...],
    workload_id: str,
    blockers: list[GitOpsWorkloadCatalogIssue],
) -> None:
    for rel in embed_paths:
        path = repo_root / rel
        if path.is_file():
            continue
        blockers.append(
            issue(
                code="runner_embed_asset_missing",
                message="Runner embed asset must exist in the workload repository at reconcile time",
                path=f"{workload_id}:{rel}" if workload_id else rel,
                source=_RUNNER_CONTRACT_SOURCE,
            )
        )


# Backward-compatible helpers for transitional imports/tests.
def parse_airflow_node_selector(airflow: Mapping[str, Any]) -> dict[str, str]:
    return resolve_airflow_runner_contract(airflow).placement.node_selector


def parse_airflow_tolerations(airflow: Mapping[str, Any]) -> tuple[GitOpsAirflowToleration, ...]:
    return resolve_airflow_runner_contract(airflow).placement.tolerations


def parse_runner_embed_paths(airflow: Mapping[str, Any]) -> tuple[str, ...]:
    return resolve_airflow_runner_contract(airflow).embed_paths


def expand_connection_projection(airflow: Mapping[str, Any]) -> dict[str, Any]:
    return resolve_airflow_runner_contract(airflow).connection_projection


__all__ = [
    "RUNNER_WORKSPACE_ROOT",
    "GitOpsAirflowRunnerContract",
    "GitOpsAirflowRunnerEmbedAsset",
    "GitOpsAirflowRunnerPlacement",
    "expand_connection_projection",
    "parse_airflow_node_selector",
    "parse_airflow_tolerations",
    "parse_runner_embed_paths",
    "resolve_airflow_runner_contract",
    "runner_workspace_path",
]
