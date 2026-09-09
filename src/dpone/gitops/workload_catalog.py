from __future__ import annotations

import copy
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from dpone.gitops.workload_catalog_models import (
    GitOpsConfigProvenance,
    GitOpsWorkloadCatalogReport,
    GitOpsWorkloadDefinition,
    issue,
)


@dataclass(frozen=True, slots=True)
class _CatalogDoc:
    path: Path
    payload: Mapping[str, Any]


class WorkloadCatalogResolver:
    """Resolve hierarchical GitOps workload catalogs into effective workload configs."""

    def __init__(self, *, repo_root: Path) -> None:
        self._repo_root = repo_root.resolve(strict=False)

    def resolve(self, workload_set: str | Path, *, env: str) -> GitOpsWorkloadCatalogReport:
        root_path = self._resolve_input_path(workload_set)
        root_payload = _mapping(_load_yaml(root_path).get("gitops"))
        root_label = self._label(root_path)
        docs, include_warnings = self._included_docs(root_path=root_path, root_payload=root_payload)
        env_defaults = _mapping(root_payload.get("environments")).get(env, {})
        root = _RootConfig(
            defaults=_mapping(root_payload.get("defaults")),
            env_defaults=_mapping(env_defaults),
            source_types=_mapping(root_payload.get("source_types")),
            sources=_mapping(root_payload.get("sources")),
        )
        workloads = [
            *self._catalog_workloads(root=root, docs=docs, env=env),
            *self._inferred_workloads(root_path, root, env=env),
        ]
        blockers = list(_duplicate_id_blockers(workloads))
        return GitOpsWorkloadCatalogReport(
            workload_set=root_label,
            env=env,
            workloads=tuple(sorted(workloads, key=lambda item: item.workload_id)),
            warnings=tuple(include_warnings),
            blockers=tuple(blockers),
        )

    def _included_docs(
        self, *, root_path: Path, root_payload: Mapping[str, Any]
    ) -> tuple[tuple[_CatalogDoc, ...], list[Any]]:
        docs: list[_CatalogDoc] = []
        warnings: list[Any] = []
        root_dir = root_path.parent
        for include in _sequence(root_payload.get("includes")):
            include_map = _mapping(include)
            raw_path = include_map.get("path")
            if not raw_path:
                continue
            matches = sorted(root_dir.glob(str(raw_path)))
            if not matches:
                warnings.append(
                    issue(
                        code="workload_catalog_include_empty",
                        message="GitOps workload catalog include matched no files",
                        path=self._label(root_dir / str(raw_path)),
                    )
                )
            for path in matches:
                docs.append(_CatalogDoc(path=path, payload=_load_yaml(path)))
        return tuple(docs), warnings

    def _catalog_workloads(
        self, *, root: _RootConfig, docs: Iterable[_CatalogDoc], env: str
    ) -> tuple[GitOpsWorkloadDefinition, ...]:
        workloads: list[GitOpsWorkloadDefinition] = []
        for doc in docs:
            payload = doc.payload
            domain = _optional_str(payload.get("domain"))
            env_catalog = _optional_str(payload.get("environment"))
            if env_catalog and env_catalog != env:
                continue
            catalog_defaults = _mapping(payload.get("defaults"))
            catalog_env = _mapping(_mapping(payload.get("environments")).get(env))
            for workload_id, raw_workload in _mapping(payload.get("workloads")).items():
                workload = _mapping(raw_workload)
                manifest = self._manifest_label(raw_path=workload.get("manifest"), base_dir=doc.path.parent)
                config, provenance = _effective_config(
                    root=root,
                    env=env,
                    domain=domain,
                    catalog_path=self._label(doc.path),
                    catalog_defaults={**catalog_defaults, **catalog_env},
                    workload=workload,
                    manifest_payload=_load_optional_mapping(self._repo_root / manifest),
                )
                workloads.append(
                    GitOpsWorkloadDefinition(
                        workload_id=str(workload_id),
                        manifest=manifest,
                        domain=domain,
                        catalog_path=self._label(doc.path),
                        effective_config=config,
                        provenance=provenance,
                    )
                )
        return tuple(workloads)

    def _inferred_workloads(self, root_path: Path, root: _RootConfig, env: str) -> tuple[GitOpsWorkloadDefinition, ...]:
        root_payload = _mapping(_load_yaml(root_path).get("gitops"))
        root_dir = root_path.parent
        workloads: list[GitOpsWorkloadDefinition] = []
        for include in _sequence(root_payload.get("includes")):
            include_map = _mapping(include)
            if not include_map.get("infer_workload"):
                continue
            for manifest in sorted(root_dir.glob(str(include_map.get("manifest_glob") or "manifests/**/*.yaml"))):
                label = self._label(manifest)
                config, provenance = _effective_config(
                    root=root,
                    env=env,
                    domain=None,
                    catalog_path=self._label(root_path),
                    catalog_defaults={},
                    workload={"manifest": label},
                    manifest_payload=_load_optional_mapping(manifest),
                )
                workloads.append(
                    GitOpsWorkloadDefinition(
                        workload_id=_inferred_workload_id(manifest.relative_to(root_dir)),
                        manifest=label,
                        domain=None,
                        catalog_path=self._label(root_path),
                        effective_config=config,
                        provenance=provenance,
                    )
                )
        return tuple(workloads)

    def _manifest_label(self, *, raw_path: object, base_dir: Path) -> str:
        if not raw_path:
            return self._label(base_dir / "manifest.yaml")
        return self._label((base_dir / str(raw_path)).resolve(strict=False))

    def _resolve_input_path(self, raw_path: str | Path) -> Path:
        path = Path(raw_path)
        return (path if path.is_absolute() else self._repo_root / path).resolve(strict=False)

    def _label(self, path: Path) -> str:
        return path.resolve(strict=False).relative_to(self._repo_root).as_posix()


@dataclass(frozen=True, slots=True)
class _RootConfig:
    defaults: Mapping[str, Any]
    env_defaults: Mapping[str, Any]
    source_types: Mapping[str, Any]
    sources: Mapping[str, Any]


def _effective_config(
    *,
    root: _RootConfig,
    env: str,
    domain: str | None,
    catalog_path: str,
    catalog_defaults: Mapping[str, Any],
    workload: Mapping[str, Any],
    manifest_payload: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, GitOpsConfigProvenance]]:
    config: dict[str, Any] = {}
    provenance: dict[str, GitOpsConfigProvenance] = {}

    _merge(config, provenance, root.defaults, scope="global", path="<workload-set>")
    _merge(config, provenance, root.env_defaults, scope="environment", path="<workload-set>", prefix="")
    source_id = _optional_str(workload.get("source"))
    source_config = _mapping(root.sources.get(source_id)) if source_id else {}
    source_type = _optional_str(source_config.get("source_type"))
    if source_type:
        _merge(config, provenance, _mapping(root.source_types.get(source_type)), scope="source_type", path=source_type)
        _merge(config, provenance, {"source_type": source_type}, scope="source", path=source_id or "")
    _merge(config, provenance, source_config, scope="source", path=source_id or "")
    if domain:
        _merge(config, provenance, {"domain": domain}, scope="domain", path=catalog_path)
    _merge(config, provenance, catalog_defaults, scope="domain", path=catalog_path)
    _merge(
        config,
        provenance,
        _mapping(manifest_payload.get("gitops")),
        scope="manifest-local",
        path=str(workload.get("manifest") or ""),
    )
    _merge(config, provenance, workload, scope="workload", path=catalog_path)
    if env:
        config.setdefault("env", env)
    return config, provenance


def _merge(
    config: dict[str, Any],
    provenance: dict[str, GitOpsConfigProvenance],
    values: Mapping[str, Any],
    *,
    scope: str,
    path: str,
    prefix: str = "",
) -> None:
    for key, value in values.items():
        if key in {"workloads", "includes", "environments", "source_types", "sources"}:
            continue
        target_key = f"{prefix}{key}"
        existing = config.get(target_key)
        if isinstance(existing, dict) and isinstance(value, Mapping):
            merged = dict(existing)
            _deep_merge_mapping(merged, provenance, value, scope=scope, path=path, prefix=f"{target_key}.")
            config[target_key] = merged
        else:
            config[target_key] = copy.deepcopy(value)
            _record_nested_provenance(provenance, value, scope=scope, path=path, prefix=f"{target_key}.")
        provenance[target_key] = GitOpsConfigProvenance(scope=scope, path=path, key=target_key)


def _deep_merge_mapping(
    target: dict[str, Any],
    provenance: dict[str, GitOpsConfigProvenance],
    values: Mapping[str, Any],
    *,
    scope: str,
    path: str,
    prefix: str,
) -> None:
    for key, value in values.items():
        nested_key = f"{prefix}{key}"
        existing = target.get(key)
        if isinstance(existing, dict) and isinstance(value, Mapping):
            merged = dict(existing)
            _deep_merge_mapping(merged, provenance, value, scope=scope, path=path, prefix=f"{nested_key}.")
            target[key] = merged
        else:
            target[key] = copy.deepcopy(value)
            _record_nested_provenance(provenance, value, scope=scope, path=path, prefix=f"{nested_key}.")
        provenance[nested_key] = GitOpsConfigProvenance(scope=scope, path=path, key=nested_key)


def _record_nested_provenance(
    provenance: dict[str, GitOpsConfigProvenance],
    value: object,
    *,
    scope: str,
    path: str,
    prefix: str,
) -> None:
    if not isinstance(value, Mapping):
        return
    for key, child in value.items():
        nested_key = f"{prefix}{key}"
        provenance[nested_key] = GitOpsConfigProvenance(scope=scope, path=path, key=nested_key)
        _record_nested_provenance(provenance, child, scope=scope, path=path, prefix=f"{nested_key}.")


def _duplicate_id_blockers(workloads: Iterable[GitOpsWorkloadDefinition]) -> tuple[Any, ...]:
    by_id: dict[str, list[GitOpsWorkloadDefinition]] = defaultdict(list)
    for workload in workloads:
        by_id[workload.workload_id].append(workload)
    return tuple(
        issue(
            code="workload_id_duplicate",
            message=f"GitOps workload id is declared {len(items)} times",
            path=workload_id,
        )
        for workload_id, items in sorted(by_id.items())
        if len(items) > 1
    )


def _load_yaml(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return _mapping(payload)


def _load_optional_mapping(path: Path) -> Mapping[str, Any]:
    return _load_yaml(path) if path.exists() else {}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> tuple[Any, ...]:
    return tuple(value) if isinstance(value, list | tuple) else ()


def _optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _inferred_workload_id(manifest: Path) -> str:
    return manifest.with_suffix("").as_posix().removeprefix("manifests/").replace("/", "__").replace("-", "_")


__all__ = ["WorkloadCatalogResolver"]
