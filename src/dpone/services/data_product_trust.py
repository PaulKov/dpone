"""File-IO facade for Data Product Trust Center commands."""

from __future__ import annotations

import glob
import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductTrustFacade:
    """Thin CLI facade; trust decisions live in readiness modules."""

    def index_lake(
        self,
        *,
        manifest_patterns: tuple[str, ...],
        evidence_dir: str | None = None,
        registry_path: str | None = None,
        bundle_dir: str | None = None,
    ) -> dict[str, Any]:
        del bundle_dir
        return (
            _trust_index()
            .TrustEvidenceLakeIndexer()
            .index(
                manifests=tuple(_read_mapping(path) for path in _expand(manifest_patterns)),
                evidence_payloads=tuple(_evidence_payloads(evidence_dir)),
                registry_records=tuple(_registry_records(registry_path)),
            )
        )

    def query(
        self,
        *,
        index_path: str,
        product_id: str | None = None,
        domain: str | None = None,
        artifact_kind: str | None = None,
        status: str | None = None,
        owner: str | None = None,
        max_results: int = 500,
    ) -> dict[str, Any]:
        return (
            _trust_index()
            .TrustQueryEngine()
            .query(
                index=_read_mapping(index_path),
                product_id=product_id,
                domain=domain,
                artifact_kind=artifact_kind,
                status=status,
                owner=owner,
                max_results=max_results,
            )
        )

    def snapshot(self, *, index_path: str, product_id: str, profile: str = "prod_strict") -> dict[str, Any]:
        return (
            _trust_snapshot()
            .TrustSnapshotBuilder()
            .snapshot(
                index=_read_mapping(index_path),
                product_id=product_id,
                profile=profile,
            )
        )

    def gate(self, *, snapshot_path: str, profile: str = "prod_strict") -> dict[str, Any]:
        return _trust_snapshot().TrustGate().evaluate(snapshot=_read_mapping(snapshot_path), profile=profile)

    def report(self, *, snapshot_path: str, gate_path: str | None = None) -> dict[str, Any]:
        return (
            _trust_rendering()
            .TrustReportRenderer()
            .report(
                snapshot=_read_mapping(snapshot_path),
                gate=_read_optional(gate_path),
            )
        )

    def export(self, *, snapshot_path: str, target: str = "json") -> dict[str, Any]:
        return _trust_rendering().TrustExportRenderer().export(snapshot=_read_mapping(snapshot_path), target=target)


def _expand(patterns: tuple[str, ...]) -> tuple[str, ...]:
    paths: list[str] = []
    for pattern in patterns:
        matches = sorted(glob.glob(pattern, recursive=True))
        paths.extend(matches or [pattern])
    return tuple(dict.fromkeys(paths))


def _evidence_payloads(path: str | None) -> tuple[Mapping[str, Any], ...]:
    if not path:
        return ()
    root = Path(path)
    if not root.exists():
        return ()
    sources = sorted([*root.rglob("*.json"), *root.rglob("*.yaml"), *root.rglob("*.yml")])
    return tuple(_read_mapping(str(source)) for source in sources)


def _registry_records(path: str | None) -> tuple[Mapping[str, Any], ...]:
    if not path or path.endswith((".sqlite", ".sqlite3", ".db")):
        return ()
    raw = _read_mapping(path)
    records = raw.get("records")
    return tuple(item for item in records if isinstance(item, Mapping)) if isinstance(records, list) else ()


def _read_optional(path: str | None) -> dict[str, Any]:
    return _read_mapping(path) if path else {}


def _read_mapping(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _trust_index() -> Any:
    return import_module("dpone.readiness.data_product_trust_index")


def _trust_snapshot() -> Any:
    return import_module("dpone.readiness.data_product_trust_snapshot")


def _trust_rendering() -> Any:
    return import_module("dpone.readiness.data_product_trust_rendering")


__all__ = ["DataProductTrustFacade"]
