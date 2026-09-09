"""File-IO facade for data product compliance control commands."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductComplianceFacade:
    """Thin CLI facade; compliance decisions live in readiness modules."""

    def plan(
        self,
        *,
        manifest_path: str,
        bundle_path: str | None = None,
        registry_path: str | None = None,
        evidence_dir: str | None = None,
    ) -> dict[str, Any]:
        bundle = _read_optional(bundle_path)
        evidence = _evidence(evidence_dir)
        evidence.update(_bundle_evidence(bundle_path, bundle))
        evidence.update(_registry_evidence(registry_path))
        return (
            _compliance()
            .ComplianceControlPlanner()
            .plan(
                manifest=_read_mapping(manifest_path),
                evidence=evidence,
                pack_id=bundle.get("pack_id") if bundle else None,
                bundle_id=bundle.get("bundle_id") if bundle else None,
            )
        )

    def evaluate(self, *, plan_path: str, observed_at: str | None = None) -> dict[str, Any]:
        return (
            _compliance()
            .ComplianceControlEvaluator()
            .evaluate(
                plan=_read_mapping(plan_path),
                observed_at=observed_at,
            )
        )

    def gate(self, *, evaluation_path: str, profile: str) -> dict[str, Any]:
        return _compliance().ComplianceGate().evaluate(evaluation=_read_mapping(evaluation_path), profile=profile)

    def audit_package(self, *, gate_path: str) -> dict[str, Any]:
        return _compliance().AuditPackageRenderer().render(gate=_read_mapping(gate_path))


def _evidence(path: str | None) -> dict[str, dict[str, Any]]:
    if not path:
        return {}
    evidence: dict[str, dict[str, Any]] = {}
    for source in sorted(Path(path).rglob("*")):
        if source.is_file() and source.suffix.lower() in {".json", ".yaml", ".yml"}:
            try:
                payload = _read_mapping(str(source))
            except (OSError, ValueError, json.JSONDecodeError, yaml.YAMLError):
                continue
            evidence[_kind_from_schema(payload) or source.stem.replace("-", "_")] = payload
    return evidence


def _bundle_evidence(bundle_path: str | None, bundle: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not bundle_path or not bundle:
        return {}
    evidence: dict[str, dict[str, Any]] = {}
    for artifact in bundle.get("artifacts", []):
        if isinstance(artifact, Mapping) and artifact.get("kind") and artifact.get("path"):
            source = _artifact_path(str(artifact["path"]), bundle_path)
            if source.exists():
                evidence[str(artifact["kind"])] = _read_mapping(str(source))
    return evidence


def _registry_evidence(path: str | None) -> dict[str, dict[str, Any]]:
    records = _registry_records(path)
    evidence: dict[str, dict[str, Any]] = {}
    for record in records:
        for ref in record.get("artifact_refs", []):
            if isinstance(ref, Mapping) and ref.get("kind"):
                evidence.setdefault(str(ref["kind"]), dict(ref))
    return evidence


def _registry_records(path: str | None) -> tuple[dict[str, Any], ...]:
    if not path:
        return ()
    source = Path(path)
    if source.suffix.lower() in {".sqlite", ".sqlite3", ".db"}:
        return _sqlite_records(source)
    payload = _read_mapping(path)
    records = payload.get("records", [])
    return tuple(dict(item) for item in records if isinstance(item, Mapping)) if isinstance(records, list) else ()


def _sqlite_records(path: Path) -> tuple[dict[str, Any], ...]:
    if not path.exists():
        return ()
    with sqlite3.connect(path) as conn:
        rows = conn.execute("select payload_json from evidence_records order by recorded_at, record_id").fetchall()
    return tuple(dict(raw) for (text,) in rows if isinstance((raw := json.loads(str(text))), Mapping))


def _artifact_path(path: str, bundle_path: str) -> Path:
    raw = Path(path)
    return raw if raw.is_absolute() or raw.exists() else Path(bundle_path).parent / raw


def _kind_from_schema(payload: Mapping[str, Any]) -> str | None:
    schema = str(payload.get("schema_version") or "")
    if schema.startswith("dpone.") and schema.endswith(".v1"):
        return schema.removeprefix("dpone.").removesuffix(".v1")
    return None


def _read_optional(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(path) if path else None


def _read_mapping(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _compliance() -> Any:
    return import_module("dpone.readiness.data_product_compliance")


__all__ = ["DataProductComplianceFacade"]
