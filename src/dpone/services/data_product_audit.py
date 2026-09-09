"""File-IO facade for data product audit archive commands."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductAuditFacade:
    """Thin CLI facade; audit decisions live in readiness modules."""

    def archive_plan(
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
        return _audit().AuditEvidenceArchivePlanner().plan(manifest=_read_mapping(manifest_path), evidence=evidence)

    def archive_run(self, *, plan_path: str, execute: bool) -> dict[str, Any]:
        return (
            _audit()
            .AuditEvidenceArchiver(_store().LocalFsAuditArchiveStore())
            .run(
                plan=_read_mapping(plan_path),
                execute=execute,
            )
        )

    def archive_verify(self, *, archive_run_path: str) -> dict[str, Any]:
        return (
            _audit()
            .AuditArchiveVerifier(_store().LocalFsAuditArchiveStore())
            .verify(archive_run=_read_mapping(archive_run_path))
        )

    def retention_plan(
        self,
        *,
        manifest_path: str,
        archive_verification_path: str,
        legal_hold_path: str | None = None,
    ) -> dict[str, Any]:
        return (
            _audit()
            .AuditRetentionPlanner()
            .plan(
                manifest=_read_mapping(manifest_path),
                archive_verification=_read_mapping(archive_verification_path),
                legal_hold=_read_optional(legal_hold_path),
            )
        )

    def legal_hold_apply(
        self,
        *,
        archive_run_path: str,
        reason: str,
        authority_gate_path: str | None = None,
    ) -> dict[str, Any]:
        return (
            _audit()
            .LegalHoldService()
            .apply(
                archive_run=_read_mapping(archive_run_path),
                reason=reason,
                authority_gate=_read_optional(authority_gate_path),
            )
        )


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
    evidence: dict[str, dict[str, Any]] = {}
    for record in _registry_records(path):
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


def _audit() -> Any:
    return import_module("dpone.readiness.data_product_audit_retention")


def _store() -> Any:
    return import_module("dpone.readiness.data_product_audit_retention_store")


__all__ = ["DataProductAuditFacade"]
