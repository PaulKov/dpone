"""File-IO facade for data product remediation commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductRemediationFacade:
    """Thin CLI facade; remediation decisions live in readiness modules."""

    def plan(
        self,
        *,
        manifest_path: str,
        trust_snapshot_path: str | None = None,
        trust_gate_path: str | None = None,
        bundle_path: str | None = None,
        evidence_dir: str | None = None,
        registry_path: str | None = None,
    ) -> dict[str, Any]:
        bundle = _read_optional(bundle_path)
        return (
            _remediation()
            .DataProductRemediationPlanner()
            .plan(
                manifest=_read_mapping(manifest_path),
                trust_snapshot=_read_optional(trust_snapshot_path),
                trust_gate=_read_optional(trust_gate_path),
                evidence_payloads=_evidence_payloads(evidence_dir),
                registry_records=_registry_records(registry_path),
                pack_id=_optional_str(bundle.get("pack_id")),
                bundle_id=_optional_str(bundle.get("bundle_id")),
            )
        )

    def runbook(self, *, plan_path: str) -> dict[str, Any]:
        return _rendering().RemediationRenderer().runbook(plan=_read_mapping(plan_path))

    def gate(self, *, plan_path: str, profile: str = "prod_strict") -> dict[str, Any]:
        return _remediation().DataProductRemediationGate().evaluate(plan=_read_mapping(plan_path), profile=profile)

    def closeout(self, *, plan_path: str, evidence_dir: str | None = None) -> dict[str, Any]:
        return (
            _remediation()
            .DataProductRemediationCloseout()
            .evaluate(plan=_read_mapping(plan_path), evidence_payloads=_evidence_payloads(evidence_dir))
        )

    def report(self, *, gate_path: str, closeout_path: str | None = None) -> dict[str, Any]:
        return (
            _rendering()
            .RemediationRenderer()
            .report(
                gate=_read_mapping(gate_path),
                closeout=_read_optional(closeout_path),
            )
        )


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


def _optional_str(raw: Any) -> str | None:
    return str(raw) if raw else None


def _remediation() -> Any:
    return import_module("dpone.readiness.data_product_remediation")


def _rendering() -> Any:
    return import_module("dpone.readiness.data_product_remediation_rendering")


__all__ = ["DataProductRemediationFacade"]
