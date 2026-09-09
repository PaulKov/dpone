"""Audit reporting helpers for schema migration evidence registry."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint
from dpone.readiness.schema_migration_evidence_registry_constants import REGISTRY_AUDIT_SCHEMA


class MigrationEvidenceAuditReporter:
    """Builds deterministic audit reports from registry records."""

    def build(
        self,
        *,
        records: Sequence[Mapping[str, Any]],
        target: str | None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict[str, Any]:
        normalized = tuple(dict(item) for item in records)
        warnings = _audit_warnings(normalized)
        payload: dict[str, Any] = {
            "schema_version": REGISTRY_AUDIT_SCHEMA,
            "target": target,
            "date_from": date_from,
            "date_to": date_to,
            "record_count": len(normalized),
            "pack_count": len({item.get("pack_id") for item in normalized if item.get("pack_id")}),
            "records": list(normalized),
            "warnings": warnings,
        }
        payload["audit_report_id"] = stable_fingerprint(payload)
        payload["markdown"] = _audit_markdown(payload)
        return payload


def _audit_warnings(records: Sequence[Mapping[str, Any]]) -> list[str]:
    warnings: list[str] = []
    stages = {str(item.get("stage")) for item in records}
    if records and not any(item.get("trust_verification_id") for item in records):
        warnings.append("missing_trust")
    for required in ("approved", "promoted", "applied"):
        if records and required not in stages:
            warnings.append(f"missing_{required}")
    if stages & {"verified", "watched", "closed"}:
        for required in ("watched", "closed"):
            if required not in stages:
                warnings.append(f"missing_{required}")
    if stages & {"remediated", "rollback_certified"} and "rollback_certified" not in stages:
        warnings.append("missing_rollback_certified")
    if stages & {"backup_certified"}:
        for required in ("backup_created", "restore_rehearsed"):
            if required not in stages:
                warnings.append(f"missing_{required}")
    if stages & {"recovery_point_recorded", "recovery_chain_verified", "recovery_restore_rehearsed"}:
        for required in ("recovery_point_recorded", "recovery_chain_verified"):
            if required not in stages:
                warnings.append(f"missing_{required}")
    if any(item.get("status") == "blocked" for item in records):
        warnings.append("blocked_records_present")
    return warnings


def _audit_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Schema Migration Evidence Audit",
        "",
        f"- target: {payload.get('target')}",
        f"- records: {payload.get('record_count')}",
        f"- packs: {payload.get('pack_count')}",
        "",
        "| Recorded At | Environment | Stage | Status | Pack | Bundle |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for record in payload.get("records", []):
        if isinstance(record, Mapping):
            lines.append(
                f"| {record.get('recorded_at')} | {record.get('environment')} | {record.get('stage')} | "
                f"{record.get('status')} | {record.get('pack_id')} | {record.get('bundle_id')} |"
            )
    warnings = payload.get("warnings", [])
    if warnings:
        lines.extend(["", "## Warnings", "", *[f"- {item}" for item in warnings]])
    return "\n".join(lines) + "\n"


__all__ = ["MigrationEvidenceAuditReporter"]
