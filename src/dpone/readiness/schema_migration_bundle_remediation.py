"""Data product remediation bundle relationship constants."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol


class BundleArtifact(Protocol):
    payload: dict[str, Any]


REMEDIATION_STATUS_CHECKS = (
    ("data_product_remediation_gate", {"allowed", "warning"}, "migration_bundle.data_product_remediation_gate_blocked"),
    (
        "data_product_remediation_runbook",
        {"rendered", "warning"},
        "migration_bundle.data_product_remediation_runbook_blocked",
    ),
    (
        "data_product_remediation_closeout",
        {"allowed", "warning"},
        "migration_bundle.data_product_remediation_closeout_blocked",
    ),
    (
        "data_product_remediation_report",
        {"allowed", "warning"},
        "migration_bundle.data_product_remediation_report_blocked",
    ),
    (
        "data_product_remediation_execution_plan",
        {"ready", "warning", "disabled"},
        "migration_bundle.data_product_remediation_execution_plan_blocked",
    ),
    (
        "data_product_remediation_execution_run",
        {"executed", "dry_run", "disabled"},
        "migration_bundle.data_product_remediation_execution_run_blocked",
    ),
    (
        "data_product_remediation_execution_certificate",
        {"certified", "warning"},
        "migration_bundle.data_product_remediation_execution_certificate_blocked",
    ),
    (
        "data_product_remediation_execution_report",
        {"certified", "warning"},
        "migration_bundle.data_product_remediation_execution_report_blocked",
    ),
)

REMEDIATION_PACK_BOUND = {
    "data_product_remediation_gate": "migration_bundle.data_product_remediation_gate_pack_id_mismatch",
    "data_product_remediation_runbook": "migration_bundle.data_product_remediation_runbook_pack_id_mismatch",
    "data_product_remediation_closeout": "migration_bundle.data_product_remediation_closeout_pack_id_mismatch",
    "data_product_remediation_report": "migration_bundle.data_product_remediation_report_pack_id_mismatch",
    "data_product_remediation_execution_plan": (
        "migration_bundle.data_product_remediation_execution_plan_pack_id_mismatch"
    ),
    "data_product_remediation_execution_run": "migration_bundle.data_product_remediation_execution_run_pack_id_mismatch",
    "data_product_remediation_execution_certificate": (
        "migration_bundle.data_product_remediation_execution_certificate_pack_id_mismatch"
    ),
    "data_product_remediation_execution_report": (
        "migration_bundle.data_product_remediation_execution_report_pack_id_mismatch"
    ),
}


def remediation_summary_ids(artifacts: Mapping[str, BundleArtifact]) -> dict[str, Any]:
    return {
        "data_product_remediation_gate_id": _payload_value(
            artifacts.get("data_product_remediation_gate"), "remediation_gate_id"
        ),
        "data_product_remediation_runbook_id": _payload_value(
            artifacts.get("data_product_remediation_runbook"), "remediation_runbook_id"
        ),
        "data_product_remediation_closeout_id": _payload_value(
            artifacts.get("data_product_remediation_closeout"), "remediation_closeout_id"
        ),
        "data_product_remediation_report_id": _payload_value(
            artifacts.get("data_product_remediation_report"), "remediation_report_id"
        ),
        "data_product_remediation_execution_plan_id": _payload_value(
            artifacts.get("data_product_remediation_execution_plan"), "remediation_execution_plan_id"
        ),
        "data_product_remediation_execution_run_id": _payload_value(
            artifacts.get("data_product_remediation_execution_run"), "remediation_execution_run_id"
        ),
        "data_product_remediation_execution_certificate_id": _payload_value(
            artifacts.get("data_product_remediation_execution_certificate"), "remediation_execution_certificate_id"
        ),
        "data_product_remediation_execution_report_id": _payload_value(
            artifacts.get("data_product_remediation_execution_report"), "remediation_execution_report_id"
        ),
    }


def _payload_value(artifact: BundleArtifact | None, key: str) -> Any:
    return artifact.payload.get(key) if artifact else None


__all__ = ["REMEDIATION_PACK_BOUND", "REMEDIATION_STATUS_CHECKS", "remediation_summary_ids"]
