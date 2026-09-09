"""Trust Center bundle relationship constants and summary IDs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol


class BundleArtifact(Protocol):
    payload: dict[str, Any]


TRUST_CENTER_STATUS_CHECKS = (
    ("data_product_trust_gate", {"allowed", "warning"}, "migration_bundle.data_product_trust_gate_blocked"),
    ("data_product_trust_report", {"allowed", "warning"}, "migration_bundle.data_product_trust_report_blocked"),
    ("data_product_trust_export", {"rendered"}, "migration_bundle.data_product_trust_export_blocked"),
)

TRUST_CENTER_PACK_BOUND = {
    "data_product_trust_gate": "migration_bundle.data_product_trust_gate_pack_id_mismatch",
    "data_product_trust_report": "migration_bundle.data_product_trust_report_pack_id_mismatch",
    "data_product_trust_export": "migration_bundle.data_product_trust_export_pack_id_mismatch",
}


def trust_center_summary_ids(artifacts: Mapping[str, BundleArtifact]) -> dict[str, Any]:
    return {
        "data_product_trust_gate_id": _payload_value(artifacts.get("data_product_trust_gate"), "trust_gate_id"),
        "data_product_trust_report_id": _payload_value(artifacts.get("data_product_trust_report"), "trust_report_id"),
        "data_product_trust_export_id": _payload_value(artifacts.get("data_product_trust_export"), "trust_export_id"),
    }


def _payload_value(artifact: BundleArtifact | None, key: str) -> Any:
    return artifact.payload.get(key) if artifact else None


__all__ = ["TRUST_CENTER_PACK_BOUND", "TRUST_CENTER_STATUS_CHECKS", "trust_center_summary_ids"]
