"""Governance publish receipts and verification."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness.data_product_governance_constants import SUPPORTED_GOVERNANCE_PROVIDERS
from dpone.readiness.migration_control import stable_fingerprint

GOVERNANCE_RECEIPT_SCHEMA = "dpone.data_product_governance_publish_receipt.v1"
GOVERNANCE_VERIFICATION_SCHEMA = "dpone.data_product_governance_publish_verification.v1"


class GovernancePublisher:
    """Builds deterministic publish receipts; network execution is explicit."""

    def publish(
        self,
        *,
        payload: Mapping[str, Any],
        provider: str,
        connection: Mapping[str, Any] | None,
        execute: bool,
    ) -> dict[str, Any]:
        normalized = provider.lower().strip()
        connection = connection or {}
        blockers = _publish_blockers(payload, normalized, connection, execute)
        endpoint = str(connection.get("endpoint") or "") if execute and not blockers else ""
        status = "blocked" if blockers else "published" if execute else "dry_run"
        receipt: dict[str, Any] = {
            "schema_version": GOVERNANCE_RECEIPT_SCHEMA,
            "status": status,
            "provider": normalized,
            "payload_id": payload.get("payload_id"),
            "payload_sha256": payload.get("payload_sha256"),
            "product_id": payload.get("product_id"),
            "pack_id": payload.get("pack_id"),
            "bundle_id": payload.get("bundle_id"),
            "idempotency_key": _idempotency_key(payload, normalized),
            "network_writes": _network_writes(endpoint, normalized, execute, blockers),
            "blockers": blockers,
            "warnings": list(payload.get("warnings", [])),
        }
        receipt["publish_receipt_id"] = stable_fingerprint(receipt)
        return receipt


class GovernancePublishVerifier:
    """Verifies publish receipts without calling external catalog APIs."""

    def verify(self, *, receipt: Mapping[str, Any]) -> dict[str, Any]:
        blockers = _verification_blockers(receipt)
        status = "blocked" if blockers else "verified"
        verification: dict[str, Any] = {
            "schema_version": GOVERNANCE_VERIFICATION_SCHEMA,
            "status": status,
            "provider": receipt.get("provider"),
            "publish_receipt_id": receipt.get("publish_receipt_id"),
            "payload_id": receipt.get("payload_id"),
            "payload_sha256": receipt.get("payload_sha256"),
            "product_id": receipt.get("product_id"),
            "pack_id": receipt.get("pack_id"),
            "bundle_id": receipt.get("bundle_id"),
            "checks": _checks(receipt, blockers),
            "blockers": blockers,
            "warnings": list(receipt.get("warnings", [])),
        }
        verification["publish_verification_id"] = stable_fingerprint(verification)
        return verification


def _publish_blockers(
    payload: Mapping[str, Any],
    provider: str,
    connection: Mapping[str, Any],
    execute: bool,
) -> list[str]:
    blockers: list[str] = []
    if provider not in SUPPORTED_GOVERNANCE_PROVIDERS:
        blockers.append(f"data_product_governance_publish.unsupported_provider:{provider}")
    if payload.get("provider") != provider:
        blockers.append("data_product_governance_publish.provider_mismatch")
    if payload.get("status") != "rendered":
        blockers.append("data_product_governance_publish.payload_not_rendered")
    blockers.extend(str(item) for item in payload.get("blockers", []) if str(item))
    if execute and not connection.get("endpoint"):
        blockers.append("data_product_governance_publish.connection_required")
    return list(dict.fromkeys(blockers))


def _idempotency_key(payload: Mapping[str, Any], provider: str) -> str:
    return stable_fingerprint({"provider": provider, "payload_id": payload.get("payload_id")})


def _network_writes(endpoint: str, provider: str, execute: bool, blockers: list[str]) -> list[dict[str, str]]:
    if not execute or blockers:
        return []
    return [{"provider": provider, "method": "POST", "endpoint": endpoint, "status": "prepared"}]


def _verification_blockers(receipt: Mapping[str, Any]) -> list[str]:
    blockers = [str(item) for item in receipt.get("blockers", []) if str(item)]
    if receipt.get("status") not in {"dry_run", "published"}:
        blockers.append("data_product_governance_verify.receipt_status_blocked")
    if not str(receipt.get("payload_id") or "").startswith("sha256:"):
        blockers.append("data_product_governance_verify.payload_id_missing")
    expected = stable_fingerprint({"provider": receipt.get("provider"), "payload_id": receipt.get("payload_id")})
    if receipt.get("idempotency_key") != expected:
        blockers.append("data_product_governance_verify.idempotency_key_mismatch")
    if _invalid_payload_hash(receipt):
        blockers.append("data_product_governance_verify.payload_hash_mismatch")
    return list(dict.fromkeys(blockers))


def _invalid_payload_hash(receipt: Mapping[str, Any]) -> bool:
    raw = str(receipt.get("payload_sha256") or "")
    if not raw.startswith("sha256:") or len(raw) != 71:
        return True
    return raw == "sha256:" + "0" * 64


def _checks(receipt: Mapping[str, Any], blockers: list[str]) -> list[dict[str, Any]]:
    return [
        {
            "name": "publish_receipt",
            "status": "blocked" if blockers else "passed",
            "provider": receipt.get("provider"),
        }
    ]


__all__ = [
    "GOVERNANCE_RECEIPT_SCHEMA",
    "GOVERNANCE_VERIFICATION_SCHEMA",
    "GovernancePublisher",
    "GovernancePublishVerifier",
]
