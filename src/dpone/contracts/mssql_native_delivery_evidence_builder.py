"""Build SqlClient delivery evidence only from verified runtime authorities."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any

from dpone.contracts.mssql_native_delivery_evidence import validate_mssql_native_delivery_evidence
from dpone.contracts.mssql_native_verification import NativeVerificationIdentityV2


@dataclass(frozen=True, slots=True)
class DeliveryTimings:
    """Independent monotonic durations; overlapping spans remain independent."""

    source_read_seconds: float
    bulk_write_seconds: float
    target_digest_seconds: float
    preparation_seconds: float
    publication_seconds: float
    confirmed_visibility_seconds: float


@dataclass(frozen=True, slots=True)
class DeliveryCorrectness:
    """Facts observed after durable publication and target reconciliation."""

    source_rows: int
    published_rows: int
    receipt_count: int
    target_digest_verified: bool
    publication_verified: bool


@dataclass(frozen=True, slots=True)
class DeliveryRecovery:
    """Retry and unknown-outcome history for this exact invocation."""

    retry_count: int
    unknown_outcome_count: int
    classification: str


def build_sqlclient_delivery_evidence(
    *,
    identity: NativeVerificationIdentityV2,
    companion: Any,
    source_commit_sha: str,
    dirty: bool,
    runner_image_sha256: str,
    environment_receipt_sha256: str,
    timings: DeliveryTimings,
    correctness: DeliveryCorrectness,
    recovery: DeliveryRecovery,
    synthetic_only: bool,
    privacy_scan_status: str,
) -> dict[str, Any]:
    """Bind a PASS candidate to the identity admitted by production composition."""
    if type(synthetic_only) is not bool:
        raise ValueError("mssql_native.delivery_evidence.synthetic_classification_required")
    if identity.import_backend != "mssql_sqlclient" or (
        getattr(companion, "artifact_sha256", None) != identity.companion_package_sha256
    ):
        raise ValueError("mssql_native.delivery_evidence.backend_identity_mismatch")
    payload = {
        "schema_version": 2,
        "kind": "dpone.mssql-native-delivery-evidence.v2",
        "subject": {
            "invocation_id": identity.invocation_key,
            "source_commit_sha": source_commit_sha,
            "dirty": dirty,
            "runner_image_sha256": runner_image_sha256,
            "environment_receipt_sha256": environment_receipt_sha256,
        },
        "backend": {
            "import_backend": identity.import_backend,
            "backend_identity_sha256": identity.capability_layout_sha256,
            "wire_identity_sha256": sha256(identity.plan.wire_fingerprint.encode("utf-8")).hexdigest(),
            "package_version": getattr(companion, "package_version", None),
        },
        "timings": asdict(timings),
        "resources": {"business_rows_read_back": 0, "bcp_process_count": 0},
        "correctness": asdict(correctness),
        "recovery": asdict(recovery),
        "privacy": {"synthetic_only": synthetic_only, "scan_status": privacy_scan_status},
        "status": "PASS",
    }
    validate_mssql_native_delivery_evidence(payload)
    return payload


__all__ = [
    "DeliveryCorrectness",
    "DeliveryRecovery",
    "DeliveryTimings",
    "build_sqlclient_delivery_evidence",
]
