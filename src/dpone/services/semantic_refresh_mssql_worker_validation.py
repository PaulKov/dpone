"""Validation of the MSSQL worker admission acknowledgement."""

from __future__ import annotations

from dpone.ports.semantic_refresh_mssql_activation import MssqlActivatedPackRegistration
from dpone.ports.semantic_refresh_mssql_run_authority import MssqlWorkerRunAuthority


def validate_semantic_refresh_admitted_run(
    protected: MssqlWorkerRunAuthority,
    *,
    registration: MssqlActivatedPackRegistration,
    operation_ids: tuple[str, ...],
) -> None:
    """Require the protected readback to equal the exact admitted run."""

    if (
        protected.admission_status != "ADMITTED"
        or protected.pack_fingerprint != registration.pack_fingerprint
        or protected.activation_authority_receipt_sha256 != registration.activation_authority_receipt_sha256
        or protected.authority_store_ref != registration.authority_store_ref
        or protected.plan_bundle_sha256 != registration.plan_bundle_sha256
        or protected.run_execution_bundle_sha256 != registration.run_execution_bundle_sha256
        or protected.projection_identity != registration.projection_identity
        or tuple(item.operation_id for item in protected.attempts) != operation_ids
        or any(item.journal_status != "PREPARING" for item in protected.attempts)
    ):
        raise ValueError("semantic-refresh admitted MSSQL run differs from worker authority")


__all__ = ["validate_semantic_refresh_admitted_run"]
