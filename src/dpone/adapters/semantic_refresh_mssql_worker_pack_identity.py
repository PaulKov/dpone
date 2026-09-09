"""Authenticate one persisted MSSQL worker-pack identity projection."""

from __future__ import annotations

from dpone.ports.semantic_refresh_mssql_worker_pack import (
    MssqlStaticProjectionIdentity,
    mssql_static_projection_identity_from_json,
    mssql_worker_pack_fingerprint,
)


def mssql_worker_pack_projection_from_storage(
    *,
    pack_fingerprint: object,
    activation_authority_receipt_sha256: object,
    authority_store_ref: object,
    plan_bundle_sha256: object,
    workflow_plan_sha256: object,
    run_execution_bundle_sha256: object,
    run_guard_closure_sha256: object,
    static_projection_identity_json: object,
) -> MssqlStaticProjectionIdentity:
    """Reconstruct and authenticate the complete immutable pack identity."""

    values = (
        pack_fingerprint,
        activation_authority_receipt_sha256,
        authority_store_ref,
        plan_bundle_sha256,
        workflow_plan_sha256,
        run_execution_bundle_sha256,
        run_guard_closure_sha256,
        static_projection_identity_json,
    )
    if any(not isinstance(value, str) for value in values):
        raise TypeError("persisted worker-pack identity values must be text")
    assert isinstance(pack_fingerprint, str)
    assert isinstance(activation_authority_receipt_sha256, str)
    assert isinstance(authority_store_ref, str)
    assert isinstance(plan_bundle_sha256, str)
    assert isinstance(workflow_plan_sha256, str)
    assert isinstance(run_execution_bundle_sha256, str)
    assert isinstance(run_guard_closure_sha256, str)
    assert isinstance(static_projection_identity_json, str)
    projection = mssql_static_projection_identity_from_json(static_projection_identity_json)
    expected_fingerprint = mssql_worker_pack_fingerprint(
        projection_identity=projection,
        run_execution_bundle_sha256=run_execution_bundle_sha256,
        activation_authority_receipt_sha256=activation_authority_receipt_sha256,
        authority_store_ref=authority_store_ref,
        run_guard_closure_sha256=run_guard_closure_sha256,
    )
    if (
        projection.plan_bundle_sha256 != plan_bundle_sha256
        or projection.workflow_plan_sha256 != workflow_plan_sha256
        or pack_fingerprint != expected_fingerprint
    ):
        raise ValueError("persisted worker-pack identity differs from its canonical fingerprint")
    return projection


__all__ = ["mssql_worker_pack_projection_from_storage"]
