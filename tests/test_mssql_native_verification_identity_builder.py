from __future__ import annotations

import pytest

from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.contracts.mssql_native_verification import NativeVerificationBackend
from dpone.contracts.mssql_native_verification_identity import (
    TARGET_LOCAL_DIGEST_ALGORITHM_ID,
    build_bcp_target_local_verification_identity,
)
from dpone.contracts.mssql_native_writer import BCP_STAGE_PROOF


def _plan() -> NativeChunkPlan:
    return NativeChunkPlan("run", "target", "query", "window", "schema", "wire")


def test_builder_produces_stable_closed_p1_bcp_identity() -> None:
    first = build_bcp_target_local_verification_identity(_plan(), timeout_seconds=900)
    second = build_bcp_target_local_verification_identity(_plan(), timeout_seconds=900)

    assert first == second
    assert first.import_backend == "bcp"
    assert first.verification_backend is NativeVerificationBackend.TARGET_LOCAL
    assert first.writer_proof_capability == BCP_STAGE_PROOF
    assert first.digest_algorithm_id == TARGET_LOCAL_DIGEST_ALGORITHM_ID
    assert all(
        len(value) == 64
        for value in (
            first.companion_protocol_sha256,
            first.companion_package_sha256,
            first.capability_layout_sha256,
            first.timeout_policy_sha256,
        )
    )


def test_builder_binds_plan_and_timeout_without_accepting_boolean_or_zero() -> None:
    baseline = build_bcp_target_local_verification_identity(_plan(), timeout_seconds=900)
    changed_timeout = build_bcp_target_local_verification_identity(_plan(), timeout_seconds=901)
    changed_plan = build_bcp_target_local_verification_identity(
        NativeChunkPlan("run", "target", "query", "other-window", "schema", "wire"),
        timeout_seconds=900,
    )

    assert baseline.invocation_key != changed_timeout.invocation_key
    assert baseline.invocation_key != changed_plan.invocation_key
    for value in (0, -1, True, 1.5, "900"):
        with pytest.raises(ValueError, match="mssql_native.invalid_barrier_timeout"):
            build_bcp_target_local_verification_identity(_plan(), timeout_seconds=value)  # type: ignore[arg-type]
