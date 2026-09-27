"""Canonical composition-root identity for the P1 target-local BCP path."""

from __future__ import annotations

from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.contracts.mssql_native_verification import (
    NativeVerificationBackend,
    NativeVerificationIdentityV2,
    canonical_sha256,
)
from dpone.contracts.mssql_native_writer import BCP_STAGE_PROOF

TARGET_LOCAL_DIGEST_ALGORITHM_ID = "mssql-native-sha256-sum-v1"

_ABSENT_PROTOCOL = {
    "domain": "dpone.mssql-native.companion-protocol.v1",
    "state": "absent-for-bcp-p1",
}
_ABSENT_PACKAGE = {
    "domain": "dpone.mssql-native.companion-package.v1",
    "state": "absent-for-bcp-p1",
}
_CAPABILITY_LAYOUT = {
    "domain": "dpone.mssql-native.capability-layout.v1",
    "digest_algorithm_id": TARGET_LOCAL_DIGEST_ALGORITHM_ID,
    "import_backend": "bcp",
    "verification_backend": NativeVerificationBackend.TARGET_LOCAL.value,
    "writer_proof_capability": BCP_STAGE_PROOF,
}


def build_bcp_target_local_verification_identity(
    plan: NativeChunkPlan,
    *,
    timeout_seconds: int,
) -> NativeVerificationIdentityV2:
    """Bind one P1 invocation to dpone's reviewed BCP proof capabilities.

    The caller supplies only the already frozen chunk plan and the bounded
    barrier timeout. Versioned capability constants stay framework-owned, so
    independent composition roots cannot invent incompatible identity hashes.
    """

    if not isinstance(plan, NativeChunkPlan):
        raise ValueError("mssql_native.invalid_v2_identity")
    if type(timeout_seconds) is not int or timeout_seconds < 1:
        raise ValueError("mssql_native.invalid_barrier_timeout")
    return NativeVerificationIdentityV2(
        plan=plan,
        import_backend="bcp",
        verification_backend=NativeVerificationBackend.TARGET_LOCAL,
        companion_protocol_sha256=canonical_sha256(_ABSENT_PROTOCOL),
        companion_package_sha256=canonical_sha256(_ABSENT_PACKAGE),
        capability_layout_sha256=canonical_sha256(_CAPABILITY_LAYOUT),
        digest_algorithm_id=TARGET_LOCAL_DIGEST_ALGORITHM_ID,
        timeout_policy_sha256=canonical_sha256(
            {
                "domain": "dpone.mssql-native.timeout-policy.v1",
                "attempt_barrier_timeout_seconds": timeout_seconds,
                "deadline": "process-local-monotonic",
            }
        ),
    )


__all__ = [
    "TARGET_LOCAL_DIGEST_ALGORITHM_ID",
    "build_bcp_target_local_verification_identity",
]
