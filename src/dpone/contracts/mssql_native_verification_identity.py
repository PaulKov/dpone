"""Canonical composition-root identity for the P1 target-local BCP path."""

from __future__ import annotations

from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.contracts.mssql_native_verification import (
    NativeVerificationBackend,
    NativeVerificationIdentityV2,
    canonical_sha256,
)
from dpone.contracts.mssql_native_writer import BCP_STAGE_PROOF, SQLCLIENT_SESSION_PROOF

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
_SQLCLIENT_PROTOCOL = "dpone.mssql-sqlclient.ipc.v1"
_SQLCLIENT_PROTOCOL_V2 = "dpone.mssql-sqlclient.ipc.v2"
_SQLCLIENT_TYPES = ("bigint", "float(53)", "nvarchar(max)", "datetime2(6)")


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


def build_sqlclient_target_local_verification_identity(
    plan: NativeChunkPlan,
    *,
    timeout_seconds: int,
    companion: object,
    layout_version: int = 1,
) -> NativeVerificationIdentityV2:
    """Bind one invocation to an already verified optional companion."""
    if not isinstance(plan, NativeChunkPlan):
        raise ValueError("mssql_native.invalid_v2_identity")
    if type(timeout_seconds) is not int or timeout_seconds < 1:
        raise ValueError("mssql_native.invalid_barrier_timeout")
    protocol = getattr(companion, "protocol", None)
    protocols = getattr(companion, "protocols", (protocol,))
    artifact = getattr(companion, "artifact_sha256", None)
    writer = getattr(companion, "writer_identity_sha256", None)
    runtime = getattr(companion, "runtime_identity_sha256", None)
    runtime_major = getattr(companion, "runtime_major", None)
    if (
        protocol != _SQLCLIENT_PROTOCOL
        or type(layout_version) is not int
        or layout_version not in {1, 2}
        or not isinstance(protocols, tuple)
        or any(type(value) is not str for value in protocols)
        or (_SQLCLIENT_PROTOCOL if layout_version == 1 else _SQLCLIENT_PROTOCOL_V2) not in protocols
        or runtime_major != 10
        or any(type(value) is not str for value in (artifact, writer, runtime))
    ):
        raise ValueError("mssql_native.invalid_sqlclient_companion")
    assert isinstance(artifact, str) and isinstance(writer, str) and isinstance(runtime, str)
    digests = (artifact, writer, runtime)
    if any(len(value) != 64 or value.lower() != value for value in digests) or any(
        any(character not in "0123456789abcdef" for character in value) for value in digests
    ):
        raise ValueError("mssql_native.invalid_sqlclient_companion")
    selected_protocol = _SQLCLIENT_PROTOCOL if layout_version == 1 else _SQLCLIENT_PROTOCOL_V2
    capability_layout = (
        {
            "domain": "dpone.mssql-native.capability-layout.v2",
            "digest_algorithm_id": TARGET_LOCAL_DIGEST_ALGORITHM_ID,
            "import_backend": "mssql_sqlclient",
            "verification_backend": NativeVerificationBackend.TARGET_LOCAL.value,
            "writer_proof_capability": SQLCLIENT_SESSION_PROOF,
            "writer_identity_sha256": writer,
            "runtime_identity_sha256": runtime,
            "runtime_major": runtime_major,
            "target_types": _SQLCLIENT_TYPES,
        }
        if layout_version == 1
        else {
            "domain": "dpone.mssql-native.capability-layout.v3",
            "digest_algorithm_id": TARGET_LOCAL_DIGEST_ALGORITHM_ID,
            "import_backend": "mssql_sqlclient",
            "verification_backend": NativeVerificationBackend.TARGET_LOCAL.value,
            "writer_proof_capability": SQLCLIENT_SESSION_PROOF,
            "writer_identity_sha256": writer,
            "runtime_identity_sha256": runtime,
            "runtime_major": runtime_major,
            "target_types": _SQLCLIENT_TYPES,
            "layout_version": 2,
            "technical_columns": (
                ("__dpone__native_row_hash", "binary(32)"),
                ("__dpone__mutation_version", "rowversion"),
            ),
        }
    )
    return NativeVerificationIdentityV2(
        plan=plan,
        import_backend="mssql_sqlclient",
        verification_backend=NativeVerificationBackend.TARGET_LOCAL,
        writer_proof_capability=SQLCLIENT_SESSION_PROOF,
        companion_protocol_sha256=canonical_sha256(
            {"domain": "dpone.mssql-sqlclient.protocol.v1", "protocol": selected_protocol}
        ),
        companion_package_sha256=artifact,
        capability_layout_sha256=canonical_sha256(capability_layout),
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
    "build_sqlclient_target_local_verification_identity",
]
