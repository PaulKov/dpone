"""Cohesive port surface for the MSSQL native delivery route.

Runtime services depend on this route boundary instead of knowing how its
window, chunk, writer, and optional SqlClient capabilities are partitioned.
The focused modules remain the canonical definitions for embedders that need a
single capability.
"""

from dpone.ports.bounded_window import (
    WindowContractError,
    WindowLease,
    WindowOutcomeUnknown,
    WindowStore,
    WindowTransientError,
)
from dpone.ports.mssql_native_chunks import (
    EncodedNativeFile,
    NativeChunkImporter,
    NativeChunkLimits,
    NativeChunkPlan,
    NativeChunkReceipt,
    NativeStageComplete,
)
from dpone.ports.mssql_native_writer import (
    BCP_STAGE_PROOF,
    SQLCLIENT_SESSION_PROOF,
    NativeStageColumnMapping,
    NativeStageProcessProof,
    NativeStageWriteGrant,
    NativeStageWriteMetrics,
    NativeStageWriteObservation,
    NativeStageWriteOutcome,
    NativeStageWriteRequest,
    OperationDeadline,
)
from dpone.ports.mssql_sqlclient import (
    PROTOCOL,
    PROTOCOL_V2,
    RESULT_MAX_BYTES,
    MssqlSqlClientCredentials,
    NativeVerificationIdentityV2,
    build_bcp_target_local_verification_identity,
    build_sqlclient_target_local_verification_identity,
    decode_observation_frame,
    encode_credentials_frame,
    encode_request_frame,
)

__all__ = [
    "BCP_STAGE_PROOF",
    "PROTOCOL",
    "PROTOCOL_V2",
    "RESULT_MAX_BYTES",
    "SQLCLIENT_SESSION_PROOF",
    "EncodedNativeFile",
    "MssqlSqlClientCredentials",
    "NativeChunkImporter",
    "NativeChunkLimits",
    "NativeChunkPlan",
    "NativeChunkReceipt",
    "NativeStageColumnMapping",
    "NativeStageComplete",
    "NativeStageProcessProof",
    "NativeStageWriteGrant",
    "NativeStageWriteMetrics",
    "NativeStageWriteObservation",
    "NativeStageWriteOutcome",
    "NativeStageWriteRequest",
    "NativeVerificationIdentityV2",
    "OperationDeadline",
    "WindowContractError",
    "WindowLease",
    "WindowOutcomeUnknown",
    "WindowStore",
    "WindowTransientError",
    "build_bcp_target_local_verification_identity",
    "build_sqlclient_target_local_verification_identity",
    "decode_observation_frame",
    "encode_credentials_frame",
    "encode_request_frame",
]
