"""Stable dependency boundary for the optional SqlClient companion."""

from dpone.contracts.mssql_native_verification import NativeVerificationIdentityV2
from dpone.contracts.mssql_native_verification_identity import (
    build_bcp_target_local_verification_identity,
    build_sqlclient_target_local_verification_identity,
)
from dpone.contracts.mssql_sqlclient_ipc import (
    PROTOCOL,
    PROTOCOL_V2,
    RESULT_MAX_BYTES,
    MssqlSqlClientCredentials,
    applock_resource,
    decode_observation_frame,
    encode_credentials_frame,
    encode_request_frame,
)

__all__ = [
    "PROTOCOL",
    "PROTOCOL_V2",
    "RESULT_MAX_BYTES",
    "MssqlSqlClientCredentials",
    "NativeVerificationIdentityV2",
    "applock_resource",
    "build_bcp_target_local_verification_identity",
    "build_sqlclient_target_local_verification_identity",
    "decode_observation_frame",
    "encode_credentials_frame",
    "encode_request_frame",
]
