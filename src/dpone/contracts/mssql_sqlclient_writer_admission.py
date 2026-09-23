"""Private pure contract operations used by exact SqlClient writer admission."""

import math
from hashlib import sha256
from typing import cast

from dpone.contracts import mssql_sqlclient_stage_identity as stage_contract
from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy
from dpone.contracts.mssql_sqlclient_input import (
    SqlClientInputDescriptor,
    decode_input_descriptor,
    encode_input_descriptor,
    input_descriptor_digest,
)
from dpone.contracts.mssql_sqlclient_permission_grant import SqlClientPermissionGrantRequest
from dpone.contracts.mssql_sqlclient_restricted_writer_verify import SqlClientRestrictedWriterVerifyRequest
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand, TdsDirectorySnapshot
from dpone.contracts.mssql_tds_validation import deadline_seconds
from dpone.contracts.mssql_tds_worker import TdsAttemptPhase, TdsAttemptSnapshot
from dpone.contracts.strict_json import canonical_json_bytes

_ERROR = "mssql_native.sqlclient_writer_admission_unknown"
_POLICY_FIELDS = (
    "backend",
    "input",
    "max_worker_address_space_bytes",
    "batch_rows",
    "startup_timeout_seconds",
    "operation_timeout_seconds",
    "terminate_timeout_seconds",
    "drop_timeout_seconds",
)


def _policy_record(policy: NativeBulkTransportPolicy) -> dict[str, object]:
    value = {name: object.__getattribute__(policy, name) for name in _POLICY_FIELDS}
    max_input = object.__getattribute__(policy, "max_input_batch_bytes")
    if (
        any(type(value[name]) is not str for name in ("backend", "input"))
        or any(type(value[name]) is not int for name in _POLICY_FIELDS[2:])
        or (max_input is not None and type(max_input) is not int)
    ):
        raise ValueError(_ERROR)
    if value["backend"] == "mssql_sqlclient":
        value["max_input_batch_bytes"] = max_input
    return value


def validate_writer_admission_limits(
    now: float,
    startup_deadline: float,
    operation_deadline: float,
    termination_timeout_seconds: int,
    max_worker_address_space_bytes: int,
) -> None:
    """Validate caller-controlled scalars without traversing live authority."""
    if (
        any(
            type(value) is not float or not math.isfinite(value)
            for value in (now, startup_deadline, operation_deadline)
        )
        or not now < startup_deadline <= operation_deadline
        or type(termination_timeout_seconds) is not int
        or not 0 < termination_timeout_seconds <= 60
        or type(max_worker_address_space_bytes) is not int
        or not 8 << 30 <= max_worker_address_space_bytes <= 16 << 30
    ):
        raise ValueError(_ERROR)


def validate_writer_admission_values(
    *,
    attempt: object,
    directory: object,
    grant_request: object,
    verify_request: object,
    stage: object,
    input_descriptor: object,
    input_snapshot: bytes,
    policy: object,
    policy_snapshot: bytes,
    startup_deadline: float,
    operation_deadline: float,
    preparation_deadline: float,
    captured_operation_deadline_ns: int,
    termination_timeout_seconds: int,
    max_worker_address_space_bytes: int,
) -> tuple[bytes, str]:
    """Validate immutable P10a values and return their canonical bindings."""
    if (
        type(attempt) is not TdsAttemptSnapshot
        or attempt.state.phase is not TdsAttemptPhase.PREPARED
        or type(directory) is not TdsDirectorySnapshot
        or not directory.state.slots
        or directory.state.slots[-1].command is not TdsCoordinatorCommand.VERIFY
        or not directory.state.slots[-1].settled
        or directory.state.parent != attempt.state.identity
        or type(verify_request) is not SqlClientRestrictedWriterVerifyRequest
        or type(grant_request) is not SqlClientPermissionGrantRequest
        or type(stage) is not stage_contract.SqlClientStageIdentity
        or any(
            type(value) is not stage_contract.SqlClientStageIdentity
            for value in (grant_request.stage, verify_request.stage)
        )
        or stage != grant_request.stage
        or stage != verify_request.stage
        or verify_request.parent != attempt.state.identity
        or grant_request.parent != attempt.state.identity
        or stage_contract.stage_object_identity(stage) != attempt.state.object_identity
        or verify_request.implementation_sha256 != attempt.state.identity.implementation_sha256
    ):
        raise ValueError(_ERROR)
    stage_snapshot = stage_contract.encode_stage_identity(stage)
    if stage_contract.decode_stage_identity(stage_snapshot) != stage:
        raise ValueError(_ERROR)
    if (
        type(input_descriptor) is not SqlClientInputDescriptor
        or encode_input_descriptor(input_descriptor) != input_snapshot
        or decode_input_descriptor(input_snapshot) != input_descriptor
        or input_descriptor.expected.file_sha256 != attempt.state.identity.file_sha256
    ):
        raise ValueError(_ERROR)
    if type(policy) is not NativeBulkTransportPolicy:
        raise ValueError(_ERROR)
    policy_record = _policy_record(policy)
    captured_ceiling = deadline_seconds(captured_operation_deadline_ns)
    if (
        policy_record["backend"] != "mssql_sqlclient"
        or canonical_json_bytes(policy_record) != policy_snapshot
        or sha256(policy_snapshot).hexdigest() != attempt.state.identity.policy_sha256
        or max_worker_address_space_bytes != policy_record["max_worker_address_space_bytes"]
        or termination_timeout_seconds > cast(int, policy_record["terminate_timeout_seconds"])
        or type(preparation_deadline) is not float
        or startup_deadline > preparation_deadline
        or operation_deadline > preparation_deadline
        or startup_deadline > captured_ceiling
        or operation_deadline > captured_ceiling
    ):
        raise ValueError(_ERROR)
    return stage_snapshot, input_descriptor_digest(input_descriptor)


__all__ = ("validate_writer_admission_limits", "validate_writer_admission_values")
