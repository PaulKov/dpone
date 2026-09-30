"""Closed private authority for source-free MSSQL native recovery."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlOperationRequest,
    MssqlTransactionAdmission,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
)

_AUTHORITY_KEYS = frozenset({"version", "kind", "bindings", "operation"})
_BINDING_KEYS = frozenset(
    {
        "authored_route_sha256",
        "verification_identity_sha256",
        "target_connection_sha256",
        "state_store_sha256",
        "work_root_sha256",
    }
)
_OPERATION_KEYS = frozenset(
    {
        "invocation",
        "target_identity",
        "route_fingerprint",
        "load_id",
        "target_database",
        "target_schema",
        "target_table",
        "strategy",
        "generation",
        "is_current_generation",
        "operation_key",
        "scope_hash",
        "owner_digest",
        "epoch",
        "lease_expires_at_utc",
    }
)
_INVOCATION_KEYS = frozenset({"run_id", "process", "task_partition"})


@dataclass(frozen=True, slots=True)
class MssqlNativeRecoveryBindings:
    """Hashes recomputed by the recovery composition root before target I/O."""

    authored_route_sha256: str
    verification_identity_sha256: str
    target_connection_sha256: str
    state_store_sha256: str
    work_root_sha256: str

    def __post_init__(self) -> None:
        for value in asdict(self).values():
            _require_sha256(value)


def build_mssql_native_recovery_authority(
    admission: MssqlTransactionAdmission,
    bindings: MssqlNativeRecoveryBindings,
) -> dict[str, Any]:
    """Serialize only the exact current operation and non-secret binding hashes."""

    operation = admission.operation
    if operation is None:
        raise ValueError("mssql_native.recovery_operation_required")
    request = operation.attempt.request
    return {
        "version": 1,
        "kind": "dpone.mssql-native-recovery-authority.v1",
        "bindings": asdict(bindings),
        "operation": {
            "invocation": asdict(request.invocation),
            "target_identity": request.target_identity.hex(),
            "route_fingerprint": request.route_fingerprint.hex(),
            "load_id": request.load_id,
            "target_database": request.target_database,
            "target_schema": request.target_schema,
            "target_table": request.target_table,
            "strategy": request.strategy,
            "generation": operation.attempt.generation,
            "is_current_generation": operation.attempt.is_current_generation,
            "operation_key": operation.operation_key.hex(),
            "scope_hash": operation.scope_hash.hex(),
            "owner_digest": operation.owner_digest.hex(),
            "epoch": operation.epoch,
            "lease_expires_at_utc": (
                None if operation.lease_expires_at_utc is None else operation.lease_expires_at_utc.isoformat()
            ),
        },
    }


def compose_mssql_native_recovery_bindings(
    admission: MssqlTransactionAdmission,
    *,
    verification_identity: object,
    state_store: object,
    work_root: Path,
) -> MssqlNativeRecoveryBindings:
    """Derive stable private bindings from already admitted runtime authorities."""

    operation = admission.operation
    receipt = admission.replay_receipt
    invocation_key = getattr(verification_identity, "invocation_key", None)
    plan = getattr(verification_identity, "plan", None)
    target_id = getattr(plan, "target_id", None)
    if (
        (operation is None) == (receipt is None)
        or not isinstance(invocation_key, str)
        or not isinstance(target_id, str)
    ):
        raise ValueError("mssql_native.recovery_operation_required")
    if operation is not None:
        route_fingerprint = operation.attempt.route_fingerprint
        target_identity = operation.attempt.target_identity
    else:
        assert receipt is not None
        route_fingerprint = receipt.route_fingerprint
        target_identity = receipt.target_identity
    path = getattr(state_store, "_path", None)
    state_location = str(Path(path).resolve()) if isinstance(path, (str, Path)) else None
    state_identity = {
        "adapter": f"{type(state_store).__module__}.{type(state_store).__qualname__}",
        "location": state_location,
        "target": target_id,
    }
    return MssqlNativeRecoveryBindings(
        authored_route_sha256=route_fingerprint.hex(),
        verification_identity_sha256=invocation_key,
        target_connection_sha256=target_identity.hex(),
        state_store_sha256=_hash_document(state_identity),
        work_root_sha256=sha256(str(work_root.resolve()).encode()).hexdigest(),
    )


def restore_mssql_native_recovery_admission(
    authority: object,
    *,
    expected_bindings: MssqlNativeRecoveryBindings,
) -> MssqlTransactionAdmission:
    """Authenticate one closed authority and reconstruct its current admission."""

    root = _closed_object(authority, _AUTHORITY_KEYS)
    if root["version"] != 1 or root["kind"] != "dpone.mssql-native-recovery-authority.v1":
        raise ValueError("mssql_native.recovery_authority_invalid")
    stored_bindings = _closed_object(root["bindings"], _BINDING_KEYS)
    if stored_bindings != asdict(expected_bindings):
        raise ValueError("mssql_native.recovery_binding_changed")
    values = _closed_object(root["operation"], _OPERATION_KEYS)
    invocation_values = _closed_object(values["invocation"], _INVOCATION_KEYS)
    try:
        request = MssqlAttemptRequest(
            InvocationIdentity(**invocation_values),
            _digest(values["target_identity"]),
            _digest(values["route_fingerprint"]),
            _text(values["load_id"]),
            _text(values["target_database"]),
            _text(values["target_schema"]),
            _text(values["target_table"]),
            _text(values["strategy"]),
        )
        attempt = MssqlTransactionAttempt(
            request,
            _positive_int(values["generation"]),
            _boolean(values["is_current_generation"]),
        )
        expiry = values["lease_expires_at_utc"]
        parsed_expiry = None if expiry is None else datetime.fromisoformat(_text(expiry))
        operation_request = MssqlOperationRequest(
            _digest(values["scope_hash"]),
            _digest(values["owner_digest"]),
            parsed_expiry,
        )
        operation_key = _digest(values["operation_key"])
        if operation_key != operation_request.operation_key(attempt):
            raise ValueError("mssql_native.recovery_operation_changed")
        operation = MssqlTransactionOperation(
            attempt,
            operation_key,
            operation_request.scope_hash,
            operation_request.owner_digest,
            _positive_int(values["epoch"]),
            operation_request.lease_expires_at_utc,
        )
    except ValueError as error:
        if str(error) == "mssql_native.recovery_operation_changed":
            raise
        raise ValueError("mssql_native.recovery_authority_invalid") from error
    return MssqlTransactionAdmission(operation=operation)


def _closed_object(value: object, keys: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys or any(not isinstance(key, str) for key in value):
        raise ValueError("mssql_native.recovery_authority_invalid")
    return value


def _require_sha256(value: object) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("mssql_native.recovery_binding_invalid")


def _digest(value: object) -> bytes:
    _require_sha256(value)
    return bytes.fromhex(cast(str, value))


def _text(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("mssql_native.recovery_authority_invalid")
    return value


def _positive_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("mssql_native.recovery_authority_invalid")
    return value


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError("mssql_native.recovery_authority_invalid")
    return value


def _hash_document(value: Mapping[str, object]) -> str:
    from dpone.contracts.strict_json import canonical_json_bytes

    return sha256(canonical_json_bytes(value)).hexdigest()


__all__ = [
    "MssqlNativeRecoveryBindings",
    "build_mssql_native_recovery_authority",
    "compose_mssql_native_recovery_bindings",
    "restore_mssql_native_recovery_admission",
]
