"""Canonical identity derivation for one SqlClient PREPARED attempt."""

from __future__ import annotations

from dataclasses import asdict
from hashlib import sha256
from typing import Protocol
from uuid import UUID

from dpone.contracts.mssql_permission_preparation_capabilities import (
    EncodedNativeFile,
    NativeChunkPlan,
    TdsAttemptIdentity,
)
from dpone.contracts.mssql_sqlclient_native_chunk_receipt import plan_sha256
from dpone.contracts.strict_json import canonical_json_bytes

ERROR = "mssql_native.sqlclient_prepared_attempt_context_invalid"
_IDENTITY_DOMAIN = b"dpone.sqlclient.prepared-attempt-context.v1\0"


class PreparedAttemptIdentityDeployment(Protocol):
    """Target and implementation identity required by canonical derivation."""

    @property
    def implementation_sha256(self) -> str: ...

    @property
    def database(self) -> str: ...

    @property
    def schema(self) -> str: ...

    @property
    def table(self) -> str: ...

    @property
    def owner_binding(self) -> str: ...


def attempt_number(plan: NativeChunkPlan, file: EncodedNativeFile, attempt_id: str) -> int:
    """Parse the closed retry ordinal from the canonical attempt identifier."""
    prefix = f"{plan.run_id}-{file.ordinal}-"
    try:
        value = int(attempt_id.removeprefix(prefix))
    except (TypeError, ValueError, OverflowError):
        raise ValueError(ERROR) from None
    if attempt_id != f"{prefix}{value}" or value not in (0, 1, 2):
        raise ValueError(ERROR)
    return value


def parent_identity(
    deployment: PreparedAttemptIdentityDeployment,
    plan: NativeChunkPlan,
    file: EncodedNativeFile,
    attempt: int,
) -> TdsAttemptIdentity:
    """Bind the source file, target and admitted implementation to one attempt."""
    policy = plan.transport
    if policy is None or policy.backend != "mssql_sqlclient":
        raise ValueError(ERROR)
    return TdsAttemptIdentity(
        plan.target_id,
        plan.run_id,
        file.ordinal,
        attempt,
        plan_sha256(plan),
        plan_sha256(policy),
        deployment.implementation_sha256,
        file.file_sha256,
        deployment.database,
        deployment.schema,
        deployment.table,
        deployment.owner_binding,
    )


def operation_id(parent: TdsAttemptIdentity, purpose: bytes) -> UUID:
    """Derive one purpose-separated operation UUID from the canonical parent."""
    digest = sha256(_IDENTITY_DOMAIN + purpose + b"\0" + canonical_json_bytes(asdict(parent))).digest()
    return UUID(bytes=digest[:16])


__all__ = ("attempt_number", "operation_id", "parent_identity")
