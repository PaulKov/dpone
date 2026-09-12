"""Pure identity and lifecycle contracts for one MSSQL R1 V3 transaction."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    require_digest,
    require_positive,
    require_sql_int,
    require_uuid,
)

_SESSION_IDENTITY_DOMAIN = b"dpone-r1-mssql-session-identity-v3\0"
_TRANSACTION_BINDING_DOMAIN = b"dpone-r1-transaction-binding-v3\0"


class MssqlR1TransactionStateV3(StrEnum):
    NEW = "new"
    ACTIVE = "active"
    COMMIT_DISPATCHED = "commit_dispatched"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    OUTCOME_UNKNOWN = "outcome_unknown"
    CLOSED = "closed"


_ALLOWED_TRANSITIONS = {
    MssqlR1TransactionStateV3.NEW: {MssqlR1TransactionStateV3.ACTIVE},
    MssqlR1TransactionStateV3.ACTIVE: {
        MssqlR1TransactionStateV3.COMMIT_DISPATCHED,
        MssqlR1TransactionStateV3.ROLLED_BACK,
    },
    MssqlR1TransactionStateV3.COMMIT_DISPATCHED: {
        MssqlR1TransactionStateV3.COMMITTED,
        MssqlR1TransactionStateV3.OUTCOME_UNKNOWN,
    },
    MssqlR1TransactionStateV3.COMMITTED: {MssqlR1TransactionStateV3.CLOSED},
    MssqlR1TransactionStateV3.ROLLED_BACK: {MssqlR1TransactionStateV3.CLOSED},
    MssqlR1TransactionStateV3.OUTCOME_UNKNOWN: {MssqlR1TransactionStateV3.CLOSED},
    MssqlR1TransactionStateV3.CLOSED: set(),
}


@dataclass(frozen=True, slots=True)
class MssqlR1SqlServerSessionIdentityV3:
    server_instance_identity_sha256: bytes
    database_id: int
    database_guid: UUID
    database_family_guid: UUID
    recovery_fork_guid: UUID
    spid: int
    session_context_transaction_id: UUID

    def __post_init__(self) -> None:
        require_digest(self.server_instance_identity_sha256, "server_instance_identity_sha256")
        require_sql_int(self.database_id, "database_id")
        for name in (
            "database_guid",
            "database_family_guid",
            "recovery_fork_guid",
            "session_context_transaction_id",
        ):
            require_uuid(getattr(self, name), name)
        require_positive(self.spid, "spid")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _SESSION_IDENTITY_DOMAIN,
            (
                self.server_instance_identity_sha256,
                self.database_id,
                self.database_guid,
                self.database_family_guid,
                self.recovery_fork_guid,
                self.spid,
                self.session_context_transaction_id,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SqlServerSessionIdentityV3:
        return cls(*decode_canonical_bytes(payload, _SESSION_IDENTITY_DOMAIN, field_count=7))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1TransactionBindingV3:
    transaction_id: UUID
    factory_binding_token: bytes
    session_identity_bytes: bytes
    session_identity_digest: bytes
    state: MssqlR1TransactionStateV3

    def __post_init__(self) -> None:
        require_uuid(self.transaction_id, "transaction_id")
        if not isinstance(self.factory_binding_token, bytes) or len(self.factory_binding_token) != 16:
            raise MssqlR1V3ContractError("factory_binding_token must be exactly 128 bits")
        session = MssqlR1SqlServerSessionIdentityV3.from_canonical_bytes(self.session_identity_bytes)
        if session.digest != require_digest(self.session_identity_digest, "session_identity_digest"):
            raise MssqlR1V3ContractError("session identity digest differs from exact canonical bytes")
        if session.session_context_transaction_id != self.transaction_id:
            raise MssqlR1V3ContractError("session context transaction differs from transaction binding")
        if not isinstance(self.state, MssqlR1TransactionStateV3):
            raise MssqlR1V3ContractError("transaction state is unsupported")

    @property
    def session_identity(self) -> MssqlR1SqlServerSessionIdentityV3:
        return MssqlR1SqlServerSessionIdentityV3.from_canonical_bytes(self.session_identity_bytes)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _TRANSACTION_BINDING_DOMAIN,
            (
                self.transaction_id,
                self.factory_binding_token,
                self.session_identity_bytes,
                self.session_identity_digest,
                self.state,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    def transitioned(self, state: MssqlR1TransactionStateV3) -> MssqlR1TransactionBindingV3:
        if state not in _ALLOWED_TRANSITIONS[self.state]:
            raise MssqlR1V3ContractError("transaction lifecycle transition is invalid")
        return MssqlR1TransactionBindingV3(
            self.transaction_id,
            self.factory_binding_token,
            self.session_identity_bytes,
            self.session_identity_digest,
            state,
        )

    def assert_active(self) -> None:
        if self.state is not MssqlR1TransactionStateV3.ACTIVE:
            raise MssqlR1V3ContractError("transaction binding is not ACTIVE")

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1TransactionBindingV3:
        values = list(decode_canonical_bytes(payload, _TRANSACTION_BINDING_DOMAIN, field_count=5))
        values[1] = expect_bytes(values[1], "factory_binding_token")
        values[2] = expect_bytes(values[2], "session_identity_bytes")
        values[3] = expect_bytes(values[3], "session_identity_digest")
        values[4] = expect_enum(MssqlR1TransactionStateV3, values[4], "transaction_state")
        return cls(*values)  # type: ignore[arg-type]


__all__ = [
    "MssqlR1SqlServerSessionIdentityV3",
    "MssqlR1TransactionBindingV3",
    "MssqlR1TransactionStateV3",
]
