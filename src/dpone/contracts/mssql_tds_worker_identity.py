"""Immutable identities and observations for one TDS attempt."""

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import UUID


def _integer(value: Any, minimum: int = 0, maximum: int = 2**63 - 1) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("mssql_native.tds_invalid_integer")


def _text(value: Any, maximum: int = 256) -> None:
    if type(value) is not str or not value or len(value) > maximum or any(ord(c) < 32 for c in value):
        raise ValueError("mssql_native.tds_invalid_text")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeError:
        raise ValueError("mssql_native.tds_invalid_text") from None


def _hash(value: Any) -> None:
    if type(value) is not str or re.fullmatch("[0-9a-f]{64}", value) is None:
        raise ValueError("mssql_native.tds_invalid_sha256")


def _uuid(value: Any) -> None:
    _text(value, 36)
    try:
        valid = str(UUID(value)) == value and UUID(value).int != 0
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("mssql_native.tds_invalid_uuid")


class TdsAttemptPhase(StrEnum):
    CREATION_INTENT = "creation_intent"
    PREPARED = "prepared"
    LAUNCH_INTENT = "launch_intent"
    SPAWNED_WAITING = "spawned_waiting"
    RUNNING = "running"
    EXITED = "exited"
    VERIFIED = "verified"
    CONTAINMENT_REQUIRED = "containment_required"
    CONTAINED = "contained"
    RETIREMENT_REQUIRED = "retirement_required"
    RETIRED = "retired"


class TdsAttemptError(StrEnum):
    STARTUP_TIMEOUT = "startup_timeout"
    OPERATION_TIMEOUT = "operation_timeout"
    RESOURCE_LIMIT = "resource_limit"
    DECODER = "decoder"
    DRIVER = "driver"
    CONSTRAINT = "constraint"
    PERMISSION = "permission"
    CONNECTION = "connection"
    PROCESS_UNKNOWN = "process_unknown"
    OWNERSHIP = "ownership"
    FENCING = "fencing"
    VERIFICATION = "verification"
    PROTOCOL = "protocol"
    CLEANUP = "cleanup"
    COORDINATOR_LOST = "coordinator_lost"


@dataclass(frozen=True)
class TdsAttemptIdentity:
    target_key: str
    run_id: str
    ordinal: int
    attempt: int
    plan_sha256: str
    policy_sha256: str
    implementation_sha256: str
    file_sha256: str
    database: str
    schema: str
    table: str
    owner_binding: str

    def __post_init__(self) -> None:
        for value in (self.target_key, self.run_id):
            _text(value)
        for value in (self.database, self.schema, self.table):
            _text(value, 128)
        _integer(self.ordinal)
        _integer(self.attempt, 0, 2)
        for value in (
            self.plan_sha256,
            self.policy_sha256,
            self.implementation_sha256,
            self.file_sha256,
            self.owner_binding,
        ):
            _hash(value)


@dataclass(frozen=True)
class TdsAttemptOwnership:
    owner: str
    fence: int
    supervisor_id: str

    def __post_init__(self) -> None:
        _text(self.owner)
        _integer(self.fence, 1)
        _uuid(self.supervisor_id)


@dataclass(frozen=True)
class TdsProcessIdentity:
    host_sha256: str
    boot_id: str
    pid: int
    start_ticks: int

    def __post_init__(self) -> None:
        _hash(self.host_sha256)
        _uuid(self.boot_id)
        _integer(self.pid, 1, 2**31 - 1)
        _integer(self.start_ticks)


@dataclass(frozen=True)
class TdsChildExit:
    """Direct-child exit and exclusive reaping; no SQL settlement is implied."""

    identity: TdsProcessIdentity
    exit_code: int
    reaped: bool

    def __post_init__(self) -> None:
        if type(self.identity) is not TdsProcessIdentity:
            raise ValueError("mssql_native.tds_exit_identity_invalid")
        if type(self.exit_code) is not int or not -255 <= self.exit_code <= 255 or type(self.reaped) is not bool:
            raise ValueError("mssql_native.tds_exit_observation_invalid")


@dataclass(frozen=True)
class TdsObjectIdentity:
    object_id: int
    fingerprint: str

    def __post_init__(self) -> None:
        _integer(self.object_id, 1, 2**31 - 1)
        _hash(self.fingerprint)


@dataclass(frozen=True)
class ParentAuthority:
    """Settled parent proof; aborted requires authoritative rollback/no commit."""

    kind: str
    receipt_sha256: str

    def __post_init__(self) -> None:
        if type(self.kind) is not str or self.kind not in {"published", "aborted"}:
            raise ValueError("mssql_native.tds_parent_unsettled")
        _hash(self.receipt_sha256)
