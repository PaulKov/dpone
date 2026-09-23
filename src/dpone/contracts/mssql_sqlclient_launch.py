"""Closed SqlClient bootstrap/readiness records; no execution or SQL authority.

Wire deadlines use Linux CLOCK_MONOTONIC integer nanoseconds, not managed
Stopwatch ticks. Digests bind canonical UTF-8 JSON (sorted keys, compact
separators, integer scalars) with an explicit domain prefix. All fields are
ASCII; cross-language producers must use these bytes, not serializer defaults.
Readiness validates reported facts against admission; observing actual files,
guards, pidfd identity and EOF remains the launch adapter's responsibility.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from typing import Any

from dpone.contracts.mssql_tds_worker import TdsProcessIdentity, _hash, _integer, _uuid
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape

MAX_LAUNCH_BYTES = 16384
_DIGEST_DOMAIN = b"dpone.sqlclient.launch.v1\x00"


@dataclass(frozen=True)
class SqlClientDescriptors:
    """Only these child descriptors survive exec, in addition to controlled stdio.

    The separate bootstrap gate is closed before exec. The input descriptor is
    already admitted and read-only; the managed worker never reopens its path.
    """

    startup: int
    credentials: int
    session: int
    grant: int
    result: int
    input: int

    def __post_init__(self) -> None:
        values = tuple(asdict(self).values())
        for value in values:
            _integer(value, 3, 2**31 - 1)
        if len(set(values)) != len(values):
            raise ValueError("mssql_native.sqlclient_descriptor_alias")


@dataclass(frozen=True)
class SqlClientLaunch:
    """Non-secret launch permission after the parent acquires the child pidfd.

    This record is not a SQL grant. Build and input digests refer to separately
    admitted immutable artifacts. No credentials or executable paths are carried.
    """

    schema_version: int
    nonce: str
    attempt_sha256: str
    build_sha256: str
    input_binding_sha256: str
    process: TdsProcessIdentity
    parent_pid: int
    startup_deadline_ns: int
    operation_deadline_ns: int
    address_space_bytes: int
    descriptors: SqlClientDescriptors

    def __post_init__(self) -> None:
        _integer(self.schema_version, 1, 1)
        _uuid(self.nonce)
        for value in (self.attempt_sha256, self.build_sha256, self.input_binding_sha256):
            _hash(value)
        _integer(self.parent_pid, 1, 2**31 - 1)
        _integer(self.startup_deadline_ns, 1)
        _integer(self.operation_deadline_ns, 1)
        _integer(self.address_space_bytes, 8 * 1024**3, 16 * 1024**3)
        if type(self.process) is not TdsProcessIdentity or type(self.descriptors) is not SqlClientDescriptors:
            raise ValueError("mssql_native.sqlclient_launch_record")
        if self.parent_pid == self.process.pid:
            raise ValueError("mssql_native.sqlclient_parent_identity")


@dataclass(frozen=True)
class SqlClientReady:
    """Measured managed-startup facts, bound to exactly one launch envelope."""

    schema_version: int
    launch_sha256: str
    process: TdsProcessIdentity
    address_space_bytes: int
    parent_death_signal: int
    core_limit_bytes: int
    runtime_version: str
    gc_heap_limit_bytes: int
    server_gc: bool
    roll_forward: str

    def __post_init__(self) -> None:
        _integer(self.schema_version, 1, 1)
        _hash(self.launch_sha256)
        if type(self.process) is not TdsProcessIdentity:
            raise ValueError("mssql_native.sqlclient_ready_process")
        _integer(self.address_space_bytes, 8 * 1024**3, 16 * 1024**3)
        _integer(self.parent_death_signal, 9, 9)
        _integer(self.core_limit_bytes, 0, 0)
        _integer(self.gc_heap_limit_bytes, 536870912, 536870912)
        if (
            type(self.runtime_version) is not str
            or self.runtime_version != "8.0.31"
            or self.server_gc is not False
            or type(self.roll_forward) is not str
            or self.roll_forward != "Disable"
        ):
            raise ValueError("mssql_native.sqlclient_runtime_profile")


def _encode(value: SqlClientLaunch | SqlClientReady, cls: type[SqlClientLaunch] | type[SqlClientReady]) -> bytes:
    if type(value) is not cls:
        raise ValueError("mssql_native.sqlclient_wire_type")
    payload = canonical_json_bytes(asdict(value))
    if len(payload) > MAX_LAUNCH_BYTES:
        raise ValueError("mssql_native.sqlclient_wire_size")
    return payload


def _decode(payload: bytes, cls: type) -> Any:
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= MAX_LAUNCH_BYTES:
            raise ValueError("size")
        value = record_shape(cls, strict_json_object(payload))
        value["process"] = construct_record(TdsProcessIdentity, value["process"])
        if cls is SqlClientLaunch:
            value["descriptors"] = construct_record(SqlClientDescriptors, value["descriptors"])
        return construct_record(cls, value)
    except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
        raise ValueError("mssql_native.sqlclient_wire_invalid") from None


def encode_launch(value: SqlClientLaunch) -> bytes:
    """Encode one unframed bootstrap object; the transport enforces EOF."""
    return _encode(value, SqlClientLaunch)


def decode_launch(payload: bytes) -> SqlClientLaunch:
    """Reject unknown/duplicate fields and coercions at every nested boundary."""
    return _decode(payload, SqlClientLaunch)


def encode_ready(value: SqlClientReady) -> bytes:
    """Encode measured readiness without asserting those facts were observed."""
    return _encode(value, SqlClientReady)


def decode_ready(payload: bytes) -> SqlClientReady:
    """Decode one bounded readiness body, independent of transport framing."""
    return _decode(payload, SqlClientReady)


def launch_digest(value: SqlClientLaunch) -> str:
    """SHA256(domain prefix + canonical launch bytes), lowercase hexadecimal."""
    return hashlib.sha256(_DIGEST_DOMAIN + encode_launch(value)).hexdigest()


def validate_ready(launch: SqlClientLaunch, ready: SqlClientReady, *, now_ns: int) -> None:
    """Require exact launch binding and both original deadlines, without renewal."""
    _integer(now_ns)
    if (
        ready.launch_sha256 != launch_digest(launch)
        or ready.process != launch.process
        or ready.address_space_bytes != launch.address_space_bytes
        or now_ns >= min(launch.startup_deadline_ns, launch.operation_deadline_ns)
    ):
        raise ValueError("mssql_native.sqlclient_readiness_mismatch")
