"""Closed, memory-only job body for the fixed TDS bootstrap.

This body contains credentials: never persist it, log it or use it as argv. It is
not worker output or evidence. Parsing binds declared identities, not file bytes,
SQL ownership or process authority; those require independent supervisor checks.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from hashlib import sha256
from pathlib import Path
from typing import Any

from dpone.contracts.mssql_tds_api import (
    EncodedNativeFile,
    NativeBulkTransportPolicy,
    TdsAttemptIdentity,
    TdsInputReceipt,
    canonical_json_bytes,
    strict_json_object,
)
from dpone.runtime.mssql_tds_decoder import validate_tds_layouts
from dpone.runtime.native_wire_models import NativeWireColumnLayout, SourceNativeWireContract

_MAX_BODY = 1 << 20
_ERROR = "tds_worker_request_invalid"


def _text(value: object, maximum: int, *, empty: bool = False) -> None:
    if type(value) is not str or (not empty and not value) or len(value) > maximum or "\0" in value:
        raise ValueError(_ERROR)
    value.encode("utf-8", errors="strict")


def _closed(value: object, model: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != {field.name for field in fields(model)}:
        raise ValueError(_ERROR)
    return value


def _python_policy(value: object) -> NativeBulkTransportPolicy:
    """Keep the existing Python job wire closed and backend-specific."""
    if type(value) is not dict:
        raise ValueError(_ERROR)
    policy = NativeBulkTransportPolicy.from_mapping(value)
    if policy.backend != "mssql_python" or policy.to_dict() != value:
        raise ValueError(_ERROR)
    return policy


def _wire(raw: object, input_mode: str) -> SourceNativeWireContract:
    value = dict(_closed(raw, SourceNativeWireContract))
    for key in (
        "schema_version",
        "source_system",
        "source_format",
        "target_format",
        "schema_hash",
        "query_hash",
        "type_layout_hash",
    ):
        _text(value[key], 256)
    if value["bcp_version"] is not None:
        _text(value["bcp_version"], 1024)
    for key in ("warnings", "blockers"):
        if type(value[key]) is not list or len(value[key]) > 64:
            raise ValueError(_ERROR)
        for item in value[key]:
            _text(item, 1024)
        value[key] = tuple(value[key])
    columns = value["columns"]
    if type(columns) is not list or not 1 <= len(columns) <= 1024:
        raise ValueError(_ERROR)
    layouts = []
    for raw_column in columns:
        column = _closed(raw_column, NativeWireColumnLayout)
        _text(column["name"], 128)
        if any(ord(char) < 32 for char in column["name"]):
            raise ValueError(_ERROR)
        for key in ("source_type", "target_type", "storage_type"):
            _text(column[key], 256)
        if type(column["nullable"]) is not bool or type(column["prefix_width"]) is not int:
            raise ValueError(_ERROR)
        for key in ("fixed_length", "precision", "scale"):
            if column[key] is not None and type(column[key]) is not int:
                raise ValueError(_ERROR)
        if column["encoding"] is not None:
            _text(column["encoding"], 64)
        layouts.append(NativeWireColumnLayout(**column))
    value["columns"] = tuple(layouts)
    result = SourceNativeWireContract(**value)
    validate_tds_layouts(result, input_mode=input_mode)
    return result


def _file(raw: object, identity: TdsAttemptIdentity) -> EncodedNativeFile:
    value = dict(_closed(raw, EncodedNativeFile))
    _text(value["path"], 4096)
    path = Path(value["path"])
    if not path.is_absolute():
        raise ValueError(_ERROR)
    if type(value["ordinal"]) is not int or value["ordinal"] != identity.ordinal:
        raise ValueError(_ERROR)
    TdsInputReceipt(value["rows"], value["encoded_bytes"], value["file_sha256"])
    if value["file_sha256"] != identity.file_sha256:
        raise ValueError(_ERROR)
    _text(value["typed_digest"], 256)
    value["path"] = path
    return EncodedNativeFile(**value)


@dataclass(frozen=True, repr=False)
class TdsWorkerJob:
    """Private typed input; repr intentionally exposes none of its fields."""

    identity: TdsAttemptIdentity
    policy: NativeBulkTransportPolicy
    file: EncodedNativeFile
    wire: SourceNativeWireContract
    connection_string: str
    max_row_bytes: int

    def __post_init__(self) -> None:
        try:
            _payload(self)
        except Exception:
            raise ValueError(_ERROR) from None


def _payload(job: TdsWorkerJob) -> dict[str, Any]:
    for value, model in (
        (job.identity, TdsAttemptIdentity),
        (job.policy, NativeBulkTransportPolicy),
        (job.file, EncodedNativeFile),
        (job.wire, SourceNativeWireContract),
    ):
        if type(value) is not model:
            raise ValueError(_ERROR)
    identity_raw = asdict(job.identity)
    identity = TdsAttemptIdentity(**identity_raw)
    policy_raw = job.policy.to_dict()
    policy = _python_policy(policy_raw)
    if sha256(canonical_json_bytes(policy_raw)).hexdigest() != identity.policy_sha256:
        raise ValueError(_ERROR)
    if type(job.file.path) is not type(Path("/")):
        raise ValueError(_ERROR)
    file_raw = asdict(job.file)
    file_raw["path"] = str(job.file.path)
    _file(file_raw, identity)
    if type(job.wire.columns) is not tuple or any(type(c) is not NativeWireColumnLayout for c in job.wire.columns):
        raise ValueError(_ERROR)
    if type(job.wire.warnings) is not tuple or type(job.wire.blockers) is not tuple:
        raise ValueError(_ERROR)
    wire_raw = job.wire.to_dict()
    _wire(wire_raw, policy.input)
    _text(job.connection_string, 16384)
    if type(job.max_row_bytes) is not int or not 1 <= job.max_row_bytes <= 2**63 - 1:
        raise ValueError(_ERROR)
    return {
        "version": 1,
        "identity": identity_raw,
        "policy": policy_raw,
        "file": file_raw,
        "wire": wire_raw,
        "connection_string": job.connection_string,
        "max_row_bytes": job.max_row_bytes,
    }


def encode_job(job: TdsWorkerJob) -> bytes:
    """Return bounded canonical private JSON bytes, without framing or any I/O."""
    try:
        if type(job) is not TdsWorkerJob:
            raise ValueError(_ERROR)
        body = canonical_json_bytes(_payload(job))
        if len(body) > _MAX_BODY:
            raise ValueError(_ERROR)
        return body
    except Exception:
        raise ValueError(_ERROR) from None


def decode_job(body: bytes) -> TdsWorkerJob:
    """Reject malformed, ambiguous or mismatched private input before execution."""
    try:
        if type(body) is not bytes or not 0 < len(body) <= _MAX_BODY:
            raise ValueError(_ERROR)
        value = strict_json_object(body)
        if set(value) != {"version", "identity", "policy", "file", "wire", "connection_string", "max_row_bytes"}:
            raise ValueError(_ERROR)
        if type(value["version"]) is not int or value["version"] != 1:
            raise ValueError(_ERROR)
        identity = TdsAttemptIdentity(**_closed(value["identity"], TdsAttemptIdentity))
        policy = _python_policy(value["policy"])
        file = _file(value["file"], identity)
        wire = _wire(value["wire"], policy.input)
        return TdsWorkerJob(identity, policy, file, wire, value["connection_string"], value["max_row_bytes"])
    except Exception:
        raise ValueError(_ERROR) from None
