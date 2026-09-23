"""Closed SqlClient completion binding, never SQL settlement or replay authority.

The caller retains raw bytes before parsing and separately observes pipe EOF,
process exit/reaping, remote settlement and exact-stage verification. An error
may precede grant acceptance even when the parent already recorded grant intent.
"""

from dataclasses import dataclass
from hashlib import sha256

from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_sqlclient_launch import SqlClientLaunch, launch_digest
from dpone.contracts.mssql_tds_result import TdsWorkerResult, decode_result_payload, encode_result_payload
from dpone.contracts.mssql_tds_validation import _hash, _integer, _uuid
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import record_shape

_ERROR = "mssql_native.sqlclient_result_invalid"
_LIMIT = 16384
_EMPTY_HASH = sha256(b"").hexdigest()


@dataclass(frozen=True)
class SqlClientResult:
    """Exactly one original launch/attempt and optional accepted grant identity."""

    schema_version: int
    launch_sha256: str
    attempt_sha256: str
    grant_id: str | None
    result: TdsWorkerResult

    def __post_init__(self) -> None:
        _integer(self.schema_version, 1, 1)
        _hash(self.launch_sha256)
        _hash(self.attempt_sha256)
        if self.grant_id is not None:
            _uuid(self.grant_id)
        if type(self.result) is not TdsWorkerResult or self.result.attempt_sha256 != self.attempt_sha256:
            raise ValueError(_ERROR)
        receipt = self.result.receipt
        if receipt is not None:
            _input(receipt)
            if (receipt.rows == 0) != (self.grant_id is None):
                raise ValueError(_ERROR)


def _input(receipt: TdsInputReceipt) -> None:
    if type(receipt) is not TdsInputReceipt:
        raise ValueError(_ERROR)
    if (receipt.rows == 0) != (receipt.encoded_bytes == 0):
        raise ValueError(_ERROR)
    if receipt.rows == 0 and receipt.file_sha256 != _EMPTY_HASH:
        raise ValueError(_ERROR)


def encode_sqlclient_result(record: SqlClientResult) -> bytes:
    """Unframed canonical body; no second framing layer around the nested result."""
    if type(record) is not SqlClientResult:
        raise ValueError(_ERROR)
    body = canonical_json_bytes(
        dict(
            schema_version=record.schema_version,
            launch_sha256=record.launch_sha256,
            attempt_sha256=record.attempt_sha256,
            grant_id=record.grant_id,
            result=strict_json_object(encode_result_payload(record.result)),
        )
    )
    if len(body) > _LIMIT:
        raise ValueError(_ERROR)
    return body


def decode_sqlclient_result(
    body: bytes, *, launch: SqlClientLaunch, expected_input: TdsInputReceipt, expected_grant_id: str | None
) -> SqlClientResult:
    """Validate original expectations after transport EOF, without checking SQL.

    expected_grant_id is the parent's original admitted grant, if any. A null
    reported grant is permitted only for an error or a verified empty input;
    it does not prove that no remote session was opened or remains active.
    """
    try:
        if type(body) is not bytes or not 0 < len(body) <= _LIMIT or type(launch) is not SqlClientLaunch:
            raise ValueError(_ERROR)
        _input(expected_input)
        if expected_grant_id is not None:
            _uuid(expected_grant_id)
            if expected_input.rows == 0:
                raise ValueError(_ERROR)
        data = record_shape(SqlClientResult, strict_json_object(body))
        data["result"] = decode_result_payload(
            canonical_json_bytes(data["result"]),
            expected_attempt_sha256=launch.attempt_sha256,
            expected_input=expected_input,
        )
        result = SqlClientResult(**data)
        if result.launch_sha256 != launch_digest(launch) or result.attempt_sha256 != launch.attempt_sha256:
            raise ValueError(_ERROR)
        if result.grant_id is not None and result.grant_id != expected_grant_id:
            raise ValueError(_ERROR)
        return result
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError(_ERROR) from None
