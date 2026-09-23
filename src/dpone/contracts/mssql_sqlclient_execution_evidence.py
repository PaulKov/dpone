"""Original transient result context and local exit evidence; no SQL settlement."""

from dataclasses import asdict, dataclass

from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_sqlclient_evidence_binding import (
    SqlClientEvidenceBinding,
    decode_evidence_binding,
    encode_evidence_binding,
)
from dpone.contracts.mssql_sqlclient_evidence_types import ERROR, SqlClientEvidenceKind, require_payload
from dpone.contracts.mssql_sqlclient_launch import SqlClientLaunch, decode_launch, encode_launch
from dpone.contracts.mssql_sqlclient_result import _input
from dpone.contracts.mssql_tds_validation import _hash, _uuid
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape


@dataclass(frozen=True, slots=True)
class SqlClientResultContext:
    """Trusted originals for raw-result validation, never a prevalidated flag."""

    launch: SqlClientLaunch
    expected_input: TdsInputReceipt
    expected_grant_id: str | None

    def __post_init__(self) -> None:
        if type(self.launch) is not SqlClientLaunch or type(self.expected_input) is not TdsInputReceipt:
            raise ValueError(ERROR)
        self.launch.__post_init__()
        decode_launch(encode_launch(self.launch))
        _input(construct_record(TdsInputReceipt, asdict(self.expected_input)))
        if self.expected_grant_id is not None:
            _uuid(self.expected_grant_id)
            if self.expected_input.rows == 0:
                raise ValueError(ERROR)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientLocalExit:
    """Matching actually reaped process; nullable result ref requires a separate ACK."""

    binding: SqlClientEvidenceBinding
    registration_sha256: str
    result_sha256: str | None
    exit: TdsChildExit
    schema: str = "dpone.sqlclient.local-exit.v1"

    def __post_init__(self) -> None:
        if type(self.schema) is not str or self.schema != "dpone.sqlclient.local-exit.v1":
            raise ValueError(ERROR)
        decode_evidence_binding(encode_evidence_binding(self.binding))
        _hash(self.registration_sha256)
        if self.result_sha256 is not None:
            _hash(self.result_sha256)
        if type(self.exit) is not TdsChildExit:
            raise ValueError(ERROR)
        self.exit.__post_init__()
        construct_record(TdsProcessIdentity, asdict(self.exit.identity))
        if self.exit.identity != self.binding.process or self.exit.reaped is not True:
            raise ValueError(ERROR)


def encode_worker_local_exit(record: SqlClientLocalExit) -> bytes:
    """Encode full process/exit observation after deep validation."""
    try:
        if type(record) is not SqlClientLocalExit:
            raise ValueError(ERROR)
        record.__post_init__()
        payload = canonical_json_bytes(asdict(record))
        require_payload(payload, SqlClientEvidenceKind.LOCAL_EXIT)
        return payload
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def decode_worker_local_exit(payload: bytes) -> SqlClientLocalExit:
    """Reject false reaping and scalar/identity aliases; do not assert remote exit."""
    try:
        require_payload(payload, SqlClientEvidenceKind.LOCAL_EXIT)
        data = record_shape(SqlClientLocalExit, strict_json_object(payload))
        data["binding"] = decode_evidence_binding(canonical_json_bytes(data["binding"]))
        exit_data = record_shape(TdsChildExit, data["exit"])
        exit_data["identity"] = construct_record(TdsProcessIdentity, exit_data["identity"])
        data["exit"] = TdsChildExit(**exit_data)
        record = SqlClientLocalExit(**data)
        if encode_worker_local_exit(record) != payload:
            raise ValueError(ERROR)
        return record
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None
