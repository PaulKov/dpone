"""Bounded immutable coordinator evidence and exact local-exit proof production.

Persistence admission checks canonical bytes and closed top-level fields only.
The trusted app validates nested domain/SQL bindings before submitting records;
this module cannot recognize arbitrary secrets or authenticate a claimed exit.
"""

from dataclasses import asdict, dataclass, field, fields
from enum import StrEnum
from hashlib import sha256

from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorLocalKind, TdsCoordinatorLocalObservation
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity, _hash, _integer
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

_INVALID = "mssql_native.tds_evidence_record_invalid"
_LOCAL_SCHEMA = "dpone.tds.coordinator-local-exit.v1"


class TdsCoordinatorEvidenceKind(StrEnum):
    CREATE_REQUEST = "create_request"
    ADMISSION = "admission"
    REGISTRATION = "registration"
    AUTHORITY = "authority"
    RESULT = "result"
    LOCAL_EXIT = "local_exit"


# Explicit outer schemas only; nested semantic codecs stay owned by the app.
_SHAPES = {
    TdsCoordinatorEvidenceKind.CREATE_REQUEST: (
        "dpone.tds.create-request.v1",
        131072,
        "schema parent object_nonce columns",
    ),
    TdsCoordinatorEvidenceKind.ADMISSION: (
        "dpone.tds.coordinator-build.v1",
        16384,
        "schema build profile",
    ),
    TdsCoordinatorEvidenceKind.REGISTRATION: (
        "dpone.tds.coordinator-registration.v1",
        32768,
        "schema startup admission_sha256",
    ),
    TdsCoordinatorEvidenceKind.AUTHORITY: (
        "dpone.tds.coordinator-authority.v1",
        16384,
        "schema operation_sha256 execution_owner process implementation_sha256 session database schema_observation "
        "lock transaction_count implicit_transactions",
    ),
    TdsCoordinatorEvidenceKind.RESULT: (
        "dpone.tds.coordinator-create-result.v1",
        262144,
        "schema result evidence failure",
    ),
    TdsCoordinatorEvidenceKind.LOCAL_EXIT: (
        _LOCAL_SCHEMA,
        16384,
        "schema operation_sha256 process exit authority_sha256",
    ),
}


def _name(operation: str, kind: TdsCoordinatorEvidenceKind, digest: str) -> str:
    return f"tds-coordinator-{operation}-{kind.value}-{digest}.json"


@dataclass(frozen=True)
class TdsCoordinatorEvidenceReceipt:
    """Exact immutable filename/bytes ACK; no process, SQL or journal authority."""

    operation_sha256: str
    kind: TdsCoordinatorEvidenceKind
    relative_name: str
    payload_sha256: str
    byte_count: int

    def __post_init__(self) -> None:
        _hash(self.operation_sha256)
        _hash(self.payload_sha256)
        if type(self.kind) is not TdsCoordinatorEvidenceKind:
            raise ValueError(_INVALID)
        _integer(self.byte_count, 1, _SHAPES[self.kind][1])
        if type(self.relative_name) is not str or self.relative_name != _name(
            self.operation_sha256, self.kind, self.payload_sha256
        ):
            raise ValueError(_INVALID)


@dataclass(frozen=True)
class TdsCoordinatorEvidenceRecord:
    """Internal bounded persistence request; never stores credential envelopes."""

    operation_sha256: str
    kind: TdsCoordinatorEvidenceKind
    payload: bytes = field(repr=False)

    def __post_init__(self) -> None:
        try:
            _hash(self.operation_sha256)
            if type(self.kind) is not TdsCoordinatorEvidenceKind or type(self.payload) is not bytes:
                raise ValueError
            schema, limit, shape = _SHAPES[self.kind]
            if not 0 < len(self.payload) <= limit:
                raise ValueError
            value = strict_json_object(self.payload)
            if set(value) != set(shape.split()) or value["schema"] != schema:
                raise ValueError
            if "operation_sha256" in value and value["operation_sha256"] != self.operation_sha256:
                raise ValueError
            if canonical_json_bytes(value) != self.payload:
                raise ValueError
        except (ValueError, TypeError, OverflowError, RecursionError):
            raise ValueError(_INVALID) from None

    @property
    def receipt(self) -> TdsCoordinatorEvidenceReceipt:
        digest = sha256(self.payload).hexdigest()
        return TdsCoordinatorEvidenceReceipt(
            self.operation_sha256, self.kind, _name(self.operation_sha256, self.kind, digest), digest, len(self.payload)
        )


@dataclass(frozen=True)
class TdsCoordinatorEvidenceObservation:
    """Only the last acknowledged actor receipt; initialization has no receipt."""

    operation_sha256: str
    receipt: TdsCoordinatorEvidenceReceipt | None = None

    def __post_init__(self) -> None:
        _hash(self.operation_sha256)
        if self.receipt is not None and (
            type(self.receipt) is not TdsCoordinatorEvidenceReceipt
            or self.receipt.operation_sha256 != self.operation_sha256
        ):
            raise ValueError(_INVALID)


@dataclass(frozen=True)
class TdsCoordinatorLocalExit:
    """Actual reaped exit supplied by trusted pidfd owner, not a settlement claim.

    authority_sha256 binds the canonical authenticated registration proof. A
    matching process in the journal and its acknowledged registration are separate
    caller prerequisites before attaching the produced local observation.
    """

    operation_sha256: str
    process: TdsProcessIdentity
    exit: TdsChildExit
    authority_sha256: str

    def __post_init__(self) -> None:
        _hash(self.operation_sha256)
        _hash(self.authority_sha256)
        if (
            type(self.process) is not TdsProcessIdentity
            or type(self.exit) is not TdsChildExit
            or self.exit.identity != self.process
            or self.exit.reaped is not True
        ):
            raise ValueError("mssql_native.tds_local_exit_invalid")


def encode_local_exit(value: TdsCoordinatorLocalExit) -> bytes:
    """Encode the full versioned exit proof, including the registration digest."""
    if type(value) is not TdsCoordinatorLocalExit:
        raise ValueError("mssql_native.tds_local_exit_invalid")
    return canonical_json_bytes(dict(asdict(value), schema=_LOCAL_SCHEMA))


def decode_local_exit(payload: bytes) -> TdsCoordinatorLocalExit:
    """Strictly reconstruct nested process/reaping fields without scalar aliases."""
    try:
        if type(payload) is not bytes or len(payload) > 16384:
            raise ValueError
        body = strict_json_object(payload)
        if set(body) != {"schema", "operation_sha256", "process", "exit", "authority_sha256"}:
            raise ValueError
        if body.pop("schema") != _LOCAL_SCHEMA:
            raise ValueError
        process, exit_value = body["process"], body["exit"]
        if type(process) is not dict or set(process) != {f.name for f in fields(TdsProcessIdentity)}:
            raise ValueError
        if type(exit_value) is not dict or set(exit_value) != {f.name for f in fields(TdsChildExit)}:
            raise ValueError
        nested = exit_value["identity"]
        if type(nested) is not dict or set(nested) != set(process):
            raise ValueError
        body["process"] = TdsProcessIdentity(**process)
        body["exit"] = TdsChildExit(TdsProcessIdentity(**nested), exit_value["exit_code"], exit_value["reaped"])
        result = TdsCoordinatorLocalExit(**body)
        if canonical_json_bytes(strict_json_object(encode_local_exit(result))) != payload:
            raise ValueError
        return result
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError("mssql_native.tds_local_exit_invalid") from None


def local_exit_observation(value: TdsCoordinatorLocalExit) -> TdsCoordinatorLocalObservation:
    """Produce CONTAINED from matching reaped exit; nonzero exit is still local.

    This never checks journal registration or establishes remote settlement. The
    supervisor separately requires zero exit for a successful CREATE outcome.
    """
    payload = encode_local_exit(value)
    return TdsCoordinatorLocalObservation(
        value.operation_sha256,
        TdsCoordinatorLocalKind.CONTAINED,
        value.process,
        value.authority_sha256,
        sha256(payload).hexdigest(),
    )
