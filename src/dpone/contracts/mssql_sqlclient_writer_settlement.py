"""Closed P10f expectations and verified stage-content evidence.

The typed digest is a versioned, order-independent SHA-256 multiset sum.  It
retains row multiplicity and is collision resistant; it is not a mathematical
proof of equality.  P10f combines it with the exact stage incarnation, row
count, writer-session departure and durable lifecycle acknowledgement.
"""

from dataclasses import asdict, dataclass

from dpone.contracts.mssql_sqlclient_observer_incarnation import SqlClientObserverIncarnation
from dpone.contracts.mssql_sqlclient_observer_incarnation_codec import (
    decode_observer_incarnation,
    encode_observer_incarnation,
)
from dpone.contracts.mssql_sqlclient_stage_identity import (
    SqlClientStageIdentity,
    decode_stage_identity,
    encode_stage_identity,
)
from dpone.contracts.mssql_sqlclient_writer_session_departure import (
    SqlClientWriterSessionDeparture,
)
from dpone.contracts.mssql_sqlclient_writer_session_departure_codec import (
    decode_writer_session_departure,
    encode_writer_session_departure,
)
from dpone.contracts.mssql_tds_validation import _hash, _integer
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape

ERROR = "mssql_native.sqlclient_writer_settlement_invalid"
DIGEST_PROFILE = "mssql-native-sha256-sum-v1"
SCHEMA = "dpone.sqlclient.writer-settlement.v1"


@dataclass(frozen=True, slots=True)
class SqlClientStageContentExpectation:
    """Parent-only content expectation captured before PREPARED effects."""

    rows: int
    file_sha256: str
    typed_digest: str
    digest_profile: str = DIGEST_PROFILE

    def __post_init__(self) -> None:
        try:
            _integer(self.rows, 0, 2**63 - 1)
            _hash(self.file_sha256)
            _hash(self.typed_digest)
            if self.digest_profile != DIGEST_PROFILE:
                raise ValueError
        except (ValueError, TypeError, OverflowError):
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientWriterSettlementProvenance:
    """Authenticated local helper completion retained by the parent."""

    startup_sha256: str
    request_sha256: str
    result_sha256: str
    local_exit_sha256: str
    implementation_sha256: str
    admission_sha256: str

    def __post_init__(self) -> None:
        try:
            for value in asdict(self).values():
                _hash(value)
        except (ValueError, TypeError, OverflowError):
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientWriterSettlementObservation:
    """Read-only remote settlement and exact stage-content observation."""

    departure: SqlClientWriterSessionDeparture
    stage_before: SqlClientStageIdentity
    stage_after: SqlClientStageIdentity
    observer_before: SqlClientObserverIncarnation
    observer_after: SqlClientObserverIncarnation
    row_count: int
    typed_digest: str
    typed_sum: int | None = None
    digest_profile: str = DIGEST_PROFILE

    def __post_init__(self) -> None:
        try:
            for value, kind in (
                (self.departure, SqlClientWriterSessionDeparture),
                (self.stage_before, SqlClientStageIdentity),
                (self.stage_after, SqlClientStageIdentity),
                (self.observer_before, SqlClientObserverIncarnation),
                (self.observer_after, SqlClientObserverIncarnation),
            ):
                if type(value) is not kind:
                    raise ValueError
                value.__post_init__()
            _integer(self.row_count, 0, 2**63 - 1)
            _hash(self.typed_digest)
            if self.typed_sum is not None:
                _integer(self.typed_sum, 0, 2**256 - 1)
            if (
                self.digest_profile != DIGEST_PROFILE
                or self.stage_before != self.stage_after
                or self.observer_before != self.observer_after
                or self.departure.observer != self.observer_before
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientWriterSettlementRecord:
    """Canonical P10f evidence bound to all prior writer acknowledgements."""

    attempt_sha256: str
    registration_sha256: str
    writer_observation_sha256: str
    grant_sha256: str
    result_sha256: str
    local_exit_sha256: str
    p9_settlement_sha256: str
    helper: SqlClientWriterSettlementProvenance
    expectation: SqlClientStageContentExpectation
    observation: SqlClientWriterSettlementObservation
    schema: str = SCHEMA

    def __post_init__(self) -> None:
        try:
            for value in (
                self.attempt_sha256,
                self.registration_sha256,
                self.writer_observation_sha256,
                self.grant_sha256,
                self.result_sha256,
                self.local_exit_sha256,
                self.p9_settlement_sha256,
            ):
                _hash(value)
            if (
                self.schema != SCHEMA
                or type(self.helper) is not SqlClientWriterSettlementProvenance
                or type(self.expectation) is not SqlClientStageContentExpectation
                or type(self.observation) is not SqlClientWriterSettlementObservation
            ):
                raise ValueError
            self.helper.__post_init__()
            self.expectation.__post_init__()
            self.observation.__post_init__()
            if (
                self.expectation.rows != self.observation.row_count
                or self.expectation.typed_digest != self.observation.typed_digest
                or self.expectation.digest_profile != self.observation.digest_profile
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None


def encode_writer_settlement(value: SqlClientWriterSettlementRecord) -> bytes:
    """Encode one bounded canonical record without credentials or business rows."""
    if type(value) is not SqlClientWriterSettlementRecord:
        raise ValueError(ERROR)
    value.__post_init__()
    body = asdict(value)
    body["expectation"] = asdict(value.expectation)
    body["helper"] = asdict(value.helper)
    body["observation"] = {
        "departure": strict_json_object(encode_writer_session_departure(value.observation.departure)),
        "stage_before": strict_json_object(encode_stage_identity(value.observation.stage_before)),
        "stage_after": strict_json_object(encode_stage_identity(value.observation.stage_after)),
        "observer_before": strict_json_object(encode_observer_incarnation(value.observation.observer_before)),
        "observer_after": strict_json_object(encode_observer_incarnation(value.observation.observer_after)),
        "row_count": value.observation.row_count,
        "typed_digest": value.observation.typed_digest,
        "digest_profile": value.observation.digest_profile,
    }
    if value.observation.typed_sum is not None:
        body["observation"]["typed_sum"] = value.observation.typed_sum
    payload = canonical_json_bytes(body)
    if not 0 < len(payload) <= 2_097_152:
        raise ValueError(ERROR)
    return payload


def decode_writer_settlement(payload: bytes) -> SqlClientWriterSettlementRecord:
    """Strictly reconstruct P10f evidence for audit and recovery tooling."""
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= 2_097_152:
            raise ValueError
        body = record_shape(SqlClientWriterSettlementRecord, strict_json_object(payload))
        body["helper"] = construct_record(SqlClientWriterSettlementProvenance, body["helper"])
        body["expectation"] = construct_record(SqlClientStageContentExpectation, body["expectation"])
        observation = body["observation"]
        if isinstance(observation, dict) and "typed_sum" not in observation:
            observation = {**observation, "typed_sum": None}
        raw = record_shape(SqlClientWriterSettlementObservation, observation)
        raw["departure"] = decode_writer_session_departure(canonical_json_bytes(raw["departure"]))
        raw["stage_before"] = decode_stage_identity(canonical_json_bytes(raw["stage_before"]))
        raw["stage_after"] = decode_stage_identity(canonical_json_bytes(raw["stage_after"]))
        raw["observer_before"] = decode_observer_incarnation(canonical_json_bytes(raw["observer_before"]))
        raw["observer_after"] = decode_observer_incarnation(canonical_json_bytes(raw["observer_after"]))
        body["observation"] = SqlClientWriterSettlementObservation(**raw)
        result = SqlClientWriterSettlementRecord(**body)
        if encode_writer_settlement(result) != payload:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError):
        raise ValueError(ERROR) from None


__all__ = (
    "DIGEST_PROFILE",
    "SqlClientStageContentExpectation",
    "SqlClientWriterSettlementProvenance",
    "SqlClientWriterSettlementObservation",
    "SqlClientWriterSettlementRecord",
    "decode_writer_settlement",
    "encode_writer_settlement",
)
