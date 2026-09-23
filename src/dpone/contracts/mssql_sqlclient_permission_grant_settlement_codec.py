"""Credential-free evidence for local release and isolated remote settlement."""

from dataclasses import asdict, dataclass
from enum import StrEnum
from hashlib import sha256
from types import MappingProxyType

from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceKind,
    SqlClientDepartureEvidenceReceipt,
)
from dpone.contracts.mssql_sqlclient_observation import session_authority_digest
from dpone.contracts.mssql_sqlclient_permission_grant_departure import (
    PermissionGrantDepartureCompletion as PermissionGrantDepartureCompletion,
)  # noqa: F401 - closed P8b2 settlement surface
from dpone.contracts.mssql_sqlclient_permission_grant_departure import (
    SqlClientPermissionGrantDepartureResult,
)
from dpone.contracts.mssql_sqlclient_permission_grant_departure_codec import validate_result as validate_result
from dpone.contracts.mssql_sqlclient_permission_grant_parent_evidence import (
    PermissionGrantParentEvidenceKind,
    PermissionGrantParentEvidenceReceipt,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import (
    PermissionWireBinding,
    PermissionWireKind,
    decode_permission_message,
    encode_permission_message,
)
from dpone.contracts.mssql_tds_coordinator import (
    CoordinatorLocalObserved as CoordinatorLocalObserved,
)
from dpone.contracts.mssql_tds_coordinator import (
    CoordinatorRemoteObserved as CoordinatorRemoteObserved,
)
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorLocalKind as TdsCoordinatorLocalKind,
)
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorLocalObservation as TdsCoordinatorLocalObservation,
)
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorRemoteKind as TdsCoordinatorRemoteKind,
)
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorRemoteObservation as TdsCoordinatorRemoteObservation,
)
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorSnapshot as TdsCoordinatorSnapshot,
)
from dpone.contracts.mssql_tds_coordinator import (
    advance_coordinator_state as advance_coordinator_state,
)
from dpone.contracts.mssql_tds_coordinator import (
    coordinator_identity_digest as coordinator_identity_digest,
)
from dpone.contracts.mssql_tds_directory import (
    TdsDirectorySnapshot as TdsDirectorySnapshot,
)
from dpone.contracts.mssql_tds_directory import (
    TdsLocalContainment as TdsLocalContainment,
)
from dpone.contracts.mssql_tds_directory import (
    TdsRemoteSettlement as TdsRemoteSettlement,
)
from dpone.contracts.mssql_tds_directory_codec import process_identity_digest as process_identity_digest
from dpone.contracts.mssql_tds_result import attempt_identity_digest as attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

ERROR = "mssql_native.sqlclient_permission_settlement_evidence_invalid"
LOCAL_EXIT_SCHEMA = "dpone.sqlclient.permission-grant-local-exit.v1"
REMOTE_SETTLEMENT_SCHEMA = "dpone.sqlclient.permission-grant-remote-settlement.v1"


class PermissionGrantSettlementEvidenceKind(StrEnum):
    RELEASE_INTENT = "release_intent"
    RELEASED = "released"
    LOCAL_EXIT = "local_exit"


class PermissionGrantRemoteSettlementEvidenceKind(StrEnum):
    """P8b2 extension kept outside the legacy three-kind iterable contract."""

    REMOTE_SETTLEMENT = "remote_settlement"


K = PermissionGrantSettlementEvidenceKind
RK = PermissionGrantRemoteSettlementEvidenceKind
SettlementKind = PermissionGrantSettlementEvidenceKind | PermissionGrantRemoteSettlementEvidenceKind
CAPS = MappingProxyType({**{kind: 16384 for kind in K}, RK.REMOTE_SETTLEMENT: 16384})
RELEASE_KIND = PermissionWireKind.RELEASE
RELEASED_KIND = PermissionWireKind.RELEASED


def _invalid() -> ValueError:
    return ValueError(ERROR)


@dataclass(frozen=True, slots=True)
class PermissionGrantSettlementEvidenceSubject:
    attempt_sha256: str
    operation_sha256: str
    result_sha256: str
    held_ready_sha256: str
    binding_sha256: str

    def __post_init__(self) -> None:
        try:
            for value in asdict(self).values():
                _hash(value)
        except (ValueError, TypeError):
            raise _invalid() from None


def permission_grant_settlement_subject(
    held_ready: PermissionGrantParentEvidenceReceipt,
    result: PermissionGrantParentEvidenceReceipt,
    binding: PermissionWireBinding,
) -> PermissionGrantSettlementEvidenceSubject:
    """Bind settlement to exact acknowledged RESULT and HELD_READY records."""
    try:
        if (
            type(held_ready) is not PermissionGrantParentEvidenceReceipt
            or held_ready.kind is not PermissionGrantParentEvidenceKind.HELD_READY
            or type(result) is not PermissionGrantParentEvidenceReceipt
            or result.kind is not PermissionGrantParentEvidenceKind.RESULT
            or held_ready.subject != result.subject
        ):
            raise _invalid()
        return PermissionGrantSettlementEvidenceSubject(
            held_ready.subject.attempt_sha256,
            held_ready.subject.operation_sha256,
            result.payload_sha256,
            held_ready.payload_sha256,
            sha256(_binding(binding)).hexdigest(),
        )
    except (ValueError, TypeError, AttributeError):
        raise _invalid() from None


def _subject(value: object) -> PermissionGrantSettlementEvidenceSubject:
    if type(value) is not PermissionGrantSettlementEvidenceSubject:
        raise _invalid()
    return PermissionGrantSettlementEvidenceSubject(**asdict(value))


def permission_grant_settlement_subject_snapshot(value: object) -> bytes:
    """Return canonical immutable bytes for one validated settlement subject."""
    return canonical_json_bytes(asdict(_subject(value)))


def _binding(value: object) -> bytes:
    if type(value) is not PermissionWireBinding:
        raise _invalid()
    return value.snapshot()


def _name(subject: PermissionGrantSettlementEvidenceSubject, kind: SettlementKind, digest: str) -> str:
    subject_digest = sha256(permission_grant_settlement_subject_snapshot(subject)).hexdigest()
    return f"tds-permission-settlement-v2-{subject_digest}-{kind.value}-{digest}.json"


def encode_permission_grant_local_exit(value: TdsChildExit) -> bytes:
    try:
        if type(value) is not TdsChildExit or value.exit_code != 0 or value.reaped is not True:
            raise _invalid()
        return canonical_json_bytes(
            {
                "schema": LOCAL_EXIT_SCHEMA,
                "process": asdict(value.identity),
                "exit_code": value.exit_code,
                "reaped": value.reaped,
            }
        )
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise _invalid() from None


def encode_permission_grant_remote_settlement(
    result: SqlClientPermissionGrantDepartureResult,
    receipts: tuple[SqlClientDepartureEvidenceReceipt, ...],
) -> bytes:
    """Bind remote settlement to the exact six-ACK verifier completion."""
    try:
        result.__post_init__()
        if (
            type(receipts) is not tuple
            or len(receipts) != 6
            or tuple(receipt.kind for receipt in receipts) != tuple(SqlClientDepartureEvidenceKind)
        ):
            raise _invalid()
        for receipt in receipts:
            receipt.__post_init__()
        return canonical_json_bytes(
            {
                "schema": REMOTE_SETTLEMENT_SCHEMA,
                "authority_sha256": session_authority_digest(result.catalog_observer.authority).hex(),
                "result_sha256": receipts[3].payload_sha256,
                "local_exit_sha256": receipts[-2].payload_sha256,
                "exclusion_sha256": receipts[-1].payload_sha256,
            }
        )
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise _invalid() from None


def require_permission_grant_local_exit(
    value: object, binding: PermissionWireBinding, *, exact_identity: bool
) -> TdsChildExit:
    """Validate one zero/reaped child exit against the retained process."""
    if (
        type(value) is not TdsChildExit
        or value.exit_code != 0
        or value.reaped is not True
        or (
            value.identity is not binding.startup.process
            if exact_identity
            else value.identity != binding.startup.process
        )
    ):
        raise _invalid()
    return value


def encode_permission_grant_release(binding: PermissionWireBinding, result_sha256: str) -> bytes:
    """Encode the sole ordinal-five RELEASE intent."""
    _hash(result_sha256)
    return encode_permission_message(binding, RELEASE_KIND, 5, {"evidence_sha256": result_sha256})


def encode_observed_permission_message(
    binding: PermissionWireBinding, message: object, *, kind: PermissionWireKind
) -> bytes:
    """Re-encode an immutable observed message under the required kind."""
    try:
        observed_kind = getattr(message, "kind")
        ordinal = getattr(message, "ordinal")
        body = strict_json_object(getattr(message, "body"))
        if observed_kind is not kind:
            raise _invalid()
        return encode_permission_message(binding, observed_kind, ordinal, body)
    except (ValueError, TypeError, AttributeError):
        raise _invalid() from None


def _decode_local_exit(payload: bytes) -> TdsChildExit:
    body = strict_json_object(payload)
    if set(body) != {"schema", "process", "exit_code", "reaped"} or body["schema"] != LOCAL_EXIT_SCHEMA:
        raise _invalid()
    process = body["process"]
    if type(process) is not dict or set(process) != {"host_sha256", "boot_id", "pid", "start_ticks"}:
        raise _invalid()
    result = TdsChildExit(TdsProcessIdentity(**process), body["exit_code"], body["reaped"])
    if encode_permission_grant_local_exit(result) != payload:
        raise _invalid()
    return result


def _wire(payload: bytes, binding: PermissionWireBinding, kind: PermissionWireKind) -> dict:
    message = decode_permission_message(payload, binding=binding, kind=kind, ordinal=5)
    body = strict_json_object(message.body)
    if set(body) != {"evidence_sha256"}:
        raise _invalid()
    return body
