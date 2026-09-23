"""Immutable original CREATE seal and bounded historical inventory observations.

The admitted store and deployment evidence custody establish origin. A digest or
constructed record alone grants no SQL authority, settlement or Prepared status.
"""

from dataclasses import asdict, dataclass, fields, is_dataclass
from hashlib import sha256
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientGrantInventory, encode_grant_inventory
from dpone.contracts.mssql_sqlclient_stage_identity import stage_identity_from_create
from dpone.contracts.mssql_sqlclient_stage_locator import SqlClientStageLocator, encode_stage_locator
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorPhase,
    TdsCoordinatorResultKind,
    TdsCoordinatorSnapshot,
    coordinator_identity_digest,
    coordinator_key,
)
from dpone.contracts.mssql_tds_coordinator_codec import decode_coordinator_state, encode_coordinator_state
from dpone.contracts.mssql_tds_coordinator_evidence import TdsCoordinatorEvidenceKind as Kind
from dpone.contracts.mssql_tds_coordinator_evidence import TdsCoordinatorEvidenceReceipt
from dpone.contracts.mssql_tds_create import TdsCreateEvidence, decode_create_evidence, encode_create_evidence
from dpone.contracts.mssql_tds_validation import _hash, _integer
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid

MAX_CREATE_SEAL_BYTES = 65536
_SCHEMA = "dpone.sqlclient.create-evidence.v1"
_ERROR = "mssql_native.sqlclient_create_evidence_invalid"


def _exact(original: Any, decoded: Any) -> None:
    """Reject nested scalar/type aliases before an encoded copy crosses ownership."""
    if type(original) is not type(decoded) or original != decoded:
        raise ValueError(_ERROR)
    if isinstance(original, UUID):
        if type(original.int) is not int:
            raise ValueError(_ERROR)
    elif is_dataclass(original):
        for field in fields(original):
            _exact(getattr(original, field.name), getattr(decoded, field.name))
    elif type(original) is tuple:
        for left, right in zip(original, decoded, strict=True):
            _exact(left, right)


def snapshot_create_state(snapshot: TdsCoordinatorSnapshot) -> TdsCoordinatorSnapshot:
    """Native codec validation plus exact original type preservation."""
    if type(snapshot) is not TdsCoordinatorSnapshot:
        raise ValueError(_ERROR)
    _integer(snapshot.revision, 1)
    state = decode_coordinator_state(encode_coordinator_state(snapshot.state), identity=snapshot.state.identity)
    copy = TdsCoordinatorSnapshot(state, snapshot.revision)
    _exact(snapshot, copy)
    return copy


def create_seal_key(locator: SqlClientStageLocator) -> str:
    encode_stage_locator(locator)
    return coordinator_key(locator.create_operation) + "/create-evidence/v1"


@dataclass(frozen=True)
class SqlClientCreateSeal:
    state_domain_id: UUID
    locator_sha256: str
    original: TdsCoordinatorSnapshot
    receipts: tuple[TdsCoordinatorEvidenceReceipt, ...]

    def __post_init__(self) -> None:
        if (
            type(self.state_domain_id) is not UUID
            or type(self.state_domain_id.int) is not int
            or not self.state_domain_id.int
        ):
            raise ValueError(_ERROR)
        _hash(self.locator_sha256)
        original = snapshot_create_state(self.original).state
        if (
            original.phase is not TdsCoordinatorPhase.RESULT_RECEIVED
            or original.sequence != 6
            or original.ownership != original.execution_owner
            or original.local is None
            or original.error is not None
            or original.remote is not None
            or original.result is None
            or original.result.outcome is not TdsCoordinatorResultKind.SUCCEEDED
        ):
            raise ValueError(_ERROR)
        if type(self.receipts) is not tuple or len(self.receipts) != len(Kind):
            raise ValueError(_ERROR)
        for receipt, kind in zip(self.receipts, Kind, strict=True):
            if type(receipt) is not TdsCoordinatorEvidenceReceipt:
                raise ValueError(_ERROR)
            receipt.__post_init__()
            if receipt.kind is not kind or receipt.operation_sha256 != coordinator_identity_digest(original.identity):
                raise ValueError(_ERROR)


def encode_create_seal(seal: SqlClientCreateSeal) -> bytes:
    """64 KiB ceiling: <=16 KiB state + six <1 KiB receipts + <1 KiB envelope.

    Revision integers remain subject to the canonical encoder and final byte cap;
    no payload or IPC limits are enlarged to accommodate an oversized record.
    """
    if type(seal) is not SqlClientCreateSeal:
        raise ValueError(_ERROR)
    seal.__post_init__()
    data = canonical_json_bytes(
        dict(
            schema=_SCHEMA,
            state_domain_id=str(seal.state_domain_id),
            locator_sha256=seal.locator_sha256,
            original_revision=seal.original.revision,
            original=strict_json_object(encode_coordinator_state(seal.original.state)),
            receipts=[asdict(receipt) for receipt in seal.receipts],
        )
    )
    if len(data) > MAX_CREATE_SEAL_BYTES:
        raise ValueError(_ERROR)
    return data


def decode_create_seal(payload: bytes, locator: SqlClientStageLocator) -> SqlClientCreateSeal:
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= MAX_CREATE_SEAL_BYTES:
            raise ValueError(_ERROR)
        data = strict_json_object(payload)
        if (
            set(data) != {"schema", "state_domain_id", "locator_sha256", "original_revision", "original", "receipts"}
            or data["schema"] != _SCHEMA
        ):
            raise ValueError(_ERROR)
        if type(data["receipts"]) is not list or len(data["receipts"]) != len(Kind):
            raise ValueError(_ERROR)
        receipts = []
        for item in data["receipts"]:
            if (
                type(item) is not dict
                or set(item) != {field.name for field in fields(TdsCoordinatorEvidenceReceipt)}
                or type(item["kind"]) is not str
            ):
                raise ValueError(_ERROR)
            receipts.append(TdsCoordinatorEvidenceReceipt(**dict(item, kind=Kind(item["kind"]))))
        state = decode_coordinator_state(canonical_json_bytes(data["original"]), identity=locator.create_operation)
        seal = SqlClientCreateSeal(
            canonical_uuid(data["state_domain_id"]),
            data["locator_sha256"],
            TdsCoordinatorSnapshot(state, data["original_revision"]),
            tuple(receipts),
        )
        if (
            seal.state_domain_id != locator.state_domain_id
            or seal.locator_sha256 != sha256(encode_stage_locator(locator)).hexdigest()
            or state.execution_owner != locator.execution_owner
            or encode_create_seal(seal) != payload
        ):
            raise ValueError(_ERROR)
        return seal
    except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
        raise ValueError(_ERROR) from None


def validate_create_lineage(original: TdsCoordinatorSnapshot, current: TdsCoordinatorSnapshot) -> None:
    """Exact immutable facts and additive sequence arithmetic; no history invention."""
    before, after = snapshot_create_state(original).state, snapshot_create_state(current).state
    immutable = (
        "identity",
        "execution_owner",
        "phase",
        "process",
        "authentication_sha256",
        "session",
        "authority_sha256",
        "grant",
        "result",
        "local",
    )
    if any(getattr(before, key) != getattr(after, key) for key in immutable) or current.revision < original.revision:
        raise ValueError(_ERROR)
    if current.revision == original.revision and encode_coordinator_state(before) != encode_coordinator_state(after):
        raise ValueError(_ERROR)
    takeovers = after.sequence - 6 - int(after.error is not None) - int(after.remote is not None)
    if after.ownership == before.ownership:
        if takeovers != 0:
            raise ValueError(_ERROR)
    elif not (1 <= takeovers <= after.ownership.fence - before.ownership.fence) or (
        takeovers == 1 and after.ownership.supervisor_id == before.ownership.supervisor_id
    ):
        raise ValueError(_ERROR)


@dataclass(frozen=True)
class SqlClientAuthenticatedCreate:
    """Compact historical proof; current remote fact is retained without settlement."""

    operation_sha256: str
    locator_sha256: str
    seal_sha256: str
    seal_revision: int
    original: TdsCoordinatorSnapshot
    current: TdsCoordinatorSnapshot
    evidence: TdsCreateEvidence


def authenticated_create_bytes(value: SqlClientAuthenticatedCreate) -> bytes:
    if type(value) is not SqlClientAuthenticatedCreate:
        raise ValueError(_ERROR)
    for digest in (value.operation_sha256, value.locator_sha256, value.seal_sha256):
        _hash(digest)
    _integer(value.seal_revision, 1)
    validate_create_lineage(value.original, value.current)
    decoded = decode_create_evidence(encode_create_evidence(value.evidence))
    _exact(value.evidence, decoded)
    if (
        value.current.state.error is not None
        or value.operation_sha256 != coordinator_identity_digest(value.original.state.identity)
        or value.evidence.operation_sha256 != value.operation_sha256
    ):
        raise ValueError(_ERROR)
    return canonical_json_bytes(
        dict(
            operation_sha256=value.operation_sha256,
            locator_sha256=value.locator_sha256,
            seal_sha256=value.seal_sha256,
            seal_revision=value.seal_revision,
            original_revision=value.original.revision,
            current_revision=value.current.revision,
            original=strict_json_object(encode_coordinator_state(value.original.state)),
            current=strict_json_object(encode_coordinator_state(value.current.state)),
            evidence=strict_json_object(encode_create_evidence(value.evidence)),
        )
    )


@dataclass(frozen=True)
class SqlClientAuthenticatedInventory:
    """Mandatory historical observation per raw member; never a Prepared token."""

    inventory: SqlClientGrantInventory
    creates: tuple[SqlClientAuthenticatedCreate, ...]


def encode_authenticated_inventory(value: SqlClientAuthenticatedInventory) -> bytes:
    if (
        type(value) is not SqlClientAuthenticatedInventory
        or type(value.creates) is not tuple
        or len(value.creates) != len(value.inventory.members)
    ):
        raise ValueError(_ERROR)
    raw = encode_grant_inventory(value.inventory)
    # Exact enclosing bytes: the empty array contributes both brackets already.
    prefix = b'{"creates":['
    suffix = b'],"inventory":' + raw + b',"schema":"dpone.sqlclient.authenticated-inventory.v1"}'
    used = len(prefix) + len(suffix)
    encoded: list[bytes] = []
    for member, proof in zip(value.inventory.members, value.creates, strict=True):
        body = authenticated_create_bytes(proof)
        if (
            proof.locator_sha256 != sha256(encode_stage_locator(member.locator.locator)).hexdigest()
            or proof.original.state.identity != member.locator.locator.create_operation
            or stage_identity_from_create(proof.evidence) != member.stage
        ):
            raise ValueError(_ERROR)
        used += len(body) + int(bool(encoded))
        if used > value.inventory.limits.observation_bytes:
            raise ValueError(_ERROR)
        encoded.append(body)
    if used > value.inventory.limits.observation_bytes:
        raise ValueError(_ERROR)
    return prefix + b",".join(encoded) + suffix
