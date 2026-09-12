"""Bounded original ownership reads inside one independently observed transaction.

The owning boundary audits the exact catalog first. These projections grant no
lifecycle authority and never acquire locks, change rows or own a connection.
Qualification states are structural records only; history admission blocks their
intersecting claims until a trusted lifecycle implementation exists.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.adapters.composition_mssql_transaction import require_shared_transaction_in
from dpone.contracts.composition_control import (
    CompositionActivationRequest,
    CompositionAdmissionError,
    CompositionPhysicalResource,
    decode_activation_request,
    encode_physical_resource,
    require_digest,
)
from dpone.contracts.composition_ownership import CompositionOwnerReference, CompositionPhysicalClaim
from dpone.contracts.composition_qualification_operation import CompositionQualificationOwner
from dpone.contracts.strict_json import canonical_json_bytes

if TYPE_CHECKING:
    from dpone.ports.composition_sql import CompositionSqlContext


@dataclass(frozen=True, slots=True)
class SharedOwnerRecord:
    """Complete original and immutable epochs; never an admission receipt."""

    reference: CompositionOwnerReference
    subject: CompositionActivationRequest | CompositionQualificationOwner
    subject_document: bytes
    subject_sha256: str
    state: str
    guard_epochs: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class SharedDomainRecord:
    """Exact enrolled physical identity and observed current pointer."""

    guard_id: str
    connector: str
    service_id: str
    physical_subject_sha256: str
    fencing_epoch: int
    owner: CompositionOwnerReference | None


def _rows(context: CompositionSqlContext, sql: str, *parameters: object) -> tuple[tuple[Any, ...], ...]:
    """Consume one explicitly bounded query before nested cursor use."""
    context.cursor.execute(sql, *parameters)
    return tuple(tuple(row) for row in context.cursor.fetchall())


def _table(context: CompositionSqlContext, name: str) -> str:
    # Every entry has already validated schema through the transaction helper.
    return f"[{context.schema}].[composition_{name}]"


def _owner_columns() -> str:
    return (
        "owner_key,owner_kind,LOWER(CONVERT(char(36),owner_id)),subject_sha256,"
        "CASE WHEN DATALENGTH(subject_document) BETWEEN 1 AND "
        "CASE owner_kind WHEN 'execution' THEN 8388608 WHEN 'qualification' THEN 1048576 ELSE 0 END "
        "THEN subject_document END,state"
    )


def _reference(key: object, kind: Any, identifier: Any) -> CompositionOwnerReference:
    reference = CompositionOwnerReference(kind, identifier)
    if reference.owner_key != key:
        raise CompositionAdmissionError("occurrence_identity")
    return reference


def _released(kind: str, state: str) -> bool:
    return state == "RETIRED" or (kind == "qualification" and state == "TRANSFERRED")


def _domain_in(
    context: CompositionSqlContext, resource: CompositionPhysicalResource | CompositionPhysicalClaim
) -> SharedDomainRecord:
    records = _rows(
        context,
        "SELECT TOP (2) connector,LOWER(CONVERT(char(36),service_id)),physical_subject_sha256,"
        f"fencing_epoch,owner_key FROM {_table(context, 'domains')} WITH (HOLDLOCK) WHERE guard_id=?;",
        resource.guard_id,
    )
    if (
        len(records) != 1
        or len(records[0]) != 5
        or records[0][:3] != (resource.connector, resource.service_id, resource.physical_subject_sha256)
    ):
        raise CompositionAdmissionError("domain_enrollment")
    epoch, pointer = records[0][3:]
    if type(epoch) is not int or not 0 <= epoch < 2**63 or (pointer is not None and epoch == 0):
        raise CompositionAdmissionError("guard_epoch")
    history = _rows(
        context,
        "SELECT COUNT_BIG(*),MIN(p.fencing_epoch),MAX(p.fencing_epoch),COUNT_BIG(o.owner_key) "
        f"FROM {_table(context, 'owner_domains')} p WITH (HOLDLOCK) "
        f"LEFT JOIN {_table(context, 'owners')} o WITH (HOLDLOCK) ON o.owner_key=p.owner_key WHERE p.guard_id=?;",
        resource.guard_id,
    )
    expected = (epoch, 1 if epoch else None, epoch or None, epoch)
    if history != (expected,) or any(type(v) is not int for v in history[0] if v is not None):
        raise CompositionAdmissionError("guard_readback")
    owner = None
    if epoch:
        latest = _rows(
            context,
            "SELECT TOP (2) p.owner_key,o.owner_kind,LOWER(CONVERT(char(36),o.owner_id)),o.state "
            f"FROM {_table(context, 'owner_domains')} p WITH (HOLDLOCK) "
            f"LEFT JOIN {_table(context, 'owners')} o WITH (HOLDLOCK) ON o.owner_key=p.owner_key "
            "WHERE p.guard_id=? AND p.fencing_epoch=?;",
            resource.guard_id,
            epoch,
        )
        if len(latest) != 1 or len(latest[0]) != 4:
            raise CompositionAdmissionError("guard_readback")
        key, kind, identifier, state = latest[0]
        reference = _reference(key, kind, identifier)
        if pointer is None:
            if not _released(kind, state):
                raise CompositionAdmissionError("guard_readback")
        elif pointer != key or _released(kind, state):
            raise CompositionAdmissionError("guard_readback")
        else:
            owner = reference
    return SharedDomainRecord(resource.guard_id, *records[0][:3], epoch, owner)


def _owner_resources(
    subject: CompositionActivationRequest | CompositionQualificationOwner,
) -> tuple[CompositionPhysicalResource | CompositionPhysicalClaim, ...]:
    return subject.resources if isinstance(subject, CompositionActivationRequest) else subject.claims


def _owner_record(context: CompositionSqlContext, row: tuple[Any, ...]) -> SharedOwnerRecord:
    if len(row) != 6:
        raise CompositionAdmissionError("occurrence_identity")
    key, kind, identifier, digest, document, state = row
    reference = _reference(key, kind, identifier)
    subject: CompositionActivationRequest | CompositionQualificationOwner
    if kind == "execution":
        subject = decode_activation_request(document, digest)
        if subject.activation_id != identifier:
            raise CompositionAdmissionError("occurrence_identity")
        states = {"PREPARED", "ACTIVE", "RETIRING", "RETIRED"}
    else:
        subject = CompositionQualificationOwner.from_bytes(document, expected_sha256=digest)
        if subject.owner_reference != reference:
            raise CompositionAdmissionError("occurrence_identity")
        states = {"PREPARED", "ACTIVE", "SEALING", "SEALED", "TRANSFERRED", "RETIRED"}
    resources = _owner_resources(subject)
    if type(state) is not str or state not in states:
        raise CompositionAdmissionError("occurrence_state")
    unique = _rows(
        context,
        f"SELECT TOP (2) owner_key FROM {_table(context, 'owners')} WITH (HOLDLOCK) "
        "WHERE owner_key=? OR (owner_kind=? AND (owner_id=? OR subject_sha256=?));",
        key,
        kind,
        identifier,
        digest,
    )
    if unique != ((key,),):
        raise CompositionAdmissionError("occurrence_identity")
    partitions = _rows(
        context,
        f"SELECT TOP ({len(resources) + 1}) guard_id,fencing_epoch "
        f"FROM {_table(context, 'owner_domains')} WITH (HOLDLOCK) WHERE owner_key=? ORDER BY guard_id;",
        key,
    )
    if len(partitions) != len(resources):
        raise CompositionAdmissionError("guard_partition")
    epochs: list[tuple[str, int]] = []
    for resource, partition in zip(resources, partitions, strict=True):
        if len(partition) != 2 or partition[0] != resource.guard_id:
            raise CompositionAdmissionError("guard_readback")
        guard, epoch = partition
        if type(epoch) is not int or not 1 <= epoch < 2**63:
            raise CompositionAdmissionError("guard_readback")
        original = (
            encode_physical_resource(resource)
            if isinstance(resource, CompositionPhysicalResource)
            else canonical_json_bytes(resource.to_dict())
        )
        claims = _rows(
            context,
            "SELECT TOP (2) CASE WHEN DATALENGTH(claim_document)=? THEN claim_document END "
            f"FROM {_table(context, 'owner_domains')} WITH (HOLDLOCK) "
            "WHERE owner_key=? AND guard_id=? AND fencing_epoch=?;",
            len(original),
            key,
            guard,
            epoch,
        )
        if claims != ((original,),) or type(claims[0][0]) is not bytes:
            raise CompositionAdmissionError("guard_readback")
        current = _domain_in(context, resource)
        if _released(kind, state):
            if (
                current.fencing_epoch < epoch
                or current.owner == reference
                or (current.fencing_epoch == epoch and current.owner is not None)
            ):
                raise CompositionAdmissionError("guard_readback")
        elif (current.fencing_epoch, current.owner) != (epoch, reference):
            raise CompositionAdmissionError("guard_readback")
        epochs.append((guard, epoch))
    return SharedOwnerRecord(reference, subject, document, digest, state, tuple(epochs))


def _require_absent_owner_in(context: CompositionSqlContext, key: str) -> None:
    if _rows(
        context,
        f"SELECT TOP (1) guard_id FROM {_table(context, 'owner_domains')} WITH (HOLDLOCK) WHERE owner_key=?;",
        key,
    ):
        raise CompositionAdmissionError("guard_partition")


def _read_owner_key_in(
    context: CompositionSqlContext, owner_key: str, *, expected_service_id: str
) -> SharedOwnerRecord | None:
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    require_digest(owner_key)
    rows = _rows(
        context,
        f"SELECT TOP (2) {_owner_columns()} FROM {_table(context, 'owners')} WITH (HOLDLOCK) WHERE owner_key=?;",
        owner_key,
    )
    if len(rows) > 1:
        raise CompositionAdmissionError("occurrence_identity")
    result = _owner_record(context, rows[0]) if rows else None
    if result is None:
        _require_absent_owner_in(context, owner_key)
    if result is not None and result.reference.owner_key != owner_key:
        raise CompositionAdmissionError("occurrence_identity")
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    return result


def read_shared_owner_in(
    context: CompositionSqlContext, reference: CompositionOwnerReference, *, expected_service_id: str
) -> SharedOwnerRecord | None:
    """Reopen the exact kind/ID original and every immutable claim partition."""
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    if type(reference) is not CompositionOwnerReference:
        raise CompositionAdmissionError("occurrence_identity")
    reference.__post_init__()
    rows = _rows(
        context,
        f"SELECT TOP (2) {_owner_columns()} FROM {_table(context, 'owners')} WITH (HOLDLOCK) "
        "WHERE owner_key=? OR (owner_kind=? AND owner_id=?);",
        reference.owner_key,
        reference.owner_kind,
        reference.owner_id,
    )
    if len(rows) > 1:
        raise CompositionAdmissionError("occurrence_identity")
    result = _owner_record(context, rows[0]) if rows else None
    if result is None:
        _require_absent_owner_in(context, reference.owner_key)
    if result is not None and result.reference != reference:
        raise CompositionAdmissionError("occurrence_identity")
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    return result


def read_shared_domain_in(
    context: CompositionSqlContext,
    resource: CompositionPhysicalResource | CompositionPhysicalClaim,
    *,
    expected_service_id: str,
) -> SharedDomainRecord:
    """Read enrollment, contiguous retained epochs and the full current owner."""
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    if type(resource) not in {CompositionPhysicalResource, CompositionPhysicalClaim}:
        raise CompositionAdmissionError("domain_enrollment")
    resource.__post_init__()
    result = _domain_in(context, resource)
    if (
        result.owner is not None
        and read_shared_owner_in(context, result.owner, expected_service_id=expected_service_id) is None
    ):
        raise CompositionAdmissionError("guard_readback")
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    return result


def iter_shared_owners_in(context: CompositionSqlContext, *, expected_service_id: str) -> Iterator[SharedOwnerRecord]:
    """Stream all complete originals; resume only inside the same transaction."""
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    orphans = _rows(
        context,
        f"SELECT TOP (1) p.guard_id FROM {_table(context, 'owner_domains')} p WITH (HOLDLOCK) "
        f"LEFT JOIN {_table(context, 'owners')} o WITH (HOLDLOCK) ON o.owner_key=p.owner_key "
        f"LEFT JOIN {_table(context, 'domains')} d WITH (HOLDLOCK) ON d.guard_id=p.guard_id "
        "WHERE o.owner_key IS NULL OR d.guard_id IS NULL;",
    )
    if orphans or _rows(
        context,
        f"SELECT TOP (1) d.guard_id FROM {_table(context, 'domains')} d WITH (HOLDLOCK) "
        f"LEFT JOIN {_table(context, 'owner_domains')} p WITH (HOLDLOCK) "
        "ON p.guard_id=d.guard_id AND p.fencing_epoch=d.fencing_epoch "
        "WHERE d.fencing_epoch>0 AND p.guard_id IS NULL;",
    ):
        raise CompositionAdmissionError("guard_partition")
    prior: str | None = None
    while True:
        require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
        page = _rows(
            context,
            f"SELECT TOP (1) {_owner_columns()} FROM {_table(context, 'owners')} WITH (HOLDLOCK) "
            + ("" if prior is None else "WHERE owner_key>? ")
            + "ORDER BY owner_key;",
            *(() if prior is None else (prior,)),
        )
        if not page:
            require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
            return
        if len(page) != 1 or len(page[0]) != 6:
            raise CompositionAdmissionError("occurrence_identity")
        require_digest(page[0][0])
        if prior is not None and page[0][0] <= prior:
            raise CompositionAdmissionError("shared_owner_order")
        record = _owner_record(context, page[0])
        prior = record.reference.owner_key
        require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
        yield record
