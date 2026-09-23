"""Actual producer seal and sequential source-free historical CREATE acquisition.

All store/filesystem I/O remains on the run's bounded actor pool and original
absolute deadline. UNKNOWN propagates the actual gateway; no repair or replay.
"""

from dataclasses import replace
from hashlib import sha256

from dpone.adapters.filesystem_evidence import PinnedEvidenceReadFactory
from dpone.adapters.mssql_sqlclient_create_evidence_gateway import (
    SqlClientCreateEvidenceActor,
    SqlClientCreateEvidenceGatewayBundle,
    TdsActorPool,
    observe_create_evidence,
    read_coordinator_evidence,
)
from dpone.adapters.mssql_tds_coordinator_journal import TdsCoordinatorJournal
from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.app.mssql_sqlclient_stage_locator_composition import _AdmittedSqlClientStoreFactory
from dpone.app.mssql_tds_coordinator_request import decode_create_response
from dpone.app.mssql_tds_coordinator_supervisor import TdsCoordinatorCreateOutcome
from dpone.app.mssql_tds_create_provenance import TdsCoordinatorCreateProvenance, validate_create_provenance
from dpone.contracts.mssql_tds_api import (
    SqlClientAuthenticatedCreate,
    SqlClientCreateSeal,
    SqlClientGrantMember,
    SqlClientStageLocator,
    WindowLease,
    authenticated_create_bytes,
    canonical_json_bytes,
    decode_authority,
    decode_create_request,
    decode_create_seal,
    decode_local_exit,
    decode_stage_locator,
    decode_startup,
    encode_create_seal,
    encode_stage_locator,
    stage_identity_from_create,
    strict_json_object,
    validate_locator_request,
)
from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_evidence import TdsCoordinatorEvidenceKind as Kind
from dpone.ports.bounded_window import WindowStore
from dpone.services.mssql_sqlclient_create_evidence import (
    SqlClientCreateEvidenceJournal,
    SqlClientCreateSealObservation,
)
from dpone.services.mssql_sqlclient_stage_locator import SqlClientStageLocatorJournal
from dpone.services.mssql_tds_attempt_continuation import TdsObserveContinuation


def _observe(
    *,
    admitted_factory: _AdmittedSqlClientStoreFactory,
    locator: SqlClientStageLocator,
    pool: TdsActorPool,
    deadline: float,
    seal: SqlClientCreateSeal | None = None,
    lease: WindowLease | None = None,
) -> SqlClientCreateSealObservation:
    if type(admitted_factory) is not _AdmittedSqlClientStoreFactory:
        raise ValueError("mssql_native.sqlclient_state_domain_admission_required")
    admitted_locator = decode_stage_locator(encode_stage_locator(locator))
    admitted_seal = None if seal is None else decode_create_seal(encode_create_seal(seal), admitted_locator)
    if (seal is None) != (lease is None) or (lease is not None and type(lease) is not WindowLease):
        raise ValueError("mssql_native.sqlclient_create_seal_mode_invalid")

    def initial(backend: WindowStore) -> SqlClientCreateSealObservation:
        parents = TdsAttemptJournal(backend, backend="mssql_sqlclient")
        directories = TdsCoordinatorDirectoryJournal(backend, parent_observer=parents)
        coordinators = TdsCoordinatorJournal(backend, directories)
        locators = SqlClientStageLocatorJournal(
            backend,
            admitted_factory._record,
            parent_observer=parents,
            directory_observer=directories,
            coordinator_observer=coordinators,
        )
        journal = SqlClientCreateEvidenceJournal(backend, locators, coordinators)
        if admitted_seal is None:
            return journal.read(admitted_locator)
        assert lease is not None
        return journal.create(admitted_locator, admitted_seal, replace(lease))

    return observe_create_evidence(
        bundle=SqlClientCreateEvidenceGatewayBundle(admitted_factory, initial, SqlClientCreateSealObservation),
        locator=admitted_locator,
        pool=pool,
        deadline=deadline,
        actor_type=SqlClientCreateEvidenceActor,
        seal=admitted_seal,
        lease=None if lease is None else replace(lease),
    )


def seal_sqlclient_create(
    *,
    admitted_factory: _AdmittedSqlClientStoreFactory,
    locator: SqlClientStageLocator,
    created: TdsCoordinatorCreateOutcome,
    lease: WindowLease,
    pool: TdsActorPool,
    deadline: float,
) -> SqlClientCreateSealObservation:
    """Seal only the retained actual original result after full provenance checks."""
    if type(created) is not TdsCoordinatorCreateOutcome or created.provenance is None:
        raise ValueError("mssql_native.sqlclient_create_provenance_required")
    p = created.provenance
    validate_locator_request(locator, p.request)
    validate_create_provenance(
        p,
        response=created.response,
        local_exit=created.local_exit,
        receipts=created.receipts,
        request=p.request,
        identity=locator.create_operation,
        ownership=locator.execution_owner,
    )
    receipts = {receipt.kind: receipt for receipt in created.receipts}
    seal = SqlClientCreateSeal(
        locator.state_domain_id,
        sha256(encode_stage_locator(locator)).hexdigest(),
        p.snapshot,
        tuple(receipts[kind] for kind in Kind),
    )
    return _observe(
        admitted_factory=admitted_factory, locator=locator, pool=pool, deadline=deadline, seal=seal, lease=lease
    )


def acquire_authenticated_create(
    *,
    admitted_factory: _AdmittedSqlClientStoreFactory,
    member: SqlClientGrantMember,
    reader_factory: PinnedEvidenceReadFactory,
    pool: TdsActorPool,
    deadline: float,
    continuation: TdsObserveContinuation | None = None,
) -> SqlClientAuthenticatedCreate:
    """Read actual six files; caller supplies neither CREATE outcome nor receipts."""
    if type(reader_factory) is not PinnedEvidenceReadFactory:
        raise ValueError("mssql_native.sqlclient_evidence_root_admission_required")
    if continuation is not None:
        if type(continuation) is not TdsObserveContinuation:
            raise ValueError("mssql_native.sqlclient_observe_owner_invalid")
        continuation.assert_current()
    locator = member.locator.locator
    observation = _observe(admitted_factory=admitted_factory, locator=locator, pool=pool, deadline=deadline)
    if continuation is not None:
        continuation.assert_current()
    if observation.locator != member.locator:
        raise ValueError("mssql_native.sqlclient_create_locator_changed")
    seal = observation.seal
    if continuation is not None:
        continuation.assert_current()
    payloads = read_coordinator_evidence(
        reader_factory=reader_factory, receipts=seal.receipts, pool=pool, deadline=deadline
    )
    if continuation is not None:
        continuation.assert_current()
    request_bytes, admission, registration_bytes, authority_bytes, result_bytes, local_bytes = payloads
    request = decode_create_request(request_bytes)
    validate_locator_request(locator, request)
    registration = strict_json_object(registration_bytes)
    startup = decode_startup(canonical_json_bytes(registration["startup"]))
    authority, local = decode_authority(authority_bytes), decode_local_exit(local_bytes)
    grant = seal.original.state.grant
    assert grant is not None
    response = decode_create_response(
        result_bytes, request=request, identity=locator.create_operation, grant=grant, authority=authority
    )
    provenance = TdsCoordinatorCreateProvenance(
        request=request,
        admission=admission,
        startup=startup,
        authority=authority,
        grant=grant,
        local_proof=local,
        snapshot=seal.original,
    )
    validate_create_provenance(
        provenance,
        response=response,
        local_exit=local.exit,
        receipts=seal.receipts,
        request=request,
        identity=locator.create_operation,
        ownership=locator.execution_owner,
    )
    evidence = response.evidence
    if evidence is None or stage_identity_from_create(evidence) != member.stage:
        raise ValueError("mssql_native.sqlclient_create_stage_changed")
    result = SqlClientAuthenticatedCreate(
        coordinator_identity_digest(locator.create_operation),
        seal.locator_sha256,
        sha256(observation.record.payload.encode("utf-8")).hexdigest(),
        observation.record.revision,
        seal.original,
        observation.current,
        evidence,
    )
    authenticated_create_bytes(result)
    pool.assert_deadline(deadline=deadline)
    return result


def recheck_authenticated_create(
    *,
    admitted_factory: _AdmittedSqlClientStoreFactory,
    member: SqlClientGrantMember,
    proof: SqlClientAuthenticatedCreate,
    pool: TdsActorPool,
    deadline: float,
) -> None:
    """Final store pass rejects drift after all members have released file actors."""
    observation = _observe(
        admitted_factory=admitted_factory, locator=member.locator.locator, pool=pool, deadline=deadline
    )
    if (
        observation.current != proof.current
        or observation.seal.original != proof.original
        or observation.locator != member.locator
        or observation.record.revision != proof.seal_revision
        or sha256(observation.record.payload.encode("utf-8")).hexdigest() != proof.seal_sha256
    ):
        raise ValueError("mssql_native.sqlclient_create_evidence_drift")
