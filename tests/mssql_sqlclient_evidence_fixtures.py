"""Synthetic immutable originals; no connection, credential or authority admission."""

from copy import deepcopy
from dataclasses import asdict, replace
from hashlib import sha256

from dpone.contracts.mssql_sqlclient_evidence_binding import SqlClientEvidenceBinding
from dpone.contracts.mssql_sqlclient_input import input_descriptor_digest
from dpone.contracts.mssql_sqlclient_launch import launch_digest
from dpone.contracts.mssql_sqlclient_registration import (
    SqlClientCredentialIntentRecord,
    SqlClientRegistration,
    encode_credential_intent,
    encode_registration,
)
from dpone.contracts.mssql_sqlclient_session_control import SqlClientSessionAnnouncement
from dpone.contracts.mssql_sqlclient_writer_evidence import SqlClientWriterObservationRecord
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_mssql_sqlclient_input_descriptor import descriptor
from tests.test_mssql_sqlclient_launch_contract import launch, ready
from tests.test_mssql_sqlclient_observation import NONCE, observation
from tests.test_mssql_sqlclient_session_control import OBJECT, OWNER
from tests.test_mssql_tds_lifecycle import identity


def registration():
    source = deepcopy(descriptor())
    attempt = identity(database="target", file_sha256=source.expected.file_sha256)
    started = replace(
        launch(), attempt_sha256=attempt_identity_digest(attempt), input_binding_sha256=input_descriptor_digest(source)
    )
    binding = SqlClientEvidenceBinding(
        launch_digest(started),
        started.attempt_sha256,
        attempt,
        replace(OWNER),
        started.process,
        replace(OBJECT),
        started.input_binding_sha256,
        started.build_sha256,
        started.operation_deadline_ns,
    )
    return SqlClientRegistration(
        binding=binding,
        launch=started,
        ready=ready(started),
        input=source,
        input_mode="rows",
        batch_rows=2,
        max_input_batch_bytes=1 << 20,
    )


def intent(reg=None):
    reg = registration() if reg is None else reg
    empty = reg.input.expected.rows == 0
    nonce = None if empty else NONCE.hex()
    projection = dict(
        schema_version=1,
        launch_sha256=reg.binding.launch_sha256,
        identity=asdict(reg.binding.identity),
        ownership=asdict(reg.binding.ownership),
        object_identity=asdict(reg.binding.object_identity),
        input=asdict(reg.input),
        input_mode=reg.input_mode,
        batch_rows=reg.batch_rows,
        max_input_batch_bytes=reg.max_input_batch_bytes,
        session_nonce=nonce,
        credentials_present=not empty,
    )
    return SqlClientCredentialIntentRecord(
        binding=reg.binding,
        registration_sha256=sha256(encode_registration(reg)).hexdigest(),
        job_binding_sha256=sha256(b"dpone.sqlclient.job-binding.v1\0" + canonical_json_bytes(projection)).hexdigest(),
        input_empty=empty,
        session_nonce=nonce,
        tls_profile=None if empty else "verified",
        capability_evidence_sha256="f" * 64,
    )


def writer():
    reg = registration()
    cred = intent(reg)
    observed = observation()
    return SqlClientWriterObservationRecord(
        binding=reg.binding,
        registration_sha256=cred.registration_sha256,
        credential_intent_sha256=sha256(encode_credential_intent(cred)).hexdigest(),
        capability_evidence_sha256=cred.capability_evidence_sha256,
        announcement=SqlClientSessionAnnouncement(
            1, reg.binding.launch_sha256, reg.binding.attempt_sha256, observed.remote_session.session_id, NONCE.hex()
        ),
        remote_session=observed.remote_session,
        authority=observed.authority,
        resolved_database_principal=observed.resolved_database_principal,
    )


def evidence_record(kind):
    from dpone.contracts.mssql_sqlclient_evidence import SqlClientEvidenceRecord
    from dpone.contracts.mssql_sqlclient_evidence_types import SqlClientEvidenceKind as Kind
    from dpone.contracts.mssql_sqlclient_execution_evidence import (
        SqlClientLocalExit,
        SqlClientResultContext,
        encode_worker_local_exit,
    )
    from dpone.contracts.mssql_sqlclient_result import SqlClientResult, encode_sqlclient_result
    from dpone.contracts.mssql_sqlclient_session_control import encode_bulk_grant
    from dpone.contracts.mssql_sqlclient_writer_evidence import encode_writer_observation
    from dpone.contracts.mssql_sqlclient_writer_settlement import (
        SqlClientStageContentExpectation,
        SqlClientWriterSettlementObservation,
        SqlClientWriterSettlementProvenance,
        SqlClientWriterSettlementRecord,
        encode_writer_settlement,
    )
    from dpone.contracts.mssql_tds_result import TdsWorkerResult
    from dpone.contracts.mssql_tds_worker import TdsChildExit
    from tests.test_mssql_sqlclient_session_control import grant

    r = registration()
    b = r.binding
    context = None
    if kind is Kind.REGISTRATION:
        payload = encode_registration(r)
    elif kind is Kind.CREDENTIAL_INTENT:
        payload = encode_credential_intent(intent(r))
    elif kind is Kind.WRITER_OBSERVATION:
        payload = encode_writer_observation(writer())
    elif kind is Kind.GRANT_INTENT:
        payload = encode_bulk_grant(
            replace(
                grant(),
                attempt_sha256=b.attempt_sha256,
                launch_sha256=b.launch_sha256,
                input_binding_sha256=b.input_binding_sha256,
            )
        )
    elif kind is Kind.RESULT:
        g = grant().grant_id
        context = SqlClientResultContext(r.launch, r.input.expected, g)
        payload = encode_sqlclient_result(
            SqlClientResult(
                1, b.launch_sha256, b.attempt_sha256, g, TdsWorkerResult(b.attempt_sha256, r.input.expected, None)
            )
        )
    elif kind is Kind.LOCAL_EXIT:
        payload = encode_worker_local_exit(
            SqlClientLocalExit(
                binding=b,
                registration_sha256=sha256(encode_registration(r)).hexdigest(),
                result_sha256=None,
                exit=TdsChildExit(b.process, 0, True),
            )
        )
    else:
        from uuid import UUID

        from dpone.contracts.mssql_sqlclient_observation import (
            SqlClientObserverAdmission,
            SqlClientSessionAuthority,
        )
        from dpone.contracts.mssql_sqlclient_observer_incarnation import observer_incarnation_digest
        from dpone.contracts.mssql_sqlclient_writer_session_departure import (
            SqlClientWriterSessionDeparture,
        )
        from tests.mssql_sqlclient_departure_v2_fixtures import sample
        from tests.test_mssql_sqlclient_stage_identity import stage

        writer_record = writer()
        base = sample(reused=False)
        authority = writer_record.authority
        observer_authority = SqlClientSessionAuthority(
            authority.server,
            authority.database,
            base.observer.authority.login,
            base.observer.authority.transport,
            base.observer.authority.principal_resolution,
        )
        observer = replace(
            base.observer,
            connection_id=UUID(int=writer_record.remote_session.connection_id.int + 1),
            session_id=writer_record.remote_session.session_id + 1,
            authority=observer_authority,
            visibility=replace(base.observer.visibility, database_id=authority.database.database_id),
        )
        guard = observer_incarnation_digest(observer)
        departure = SqlClientWriterSessionDeparture(
            original=writer_record.remote_session,
            writer_admission=SqlClientObserverAdmission(
                authority.server,
                authority.database,
                authority.login,
                authority.transport,
            ),
            writer_authority=authority,
            principal=writer_record.resolved_database_principal,
            observer=observer,
            samples=tuple(replace(item, before_sha256=guard, after_sha256=guard) for item in base.samples),
        )
        observed_stage = stage()
        observed = SqlClientWriterSettlementObservation(
            departure=departure,
            stage_before=observed_stage,
            stage_after=observed_stage,
            observer_before=departure.observer,
            observer_after=departure.observer,
            row_count=1,
            typed_digest="1" * 64,
        )
        payload = encode_writer_settlement(
            SqlClientWriterSettlementRecord(
                attempt_sha256=b.attempt_sha256,
                registration_sha256="2" * 64,
                writer_observation_sha256="3" * 64,
                grant_sha256="4" * 64,
                result_sha256="5" * 64,
                local_exit_sha256="6" * 64,
                p9_settlement_sha256="7" * 64,
                helper=SqlClientWriterSettlementProvenance(
                    startup_sha256="9" * 64,
                    request_sha256="a" * 64,
                    result_sha256="b" * 64,
                    local_exit_sha256="c" * 64,
                    implementation_sha256="d" * 64,
                    admission_sha256="e" * 64,
                ),
                expectation=SqlClientStageContentExpectation(1, "8" * 64, "1" * 64),
                observation=observed,
            )
        )
    return SqlClientEvidenceRecord(b.attempt_sha256, kind, payload, context)


def maximum_registration(character):
    """Actual contract maxima, including 100 distinct escaped/Unicode SQL names."""
    r = registration()
    sql_name = character * (128 // (len(character.encode("utf-16le")) // 2))
    source = replace(
        r.input,
        columns=tuple(
            replace(
                r.input.columns[i % len(r.input.columns)],
                name=character * (124 // (len(character.encode("utf-16le")) // 2)) + f"{i:04}",
            )
            for i in range(100)
        ),
        max_row_bytes=2**31 - 1,
    )
    receipt = replace(source.expected, rows=2**63 - 1, encoded_bytes=2**63 - 1)
    source = replace(
        source,
        expected=receipt,
        file_identity=replace(
            source.file_identity,
            device=2**64 - 1,
            inode=2**64 - 1,
            size=receipt.encoded_bytes,
            mtime_ns=2**63 - 1,
            ctime_ns=2**63 - 1,
        ),
    )
    attempt = replace(
        r.binding.identity,
        target_key=character * 256,
        run_id=character * 256,
        database=sql_name,
        schema=sql_name,
        table=sql_name,
        ordinal=2**63 - 1,
        attempt=2,
    )
    process = replace(r.launch.process, pid=2**31 - 1, start_ticks=2**63 - 1)
    started = replace(
        r.launch,
        process=process,
        attempt_sha256=attempt_identity_digest(attempt),
        input_binding_sha256=input_descriptor_digest(source),
        parent_pid=2**31 - 2,
        address_space_bytes=16 * 1024**3,
        startup_deadline_ns=2**63 - 1,
        operation_deadline_ns=2**63 - 1,
    )
    binding = replace(
        r.binding,
        identity=attempt,
        ownership=replace(r.binding.ownership, owner=character * 256, fence=2**63 - 1),
        object_identity=replace(r.binding.object_identity, object_id=2**31 - 1),
        process=process,
        launch_sha256=launch_digest(started),
        attempt_sha256=started.attempt_sha256,
        input_binding_sha256=started.input_binding_sha256,
        operation_deadline_ns=started.operation_deadline_ns,
    )
    return replace(
        r,
        binding=binding,
        launch=started,
        ready=ready(started),
        input=source,
        batch_rows=65536,
        max_input_batch_bytes=256 << 20,
    )


def maximum_writer(character):
    """Full authority at admitted name/SID/ID bounds; no real SQL observation."""
    from dpone.contracts.mssql_sqlclient_observation import session_authority_digest

    r = maximum_registration(character)
    c = intent(r)
    w = writer()
    name = r.binding.identity.database
    authority = replace(
        w.authority,
        server=replace(
            w.authority.server, server_name=name, machine_name=name, instance_name=name, physical_machine_name=name
        ),
        database=replace(w.authority.database, database_id=2**31 - 1, database_name=name, owner_sid="bb" * 85),
        login=replace(
            w.authority.login,
            principal_id=2**31 - 1,
            name=name,
            original_name=name,
            sid="aa" * 85,
            original_sid="aa" * 85,
        ),
        principal_resolution=replace(
            w.authority.principal_resolution, principal_id=2**31 - 1, name=name, sid="aa" * 85
        ),
    )
    remote = replace(w.remote_session, session_id=32767, authority_sha256=session_authority_digest(authority))
    return replace(
        w,
        binding=r.binding,
        registration_sha256=c.registration_sha256,
        credential_intent_sha256=sha256(encode_credential_intent(c)).hexdigest(),
        announcement=replace(
            w.announcement,
            launch_sha256=r.binding.launch_sha256,
            attempt_sha256=r.binding.attempt_sha256,
            session_id=remote.session_id,
        ),
        remote_session=remote,
        authority=authority,
        resolved_database_principal=authority.principal_resolution.principal,
    )
