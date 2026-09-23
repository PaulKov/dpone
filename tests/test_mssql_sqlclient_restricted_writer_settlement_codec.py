"""P9b wire contracts bind exact P9a facts and explicit COUNT_BIG proof."""

from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceRecord
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind
from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientGrantPrincipal
from dpone.contracts.mssql_sqlclient_permission_grant import permission_grant_digest
from dpone.contracts.mssql_sqlclient_restricted_session_departure import (
    RestrictedDepartureSample,
    RestrictedDepartureSampleKind,
    RestrictedObserverRequest,
    RestrictedSessionDeparture,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement import (
    RestrictedWriterDepartureCredentials,
    RestrictedWriterDepartureEvidenceContext,
    RestrictedWriterDeparturePlan,
    RestrictedWriterDepartureRequest,
    RestrictedWriterDepartureResult,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement_codec import (
    RESULT_FRAME_LIMIT,
    decode_credentials,
    decode_plan,
    decode_request,
    decode_result,
    encode_credentials,
    encode_plan,
    encode_request,
    encode_result,
    evidence_payload,
    validate_result,
)
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_authority import authority_digest
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_session import TdsRestrictedRemoteSessionIdentity
from dpone.contracts.mssql_tds_worker import TdsChildExit
from tests.test_mssql_sqlclient_permission_grant_departure import grant_departure_fixture
from tests.test_mssql_sqlclient_permission_grant_wire import fixture as permission_grant_fixture
from tests.test_mssql_sqlclient_restricted_writer_verify import verify_request, verify_result


def settlement_fixture(held=None):
    _, grant_request, observed = grant_departure_fixture(held)
    request = replace(
        verify_request(),
        implementation_sha256=grant_request.plan.grant_evidence.operation.implementation_sha256,
        stage=grant_request.plan.grant_evidence.request.stage,
    )
    verified = verify_result(request)
    old_session = observed.absence.original
    restricted_session = TdsRestrictedRemoteSessionIdentity(
        old_session.connection_id,
        old_session.session_id,
        old_session.login_time,
        old_session.nonce,
        old_session.authority_sha256,
    )
    verified_context = replace(verified.opening, session=restricted_session)
    verified = replace(verified, opening=verified_context, closing=verified_context)
    grant = replace(
        grant_request.plan.grant_evidence,
        request=replace(
            grant_request.plan.grant_evidence.request,
            parent=request.parent,
            stage=request.stage,
            writer=request.writer,
            writer_login=request.writer_login,
        ),
    )
    plan = RestrictedWriterDeparturePlan(
        helper_id=UUID(int=902),
        grant_evidence=grant,
        verify_request=request,
        verify_result=verified,
        management_admission=grant_request.plan.management_admission,
        writer_admission=grant_request.plan.writer_admission,
        implementation_sha256=request.implementation_sha256,
        package_root="/tmp/dpone",
        admission_sha256="a" * 64,
        startup_deadline=1.0,
        operation_deadline=2.0,
        max_address_space_bytes=1024,
    )
    departure = RestrictedWriterDepartureRequest(plan=plan, startup=grant_request.startup)
    writer = SqlClientGrantPrincipal(
        request.writer.principal_id, request.writer.name, request.writer.sid, "SQL_USER", "INSTANCE"
    )
    public = SqlClientGrantPrincipal(0, "public", "00", "DATABASE_ROLE", "NONE")
    restricted_samples = []
    for sample in observed.absence.samples:
        request_sample = sample.request
        restricted_samples.append(
            RestrictedDepartureSample(
                kind=RestrictedDepartureSampleKind(sample.kind.value),
                raw_count=sample.raw_count,
                own_count=sample.own_count,
                original_epoch_count=0 if sample.kind.value == "sessions" else None,
                request=None
                if request_sample is None
                else RestrictedObserverRequest(
                    connection_id=request_sample.connection_id,
                    session_id=request_sample.session_id,
                    request_id=request_sample.request_id,
                    start_time=request_sample.start_time,
                ),
                before_sha256=sample.before_sha256,
                after_sha256=sample.after_sha256,
            )
        )
    absence = RestrictedSessionDeparture(
        original=restricted_session,
        database=observed.absence.database,
        admission=observed.absence.admission,
        principal=observed.absence.principal,
        observer=observed.absence.observer,
        samples=tuple(restricted_samples),
    )
    result = RestrictedWriterDepartureResult(
        request_sha256=sha256(encode_request(departure)).hexdigest(),
        absence=absence,
        catalog_observer=observed.catalog_observer,
        principals=(writer, public),
        direct_permissions=observed.direct_permissions,
        stage=replace(observed.stage, before=request.stage, after=request.stage),
        row_count=0,
    )
    return plan, departure, result


def test_round_trips_exact_plan_request_result_and_private_credentials():
    plan, request, result = settlement_fixture()
    assert decode_plan(encode_plan(plan)) == plan
    assert decode_request(encode_request(request)) == request
    assert decode_result(encode_result(result, request), request) == result
    credentials = RestrictedWriterDepartureCredentials(
        request=request,
        connection_material=TdsConnectionMaterial(
            "localhost",
            1433,
            plan.grant_evidence.authority.database.name,
            plan.management_admission.login.name,
            "secret",
        ),
        session_nonce=b"n" * 32,
    )
    assert decode_credentials(encode_credentials(credentials)) == credentials


def test_plan_codec_keeps_grant_and_restricted_helper_implementations_distinct():
    plan, _, _ = settlement_fixture()
    restricted_implementation = "c" * 64
    assert plan.grant_evidence.operation.implementation_sha256 != restricted_implementation
    changed = replace(
        plan,
        verify_request=replace(plan.verify_request, implementation_sha256=restricted_implementation),
        implementation_sha256=restricted_implementation,
    )
    assert decode_plan(encode_plan(changed)) == changed


@pytest.mark.parametrize("change", [{"row_count": 1}, {"principals": ()}, {"direct_permissions": ()}])
def test_result_rejects_missing_exact_count_principal_or_grant_projection(change):
    _, request, result = settlement_fixture()
    with pytest.raises(ValueError):
        validate_result(replace(result, **change), request)


def test_result_accepts_observed_database_specific_public_role_sid():
    _, request, result = settlement_fixture()
    observed_public = replace(result.principals[1], sid="010500000000000904000000")
    changed = replace(result, principals=(result.principals[0], observed_public))
    assert decode_result(encode_result(changed, request), request) == changed


def test_canonical_hundred_column_result_fits_bounded_ipc_frame():
    binding, _, _, evidence = permission_grant_fixture()
    base = evidence.request.stage.columns[0]
    columns = tuple(replace(base, ordinal=index, name=f"c{index:03d}" + "x" * 124) for index in range(1, 101))
    grant_request = replace(evidence.request, stage=replace(evidence.request.stage, columns=columns))
    operation = replace(evidence.operation, command_sha256=permission_grant_digest(grant_request))
    operation_sha = coordinator_identity_digest(operation)
    authority = replace(evidence.authority, operation_sha256=operation_sha)
    grant = replace(evidence.grant, operation_sha256=operation_sha, authority_sha256=authority_digest(authority))
    wide_evidence = replace(
        evidence,
        request=grant_request,
        operation=operation,
        grant=grant,
        authority=authority,
    )
    plan, wide_request, wide_result = settlement_fixture(
        SimpleNamespace(result=wide_evidence, binding=SimpleNamespace(startup=binding.startup))
    )

    payload = encode_result(wide_result, wide_request)

    assert 32768 < len(payload) <= RESULT_FRAME_LIMIT
    assert decode_result(payload, wide_request) == wide_result


def test_result_decoder_rejects_payload_above_frame_limit():
    _, request, _ = settlement_fixture()
    with pytest.raises(ValueError):
        decode_result(b"x" * (RESULT_FRAME_LIMIT + 1), request)


def test_value_codec_is_canonical_while_identity_is_left_to_owner_boundary():
    plan, _, _ = settlement_fixture()
    substituted = replace(plan.verify_request)
    # The value contract accepts canonical bytes; exact-object custody is enforced by composition/service.
    assert replace(plan, verify_request=substituted) == plan


@pytest.mark.parametrize("kind", tuple(SqlClientDepartureEvidenceKind))
def test_generic_departure_evidence_accepts_only_exact_p9b_context(kind):
    plan, request, result = settlement_fixture()
    index = tuple(SqlClientDepartureEvidenceKind).index(kind)
    local_exit = TdsChildExit(request.startup.process, 0, True)
    payload = evidence_payload(
        plan,
        kind,
        startup=request.startup if index >= 1 else None,
        request=request if index >= 2 else None,
        result=result if index >= 3 else None,
        local_exit=local_exit if index >= 4 else None,
        previous_sha256=tuple(str(value + 1) * 64 for value in range(index)),
    )
    record = SqlClientDepartureEvidenceRecord(
        plan.helper_id,
        attempt_identity_digest(plan.attempt),
        kind,
        payload,
        restricted_writer_context=RestrictedWriterDepartureEvidenceContext(plan),
    )
    assert record.receipt.kind is kind
