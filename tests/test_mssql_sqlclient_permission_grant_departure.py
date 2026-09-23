"""Closed GRANT departure wire contracts remain distinct from OBSERVE."""

from dataclasses import replace
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientPermissionRow
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientObserverAdmission,
    SqlClientSessionAuthority,
)
from dpone.contracts.mssql_sqlclient_observer_incarnation import observer_incarnation_digest
from dpone.contracts.mssql_sqlclient_permission_grant_departure import (
    PermissionGrantDepartureEvidenceContext,
    SqlClientPermissionGrantDepartureCredentials,
    SqlClientPermissionGrantDeparturePlan,
    SqlClientPermissionGrantDepartureRequest,
    SqlClientPermissionGrantDepartureResult,
)
from dpone.contracts.mssql_sqlclient_permission_grant_departure_codec import (
    decode_credentials,
    decode_plan,
    decode_request,
    decode_result,
    encode_credentials,
    encode_plan,
    encode_request,
    encode_result,
    evidence_payload,
    evidence_subject,
)
from dpone.contracts.mssql_sqlclient_stage_observation import SqlClientStageObservation
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation, authority_digest
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_session import coordinator_authority_digest
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.mssql_sqlclient_departure_v2_fixtures import sample
from tests.test_mssql_tds_permission_grant import setup_run


def grant_departure_fixture(held=None):
    if held is None:
        invoke, _, _, _, _, _ = setup_run()
        held = invoke()
    evidence = held.result
    assert evidence is not None and held.binding is not None
    request = evidence.request
    old = sample()
    database = SqlClientDatabaseAuthority(
        request.stage.database_id,
        request.stage.database_name,
        str(request.stage.database_guid),
        old.admission.database.owner_sid,
    )
    management = SqlClientObserverAdmission(
        old.admission.server, database, request.management_login, old.admission.transport
    )
    writer = replace(management, login=request.writer_login)
    authority_sha = coordinator_authority_digest(
        [
            management.server.server_name,
            management.server.machine_name,
            management.server.instance_name,
            management.server.physical_machine_name,
            management.database.database_name,
            management.database.database_id,
            request.stage.database_guid,
            management.login.name,
            bytes.fromhex(management.login.sid),
            management.login.original_name,
            bytes.fromhex(management.login.original_sid),
            request.management.name,
            request.management.principal_id,
            bytes.fromhex(request.management.sid),
        ]
    )
    original = replace(evidence.authority.session, authority_sha256=authority_sha)
    coordinator_authority = replace(evidence.authority, session=original)
    evidence = replace(
        evidence,
        authority=coordinator_authority,
        grant=replace(
            evidence.grant,
            session=original,
            authority_sha256=authority_digest(coordinator_authority),
        ),
    )
    plan = SqlClientPermissionGrantDeparturePlan(
        helper_id=UUID(int=901),
        grant_evidence=evidence,
        management_admission=management,
        writer_admission=writer,
        implementation_sha256=evidence.operation.implementation_sha256,
        package_root="/tmp/dpone",
        admission_sha256="a" * 64,
        startup_deadline=1.0,
        operation_deadline=2.0,
        max_address_space_bytes=1024,
    )
    startup = TdsCoordinatorStartup(
        held.binding.startup.process, plan.implementation_sha256, plan.package_root, b"s" * 32
    )
    departure_request = SqlClientPermissionGrantDepartureRequest(plan=plan, startup=startup)
    authority = SqlClientSessionAuthority(
        management.server,
        management.database,
        management.login,
        management.transport,
        request.management,
    )
    observer = replace(
        old.observer,
        session_id=evidence.authority.session.session_id + 1,
        authority=authority,
        visibility=replace(old.observer.visibility, database_id=request.stage.database_id),
    )
    observer_sha = observer_incarnation_digest(observer)
    samples = tuple(
        replace(
            value,
            raw_count=0,
            own_count=0,
            request=None,
            before_sha256=observer_sha,
            after_sha256=observer_sha,
        )
        for value in old.samples
    )
    absence = replace(
        old,
        original=original,
        database=TdsDatabaseObservation(
            request.stage.database_name, request.stage.database_id, request.stage.database_guid
        ),
        admission=management,
        principal=request.management.principal,
        observer=observer,
        samples=samples,
    )
    permission_types = {"INSERT": "IN", "SELECT": "SL", "VIEW DEFINITION": "VW"}
    rows = tuple(
        SqlClientPermissionRow(
            value.class_id,
            value.major_id,
            value.minor_id,
            value.grantee_id,
            value.grantor_id,
            permission_types[value.permission_name],
            value.permission_name,
            value.state,
        )
        for value in evidence.direct_permissions
    )
    stage = SqlClientStageObservation(
        before=request.stage,
        after=request.stage,
        management_before=observer,
        management_after=observer,
        empty=1,
        metadata_permissions=(1, 1, 1),
    )
    result = SqlClientPermissionGrantDepartureResult(
        request_sha256=sha256(encode_request(departure_request)).hexdigest(),
        absence=absence,
        catalog_observer=observer,
        direct_permissions=rows,
        stage=stage,
    )
    return held, departure_request, result


def test_distinct_plan_request_result_and_private_credentials_roundtrip():
    held, request, result = grant_departure_fixture()
    plan_raw, request_raw, result_raw = (
        encode_plan(request.plan),
        encode_request(request),
        encode_result(result, request),
    )
    assert decode_plan(plan_raw) == request.plan
    assert decode_request(request_raw) == request
    assert decode_result(result_raw, request) == result
    material = TdsConnectionMaterial(
        "host",
        1433,
        request.plan.grant_evidence.authority.database.name,
        request.plan.management_admission.login.name,
        "secret",
    )
    value = SqlClientPermissionGrantDepartureCredentials(
        request=request, connection_material=material, session_nonce=b"n" * 32
    )
    raw = encode_credentials(value)
    assert b"secret" in raw and "secret" not in repr(value)
    assert (
        decode_credentials(
            raw,
            startup=request.startup,
            admission_sha256=request.plan.admission_sha256,
            startup_deadline=request.plan.startup_deadline,
            operation_deadline=request.plan.operation_deadline,
            max_address_space_bytes=request.plan.max_address_space_bytes,
        )
        == value
    )
    assert held.result != request.plan.grant_evidence


def grant_evidence_payloads():
    _, request, result = grant_departure_fixture()
    local_exit = TdsChildExit(request.startup.process, 0, True)
    payloads, digests = {}, []
    for kind in Kind:
        payload = evidence_payload(
            request.plan,
            kind,
            startup=None if kind is Kind.LAUNCH_INTENT else request.startup,
            request=request if list(Kind).index(kind) >= 2 else None,
            result=result if list(Kind).index(kind) >= 3 else None,
            local_exit=local_exit if list(Kind).index(kind) >= 4 else None,
            previous_sha256=tuple(digests),
        )
        payloads[kind] = payload
        digests.append(sha256(payload).hexdigest())
    return request, result, payloads


@pytest.mark.parametrize("kind", list(Kind))
def test_all_six_grant_evidence_kinds_have_closed_subject_validation(kind):
    request, _, payloads = grant_evidence_payloads()
    context = PermissionGrantDepartureEvidenceContext(request.plan)
    assert evidence_subject(payloads[kind], kind, context)[0] == request.plan.helper_id
    body = strict_json_object(payloads[kind])
    body["kind"] = next(value for value in Kind if value is not kind).value
    with pytest.raises(ValueError):
        evidence_subject(canonical_json_bytes(body), kind, context)


def test_registration_ignores_already_bound_request_until_credential_intent():
    _, request, _ = grant_departure_fixture()
    without_request = evidence_payload(
        request.plan,
        Kind.REGISTRATION,
        startup=request.startup,
        request=None,
        result=None,
        local_exit=None,
        previous_sha256=("a" * 64,),
    )
    with_bound_request = evidence_payload(
        request.plan,
        Kind.REGISTRATION,
        startup=request.startup,
        request=request,
        result=None,
        local_exit=None,
        previous_sha256=("a" * 64,),
    )
    assert with_bound_request == without_request
    evidence_subject(with_bound_request, Kind.REGISTRATION, PermissionGrantDepartureEvidenceContext(request.plan))


@pytest.mark.parametrize("change", ["absence", "observer", "permission", "permission_type", "stage"])
def test_remote_result_rejects_every_foreign_persistence_predicate(change):
    _, request, result = grant_departure_fixture()
    with pytest.raises(ValueError):
        if change == "absence":
            value = replace(
                result,
                absence=replace(result.absence, original=replace(result.absence.original, session_id=1)),
            )
        elif change == "observer":
            value = replace(result, catalog_observer=replace(result.catalog_observer, session_id=1))
        elif change == "permission":
            value = replace(result, direct_permissions=result.direct_permissions[:-1])
        elif change == "permission_type":
            value = replace(
                result,
                direct_permissions=(replace(result.direct_permissions[0], type="XX"), *result.direct_permissions[1:]),
            )
        else:
            value = replace(result, stage=replace(result.stage, empty=0))
        encode_result(value, request)


@pytest.mark.parametrize("substitution", [None, "admission", "writer", "public", "public_sid"])
def test_adapter_uses_one_management_session_for_absence_grants_and_stage(monkeypatch, substitution):
    from dataclasses import astuple
    from types import SimpleNamespace

    from dpone.adapters import mssql_sqlclient_permission_grant_departure as adapter
    from dpone.adapters.mssql_tds_coordinator_connection import TdsSqlConnection

    _, request, expected = grant_departure_fixture()
    cursor, calls = object(), []
    connection = TdsSqlConnection(SimpleNamespace(close=lambda: None), cursor)

    class Sql:
        def __init__(self, value, *args):
            assert value is connection

        def acquire(self, nonce, *, deadline):
            calls.append(("acquire", nonce))

        def check_deadline(self, *, deadline):
            calls.append(("deadline", deadline))

    class Exclusion:
        def __init__(self, value, **kwargs):
            assert value is cursor

        def observe_departure_v2(self, **kwargs):
            calls.append(("absence", kwargs["original"]))
            return expected.absence

    session = object()

    class Catalog:
        @classmethod
        def _sharing(cls, sql, *, deadline, session):
            calls.append(("catalog", session))
            return cls()

        def read_own_incarnation(self):
            return ("incarnation",)

        def writer_admission(self, *args):
            admission = request.plan.writer_admission
            rows = (
                (
                    admission.server.server_name,
                    admission.server.machine_name,
                    admission.server.instance_name,
                    admission.server.physical_machine_name,
                    admission.database.database_id,
                    admission.database.database_name,
                    request.plan.grant_evidence.request.stage.database_guid,
                    bytes.fromhex(admission.database.owner_sid),
                    admission.login.principal_id,
                    admission.login.name,
                    bytes.fromhex(admission.login.sid),
                    int(admission.login.is_sysadmin),
                    "SQL_LOGIN",
                ),
            )
            return ((1,),) if substitution == "admission" else rows

        def principals(self, *args):
            writer = request.plan.grant_evidence.request.writer
            writer_name = "substitute" if substitution == "writer" else writer.name
            public_sid = b"well-known-public-role" if substitution == "public_sid" else b"\x00"
            public_name = "substitute" if substitution == "public" else "public"
            return (
                (writer.principal_id, writer_name, bytes.fromhex(writer.sid), "SQL_USER", "INSTANCE"),
                (0, public_name, public_sid, "DATABASE_ROLE", "NONE"),
            )

        def permissions(self, *args, **kwargs):
            return tuple(astuple(value) for value in expected.direct_permissions)

    class Stage:
        @classmethod
        def _sharing(cls, current, **kwargs):
            assert current is session
            return cls()

        def observe(self, value):
            calls.append(("stage", value))
            return expected.stage

    monkeypatch.setattr(adapter, "TdsCoordinatorSql", Sql)
    monkeypatch.setattr(adapter, "SqlClientCreateExclusionObserverV2", Exclusion)
    monkeypatch.setattr(adapter, "ObservationCursor", lambda value, **kwargs: session)
    monkeypatch.setattr(adapter, "SqlClientGrantCatalog", Catalog)
    monkeypatch.setattr(adapter, "SqlClientStageObserver", Stage)
    monkeypatch.setattr(adapter, "parse_observer_incarnation_rows", lambda *args, **kwargs: expected.catalog_observer)
    if substitution not in (None, "public_sid"):
        with pytest.raises(RuntimeError):
            adapter.SqlClientPermissionGrantDepartureObserver(connection, request).observe(b"n" * 32)
        return
    observed = adapter.SqlClientPermissionGrantDepartureObserver(connection, request).observe(b"n" * 32)
    assert observed == expected
    assert [name for name, _ in calls if name in {"absence", "catalog", "stage"}] == [
        "absence",
        "catalog",
        "stage",
    ]
