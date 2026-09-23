"""External process fakes for the exact SQLClient authorization-chain test."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from dpone.contracts import mssql_sqlclient_restricted_writer_settlement_codec as restricted_codec
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientObserverAdmission,
    SqlClientSessionAuthority,
)
from dpone.contracts.mssql_sqlclient_observer_incarnation import observer_incarnation_digest
from dpone.contracts.mssql_sqlclient_permission_grant import (
    PERMISSIONS,
    SqlClientDirectPermission,
    SqlClientPermissionGrantEvidence,
    encode_permission_grant_evidence,
)
from dpone.contracts.mssql_sqlclient_permission_grant_departure import (
    SqlClientPermissionGrantDepartureResult,
)
from dpone.contracts.mssql_sqlclient_permission_grant_departure_codec import (
    decode_credentials as decode_grant_departure_credentials,
)
from dpone.contracts.mssql_sqlclient_permission_grant_departure_codec import (
    encode_request as encode_grant_departure_request,
)
from dpone.contracts.mssql_sqlclient_permission_grant_departure_codec import (
    encode_result as encode_grant_departure_result,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import (
    PermissionBoundary,
    PermissionWireBinding,
    PermissionWireKind,
    decode_permission_message,
    encode_permission_message,
)
from dpone.contracts.mssql_sqlclient_restricted_session_departure import (
    RestrictedDepartureSample,
    RestrictedDepartureSampleKind,
    RestrictedObserverRequest,
    RestrictedSessionDeparture,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_verify import (
    RestrictedWriterVerifyRegistration,
    verify_request_digest,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_codec import encode_verify_request
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_handshake import RestrictedWriterVerifyOpening
from dpone.contracts.mssql_sqlclient_stage_observation import SqlClientStageObservation
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorGrant, coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_authority import (
    TdsCoordinatorAuthority,
    TdsDatabaseObservation,
    TdsLockObservation,
    TdsSchemaObservation,
    authority_digest,
    encode_authority,
)
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup, encode_startup
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity, coordinator_authority_digest
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.contracts.strict_json import strict_json_object
from tests.mssql_sqlclient_departure_v2_fixtures import sample
from tests.test_mssql_sqlclient_restricted_writer_verify import verify_result
from tests.test_mssql_tds_coordinator import PROCESS


def _session_digest(admission, principal, database_guid):
    server, database, login = admission.server, admission.database, admission.login
    return coordinator_authority_digest(
        [
            server.server_name,
            server.machine_name,
            server.instance_name,
            server.physical_machine_name,
            database.database_name,
            database.database_id,
            database_guid,
            login.name,
            bytes.fromhex(login.sid),
            login.original_name,
            bytes.fromhex(login.original_sid),
            principal.name,
            principal.principal_id,
            bytes.fromhex(principal.sid),
        ]
    )


def _observer(plan, original, *, original_admission, original_principal):
    base = sample()
    grant = plan.grant_evidence
    request = grant.request
    database = SqlClientDatabaseAuthority(
        request.stage.database_id,
        request.stage.database_name,
        str(request.stage.database_guid),
        plan.management_admission.database.owner_sid,
    )
    management = SqlClientObserverAdmission(
        plan.management_admission.server,
        database,
        request.management_login,
        plan.management_admission.transport,
    )
    authority = SqlClientSessionAuthority(
        management.server,
        management.database,
        management.login,
        management.transport,
        request.management,
    )
    observer = replace(
        base.observer,
        session_id=original.session_id + 1,
        authority=authority,
        visibility=replace(base.observer.visibility, database_id=request.stage.database_id),
    )
    digest = observer_incarnation_digest(observer)
    samples = tuple(
        replace(value, raw_count=0, own_count=0, request=None, before_sha256=digest, after_sha256=digest)
        for value in base.samples
    )
    absence = replace(
        base,
        original=original,
        database=TdsDatabaseObservation(
            request.stage.database_name, request.stage.database_id, request.stage.database_guid
        ),
        admission=original_admission,
        principal=original_principal.principal,
        observer=observer,
        samples=samples,
    )
    return observer, absence


def _permission_rows(evidence):
    types = {"INSERT": "IN", "SELECT": "SL", "VIEW DEFINITION": "VW"}
    row_type = restricted_codec.SqlClientPermissionRow
    return tuple(
        row_type(
            value.class_id,
            value.major_id,
            value.minor_id,
            value.grantee_id,
            value.grantor_id,
            types[value.permission_name],
            value.permission_name,
            value.state,
        )
        for value in evidence.direct_permissions
    )


def grant_departure_result(request):
    plan, evidence = request.plan, request.plan.grant_evidence
    observer, absence = _observer(
        plan,
        evidence.authority.session,
        original_admission=plan.management_admission,
        original_principal=evidence.request.management,
    )
    return SqlClientPermissionGrantDepartureResult(
        request_sha256=sha256(encode_grant_departure_request(request)).hexdigest(),
        absence=absence,
        catalog_observer=observer,
        direct_permissions=_permission_rows(evidence),
        stage=SqlClientStageObservation(
            before=evidence.request.stage,
            after=evidence.request.stage,
            management_before=observer,
            management_after=observer,
            empty=1,
            metadata_permissions=(1, 1, 1),
        ),
    )


def restricted_departure_result(request):
    plan, evidence = request.plan, request.plan.grant_evidence
    original = plan.verify_result.opening.session
    server_identity_proxy = TdsRemoteSessionIdentity(
        original.client_connection_id,
        original.session_id,
        original.login_time,
        original.login_time,
        original.nonce,
        original.authority_sha256,
    )
    observer, legacy_absence = _observer(
        plan,
        server_identity_proxy,
        original_admission=plan.writer_admission,
        original_principal=evidence.request.writer,
    )
    samples = tuple(
        RestrictedDepartureSample(
            kind=RestrictedDepartureSampleKind(value.kind.value),
            raw_count=value.raw_count,
            own_count=value.own_count,
            original_epoch_count=0 if value.kind.value == "sessions" else None,
            request=None
            if value.request is None
            else RestrictedObserverRequest(
                connection_id=value.request.connection_id,
                session_id=value.request.session_id,
                request_id=value.request.request_id,
                start_time=value.request.start_time,
            ),
            before_sha256=value.before_sha256,
            after_sha256=value.after_sha256,
        )
        for value in legacy_absence.samples
    )
    absence = RestrictedSessionDeparture(
        original=original,
        database=legacy_absence.database,
        admission=legacy_absence.admission,
        principal=legacy_absence.principal,
        observer=observer,
        samples=samples,
    )
    writer = restricted_codec.SqlClientGrantPrincipal(
        evidence.request.writer.principal_id,
        evidence.request.writer.name,
        evidence.request.writer.sid,
        "SQL_USER",
        "INSTANCE",
    )
    public = restricted_codec.SqlClientGrantPrincipal(0, "public", "00", "DATABASE_ROLE", "NONE")
    return restricted_codec.RestrictedWriterDepartureResult(
        request_sha256=sha256(restricted_codec.encode_request(request)).hexdigest(),
        absence=absence,
        catalog_observer=observer,
        principals=(writer, public),
        direct_permissions=_permission_rows(evidence),
        stage=SqlClientStageObservation(
            before=plan.verify_request.stage,
            after=plan.verify_request.stage,
            management_before=observer,
            management_after=observer,
            empty=1,
            metadata_permissions=(1, 1, 1),
        ),
        row_count=0,
    )


class PermissionProcess:
    def __init__(self, launch, admission: bytes, management_admission, exit_identity) -> None:
        self.launch, self.admission = launch, admission
        self.exit_identity = exit_identity
        self.startup = TdsCoordinatorStartup(PROCESS, launch.operation.implementation_sha256, "/tmp/dpone", b"s" * 32)
        self.binding = PermissionWireBinding(
            launch.request,
            launch.operation,
            self.startup,
            launch.execution_owner,
            deadline_nanoseconds(launch.operation_deadline),
        )
        stage = launch.request.stage
        authority_sha = _session_digest(management_admission, launch.request.management, stage.database_guid)
        remote = TdsRemoteSessionIdentity(
            UUID(int=700), 72, datetime(2026, 1, 2), datetime(2026, 1, 2), b"n" * 32, authority_sha
        )
        self.authority = TdsCoordinatorAuthority(
            coordinator_identity_digest(launch.operation),
            launch.execution_owner,
            PROCESS,
            launch.operation.implementation_sha256,
            remote,
            TdsDatabaseObservation(stage.database_name, stage.database_id, stage.database_guid),
            TdsSchemaObservation(stage.schema_id, stage.schema_name),
            TdsLockObservation(0),
        )
        self.request_payload = self.evidence = None

    def send_public(self, kind, ordinal, body, *, deadline=None):
        payload = encode_permission_message(self.binding, kind, ordinal, body)
        if kind is PermissionWireKind.REQUEST:
            self.request_payload = payload
        elif kind is PermissionWireKind.EXECUTE:
            grant = TdsCoordinatorGrant(
                coordinator_identity_digest(self.launch.operation),
                self.launch.execution_owner,
                PROCESS,
                self.authority.session,
                authority_digest(self.authority),
                UUID(body["grant"]["grant_id"]),
            )
            rows = tuple(
                SqlClientDirectPermission(
                    1,
                    self.launch.request.stage.object_id,
                    0,
                    None,
                    "G",
                    permission,
                    self.launch.request.writer.principal_id,
                    self.launch.request.writer.name,
                    self.launch.request.writer.sid,
                    "SQL_USER",
                    self.launch.request.management.principal_id,
                    self.launch.request.management.name,
                    self.launch.request.management.sid,
                    "SQL_USER",
                )
                for permission in PERMISSIONS
            )
            self.evidence = SqlClientPermissionGrantEvidence(
                request=self.launch.request,
                operation=self.launch.operation,
                grant=grant,
                authority=self.authority,
                direct_permissions=rows,
            )
        return decode_permission_message(payload, binding=self.binding, kind=kind, ordinal=ordinal)

    def receive_public(self, kind, ordinal, *, deadline=None):
        if kind is PermissionWireKind.STARTUP:
            body = {"startup": strict_json_object(encode_startup(self.startup))}
        elif kind is PermissionWireKind.REQUEST_ACCEPTED:
            body = {"request_payload_sha256": sha256(self.request_payload).hexdigest()}
        elif kind is PermissionWireKind.AUTHORITY:
            body = {"authority": strict_json_object(encode_authority(self.authority))}
        elif kind is PermissionWireKind.PERMISSION_HELD:
            body = {"evidence": strict_json_object(encode_permission_grant_evidence(self.evidence))}
        elif kind is PermissionWireKind.HELD:
            digest = sha256(encode_permission_grant_evidence(self.evidence)).hexdigest()
            body = {
                "boundary": PermissionBoundary.READY.value,
                "evidence_sha256": digest,
                "authority": strict_json_object(encode_authority(self.authority)),
            }
        else:
            body = {"evidence_sha256": sha256(encode_permission_grant_evidence(self.evidence)).hexdigest()}
        payload = encode_permission_message(self.binding, kind, ordinal, body)
        return decode_permission_message(payload, binding=self.binding, kind=kind, ordinal=ordinal)

    def send_credentials(self, payload, *, deadline=None):
        return None

    def observe_eof(self, *, deadline):
        return None

    def settle(self, *, natural_deadline, containment_deadline):
        return TdsChildExit(self.exit_identity(), 0, True)


class PermissionLauncher:
    def __init__(self, deployment, management_admission, exit_identity) -> None:
        self.__dict__.update(deployment)
        self.management_admission = management_admission
        self.exit_identity = exit_identity

    def launch(self, inputs):
        return PermissionProcess(inputs, self.admission, self.management_admission, self.exit_identity)


class DepartureChild:
    def __init__(self, launcher, restricted: bool) -> None:
        self.launcher, self.restricted = launcher, restricted
        self.identity = PROCESS
        self.request = None

    def startup(self, *, deadline):
        return TdsCoordinatorStartup(
            PROCESS, self.launcher.implementation_sha256, str(self.launcher.package_root), b"d" * 32
        )

    def send_request(self, payload, *, deadline):
        self.payload = payload
        startup = self.startup(deadline=deadline)
        try:
            if self.restricted:
                self.request = restricted_codec.decode_credentials(payload).request
            else:
                self.request = decode_grant_departure_credentials(
                    payload,
                    startup=startup,
                    admission_sha256=self.launcher.admission_sha256,
                    startup_deadline=self.launcher.startup_deadline,
                    operation_deadline=self.launcher.operation_deadline,
                    max_address_space_bytes=self.launcher.max_address_space_bytes,
                ).request
        except BaseException as error:
            self.error = error
            raise

    def receive_result(self, *, deadline, max_payload=None):
        if self.restricted:
            result = restricted_departure_result(self.request)
            return restricted_codec.encode_result(result, self.request)
        return encode_grant_departure_result(grant_departure_result(self.request), self.request)

    def receive_result_bounded(self, *, deadline, max_payload):
        return self.receive_result(deadline=deadline, max_payload=max_payload)

    def wait(self, *, deadline):
        return TdsChildExit(PROCESS, 0, True)

    def close(self):
        return None


class DepartureLauncher:
    def __init__(self, deployment, *, restricted: bool) -> None:
        self.__dict__.update(deployment)
        self.restricted = restricted
        self.startup_deadline = self.operation_deadline = 0.0

    def spawn(self, *, startup_deadline, operation_deadline):
        self.startup_deadline, self.operation_deadline = startup_deadline, operation_deadline
        return DepartureChild(self, self.restricted)


class VerifyProcess:
    def __init__(self, reservation, request, writer_admission) -> None:
        self.reservation, self.request = reservation, request
        result = verify_result(request)
        authority_sha = _session_digest(writer_admission, request.writer, request.stage.database_guid)
        session = replace(result.opening.session, authority_sha256=authority_sha)
        context = replace(result.opening, session=session)
        self.verify_result = replace(result, opening=context, closing=context)
        self.opening = RestrictedWriterVerifyOpening(
            verify_request_digest(encode_verify_request(request)),
            self.verify_result.opening,
        )

    def registration(self, *, deadline):
        return RestrictedWriterVerifyRegistration(self.reservation, PROCESS, self.reservation.execution_owner)

    def send_credentials(self, payload, *, deadline):
        return None

    def scrub_credentials(self, payload):
        return None

    def assert_credentials_scrubbed(self):
        return None

    def writer_session(self, *, deadline):
        return self.opening

    def authorize_probe(self, opening, *, deadline):
        return None

    def result(self, *, deadline):
        return self.verify_result

    def require_eof_and_zero(self, *, deadline):
        return TdsChildExit(PROCESS, 0, True)

    def cleanup(self):
        return None


class VerifyLauncher:
    def __init__(self, deployment, writer_admission) -> None:
        self.__dict__.update(deployment)
        self.writer_admission = writer_admission

    def launch(self, inputs, reservation, *, public_payload):
        return VerifyProcess(reservation, inputs.request, self.writer_admission)


def deployment(admission: bytes, digest: str, implementation: str, root: Path) -> dict:
    return {
        "admission": admission,
        "admission_sha256": digest,
        "implementation_sha256": implementation,
        "package_root": root,
        "max_address_space_bytes": 1 << 20,
    }
