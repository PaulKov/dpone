"""Frozen nonsecret OBSERVE intent and selected-stage observations.

The digest covers this request alone. Credentials and later process identities
are separate bindings; no observation grants mutation or Prepared authority.
"""

from dataclasses import asdict, dataclass
from hashlib import sha256

from dpone.contracts.mssql_sqlclient_grant_inventory import (
    PROFILE,
    SqlClientGrantInventoryLimits,
    SqlClientGrantPrincipal,
    validate_inventory_original,
)
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_sqlclient_observer_incarnation_codec import (
    decode_observer_incarnation,
    encode_observer_incarnation,
)
from dpone.contracts.mssql_sqlclient_stage_identity import (
    SqlClientStageIdentity,
    decode_stage_identity,
    encode_stage_identity,
)
from dpone.contracts.mssql_sqlclient_stage_observation import SqlClientStageObservation, snapshot_stage_admission
from dpone.contracts.mssql_tds_validation import _integer
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape

MAX_REQUEST_PAYLOAD_BYTES = 262144
ERROR = "mssql_native.sqlclient_observe_invalid"


@dataclass(frozen=True, kw_only=True)
class SqlClientObserveRequest:
    parent: TdsAttemptIdentity
    selected_stage: SqlClientStageIdentity
    management_admission: SqlClientObserverAdmission
    writer_admission: SqlClientObserverAdmission
    writer_principal: SqlClientGrantPrincipal
    limits: SqlClientGrantInventoryLimits
    operation_deadline_ns: int
    schema: str = "dpone.sqlclient.observe.request.v1"
    profile: str = PROFILE

    def __post_init__(self) -> None:
        for value, cls in (
            (self.parent, TdsAttemptIdentity),
            (self.selected_stage, SqlClientStageIdentity),
            (self.writer_principal, SqlClientGrantPrincipal),
            (self.limits, SqlClientGrantInventoryLimits),
        ):
            validate_inventory_original(value, cls)
        management = snapshot_stage_admission(self.management_admission)
        writer = snapshot_stage_admission(self.writer_admission)
        stage, principal = self.selected_stage, self.writer_principal
        _integer(self.operation_deadline_ns, 1, 2**63 - 1)
        if (
            type(self.schema) is not str
            or self.schema != "dpone.sqlclient.observe.request.v1"
            or type(self.profile) is not str
            or self.profile != PROFILE
            or management.login.is_sysadmin is not True
            or writer.login.is_sysadmin is not False
            or writer.login.sid == writer.database.owner_sid
            or (management.server, management.database) != (writer.server, writer.database)
            or (stage.database_name, stage.database_id, str(stage.database_guid))
            != (management.database.database_name, management.database.database_id, management.database.database_guid)
            or self.parent.database != stage.database_name
            or self.parent.schema != stage.schema_name
            or principal.principal_id <= 4
            or principal.sid != writer.login.sid
            or principal.type_desc != "SQL_USER"
            or principal.authentication_type_desc != "INSTANCE"
        ):
            raise ValueError(ERROR)

    @property
    def command_sha256(self) -> str:
        return sha256(encode_request(self)).hexdigest()


def _admission(data: dict) -> SqlClientObserverAdmission:
    body = record_shape(SqlClientObserverAdmission, data)
    for name, cls in (
        ("server", SqlClientServerAuthority),
        ("database", SqlClientDatabaseAuthority),
        ("login", SqlClientLoginAuthority),
        ("transport", SqlClientTransportAuthority),
    ):
        body[name] = construct_record(cls, body[name])
    return SqlClientObserverAdmission(**body)


def encode_request(value: SqlClientObserveRequest) -> bytes:
    if type(value) is not SqlClientObserveRequest:
        raise ValueError(ERROR)
    value.__post_init__()
    body = {
        name: asdict(getattr(value, name))
        for name in ("parent", "management_admission", "writer_admission", "writer_principal", "limits")
    }
    body.update(
        schema=value.schema,
        profile=value.profile,
        operation_deadline_ns=value.operation_deadline_ns,
        selected_stage=strict_json_object(encode_stage_identity(value.selected_stage)),
    )
    payload = canonical_json_bytes(body)
    if len(payload) > MAX_REQUEST_PAYLOAD_BYTES:
        raise ValueError(ERROR)
    return payload


def decode_request(payload: bytes) -> SqlClientObserveRequest:
    if type(payload) is not bytes or not 0 < len(payload) <= MAX_REQUEST_PAYLOAD_BYTES:
        raise ValueError(ERROR)
    body = record_shape(SqlClientObserveRequest, strict_json_object(payload))
    body["parent"] = construct_record(TdsAttemptIdentity, body["parent"])
    body["selected_stage"] = decode_stage_identity(canonical_json_bytes(body["selected_stage"]))
    for name in ("management_admission", "writer_admission"):
        body[name] = _admission(body[name])
    body["writer_principal"] = construct_record(SqlClientGrantPrincipal, body["writer_principal"])
    body["limits"] = construct_record(SqlClientGrantInventoryLimits, body["limits"])
    value = SqlClientObserveRequest(**body)
    if encode_request(value) != payload:
        raise ValueError(ERROR)
    return value


def observation_body(value: SqlClientStageObservation, request: SqlClientObserveRequest) -> dict:
    validate_inventory_original(value, SqlClientStageObservation)
    if value.before != request.selected_stage:
        raise ValueError(ERROR)
    actual = value.management_before.authority
    admitted = request.management_admission
    if (actual.server, actual.database, actual.login, actual.transport) != (
        admitted.server,
        admitted.database,
        admitted.login,
        admitted.transport,
    ):
        raise ValueError(ERROR)
    body = dict(
        schema="dpone.sqlclient.stage.observation.v1",
        empty=value.empty,
        metadata_permissions=list(value.metadata_permissions),
        profile=value.profile,
    )
    for name in ("before", "after"):
        body[name] = strict_json_object(encode_stage_identity(getattr(value, name)))
    for name in ("management_before", "management_after"):
        body[name] = strict_json_object(encode_observer_incarnation(getattr(value, name)))
    return body


def observation_from_body(body: dict, request: SqlClientObserveRequest) -> SqlClientStageObservation:
    if type(body) is not dict or body.get("schema") != "dpone.sqlclient.stage.observation.v1":
        raise ValueError(ERROR)
    data = record_shape(SqlClientStageObservation, {k: v for k, v in body.items() if k != "schema"})
    for name in ("before", "after"):
        data[name] = decode_stage_identity(canonical_json_bytes(data[name]))
    for name in ("management_before", "management_after"):
        data[name] = decode_observer_incarnation(canonical_json_bytes(data[name]))
    permissions = data["metadata_permissions"]
    if type(permissions) is not list or len(permissions) != 3 or any(type(v) is not int for v in permissions):
        raise ValueError(ERROR)
    data["metadata_permissions"] = tuple(permissions)
    value = SqlClientStageObservation(**data)
    if observation_body(value, request) != body:
        raise ValueError(ERROR)
    return value
