"""Privileged immutable SQL registration storage and independent exact readback."""

from __future__ import annotations

from collections.abc import Callable

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.dbt_mssql_physical_registration_schema import (
    COLUMNS,
    TABLE,
    verify_registration_table_sql,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import (
    decode_physical_runtime_registration,
    encode_physical_runtime_registration,
    physical_runtime_registration_digest,
)
from dpone.contracts.dbt_mssql_physical_registration_values import DedicatedObserver
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.contracts.native_originals import encode_native_original_subject
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


class PhysicalRegistrationStorageError(RuntimeError):
    """The catalog did not prove the complete expected immutable registration."""


def registration_columns(value: MssqlPhysicalRuntimeRegistration) -> dict[str, object]:
    """Project the validated codec value into every named SQL storage column.

    Dedicated observers use empty permission-contract bytes. Shared observers
    retain the real inherited mappings and their explicit permission digest.
    This projection authenticates neither the digest nor the referenced bytes.
    """
    payload = encode_physical_runtime_registration(value)
    columns: dict[str, object] = {
        "registration_id": value.registration_id,
        "payload": payload,
        "registration_digest": physical_runtime_registration_digest(value).encode("ascii"),
        "platform_subject": encode_native_original_subject(value.platform_subject),
    }
    for name in ("control_authority", "capacity_authority"):
        reference = getattr(value, name)
        columns[name + "_locator"] = reference.locator.encode("utf-8")
        columns[name + "_digest"] = reference.sha256.encode("ascii")
    for name in ("trusted_profile", "trusted_toolchain"):
        selection = getattr(value, name)
        columns[name + "_locator"] = selection.reference.locator.encode("utf-8")
        columns[name + "_digest"] = selection.reference.sha256.encode("ascii")
        columns[name + "_subject"] = encode_native_original_subject(selection.subject)
    for name in (
        "qualification_policy_id",
        "control_connection_ref",
        "model_connection_ref",
        "service_authority_sha256",
    ):
        columns[name] = getattr(value, name).encode("utf-8")
    for namespace in ("control", "model"):
        pin = getattr(value, namespace + "_database")
        for name in ("database_name", "database_id", "create_token", "database_guid"):
            suffix = name if name.startswith("database_") else "database_" + name
            columns[namespace + "_" + suffix] = (
                str(getattr(pin, name)) if name == "database_guid" else getattr(pin, name)
            )
    columns.update(control_schema=value.control_schema, local_schema=value.local_schema)
    for name, field in value.program.to_dict().items():
        assert isinstance(field, str)
        columns[name] = field.encode("utf-8")
    columns.update(value.limits.to_dict())
    observer = value.principals.observer
    if isinstance(observer, DedicatedObserver):
        observer_mapping = observer.mapping
        mode, permission = "DEDICATED", b""
    else:
        observer_mapping = value.principals.metadata if observer.mode == "SHARE_METADATA" else value.principals.build
        mode, permission = observer.mode, observer.permission_contract_sha256.encode("ascii")
    for role, mapping in (
        ("metadata", value.principals.metadata),
        ("build", value.principals.build),
        ("observer", observer_mapping),
    ):
        for namespace in ("control", "model"):
            principal = getattr(mapping, namespace)
            columns[f"{role}_{namespace}_principal_id"] = principal.principal_id
            columns[f"{role}_{namespace}_sid"] = bytes.fromhex(principal.sid_hex)
    columns.update(observer_mode=mode.encode("ascii"), observer_permission_contract_sha256=permission)
    return {name: columns[name] for name, _, _ in COLUMNS}


def _select_columns() -> str:
    expressions = []
    for name, kind, _ in COLUMNS:
        if kind == "datetime2(7)":
            # No Python datetime conversion: retain all seven fractional digits,
            # including .0000000, regardless of the driver's microsecond limit.
            expressions.append(
                f"CONVERT(char(19),{name},126)+'.'+RIGHT('0000000'+"
                f"CONVERT(varchar(7),DATEPART(NANOSECOND,{name})/100),7) AS {name}"
            )
        elif kind == "uniqueidentifier" and name != "registration_id":
            expressions.append(f"LOWER(CONVERT(char(36),{name})) AS {name}")
        else:
            expressions.append(name)
    return ",".join(expressions)


def _preflight_sql(schema: str) -> str:
    declarations = ",".join(f"@{name} {kind}=?" for name, kind, _ in COLUMNS)
    principal_checks = "\n".join(
        f"""IF NOT EXISTS (SELECT 1 FROM sys.database_principals p
 WHERE p.principal_id=@{role}_model_principal_id AND p.sid=@{role}_model_sid
 AND DATALENGTH(p.sid)=DATALENGTH(@{role}_model_sid) AND p.type IN ('S','U','E')
 AND p.principal_id>4 AND IS_ROLEMEMBER('db_owner',p.name)=0)
 THROW 51404, 'DPONE_PHYSICAL_REGISTRATION_PRINCIPAL_MISMATCH', 1;
IF EXISTS (SELECT required.permission_name FROM
 (VALUES ('SELECT'),('INSERT'),('UPDATE'),('DELETE'),('ALTER'),('TAKE OWNERSHIP')) required(permission_name)
 EXCEPT SELECT permission_name FROM sys.database_permissions
 WHERE class=1 AND major_id=OBJECT_ID(N'[{schema}].[{TABLE}]') AND minor_id=0
 AND grantee_principal_id=@{role}_model_principal_id AND state='D')
 OR EXISTS (SELECT required.permission_name FROM (VALUES ('ALTER'),('TAKE OWNERSHIP')) required(permission_name)
 EXCEPT SELECT permission_name FROM sys.database_permissions WHERE class=3 AND major_id=SCHEMA_ID(N'{schema}')
 AND grantee_principal_id=@{role}_model_principal_id AND state='D')
 THROW 51406, 'DPONE_PHYSICAL_REGISTRATION_PROTECTION_MISSING', 1;"""
        for role in ("metadata", "build", "observer")
    )
    return f"""SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE {declarations};
IF NOT EXISTS (SELECT 1 FROM sys.databases d JOIN sys.database_recovery_status r
 ON r.database_id=d.database_id WHERE d.database_id=DB_ID() AND d.database_id=@model_database_id
 AND CONVERT(varbinary(max),d.name)=CONVERT(varbinary(max),@model_database_name)
 AND DATALENGTH(d.name)=DATALENGTH(@model_database_name)
 AND CONVERT(datetime2(7),d.create_date)=@model_database_create_token AND r.database_guid=@model_database_guid)
 THROW 51405, 'DPONE_PHYSICAL_REGISTRATION_DATABASE_MISMATCH', 1;
{principal_checks}
IF @registration_digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@payload),2)))
 THROW 51402, 'DPONE_PHYSICAL_REGISTRATION_DIGEST_MISMATCH', 1;"""


def _insert_sql(schema: str) -> str:
    names = ",".join(name for name, _, _ in COLUMNS)
    parameters = ",".join("@" + name for name, _, _ in COLUMNS)
    exact = " AND ".join(
        f"CONVERT(varbinary(max),{name})=CONVERT(varbinary(max),@{name}) AND DATALENGTH({name})=DATALENGTH(@{name})"
        for name, _, _ in COLUMNS
    )
    return f"""{_preflight_sql(schema)}
IF EXISTS (SELECT 1 FROM [{schema}].[{TABLE}] WITH (UPDLOCK,HOLDLOCK) WHERE registration_id=@registration_id)
BEGIN
 IF NOT EXISTS (SELECT 1 FROM [{schema}].[{TABLE}] WHERE {exact})
  THROW 51403, 'DPONE_PHYSICAL_REGISTRATION_CONFLICT', 1;
END
ELSE INSERT [{schema}].[{TABLE}] ({names}) VALUES ({parameters});"""


class MssqlPhysicalRegistrationStore:
    """Store previously authenticated platform input with privileged connections.

    Factories select the authenticated model database and return fresh dedicated
    connections with finite connect/statement timeouts. Upstream provisioning
    authenticates retained policy/profile/toolchain/program bytes, pins, role
    mappings, permission contracts and deployed inventory before calling here.
    This API is not exposed to runtime and does not grant model admission.
    """

    def __init__(self, *, connection_factory: Callable[[], SqlControlConnection], local_schema: str) -> None:
        self._schema = native_control_schema(local_schema)
        self._connect = connection_factory

    def register(self, value: MssqlPhysicalRuntimeRegistration) -> MssqlPhysicalRuntimeRegistration:
        """Insert once or acknowledge exact replay, then independently read back.

        An execute/commit failure triggers one read-only reconciliation. Missing,
        different or unreadable rows fail; no automatic mutation retry/new UUID.
        """
        columns = self._columns(value)
        failure: Exception | None = None
        try:
            self._execute(columns, insert=True)
        except Exception as exc:
            failure = exc
        try:
            return self.resolve(value)
        except Exception as exc:
            raise PhysicalRegistrationStorageError("registration outcome could not be independently verified") from (
                failure or exc
            )

    def resolve(self, expected: MssqlPhysicalRuntimeRegistration) -> MssqlPhysicalRuntimeRegistration:
        """Read only: require canonical payload, digest and every exact projection."""
        columns = self._columns(expected)
        observed = self._execute(columns, insert=False)
        if observed is None or observed != tuple(columns.values()):
            raise PhysicalRegistrationStorageError(
                "registration row is absent or differs from expected bytes/projections"
            )
        payload = observed[1]
        if type(payload) is not bytes:
            raise PhysicalRegistrationStorageError("registration payload must be bytes")
        return decode_physical_runtime_registration(payload)

    def _columns(self, value: MssqlPhysicalRuntimeRegistration) -> dict[str, object]:
        columns = registration_columns(value)
        if value.local_schema != self._schema:
            raise ValueError("registration local schema differs from selected storage")
        return columns

    def _execute(self, columns: dict[str, object], *, insert: bool) -> tuple[object, ...] | None:
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute(verify_registration_table_sql(self._schema))
            if insert:
                cursor.execute(_insert_sql(self._schema), *columns.values())
                connection.commit()
                return None
            cursor.execute(
                f"{_preflight_sql(self._schema)}\nSELECT {_select_columns()} FROM [{self._schema}].[{TABLE}] "
                "WHERE registration_id=@registration_id",
                *columns.values(),
            )
            row = dbapi_lifecycle.row(cursor)
            if dbapi_lifecycle.row(cursor) is not None:
                raise PhysicalRegistrationStorageError("registration read returned more than one row")
            if row is not None:
                # Drivers may expose uniqueidentifier as UUID or upper-case text.
                row = (str(row[0]).lower(), *row[1:])
            connection.commit()
            return row
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)
