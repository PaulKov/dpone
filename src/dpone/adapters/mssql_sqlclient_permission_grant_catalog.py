"""Exact SQL Server catalog observation for a single permission grant."""

from dataclasses import replace
from typing import Any, Protocol
from uuid import UUID

from dpone.adapters.mssql_sqlclient_stage_catalog_sql import (
    COLUMNS_SQL,
    FEATURES_SQL,
    OBJECT_SQL,
    SCHEMA_SQL,
    STAGE_VISIBILITY_SQL,
)
from dpone.contracts.mssql_tds_api import (
    parse_direct_permissions,
    parse_stage_columns,
    parse_stage_object,
    quote_stage_identifier,
    validate_direct_permissions,
)

WRITER_SQL = """
SELECT dp.principal_id,dp.name,dp.sid,dp.type_desc,dp.authentication_type_desc,
 sp.principal_id,sp.name,sp.sid,sp.type_desc,CONVERT(int,sp.is_disabled)
FROM sys.database_principals dp
LEFT JOIN sys.server_principals sp ON sp.sid=dp.sid
WHERE dp.principal_id=?;
"""
MANAGEMENT_SQL = """
SELECT sp.principal_id,ORIGINAL_LOGIN(),SUSER_SID(ORIGINAL_LOGIN()),
 SUSER_SNAME(),SUSER_SID(),sp.type_desc,CONVERT(int,sp.is_disabled),
 IS_SRVROLEMEMBER(N'sysadmin'),USER_ID(),USER_NAME(),dp.sid,dp.type_desc,
 ses.authenticating_database_id
FROM sys.server_principals sp
LEFT JOIN sys.database_principals dp ON dp.principal_id=USER_ID()
JOIN sys.dm_exec_sessions ses ON ses.session_id=@@SPID
WHERE sp.sid=SUSER_SID();
"""
OWNERS_SQL = """
SELECT d.owner_sid,s.principal_id,so.sid,so.name,so.type_desc,
 t.principal_id,oo.sid,oo.name,oo.type_desc
FROM sys.databases d JOIN sys.schemas s ON s.schema_id=?
JOIN sys.tables t ON t.schema_id=s.schema_id AND t.object_id=?
LEFT JOIN sys.database_principals so ON so.principal_id=s.principal_id
LEFT JOIN sys.database_principals oo ON oo.principal_id=t.principal_id
WHERE d.database_id=DB_ID();
"""
PERMISSIONS_SQL = """
SELECT TOP (4097) p.class,p.major_id,p.minor_id,c.name,p.state,p.permission_name,
 grantee.principal_id,grantee.name,grantee.sid,grantee.type_desc,
 grantor.principal_id,grantor.name,grantor.sid,grantor.type_desc
FROM sys.database_permissions p
LEFT JOIN sys.database_principals grantee ON grantee.principal_id=p.grantee_principal_id
LEFT JOIN sys.database_principals grantor ON grantor.principal_id=p.grantor_principal_id
LEFT JOIN sys.columns c ON c.object_id=p.major_id AND c.column_id=p.minor_id AND p.minor_id<>0
WHERE p.class=1 AND p.major_id=?
ORDER BY p.grantee_principal_id,p.grantor_principal_id,p.minor_id,p.permission_name,p.state;
"""


class CatalogOwner(Protocol):
    request: Any

    def _one(self, text: str, args: tuple, deadline: float) -> tuple: ...
    def _query(self, text: str, args: tuple, deadline: float, *, limit: int = 1, statement: bool = False) -> tuple: ...
    def _reject(self) -> Any: ...


def observe_permission_grant_catalog(owner: CatalogOwner, deadline: float, *, granted: bool) -> tuple:
    """Reconstruct and validate stage, identities, ownership, and direct grants."""
    assert owner.request is not None
    r, q = owner.request, quote_stage_identifier
    target = q(r.stage.schema_name) + "." + q(r.stage.table_name)
    visible = owner._one(STAGE_VISIBILITY_SQL, (target,), deadline)
    if (
        len(visible) != 4
        or type(visible[0]) is not int
        or visible[0] not in (16, 17)
        or any(type(v) is not int or v != 1 for v in visible[1:])
    ):
        owner._reject()
    schema = owner._one(SCHEMA_SQL, (r.stage.schema_id,), deadline)
    if (
        len(schema) != 2
        or type(schema[0]) is not int
        or type(schema[1]) is not str
        or schema != (r.stage.schema_id, r.stage.schema_name)
    ):
        owner._reject()
    obj = parse_stage_object(owner._one(OBJECT_SQL, (r.stage.schema_id, r.stage.table_name), deadline))
    features = owner._one(FEATURES_SQL, (obj[0],), deadline)
    if len(features) != 12 or any(type(v) is not int or v != 0 for v in features):
        owner._reject()
    columns = parse_stage_columns(owner._query(COLUMNS_SQL, (obj[0],), deadline, limit=100))
    observed = replace(
        r.stage,
        object_id=obj[0],
        table_name=obj[1],
        create_date=obj[2],
        owner_binding=obj[3],
        object_nonce=UUID(obj[4]),
        columns=columns,
    )
    if observed != r.stage:
        owner._reject()
    writer = owner._one(WRITER_SQL, (r.writer.principal_id,), deadline)
    expected_writer = (
        r.writer.principal_id,
        r.writer.name,
        bytes.fromhex(r.writer.sid),
        "SQL_USER",
        "INSTANCE",
        r.writer_login.principal_id,
        r.writer_login.name,
        bytes.fromhex(r.writer_login.sid),
        "SQL_LOGIN",
        0,
    )
    management = owner._one(MANAGEMENT_SQL, (), deadline)
    login, user = r.management_login, r.management
    expected_management = (
        login.principal_id,
        login.original_name,
        bytes.fromhex(login.original_sid),
        login.name,
        bytes.fromhex(login.sid),
        "SQL_LOGIN",
        0,
        1,
        user.principal_id,
        user.name,
        bytes.fromhex(user.sid),
        "SQL_USER",
        login.authenticating_database_id,
    )
    for actual, expected in ((writer, expected_writer), (management, expected_management)):
        if len(actual) != len(expected) or any(type(a) is not type(b) or a != b for a, b in zip(actual, expected)):
            owner._reject()
    owners = owner._one(OWNERS_SQL, (r.stage.schema_id, r.stage.object_id), deadline)
    if len(owners) != 9 or type(owners[0]) is not bytes or not 1 <= len(owners[0]) <= 85:
        owner._reject()
    if owners[0] == bytes.fromhex(r.writer.sid):
        owner._reject()
    for offset in (1, 5):
        principal, sid, name, kind = owners[offset : offset + 4]
        if offset == 5 and principal is None:
            if any(v is not None for v in (sid, name, kind)):
                owner._reject()
            continue
        if (
            type(principal) is not int
            or not 1 <= principal <= 2**31 - 1
            or type(sid) is not bytes
            or not 1 <= len(sid) <= 85
            or type(name) is not str
            or not name
            or type(kind) is not str
            or not kind
            or principal == r.writer.principal_id
            or sid == bytes.fromhex(r.writer.sid)
        ):
            owner._reject()
    permissions = parse_direct_permissions(owner._query(PERMISSIONS_SQL, (obj[0],), deadline, limit=4096))
    if granted:
        validate_direct_permissions(r, permissions)
    elif permissions:
        owner._reject()
    return observed, writer, management, owners, permissions
