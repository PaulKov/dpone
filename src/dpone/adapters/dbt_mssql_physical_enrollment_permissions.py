"""Closed new enrollment certificate, signature, and grant inventories.

The deployment lifecycle supplies already observed database pins and runtime
names under one protected DDL transaction. This leaf switches only among those
pinned databases and master; it never changes existing boundary certificates,
module bodies, or grants. Every existing inventory must match exactly.
"""

from dpone.adapters.dbt_mssql_physical_enrollment_tables import TABLES
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.mssql_object_name import quote_mssql_identifier
from dpone.ports.sql_connection import SqlControlCursor

E = "dpone_physical_enrollment_v1_cert"
C = "dpone_physical_connection_v1_cert"
EU = "dpone_physical_enrollment_v1_user"
CU = "dpone_physical_connection_v1_user"
CL = "dpone_physical_connection_v1_login"
ENROLL = "physical_enroll_plan_set_v1"
READ = "physical_read_plan_enrollment_v1"
CONTROL = "physical_control_require_enrollment_v1"
ATTACH = "physical_attach_session_v1"
OBSERVER = "physical_observe_current_connection_v1"


EnrollmentModules = list[tuple[str, str, str, str, str]]


def enrollment_permission_inventory(
    cursor: SqlControlCursor,
    value: MssqlPhysicalRuntimeRegistration,
    modules: EnrollmentModules,
    names: dict[tuple[str, int], str],
    *,
    create: bool,
    before: bool,
) -> None:
    same = value.model_database == value.control_database
    for namespace in ("model",) if same else ("model", "control"):
        pin = getattr(value, namespace + "_database")
        cursor.execute(f"USE {quote_mssql_identifier(pin.database_name)};")
        local = [row for row in modules if same or row[0] == namespace]
        grants = []
        for _, schema, module, certificate, _ in local:
            user = CU if certificate == C else EU
            grants.append((user, schema, module, "VIEW DEFINITION"))
            if module == CONTROL:
                grants.append((EU, schema, module, "EXECUTE"))
            elif module == OBSERVER:
                grants.append((EU, schema, module, "EXECUTE"))
        if namespace == "model" or same:
            grants.append((EU, "", "", "VIEW DEFINITION"))
            grants.extend(
                (EU, "dpone_physical", table, permission) for table in TABLES for permission in ("SELECT", "INSERT")
            )
        if namespace == "control" or same:
            grants.extend(
                (EU, value.control_schema, table, "SELECT")
                for table in ("native_generations_v1", "native_original_bindings_v1")
            )
        for certificate, user in ((E, EU), (C, CU)):
            selected = [(schema, module, kind) for _, schema, module, cert, kind in local if cert == certificate]
            if not selected:
                continue
            expected = ",".join(f"(OBJECT_ID(N'[{schema}].[{module}]'),'{kind}')" for schema, module, kind in selected)
            actual = f"SELECT major_id,crypt_type FROM sys.crypt_properties WHERE class=1 AND thumbprint=(SELECT thumbprint FROM sys.certificates WHERE name=N'{certificate}')"
            mismatch = (
                f"EXISTS ({actual})"
                if create and before
                else f"EXISTS ({actual} EXCEPT SELECT * FROM (VALUES {expected}) e(id,kind)) OR EXISTS (SELECT * FROM (VALUES {expected}) e(id,kind) EXCEPT {actual})"
            )
            cursor.execute(f"""IF {mismatch} OR EXISTS (SELECT 1 FROM sys.crypt_properties WHERE class<>1 AND thumbprint=(SELECT thumbprint FROM sys.certificates WHERE name=N'{certificate}'))
 THROW 51632,'DPONE_ENROLLMENT_SIGNATURE_INVENTORY_MISMATCH',1;""")
            if create and before:
                cursor.execute(
                    f"IF DATABASE_PRINCIPAL_ID(N'{user}') IS NOT NULL THROW 51633,'DPONE_ENROLLMENT_PARTIAL_USER_INVENTORY',1;"
                )
                continue
            _user(cursor, certificate, user, [(s, m, p) for u, s, m, p in grants if u == user], create=create)
        if create and before:
            continue
        runtime = []
        if namespace == "model" or same:
            runtime = [(value.principals.metadata.model.principal_id, name) for name in (ENROLL, READ)] + [
                (value.principals.build.model.principal_id, ATTACH)
            ]
            if create:
                for principal, module in runtime:
                    cursor.execute(
                        f"GRANT EXECUTE ON OBJECT::[dpone_physical].[{module}] TO {quote_mssql_identifier(names[('model', principal)])};"
                    )
        targets = [(s, m) for _, s, m, _, _ in local]
        if namespace == "model" or same:
            targets.extend(("dpone_physical", name) for name in TABLES)
        for schema, module in targets:
            object_grants = [
                f"(DATABASE_PRINCIPAL_ID(N'{u}'),N'{p}',N'G',0)" for u, s, m, p in grants if (s, m) == (schema, module)
            ]
            object_grants.extend(f"({principal},N'EXECUTE',N'G',0)" for principal, name in runtime if name == module)
            actual = f"SELECT grantee_principal_id,permission_name,state,minor_id FROM sys.database_permissions WHERE class=1 AND major_id=OBJECT_ID(N'[{schema}].[{module}]')"
            values = ",".join(object_grants)
            cursor.execute(
                f"IF EXISTS ({actual} EXCEPT SELECT * FROM (VALUES {values}) e(u,p,s,m)) OR EXISTS (SELECT * FROM (VALUES {values}) e(u,p,s,m) EXCEPT {actual}) THROW 51634,'DPONE_ENROLLMENT_GRANT_INVENTORY_MISMATCH',1;"
            )
    cursor.execute("USE [master];")
    _server(cursor, create=create, before=before)


def _user(
    cursor: SqlControlCursor, certificate: str, user: str, grants: list[tuple[str, str, str]], *, create: bool
) -> None:
    if create:
        cursor.execute(f"CREATE USER [{user}] FROM CERTIFICATE [{certificate}]; REVOKE CONNECT FROM [{user}];")
    expected = ",".join(
        f"(1,OBJECT_ID(N'[{s}].[{m}]'),0,N'{p}',N'G')" if s else f"(0,0,0,N'{p}',N'G')" for s, m, p in grants
    )
    actual = f"SELECT class,major_id,minor_id,permission_name,state FROM sys.database_permissions WHERE grantee_principal_id=DATABASE_PRINCIPAL_ID(N'{user}')"
    cursor.execute(f"""DECLARE @user int=DATABASE_PRINCIPAL_ID(N'{user}');
IF @user IS NULL OR NOT EXISTS (SELECT 1 FROM sys.database_principals p JOIN sys.certificates c ON p.sid=c.sid AND DATALENGTH(p.sid)=DATALENGTH(c.sid) WHERE p.principal_id=@user AND p.type='C' AND c.name=N'{certificate}')
 OR EXISTS (SELECT 1 FROM sys.database_principals WHERE sid=(SELECT sid FROM sys.certificates WHERE name=N'{certificate}') AND principal_id<>@user)
 OR EXISTS (SELECT 1 FROM sys.database_role_members WHERE member_principal_id=@user)
 OR EXISTS (SELECT 1 FROM sys.schemas WHERE principal_id=@user)
 OR EXISTS (SELECT 1 FROM sys.objects WHERE principal_id=@user)
 OR EXISTS (SELECT 1 FROM sys.server_principals WHERE sid=(SELECT sid FROM sys.certificates WHERE name=N'{E}'))
 OR EXISTS ({actual} EXCEPT SELECT * FROM (VALUES {expected}) e(c,o,m,p,s))
 THROW 51635,'DPONE_ENROLLMENT_CERTIFICATE_USER_UNSAFE',1;""")
    if create:
        for schema, module, permission in grants:
            target = f" ON OBJECT::[{schema}].[{module}]" if schema else ""
            cursor.execute(f"GRANT {permission}{target} TO [{user}];")
    cursor.execute(
        f"IF EXISTS (SELECT * FROM (VALUES {expected}) e(c,o,m,p,s) EXCEPT {actual}) THROW 51635,'DPONE_ENROLLMENT_CERTIFICATE_USER_UNSAFE',1;"
    )


def _server(cursor: SqlControlCursor, *, create: bool, before: bool) -> None:
    if create and before:
        cursor.execute(
            f"IF SUSER_ID(N'{CL}') IS NOT NULL OR EXISTS (SELECT 1 FROM sys.server_principals WHERE sid=(SELECT sid FROM sys.certificates WHERE name=N'{C}')) THROW 51636,'DPONE_ENROLLMENT_PARTIAL_LOGIN_INVENTORY',1;"
        )
        return
    if create:
        cursor.execute(
            f"CREATE LOGIN [{CL}] FROM CERTIFICATE [{C}]; REVOKE CONNECT SQL FROM [{CL}]; GRANT VIEW SERVER PERFORMANCE STATE TO [{CL}];"
        )
    cursor.execute(f"""DECLARE @login int=SUSER_ID(N'{CL}');
IF @login IS NULL OR NOT EXISTS (SELECT 1 FROM sys.server_principals p JOIN sys.certificates c ON p.sid=c.sid AND DATALENGTH(p.sid)=DATALENGTH(c.sid) WHERE p.principal_id=@login AND p.type='C' AND c.name=N'{C}')
 OR EXISTS (SELECT 1 FROM sys.server_principals WHERE sid=(SELECT sid FROM sys.certificates WHERE name=N'{C}') AND principal_id<>@login)
 OR EXISTS (SELECT 1 FROM sys.server_role_members WHERE member_principal_id=@login OR role_principal_id=@login)
 OR EXISTS (SELECT 1 FROM sys.server_principals WHERE owning_principal_id=@login)
 OR EXISTS (SELECT 1 FROM sys.databases WHERE owner_sid=SUSER_SID(N'{CL}'))
 OR EXISTS (SELECT 1 FROM sys.database_principals WHERE sid=(SELECT sid FROM sys.certificates WHERE name=N'{C}'))
 OR EXISTS (SELECT 1 FROM sys.crypt_properties WHERE thumbprint=(SELECT thumbprint FROM sys.certificates WHERE name=N'{C}'))
 OR EXISTS (SELECT class,major_id,permission_name,state FROM sys.server_permissions WHERE grantee_principal_id=@login EXCEPT SELECT 100,0,N'VIEW SERVER PERFORMANCE STATE',N'G')
 OR NOT EXISTS (SELECT 1 FROM sys.server_permissions WHERE grantee_principal_id=@login AND class=100 AND major_id=0 AND permission_name='VIEW SERVER PERFORMANCE STATE' AND state='G')
 THROW 51637,'DPONE_ENROLLMENT_CONNECTION_LOGIN_UNSAFE',1;""")
