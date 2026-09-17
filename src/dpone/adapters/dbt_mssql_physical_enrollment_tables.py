"""Read-only exact catalog inventory for the two immutable enrollment tables.

Canonical DDL belongs to the enrollment SQL package. This verifier compares
observed catalog facts, including column order and complete constraints/indexes;
it never repairs drift or treats an absent table as installed. SQL Server 2022
catalog visibility under the platform owner's connection is a prerequisite.
"""

from dpone.adapters import dbapi_lifecycle
from dpone.ports.sql_connection import SqlControlCursor

ENROLLMENTS = "physical_plan_enrollments_v1"
SESSIONS = "physical_model_sessions_v1"
TABLES = (ENROLLMENTS, SESSIONS)
_COLUMNS = {
    ENROLLMENTS: (
        ("generation_id", "uniqueidentifier", 16, 0),
        ("executor_invocation_id", "uniqueidentifier", 16, 0),
        ("registration_id", "uniqueidentifier", 16, 0),
        ("registration_digest", "varbinary", 71, 0),
        ("payload_digest", "varbinary", 71, 0),
        ("payload", "varbinary", -1, 0),
    ),
    SESSIONS: (
        ("session_registration_id", "uniqueidentifier", 16, 0),
        ("generation_id", "uniqueidentifier", 16, 0),
        ("model_unique_id_hash", "binary", 32, 0),
        ("model_unique_id", "varbinary", -1, 0),
        ("model_plan_digest", "varbinary", 71, 0),
        ("enrollment_digest", "varbinary", 71, 0),
        ("connection_id", "uniqueidentifier", 16, 0),
        ("connect_time", "datetime2", 8, 7),
        ("session_id", "int", 4, 0),
        ("login_time", "datetime2", 8, 7),
        ("build_model_principal_id", "int", 4, 0),
        ("build_model_principal_sid", "varbinary", 85, 0),
        ("build_control_principal_id", "int", 4, 0),
        ("build_control_principal_sid", "varbinary", 85, 0),
        ("attached_at_utc", "datetime2", 8, 7),
    ),
}
_CHECKS = {
    ENROLLMENTS: (
        ("registration_digest", "datalengthregistration_digest=71"),
        ("payload_digest", "datalengthpayload_digest=71"),
        ("payload", "datalengthpayload>=1anddatalengthpayload<=1048576"),
    ),
    SESSIONS: (
        ("model_unique_id", "datalengthmodel_unique_id>=1anddatalengthmodel_unique_id<=4096"),
        ("model_plan_digest", "datalengthmodel_plan_digest=71"),
        ("enrollment_digest", "datalengthenrollment_digest=71"),
        ("session_id", "session_id>0"),
        (
            "build_model_principal_sid",
            "datalengthbuild_model_principal_sid>=1anddatalengthbuild_model_principal_sid<=85",
        ),
        (
            "build_control_principal_sid",
            "datalengthbuild_control_principal_sid>=1anddatalengthbuild_control_principal_sid<=85",
        ),
    ),
}


def _require(cursor: SqlControlCursor, expected: tuple[tuple[object, ...], ...]) -> None:
    for item in expected:
        if dbapi_lifecycle.row(cursor) != item:
            raise RuntimeError("enrollment table inventory mismatch")
    if dbapi_lifecycle.row(cursor) is not None:
        raise RuntimeError("enrollment table inventory has unexpected rows")


def verify_enrollment_tables(cursor: SqlControlCursor) -> None:
    """Require exact dbo-owned tables and reject executable or hidden behavior.

    All queries retain the caller's transaction. Column/check/index facts are
    compared as complete ordered inventories, so missing and additional facts
    both reject. Runtime grants are verified by the provisioning boundary.
    """
    for table in TABLES:
        name = f"[dpone_physical].[{table}]"
        cursor.execute(
            """SELECT s.principal_id,COALESCE(t.principal_id,s.principal_id),t.temporal_type,
 t.is_memory_optimized,t.durability,t.is_filetable,t.is_replicated,t.is_merge_published,
 t.is_tracked_by_cdc,t.ledger_type,t.is_remote_data_archive_enabled,
 (SELECT COUNT(*) FROM sys.triggers WHERE parent_id=t.object_id),
 (SELECT COUNT(*) FROM sys.foreign_keys WHERE parent_object_id=t.object_id OR referenced_object_id=t.object_id),
 (SELECT COUNT(*) FROM sys.security_predicates WHERE target_object_id=t.object_id)
FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE t.object_id=OBJECT_ID(?,N'U');""",
            name,
        )
        _require(cursor, ((1, 1, *([0] * 12)),))
        cursor.execute(
            """SELECT c.column_id,c.name,TYPE_NAME(c.system_type_id),c.max_length,c.scale,
 c.is_nullable,c.is_computed,c.is_identity,c.default_object_id,c.rule_object_id,
 c.is_sparse,c.is_column_set,c.generated_always_type,c.is_hidden,c.is_rowguidcol,
 c.is_filestream,c.is_masked,c.encryption_type,c.user_type_id-c.system_type_id
FROM sys.columns c WHERE c.object_id=OBJECT_ID(?,N'U') ORDER BY c.column_id;""",
            name,
        )
        _require(cursor, tuple((i, *column, *([0] * 12), None, 0) for i, column in enumerate(_COLUMNS[table], 1)))
        cursor.execute(
            """SELECT name,parent_column_id,is_disabled,is_not_trusted,is_not_for_replication,
 LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(definition,' ',''),'(',''),')',''),'[',''),']',''))
FROM sys.check_constraints WHERE parent_object_id=OBJECT_ID(?,N'U') ORDER BY name;""",
            name,
        )
        _require(
            cursor,
            tuple(
                sorted(
                    (
                        f"CK_{table}_{column}",
                        next(i for i, c in enumerate(_COLUMNS[table], 1) if c[0] == column),
                        0,
                        0,
                        0,
                        expression,
                    )
                    for column, expression in _CHECKS[table]
                )
            ),
        )
        cursor.execute(
            """SELECT i.name,i.type,i.is_unique,i.is_primary_key,i.is_unique_constraint,
 i.is_disabled,i.has_filter,i.ignore_dup_key,i.is_hypothetical,
 c.name,ic.key_ordinal,ic.is_descending_key,ic.is_included_column
FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
 JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
WHERE i.object_id=OBJECT_ID(?,N'U') ORDER BY i.name,ic.index_column_id;""",
            name,
        )
        primary = "generation_id" if table == ENROLLMENTS else "session_registration_id"
        indexes = [(f"PK_{table}", 1, 1, 1, 0, 0, 0, 0, 0, primary, 1, 0, 0)]
        if table == SESSIONS:
            indexes.extend(
                (f"UQ_{table}_generation_model", 2, 1, 0, 1, 0, 0, 0, 0, column, ordinal, 0, 0)
                for ordinal, column in enumerate(("generation_id", "model_unique_id_hash"), 1)
            )
        _require(cursor, tuple(indexes))
        cursor.execute("""SELECT COUNT(*) FROM sys.indexes WHERE object_id=OBJECT_ID(?,N'U');""", name)
        _require(cursor, ((1 if table == ENROLLMENTS else 2,),))


def enrollment_tables(*, enrollment_sql: bytes) -> str:
    """Extract the exact two CREATE TABLE statements from authenticated bytes.

    This finite resource projection does not authenticate the package. Unknown
    boundary shape or unresolved rendering markers fails before any deployment.
    """
    if type(enrollment_sql) is not bytes or not enrollment_sql:
        raise ValueError("authenticated enrollment package bytes are required")
    parts = enrollment_sql.decode("utf-8").split("\n-- DPONE ENROLLMENT TABLE BOUNDARY\n")
    if len(parts) != 2 or "{{" in parts[0] or "}}" in parts[0]:
        raise ValueError("enrollment requires one resolved table boundary")
    definitions = parts[0]
    if definitions.count("CREATE TABLE ") != 2 or any(
        definitions.count(f"CREATE TABLE [dpone_physical].[{table}](") != 1 for table in TABLES
    ):
        raise ValueError("enrollment requires the exact finite table pair")
    return definitions
