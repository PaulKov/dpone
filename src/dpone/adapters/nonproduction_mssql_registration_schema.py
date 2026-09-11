"""External append-only grant/membership schema, without runtime installation.

The protected producer needs SELECT/INSERT and complete catalog visibility.
External administrators must deny UPDATE/DELETE, ALTER/TRUNCATE, impersonation,
trigger bypass and bulk-copy paths omitting FIRE_TRIGGERS. Catalog inspection is
necessary drift detection, not effective-permission proof or live certification.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.adapters.composition_mssql_catalog_types import module_sha256
from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_LEDGER_LOCK, require_control_schema
from dpone.contracts.nonproduction_document import MAX_DOCUMENT_BYTES, NonproductionAuthorityError
from dpone.contracts.runtime_artifact_attestation import MAX_ATTESTATION_BUNDLE_BYTES

if TYPE_CHECKING:
    from dpone.ports.sql_connection import SqlControlCursor

_BIN = "Latin1_General_100_BIN2"
_QUALIFICATION_FILTER = "([phase]='qualification')"
# @@OPTIONS: ANSI_WARNINGS/PADDING/NULLS, ARITHABORT, QUOTED_IDENTIFIER,
# CONCAT_NULL_YIELDS_NULL must be ON; NUMERIC_ROUNDABORT must be OFF.
_INSERT_ON = 8 | 16 | 32 | 64 | 256 | 4096
_INSERT_OFF = 8192
_G = (
    ("consumption_subject_sha256", "varchar", 71, _BIN, 0),
    ("grant_id", "uniqueidentifier", 16, None, 0),
    ("phase", "varchar", 13, _BIN, 0),
    ("environment_id", "uniqueidentifier", 16, None, 0),
    ("campaign_id", "uniqueidentifier", 16, None, 0),
    ("phase_subject_id", "uniqueidentifier", 16, None, 0),
    ("schema_version", "int", 4, None, 0),
    ("grant_document", "varbinary", -1, None, 0),
    ("grant_sha256", "varchar", 71, _BIN, 0),
    ("signature_bundle", "varbinary", -1, None, 0),
    ("bundle_sha256", "varchar", 71, _BIN, 0),
    ("signature_subject_document", "varbinary", -1, None, 0),
    ("signature_subject_sha256", "varchar", 71, _BIN, 0),
    ("request_document", "varbinary", -1, None, 1),
    ("request_sha256", "varchar", 71, _BIN, 1),
    ("trust_revision", "bigint", 8, None, 0),
    ("registered_at", "varchar", 20, _BIN, 0),
)
_M = (
    ("environment_id", "uniqueidentifier", 16, None, 0),
    ("campaign_id", "uniqueidentifier", 16, None, 0),
    ("phase", "varchar", 13, _BIN, 0),
    ("workload_sha256", "varchar", 71, _BIN, 0),
    ("workload_document", "varbinary", -1, None, 0),
    ("first_grant_sha256", "varchar", 71, _BIN, 0),
)
REGISTRATION_COLUMNS = tuple(item[0] for item in _G)
_KEYS = {
    "grants": (
        ("pk_np_grants", True, ("consumption_subject_sha256",)),
        ("uq_np_grant_phase", False, ("grant_id", "phase")),
    ),
    "memberships": (("pk_np_memberships", True, ("environment_id", "campaign_id", "phase", "workload_sha256")),),
}


def registration_projection_sql() -> str:
    """Render the schema's bounded original-byte columns in persistence order."""
    parts = []
    for name in REGISTRATION_COLUMNS:
        if name in {"grant_id", "environment_id", "campaign_id", "phase_subject_id"}:
            parts.append(f"LOWER(CONVERT(char(36), {name}))")
        elif name.endswith("_document") or name == "signature_bundle":
            maximum = MAX_ATTESTATION_BUNDLE_BYTES if name == "signature_bundle" else MAX_DOCUMENT_BYTES
            parts.append(
                f"CASE WHEN {name} IS NULL THEN NULL WHEN DATALENGTH({name}) BETWEEN 1 AND {maximum} "
                f"THEN {name} ELSE 0x END"
            )
        else:
            parts.append(name)
    return ", ".join(parts)


def _table(schema: str, kind: str) -> str:
    if kind not in {"grants", "memberships"}:
        raise NonproductionAuthorityError("registration_schema_table")
    return f"[{require_control_schema(schema)}].[composition_nonproduction_{kind}]"


def _hash_check(document: str, digest: str, maximum: int = MAX_DOCUMENT_BYTES) -> str:
    return (
        f"DATALENGTH({document}) NOT BETWEEN 1 AND {maximum} OR DATALENGTH({digest}) <> 71 "
        f"OR {digest} <> 'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', {document}), 2))"
    )


def nonproduction_registration_trigger_sql(control_schema: str, kind: str) -> str:
    """Exact standalone SQL module; the lock precedes its single physical append."""
    table = _table(control_schema, kind)
    schema = require_control_schema(control_schema)
    columns = _G if kind == "grants" else _M
    if kind == "grants":
        checks = " OR ".join(
            (
                _hash_check("grant_document", "grant_sha256"),
                _hash_check("signature_bundle", "bundle_sha256", MAX_ATTESTATION_BUNDLE_BYTES),
                _hash_check("signature_subject_document", "signature_subject_sha256"),
            )
        )
        checks += f""" OR schema_version <> 1 OR trust_revision < 1 OR DATALENGTH(registered_at) <> 20
        OR NOT ((phase = 'qualification' AND DATALENGTH(phase) = 13)
            OR (phase = 'execution' AND DATALENGTH(phase) = 9))
        OR (phase = 'qualification' AND (request_document IS NOT NULL OR request_sha256 IS NOT NULL))
        OR (phase = 'execution' AND (request_document IS NULL OR request_sha256 IS NULL
            OR {_hash_check("request_document", "request_sha256")}))
        OR DATALENGTH(consumption_subject_sha256) <> 71
        OR consumption_subject_sha256 <> 'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256',
            CONVERT(varbinary(max), '{{"grant_id":"' + LOWER(CONVERT(char(36), grant_id)) +
                '","phase":"' + phase + '"}}')), 2))
        OR grant_id = '00000000-0000-0000-0000-000000000000'
        OR phase_subject_id = '00000000-0000-0000-0000-000000000000'"""
    else:
        checks = _hash_check("workload_document", "workload_sha256")
        checks += f""" OR phase <> 'execution' OR DATALENGTH(phase) <> 9
        OR NOT EXISTS (SELECT 1 FROM [{schema}].[composition_nonproduction_grants] g WITH (HOLDLOCK)
            WHERE g.consumption_subject_sha256 = inserted.first_grant_sha256
              AND g.environment_id = inserted.environment_id AND g.campaign_id = inserted.campaign_id
              AND g.phase = inserted.phase)"""
    checks += """ OR environment_id = '00000000-0000-0000-0000-000000000000'
        OR campaign_id = '00000000-0000-0000-0000-000000000000'"""
    names = ", ".join(item[0] for item in columns)
    return f"""CREATE TRIGGER [{schema}].[composition_nonproduction_{kind}_append]
ON {table} INSTEAD OF INSERT, UPDATE, DELETE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM deleted) OR (SELECT COUNT_BIG(*) FROM inserted) <> 1
        THROW 51000, 'DPONE_NONPRODUCTION_REGISTRATION_APPEND_ONLY', 1;
    DECLARE @lock_result int;
    EXEC @lock_result = sys.sp_getapplock @Resource = N'{COMPOSITION_MSSQL_LEDGER_LOCK}',
        @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 0;
    IF @lock_result < 0 THROW 51000, 'DPONE_NONPRODUCTION_REGISTRATION_LOCK', 1;
    IF EXISTS (SELECT 1 FROM inserted WHERE {checks})
        THROW 51000, 'DPONE_NONPRODUCTION_REGISTRATION_ROW', 1;
    INSERT INTO {table} ({names}) SELECT {names} FROM inserted;
END;"""


def render_nonproduction_mssql_registration_schema(control_schema: str = "dpone_control") -> str:
    """Render one-time external installation; no adoption, repair or permissions."""
    schema = require_control_schema(control_schema)
    statements = [
        "SET XACT_ABORT ON;",
        "SET ANSI_NULLS ON;",
        "SET ANSI_PADDING ON;",
        "SET ANSI_WARNINGS ON;",
        "SET ARITHABORT ON;",
        "SET CONCAT_NULL_YIELDS_NULL ON;",
        "SET QUOTED_IDENTIFIER ON;",
        "SET NUMERIC_ROUNDABORT OFF;",
        "BEGIN TRANSACTION;",
    ]
    for kind, columns in (("grants", _G), ("memberships", _M)):
        definitions = []
        for name, dtype, length, collation, nullable in columns:
            size = f"({'max' if length == -1 else length})" if dtype in {"varbinary", "varchar"} else ""
            suffix = f" COLLATE {collation}" if collation else ""
            definitions.append(f"    {name} {dtype}{size}{suffix} {'NULL' if nullable else 'NOT NULL'}")
        for name, primary, keys in _KEYS[kind]:
            definitions.append(
                f"    CONSTRAINT [{name}] {'PRIMARY KEY CLUSTERED' if primary else 'UNIQUE NONCLUSTERED'} "
                f"({', '.join(keys)})"
            )
        foreign = (
            f"FOREIGN KEY (environment_id, trust_revision) REFERENCES [{schema}].[composition_nonproduction_trust] "
            "(environment_id, revision)"
            if kind == "grants"
            else f"FOREIGN KEY (first_grant_sha256) REFERENCES [{schema}].[composition_nonproduction_grants] "
            "(consumption_subject_sha256)"
        )
        definitions.append(f"    CONSTRAINT [fk_np_{kind}] {foreign}")
        statements.append(f"CREATE TABLE {_table(schema, kind)} (\n" + ",\n".join(definitions) + "\n);")
        if kind == "grants":
            statements.append(
                f"CREATE UNIQUE NONCLUSTERED INDEX [uq_np_qualification_run] ON {_table(schema, kind)} "
                f"(phase_subject_id) WHERE {_QUALIFICATION_FILTER};"
            )
        trigger = nonproduction_registration_trigger_sql(schema, kind).replace("'", "''")
        statements.append(f"EXEC(N'{trigger}');")
    return "\n".join((*statements, "COMMIT TRANSACTION;", ""))


def require_nonproduction_registration_schema(cursor: SqlControlCursor, control_schema: str) -> None:
    """Audit both complete catalog projections and exact UTF-16 SQL modules."""
    try:
        schema = require_control_schema(control_schema)
        for kind, columns in (("grants", _G), ("memberships", _M)):
            _inspect(cursor, schema, kind, columns)
    except NonproductionAuthorityError:
        raise
    except Exception:
        raise NonproductionAuthorityError("registration_schema_unavailable") from None


def require_nonproduction_registration_insert_options(cursor: SqlControlCursor) -> None:
    """Require filtered-index DML options without changing the caller's session.

    The external connection owner configures the same seven options as the
    renderer. Historical SELECT-only operations do not require these DML options.
    """
    try:
        cursor.execute("SELECT @@OPTIONS;")
        rows = tuple(tuple(value) for value in cursor.fetchall())
        if (
            len(rows) != 1
            or len(rows[0]) != 1
            or type(rows[0][0]) is not int
            or rows[0][0] & _INSERT_ON != _INSERT_ON
            or rows[0][0] & _INSERT_OFF
        ):
            raise NonproductionAuthorityError("registration_session")
    except NonproductionAuthorityError:
        raise
    except Exception:
        raise NonproductionAuthorityError("registration_session_unavailable") from None


def _inspect(cursor: SqlControlCursor, schema: str, kind: str, columns: tuple) -> None:
    def require(sql: str, expected: tuple[tuple[object, ...], ...], reason: str) -> None:
        cursor.execute(sql, _table(schema, kind))
        if tuple(tuple(value) for value in cursor.fetchall()) != expected:
            raise NonproductionAuthorityError("registration_schema_" + reason)

    require(
        "SELECT temporal_type, CONVERT(int, is_memory_optimized), CONVERT(int, is_filetable) "
        "FROM sys.tables WHERE object_id = OBJECT_ID(?, N'U');",
        ((0, 0, 0),),
        "table",
    )
    require(
        "SELECT c.name, TYPE_NAME(c.system_type_id), c.max_length, c.collation_name, "
        "CONVERT(int, c.is_nullable), CONVERT(int, c.is_identity), CONVERT(int, c.is_computed), "
        "CASE WHEN c.user_type_id = c.system_type_id THEN 1 ELSE 0 END "
        "FROM sys.columns c WHERE c.object_id = OBJECT_ID(?, N'U') ORDER BY c.column_id;",
        tuple((*value, 0, 0, 1) for value in columns),
        "columns",
    )
    keys: tuple[tuple[object, ...], ...] = tuple(
        (name, key, ordinal, 0, 0, int(primary), 1, 0, 1 if primary else 2, 0, 0, None)
        for name, primary, values in sorted(_KEYS[kind])
        for ordinal, key in enumerate(values, 1)
    )
    if kind == "grants":
        keys = (
            *keys,
            ("uq_np_qualification_run", "phase_subject_id", 1, 0, 0, 0, 1, 0, 2, 1, 0, _QUALIFICATION_FILTER),
        )
    require(
        "SELECT i.name, c.name, ic.key_ordinal, CONVERT(int, ic.is_descending_key), "
        "CONVERT(int, ic.is_included_column), CONVERT(int, i.is_primary_key), CONVERT(int, i.is_unique), "
        "CONVERT(int, i.is_disabled), i.type, CONVERT(int, i.has_filter), CONVERT(int, i.ignore_dup_key), i.filter_definition "
        "FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id "
        "JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id "
        "WHERE i.object_id=OBJECT_ID(?, N'U') AND i.index_id>0 ORDER BY i.name, ic.index_column_id;",
        keys,
        "keys",
    )
    require(
        "SELECT COUNT(*) FROM sys.objects WHERE parent_object_id=OBJECT_ID(?, N'U') AND type IN ('C','D');",
        ((0,),),
        "constraints",
    )
    references = (
        (
            ("environment_id", "composition_nonproduction_trust", "environment_id"),
            ("trust_revision", "composition_nonproduction_trust", "revision"),
        )
        if kind == "grants"
        else (("first_grant_sha256", "composition_nonproduction_grants", "consumption_subject_sha256"),)
    )
    require(
        "SELECT f.name, pc.name, OBJECT_SCHEMA_NAME(f.referenced_object_id), rt.name, rc.name, "
        "CONVERT(int,f.is_disabled), CONVERT(int,f.is_not_trusted), f.delete_referential_action, "
        "f.update_referential_action, CONVERT(int,f.is_not_for_replication) FROM sys.foreign_keys f "
        "JOIN sys.foreign_key_columns k ON k.constraint_object_id=f.object_id "
        "JOIN sys.columns pc ON pc.object_id=k.parent_object_id AND pc.column_id=k.parent_column_id "
        "JOIN sys.tables rt ON rt.object_id=k.referenced_object_id "
        "JOIN sys.columns rc ON rc.object_id=k.referenced_object_id AND rc.column_id=k.referenced_column_id "
        "WHERE f.parent_object_id=OBJECT_ID(?, N'U') ORDER BY f.name,k.constraint_column_id;",
        tuple((f"fk_np_{kind}", parent, schema, target, child, 0, 0, 0, 0, 0) for parent, target, child in references),
        "foreign_keys",
    )
    require(
        "SELECT t.name, CONVERT(int,t.is_disabled), CONVERT(int,t.is_instead_of_trigger), "
        "CONVERT(int,t.is_not_for_replication), CONVERT(int,m.uses_ansi_nulls), "
        "CONVERT(int,m.uses_quoted_identifier), m.execute_as_principal_id,HASHBYTES('SHA2_256',m.definition) "
        "FROM sys.triggers t LEFT JOIN sys.sql_modules m ON m.object_id=t.object_id "
        "WHERE t.parent_id=OBJECT_ID(?, N'U') ORDER BY t.name;",
        (
            (
                f"composition_nonproduction_{kind}_append",
                0,
                1,
                0,
                1,
                1,
                None,
                module_sha256(nonproduction_registration_trigger_sql(schema, kind)),
            ),
        ),
        "trigger",
    )
    require(
        "SELECT e.type_desc FROM sys.trigger_events e JOIN sys.objects o ON o.object_id=e.object_id "
        "WHERE o.parent_object_id=OBJECT_ID(?, N'U') ORDER BY e.type_desc;",
        (("DELETE",), ("INSERT",), ("UPDATE",)),
        "events",
    )
    require(
        "SELECT COUNT(*) FROM sys.security_predicates WHERE target_object_id=OBJECT_ID(?, N'U');",
        ((0,),),
        "row_security",
    )
