"""External SQL trust extension and exact catalog inspection, without enrollment.

An independent administrator installs this renderer once in an existing control
schema. A trusted provisioner may SELECT/INSERT revisions, but must not have
ALTER, TRUNCATE, trigger-disabling, impersonation or other bypass authority.
Readers require complete catalog visibility. These inspections detect drift;
they do not prove that effective database/server permissions enforce isolation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.adapters.composition_mssql_catalog_types import module_sha256
from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_LEDGER_LOCK, require_control_schema
from dpone.contracts.nonproduction_document import MAX_DOCUMENT_BYTES, NonproductionAuthorityError

if TYPE_CHECKING:
    from dpone.ports.sql_connection import SqlControlCursor

NONPRODUCTION_MSSQL_SCHEMA_VERSION = 1
_COLLATION = "Latin1_General_100_BIN2"
_COLUMNS = (
    ("environment_id", "uniqueidentifier", 16, None),
    ("revision", "bigint", 8, None),
    ("schema_version", "int", 4, None),
    ("policy_document", "varbinary", -1, None),
    ("policy_sha256", "varchar", 71, _COLLATION),
    ("verifier_policy_document", "varbinary", -1, None),
    ("verifier_policy_sha256", "varchar", 71, _COLLATION),
    ("current_revocation_epoch", "bigint", 8, None),
)


def nonproduction_trust_trigger_sql(control_schema: str) -> str:
    """One exact module batch; never UPDATE/DELETE or reuse an environment revision.

    The existing global transaction lock orders appends with future admission
    writes. Only one supplied row is accepted; concurrent provisioners must
    reread the committed revision after conflict, never replay a blind append.
    """
    schema = require_control_schema(control_schema)
    table = f"[{schema}].[composition_nonproduction_trust]"
    return f"""CREATE TRIGGER [{schema}].[composition_nonproduction_trust_append]
ON {table} INSTEAD OF INSERT, UPDATE, DELETE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM deleted) OR (SELECT COUNT_BIG(*) FROM inserted) <> 1
        THROW 51000, 'DPONE_NONPRODUCTION_TRUST_APPEND_ONLY', 1;
    DECLARE @lock_result int;
    EXEC @lock_result = sys.sp_getapplock @Resource = N'{COMPOSITION_MSSQL_LEDGER_LOCK}',
        @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 0;
    IF @lock_result < 0 THROW 51000, 'DPONE_NONPRODUCTION_TRUST_LOCK', 1;
    DECLARE @previous_revision bigint = 0, @previous_epoch bigint = 0;
    SELECT TOP (1) @previous_revision = revision, @previous_epoch = current_revocation_epoch
        FROM {table} WITH (UPDLOCK, HOLDLOCK)
        WHERE environment_id = (SELECT environment_id FROM inserted) ORDER BY revision DESC;
    IF EXISTS (SELECT 1 FROM inserted WHERE
        environment_id = '00000000-0000-0000-0000-000000000000'
        OR schema_version <> {NONPRODUCTION_MSSQL_SCHEMA_VERSION}
        OR revision <> @previous_revision + 1
        OR current_revocation_epoch < @previous_epoch
        OR DATALENGTH(policy_document) NOT BETWEEN 1 AND {MAX_DOCUMENT_BYTES}
        OR DATALENGTH(verifier_policy_document) NOT BETWEEN 1 AND {MAX_DOCUMENT_BYTES}
        OR DATALENGTH(policy_sha256) <> 71 OR DATALENGTH(verifier_policy_sha256) <> 71
        OR policy_sha256 <> 'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', policy_document), 2))
        OR verifier_policy_sha256 <> 'sha256:' +
            LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', verifier_policy_document), 2)))
        THROW 51000, 'DPONE_NONPRODUCTION_TRUST_REVISION', 1;
    INSERT INTO {table} (environment_id, revision, schema_version, policy_document, policy_sha256,
        verifier_policy_document, verifier_policy_sha256, current_revocation_epoch)
        SELECT environment_id, revision, schema_version, policy_document, policy_sha256,
            verifier_policy_document, verifier_policy_sha256, current_revocation_epoch FROM inserted;
END;"""


def render_nonproduction_mssql_schema(control_schema: str = "dpone_control") -> str:
    """Render an external one-time installation; never execute or silently adopt it.

    CREATE TRIGGER is its entire dynamic batch, preserving exact NVARCHAR module
    bytes for inspection. Public document digests instead hash original UTF-8
    VARBINARY bytes. Permissions and externally selected pins are not installed.
    """
    schema = require_control_schema(control_schema)
    trigger = nonproduction_trust_trigger_sql(schema).replace("'", "''")
    return f"""SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
BEGIN TRANSACTION;
IF OBJECT_ID(N'[{schema}].[composition_authority]', N'U') IS NULL
    THROW 51000, 'DPONE_NONPRODUCTION_CONTROL_REQUIRED', 1;
CREATE TABLE [{schema}].[composition_nonproduction_trust] (
    environment_id uniqueidentifier NOT NULL,
    revision bigint NOT NULL,
    schema_version int NOT NULL,
    policy_document varbinary(max) NOT NULL,
    policy_sha256 varchar(71) COLLATE {_COLLATION} NOT NULL,
    verifier_policy_document varbinary(max) NOT NULL,
    verifier_policy_sha256 varchar(71) COLLATE {_COLLATION} NOT NULL,
    current_revocation_epoch bigint NOT NULL,
    PRIMARY KEY CLUSTERED (environment_id, revision)
);
EXEC(N'{trigger}');
COMMIT TRANSACTION;
"""


def require_nonproduction_mssql_schema(cursor: SqlControlCursor, control_schema: str) -> None:
    """Reject missing/altered catalog or trigger modules; never repair or grant.

    Schema administrators remain trusted. A complete, correct catalog projection
    is necessary but cannot establish effective permissions or stop a privileged
    actor changing DDL after this read. Real module persistence needs live proof.
    """
    try:
        _inspect(cursor, require_control_schema(control_schema))
    except NonproductionAuthorityError:
        raise
    except Exception:
        raise NonproductionAuthorityError("trust_schema_unavailable") from None


def _inspect(cursor: SqlControlCursor, schema: str) -> None:
    table = f"[{schema}].[composition_nonproduction_trust]"

    def require_rows(sql: str, expected: tuple[tuple[object, ...], ...], reason: str) -> None:
        cursor.execute(sql, table)
        if tuple(tuple(value) for value in cursor.fetchall()) != expected:
            raise NonproductionAuthorityError("trust_schema_" + reason)

    require_rows(
        "SELECT temporal_type, CONVERT(int, is_memory_optimized), CONVERT(int, is_filetable) "
        "FROM sys.tables WHERE object_id = OBJECT_ID(?, N'U');",
        ((0, 0, 0),),
        "table",
    )
    require_rows(
        "SELECT c.name, TYPE_NAME(c.system_type_id), c.max_length, c.collation_name, "
        "CONVERT(int, c.is_nullable), CONVERT(int, c.is_identity), CONVERT(int, c.is_computed), "
        "CASE WHEN c.user_type_id = c.system_type_id THEN 1 ELSE 0 END "
        "FROM sys.columns c WHERE c.object_id = OBJECT_ID(?, N'U') ORDER BY c.column_id;",
        tuple((*value, 0, 0, 0, 1) for value in _COLUMNS),
        "columns",
    )
    require_rows(
        "SELECT c.name, ic.key_ordinal, CONVERT(int, ic.is_descending_key), CONVERT(int, ic.is_included_column), "
        "CONVERT(int, i.is_primary_key), CONVERT(int, i.is_unique), CONVERT(int, i.is_disabled), "
        "i.type, CONVERT(int, i.has_filter), CONVERT(int, i.ignore_dup_key) "
        "FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
        "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "WHERE i.object_id = OBJECT_ID(?, N'U') AND i.index_id > 0 ORDER BY i.index_id, ic.index_column_id;",
        (("environment_id", 1, 0, 0, 1, 1, 0, 1, 0, 0), ("revision", 2, 0, 0, 1, 1, 0, 1, 0, 0)),
        "primary_key",
    )
    require_rows(
        "SELECT COUNT(*) FROM sys.objects WHERE parent_object_id = OBJECT_ID(?, N'U') AND type IN ('C', 'D', 'F');",
        ((0,),),
        "constraints",
    )
    require_rows(
        "SELECT t.name, CONVERT(int, t.is_disabled), CONVERT(int, t.is_instead_of_trigger), "
        "CONVERT(int, t.is_not_for_replication), CONVERT(int, m.uses_ansi_nulls), "
        "CONVERT(int, m.uses_quoted_identifier), m.execute_as_principal_id, HASHBYTES('SHA2_256', m.definition) "
        "FROM sys.triggers t LEFT JOIN sys.sql_modules m ON m.object_id = t.object_id "
        "WHERE t.parent_id = OBJECT_ID(?, N'U') ORDER BY t.name;",
        (
            (
                "composition_nonproduction_trust_append",
                0,
                1,
                0,
                1,
                1,
                None,
                module_sha256(nonproduction_trust_trigger_sql(schema)),
            ),
        ),
        "trigger",
    )
    require_rows(
        "SELECT e.type_desc FROM sys.trigger_events e JOIN sys.objects o ON o.object_id = e.object_id "
        "WHERE o.parent_object_id = OBJECT_ID(?, N'U') ORDER BY e.type_desc;",
        (("DELETE",), ("INSERT",), ("UPDATE",)),
        "events",
    )
    require_rows(
        "SELECT COUNT(*) FROM sys.security_predicates WHERE target_object_id = OBJECT_ID(?, N'U');",
        ((0,),),
        "row_security",
    )
