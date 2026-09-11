"""Externally installed cross-database transfer authority; runtime never installs.

The procedure executes as the control schema owner and only reads protected
rows. Issued workers receive EXECUTE on this one procedure and CONNECT on the
control database, never table permissions or membership in a controller role.
The installer must exclude privilege bypass, concurrent DDL and untrusted owners.
"""

from dpone.adapters.composition_mssql_catalog import inspect_composition_table
from dpone.adapters.composition_mssql_catalog_types import (
    CompositionColumn as Column,
)
from dpone.adapters.composition_mssql_catalog_types import (
    CompositionKey as Key,
)
from dpone.adapters.composition_mssql_catalog_types import (
    CompositionTable,
    CompositionTrigger,
    module_sha256,
)
from dpone.adapters.composition_mssql_schema import (
    COMPOSITION_MSSQL_LEDGER_LOCK,
    render_composition_table,
    require_control_schema,
)
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.ports.sql_connection import SqlControlCursor

TRANSFER_PROCEDURE = "composition_require_transfer"
BINDING_TABLE = CompositionTable(
    "transfer_bindings",
    (
        Column("binding_sha256", "binary", 32),
        Column("binding_document", "varbinary", -1),
        Column("operation_key", "varchar", 71),
        Column("owner_key", "varchar", 71),
        Column("owner_document", "varbinary", -1),
        Column("attempt_document", "varbinary", -1),
        Column("service_id", "uniqueidentifier", 16),
        Column("login_sid", "binary", 16),
        Column("target_database", "nvarchar", 256),
        Column("target_schema", "nvarchar", 256),
        Column("target_table", "nvarchar", 256),
        Column("guard_epochs", "nvarchar", -1),
    ),
    (Key("pk_composition_transfer_bindings", ("binding_sha256",), True),),
)


def binding_trigger(schema: str) -> CompositionTrigger:
    """Reject every rewrite or removal, including changes to identical bytes."""
    schema = require_control_schema(schema)
    name = "composition_transfer_binding_immutable"
    return CompositionTrigger(
        name,
        f"""CREATE TRIGGER [{schema}].[{name}]
ON [{schema}].[composition_transfer_bindings] AFTER UPDATE, DELETE AS
BEGIN THROW 51000, 'DPONE_COMPOSITION_TRANSFER_IMMUTABLE', 1; END;""",
        ("DELETE", "UPDATE"),
    )


def transfer_procedure_sql(schema: str = "dpone_control") -> str:
    """Read original rows under the control-database global transaction lock.

    The caller's actual target database and transaction are observed before the
    three-part EXEC. There is no BEGIN, COMMIT, ROLLBACK or dynamic SQL here.
    Original login SID survives EXECUTE AS OWNER. Immutable binding originals
    were admitted through the complete core/history validator by the controller.
    """
    schema = require_control_schema(schema)
    prefix = f"[{schema}].[composition_"
    return f"""CREATE PROCEDURE [{schema}].[{TRANSFER_PROCEDURE}]
    @binding_sha256 binary(32), @binding_document varbinary(max),
    @transaction_id bigint, @target_database sysname, @target_schema sysname, @target_table sysname
WITH EXECUTE AS OWNER AS
BEGIN
    SET NOCOUNT ON;
    IF @@TRANCOUNT < 1 OR XACT_STATE() <> 1 OR CURRENT_TRANSACTION_ID() <> @transaction_id
        THROW 51000, 'DPONE_COMPOSITION_TRANSFER_TRANSACTION', 1;
    DECLARE @lock int;
    EXEC @lock=sys.sp_getapplock @Resource=N'{COMPOSITION_MSSQL_LEDGER_LOCK}',
        @LockMode=N'Exclusive', @LockOwner=N'Transaction', @LockTimeout=0, @DbPrincipal=N'public';
    IF @lock < 0 THROW 51000, 'DPONE_COMPOSITION_TRANSFER_LOCK', 1;
    DECLARE @operation varchar(71), @owner varchar(71), @sid binary(16), @service uniqueidentifier,
        @owner_document varbinary(max), @attempt_document varbinary(max), @epochs nvarchar(max);
    SELECT @operation=operation_key, @owner=owner_key, @sid=login_sid, @service=service_id,
        @owner_document=owner_document, @attempt_document=attempt_document, @epochs=guard_epochs
    FROM {prefix}transfer_bindings] WITH (HOLDLOCK)
    WHERE binding_sha256=@binding_sha256 AND binding_document=@binding_document
        AND DATALENGTH(binding_document)=DATALENGTH(@binding_document)
        AND HASHBYTES('SHA2_256',binding_document)=@binding_sha256
        AND target_database=@target_database COLLATE Latin1_General_100_BIN2
        AND target_schema=@target_schema COLLATE Latin1_General_100_BIN2
        AND target_table=@target_table COLLATE Latin1_General_100_BIN2
        AND DATALENGTH(target_database)=DATALENGTH(@target_database)
        AND DATALENGTH(target_schema)=DATALENGTH(@target_schema)
        AND DATALENGTH(target_table)=DATALENGTH(@target_table);
    IF @operation IS NULL OR SUSER_SID(ORIGINAL_LOGIN()) IS NULL OR @sid <> SUSER_SID(ORIGINAL_LOGIN())
        THROW 51000, 'DPONE_COMPOSITION_TRANSFER_BINDING', 1;
    IF (SELECT COUNT_BIG(*) FROM {prefix}authority] WITH (HOLDLOCK)) <> 1 OR NOT EXISTS (
        SELECT 1 FROM {prefix}authority] WITH (HOLDLOCK)
        WHERE singleton=1 AND schema_version=2 AND service_id=@service)
        THROW 51000, 'DPONE_COMPOSITION_TRANSFER_SERVICE', 1;
    IF NOT EXISTS (SELECT 1 FROM {prefix}owners] WITH (HOLDLOCK)
        WHERE owner_key=@owner AND owner_kind='execution' AND state='ACTIVE'
        AND subject_document=@owner_document AND DATALENGTH(subject_document)=DATALENGTH(@owner_document)
        AND subject_sha256='sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@owner_document),2)))
        THROW 51000, 'DPONE_COMPOSITION_TRANSFER_OWNER', 1;
    IF NOT EXISTS (SELECT 1 FROM {prefix}operations] WITH (HOLDLOCK)
        WHERE operation_key=@operation AND replay_key=@operation AND owner_key=@owner
        AND operation_family='execution' AND state='RUNNING'
        AND operation_document=@attempt_document AND DATALENGTH(operation_document)=DATALENGTH(@attempt_document)
        AND operation_key='sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@attempt_document),2))
        AND owner_subject_sha256='sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@owner_document),2)))
        THROW 51000, 'DPONE_COMPOSITION_TRANSFER_ATTEMPT', 1;
    IF NOT EXISTS (SELECT 1 FROM {prefix}login_gates] g WITH (HOLDLOCK)
        JOIN {prefix}issued_authorities] a WITH (HOLDLOCK) ON a.operation_key=g.operation_key
        WHERE g.operation_key=@operation AND g.operation_family='execution' AND g.gate_state='READY'
        AND g.login_sid=@sid AND g.disabled_evidence_sha256 IS NULL
        AND g.login_name=ORIGINAL_LOGIN() COLLATE Latin1_General_100_BIN2
        AND DATALENGTH(g.login_name)=DATALENGTH(ORIGINAL_LOGIN())
        AND a.connector='mssql' AND a.service_id=@service
        AND a.principal_id='mssql-sid:'+LOWER(CONVERT(varchar(32),@sid,2)))
        THROW 51000, 'DPONE_COMPOSITION_TRANSFER_SID', 1;
    IF ISJSON(@epochs) <> 1 OR NOT EXISTS (SELECT 1 FROM OPENJSON(@epochs))
        THROW 51000, 'DPONE_COMPOSITION_TRANSFER_EPOCH', 1;
    IF EXISTS (
        SELECT j.guard_id,j.fencing_epoch FROM OPENJSON(@epochs)
            WITH (guard_id varchar(71) '$[0]',fencing_epoch bigint '$[1]') j
        EXCEPT SELECT guard_id,fencing_epoch FROM {prefix}operation_domains] WITH (HOLDLOCK)
            WHERE operation_key=@operation AND owner_key=@owner
    ) OR EXISTS (
        SELECT guard_id,fencing_epoch FROM {prefix}operation_domains] WITH (HOLDLOCK)
            WHERE operation_key=@operation AND owner_key=@owner
        EXCEPT SELECT j.guard_id,j.fencing_epoch FROM OPENJSON(@epochs)
            WITH (guard_id varchar(71) '$[0]',fencing_epoch bigint '$[1]') j
    ) OR EXISTS (
        SELECT 1 FROM {prefix}operation_domains] p WITH (HOLDLOCK)
        LEFT JOIN {prefix}domains] d WITH (HOLDLOCK) ON d.guard_id=p.guard_id
        LEFT JOIN {prefix}owner_domains] o WITH (HOLDLOCK)
            ON o.guard_id=p.guard_id AND o.owner_key=@owner AND o.fencing_epoch=p.fencing_epoch
        WHERE p.operation_key=@operation AND (d.guard_id IS NULL OR o.guard_id IS NULL
            OR d.owner_key IS NULL OR d.owner_key<>@owner OR d.fencing_epoch<>p.fencing_epoch)
    ) THROW 51000, 'DPONE_COMPOSITION_TRANSFER_EPOCH', 1;
    IF XACT_STATE() <> 1 OR CURRENT_TRANSACTION_ID() <> @transaction_id
        THROW 51000, 'DPONE_COMPOSITION_TRANSFER_TRANSACTION', 1;
    SELECT @transaction_id,@binding_sha256;
END;"""


def render_composition_mssql_transaction_fence(*, control_database: str, control_schema: str = "dpone_control") -> str:
    """Administrator-only additive installation batches; grants are per issued SID."""
    database, schema = require_control_schema(control_database), require_control_schema(control_schema)
    return "\nGO\n".join(
        (
            f"USE [{database}];",
            "SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;",
            render_composition_table(BINDING_TABLE, schema),
            binding_trigger(schema).definition,
            transfer_procedure_sql(schema),
            "",
        )
    )


def require_transaction_fence_schema(cursor: SqlControlCursor, control_schema: str = "dpone_control") -> None:
    """Audit additive table and exact owner-executed module before registration.

    The caller separately invokes the complete protected core/history audit.
    No CREATE/ALTER or grants are executed by this observer.
    """
    schema = require_control_schema(control_schema)
    inspect_composition_table(cursor, schema, BINDING_TABLE, {}, {}, binding_trigger(schema))
    cursor.execute(
        "SELECT TOP (2) p.type,CONVERT(int,m.uses_ansi_nulls),CONVERT(int,m.uses_quoted_identifier),"
        "m.execute_as_principal_id,HASHBYTES('SHA2_256',m.definition),"
        "CONVERT(int,p.is_auto_executed),CONVERT(int,p.is_execution_replicated),"
        "p.principal_id,s.principal_id FROM sys.procedures p JOIN sys.sql_modules m ON m.object_id=p.object_id "
        "JOIN sys.schemas s ON s.schema_id=p.schema_id WHERE p.object_id=OBJECT_ID(?,N'P');",
        f"[{schema}].[{TRANSFER_PROCEDURE}]",
    )
    rows = tuple(tuple(row) for row in cursor.fetchall())
    if (
        len(rows) != 1
        or rows[0][:7] != ("P", 1, 1, -2, module_sha256(transfer_procedure_sql(schema)), 0, 0)
        or rows[0][7:] != (None, 1)
    ):
        raise CompositionAdmissionError("transfer_module_schema")
