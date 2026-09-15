"""Recognized initial-generation ledger upgrade inside an administrator transaction.

No runtime adapter imports this migration. The caller owns commit/rollback and
must validate the retained tables before any extension. Unknown procedures are
never replaced merely because their names match an expected name.
"""

from __future__ import annotations

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.native_generation_mssql_queries import generation_procedure, generation_procedure_name
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.ports.sql_connection import SqlControlCursor

GENERATION_ADMISSION_CHECK = """guard_epoch>0 AND revision>0
 AND (writer_admission='OPEN' OR writer_admission='CLOSED')
 AND (outcome='ACTIVE' OR outcome='FAILED' OR outcome='UNKNOWN')
 AND ((executor IS NULL AND revision=1 AND admission_sequence=0
       AND writer_admission='OPEN' AND (outcome='ACTIVE' OR outcome='FAILED') AND admission_closure IS NULL)
 OR (executor IS NOT NULL AND admission_sequence=1
     AND ((writer_admission='OPEN' AND revision>=2 AND admission_closure IS NULL)
       OR (writer_admission='CLOSED' AND revision>=3 AND admission_closure IS NOT NULL
           AND DATALENGTH(admission_closure)>=1 AND DATALENGTH(admission_closure)<=1048576))))"""
GENERATION_ADMISSION_CHECK = " ".join(GENERATION_ADMISSION_CHECK.split())

_OPERATIONS = ("reserve", "bind", "read", "close", "closure_read")


def upgrade_generation_ledger(
    cursor: SqlControlCursor,
    *,
    schema: str,
    legacy: bool,
    fresh: bool,
    legacy_verification: str,
    current_verification: str,
) -> None:
    """Upgrade only exact known definitions, or verify the current layout unchanged.

    `fresh` distinguishes a newly created initial layout from a retained ledger:
    missing procedures on a retained layout are corruption, not an invitation to
    silently reconstruct its authority. All five definitions are inspected before
    any ALTER; their exact bodies retain the existing caller/authority checks.
    """
    native_control_schema(schema)
    if fresh and not legacy:
        raise ValueError("a fresh ledger must start from the initial layout")
    cursor.execute(legacy_verification if legacy else current_verification)
    observed: dict[str, str | None] = {}
    for operation in _OPERATIONS:
        name = f"[{schema}].[{generation_procedure_name(operation)}]"
        cursor.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?))", name)
        row = dbapi_lifecycle.row(cursor)
        if row is None or len(row) != 1 or dbapi_lifecycle.row(cursor) is not None:
            raise RuntimeError("cannot inspect retained generation procedure")
        definition = row[0]
        expected = None
        if not fresh and (not legacy or operation in {"reserve", "bind", "read"}):
            expected = generation_procedure(schema, operation, extended=not legacy)
        allowed = {expected}
        if expected is not None and not legacy:
            # SQL Server retains the ALTER spelling of a recognized upgraded
            # definition; allow only that exact spelling, never arbitrary bodies.
            allowed.add(expected.replace("CREATE PROCEDURE", "ALTER PROCEDURE", 1))
        if definition not in allowed:
            raise RuntimeError("existing generation procedure differs from a recognized layout")
        observed[operation] = definition
    if legacy:
        for statement in _extend_initial_table(schema):
            cursor.execute(statement)
        cursor.execute(current_verification)
        for operation in _OPERATIONS:
            definition = generation_procedure(schema, operation)
            if observed[operation] is not None:
                definition = definition.replace("CREATE PROCEDURE", "ALTER PROCEDURE", 1)
            cursor.execute(definition)


def _extend_initial_table(schema: str) -> tuple[str, ...]:
    """Preserve every retained identity and quota while backfilling known phases."""
    table = f"[{schema}].[native_generations_v1]"
    return (
        f"""DECLARE @constraint sysname=(SELECT name FROM sys.check_constraints
 WHERE parent_object_id=OBJECT_ID(N'{table}',N'U'));
IF @constraint IS NULL THROW 51310, 'DPONE_NATIVE_GENERATION_LEGACY_CHECK_MISSING', 1;
DECLARE @drop nvarchar(max)=N'ALTER TABLE {table} DROP CONSTRAINT '+QUOTENAME(@constraint);
EXEC sys.sp_executesql @drop;
ALTER TABLE {table} ADD writer_admission varchar(6) NULL, outcome varchar(7) NULL,
 admission_sequence bigint NULL, admission_closure varbinary(max) NULL;""",
        f"""UPDATE {table} SET writer_admission='OPEN',outcome='ACTIVE',
 admission_sequence=CASE WHEN executor IS NULL THEN 0 ELSE 1 END;""",
        f"""ALTER TABLE {table} ALTER COLUMN writer_admission varchar(6) NOT NULL;
ALTER TABLE {table} ALTER COLUMN outcome varchar(7) NOT NULL;
ALTER TABLE {table} ALTER COLUMN admission_sequence bigint NOT NULL;
ALTER TABLE {table} WITH CHECK ADD CONSTRAINT CK_native_generation_admission_v1
 CHECK ({GENERATION_ADMISSION_CHECK});""",
    )
