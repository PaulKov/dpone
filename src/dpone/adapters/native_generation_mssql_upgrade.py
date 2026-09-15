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
COMPLETION_OPERATIONS = (*_OPERATIONS, "complete", "completion_read")
FREEZE_OPERATIONS = (*COMPLETION_OPERATIONS, "freeze", "freeze_read")
FREEZE_INSPECTION_OPERATIONS = (*FREEZE_OPERATIONS, "freeze_inspect")


def _original_triple_check(prefix: str) -> str:
    return f"""(({prefix}_payload IS NULL AND {prefix}_locator IS NULL AND {prefix}_digest IS NULL)
 OR ({prefix}_payload IS NOT NULL AND {prefix}_locator IS NOT NULL AND {prefix}_digest IS NOT NULL
 AND DATALENGTH({prefix}_payload) BETWEEN 1 AND 1048576
 AND DATALENGTH({prefix}_locator) BETWEEN 1 AND 4096 AND DATALENGTH({prefix}_digest)=71))"""


GENERATION_COMPLETION_CHECK = " ".join(
    f"""({GENERATION_ADMISSION_CHECK})
 AND {_original_triple_check("completion")} AND {_original_triple_check("frozen")}
 AND ((phase='RESERVED' AND executor IS NULL AND completion_payload IS NULL AND frozen_payload IS NULL)
 OR (phase='BUILDING' AND executor IS NOT NULL AND frozen_payload IS NULL
  AND (completion_payload IS NULL OR (writer_admission='CLOSED' AND revision>=4)))
 OR (phase='FROZEN' AND executor IS NOT NULL AND writer_admission='CLOSED' AND revision>=5
  AND completion_payload IS NOT NULL AND frozen_payload IS NOT NULL))""".split()
)


def upgrade_generation_ledger(
    cursor: SqlControlCursor,
    *,
    schema: str,
    legacy: bool,
    fresh: bool,
    legacy_verification: str,
    current_verification: str,
    completed: bool = False,
    completion_verification: str | None = None,
    freeze_enabled: bool = False,
    inspection_enabled: bool = False,
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
    if completed and (legacy or completion_verification is None):
        raise ValueError("completion layout requires its exact verification")
    if type(freeze_enabled) is not bool or (freeze_enabled and completion_verification is None):
        raise ValueError("freeze requires the verified completion layout")
    if type(inspection_enabled) is not bool or (inspection_enabled and not freeze_enabled):
        raise ValueError("freeze inspection requires the freeze procedure pair")
    verification = completion_verification if completed else (legacy_verification if legacy else current_verification)
    assert verification is not None
    cursor.execute(verification)
    operations: tuple[str, ...] = COMPLETION_OPERATIONS if completion_verification is not None else _OPERATIONS
    if freeze_enabled:
        operations = FREEZE_OPERATIONS
    if inspection_enabled:
        operations = FREEZE_INSPECTION_OPERATIONS
    observed: dict[str, str | None] = {}
    for operation in operations:
        name = f"[{schema}].[{generation_procedure_name(operation)}]"
        cursor.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?))", name)
        row = dbapi_lifecycle.row(cursor)
        if row is None or len(row) != 1 or dbapi_lifecycle.row(cursor) is not None:
            raise RuntimeError("cannot inspect retained generation procedure")
        definition = row[0]
        expected = None
        if not fresh and (
            completed or operation in _OPERATIONS and (not legacy or operation in {"reserve", "bind", "read"})
        ):
            expected = generation_procedure(schema, operation, extended=not legacy, completed=completed)
        allowed = {expected}
        if expected is not None and not legacy:
            # SQL Server retains the ALTER spelling of a recognized upgraded
            # definition; allow only that exact spelling, never arbitrary bodies.
            allowed.add(expected.replace("CREATE PROCEDURE", "ALTER PROCEDURE", 1))
        if completed and operation in {"freeze", "freeze_read", "freeze_inspect"}:
            allowed.add(None)
        if definition not in allowed:
            raise RuntimeError("existing generation procedure differs from a recognized layout")
        observed[operation] = definition
    freeze_missing = freeze_enabled and observed["freeze"] is None
    if freeze_enabled and freeze_missing != (observed["freeze_read"] is None):
        raise RuntimeError("partial freeze procedure pair cannot be silently repaired")
    inspection_missing = inspection_enabled and observed["freeze_inspect"] is None
    if inspection_enabled and freeze_missing and not inspection_missing:
        raise RuntimeError("freeze inspection without its procedure pair is a partial layout")
    if legacy:
        for statement in _extend_initial_table(schema):
            cursor.execute(statement)
        cursor.execute(current_verification)
    extend_completion = completion_verification is not None and not completed
    if extend_completion:
        assert completion_verification is not None
        for statement in _extend_completion_table(schema):
            cursor.execute(statement)
        cursor.execute(completion_verification)
    if legacy or extend_completion or freeze_missing or inspection_missing:
        for operation in operations:
            if not (legacy or extend_completion) and observed[operation] is not None:
                continue
            definition = generation_procedure(schema, operation, completed=completion_verification is not None)
            if observed[operation] is not None:
                definition = definition.replace("CREATE PROCEDURE", "ALTER PROCEDURE", 1)
            cursor.execute(definition)


def _extend_completion_table(schema: str) -> tuple[str, ...]:
    """Backfill phase only; never manufacture completion, revisions or quota."""
    table = f"[{schema}].[native_generations_v1]"
    return (
        f"""DECLARE @constraint sysname=(SELECT name FROM sys.check_constraints
 WHERE parent_object_id=OBJECT_ID(N'{table}',N'U'));
IF @constraint IS NULL THROW 51310, 'DPONE_NATIVE_GENERATION_ADMISSION_CHECK_MISSING', 1;
DECLARE @drop nvarchar(max)=N'ALTER TABLE {table} DROP CONSTRAINT '+QUOTENAME(@constraint);
EXEC sys.sp_executesql @drop;
ALTER TABLE {table} ADD phase varchar(8) NULL,
 completion_payload varbinary(max) NULL,completion_locator varbinary(max) NULL,completion_digest varbinary(71) NULL,
 frozen_payload varbinary(max) NULL,frozen_locator varbinary(max) NULL,frozen_digest varbinary(71) NULL;""",
        f"UPDATE {table} SET phase=CASE WHEN executor IS NULL THEN 'RESERVED' ELSE 'BUILDING' END;",
        f"""ALTER TABLE {table} ALTER COLUMN phase varchar(8) NOT NULL;
ALTER TABLE {table} WITH CHECK ADD CONSTRAINT CK_native_generation_completion_v1
 CHECK ({GENERATION_COMPLETION_CHECK});""",
    )


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
