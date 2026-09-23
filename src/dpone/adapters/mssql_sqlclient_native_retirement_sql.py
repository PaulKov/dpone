"""Fixed parameterized SQL for exact SQL Server native-stage retirement."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from dpone.ports.mssql_sqlclient_writer_settlement import SqlClientStageIdentity

_ERROR = "mssql_native.sqlclient_retirement_catalog_invalid"

OBSERVE_EXACT_STAGE_SQL = """
SELECT t.object_id,t.schema_id,t.name,t.create_date,own.value,inc.value,
 DB_ID(),CONVERT(nvarchar(36),DATABASEPROPERTYEX(DB_NAME(),'DatabaseGUID')),DB_NAME()
FROM sys.tables AS t
LEFT JOIN sys.extended_properties AS own
 ON own.class=1 AND own.major_id=t.object_id AND own.minor_id=0 AND own.name=N'dpone_native_owner'
LEFT JOIN sys.extended_properties AS inc
 ON inc.class=1 AND inc.major_id=t.object_id AND inc.minor_id=0 AND inc.name=N'dpone_tds_incarnation'
WHERE t.object_id=? OR (t.schema_id=? AND t.name=?);
"""

DROP_EXACT_STAGE_SQL = """
SET XACT_ABORT ON;
DECLARE @actual int=(SELECT COUNT_BIG(*) FROM sys.tables AS t
 LEFT JOIN sys.extended_properties AS own
  ON own.class=1 AND own.major_id=t.object_id AND own.minor_id=0 AND own.name=N'dpone_native_owner'
 LEFT JOIN sys.extended_properties AS inc
  ON inc.class=1 AND inc.major_id=t.object_id AND inc.minor_id=0 AND inc.name=N'dpone_tds_incarnation'
 WHERE t.object_id=? AND t.schema_id=? AND t.name=? AND t.create_date=?
 AND CONVERT(nvarchar(4000),own.value)=? AND CONVERT(nvarchar(36),inc.value)=?);
IF @actual<>1 THROW 51000,'DPONE_RETIREMENT_IDENTITY_MISMATCH',1;
DECLARE @sql nvarchar(776)=N'DROP TABLE '+QUOTENAME(?) + N'.' + QUOTENAME(?);
EXEC sys.sp_executesql @sql;
"""


def exact_stage_parameters(stage: SqlClientStageIdentity) -> tuple[object, ...]:
    """Return the closed catalog identity used by both observation and DROP."""
    if type(stage) is not SqlClientStageIdentity:
        raise ValueError(_ERROR)
    stage.__post_init__()
    return (
        stage.object_id,
        stage.schema_id,
        stage.table_name,
        stage.create_date,
        stage.owner_binding,
        str(stage.object_nonce),
    )


def drop_exact_parameters(stage: SqlClientStageIdentity) -> tuple[object, ...]:
    """Bind guards and quoted identifier inputs without interpolating names."""
    return (*exact_stage_parameters(stage), stage.schema_name, stage.table_name)


def observe_stage_parameters(stage: SqlClientStageIdentity) -> tuple[object, ...]:
    """Address both the old object and its qualified namespace."""
    exact_stage_parameters(stage)
    return stage.object_id, stage.schema_id, stage.table_name


def stage_catalog_status(rows: tuple[tuple[Any, ...], ...], stage: SqlClientStageIdentity) -> str:
    """Classify exact presence, complete absence, or a foreign replacement."""
    if not rows:
        return "absent"
    return "exact" if stage_is_exact(rows, stage) else "foreign"


def stage_is_exact(rows: tuple[tuple[Any, ...], ...], stage: SqlClientStageIdentity) -> bool:
    """Validate the single bounded catalog row against the admitted incarnation."""
    if len(rows) != 1 or len(rows[0]) != 9:
        return False
    row = rows[0]
    try:
        nonce = UUID(str(row[5]))
        database_guid = UUID(str(row[7]))
    except (ValueError, TypeError, AttributeError):
        return False
    return (
        type(row[0]) is int
        and type(row[1]) is int
        and type(row[2]) is str
        and type(row[3]) is datetime
        and type(row[4]) is str
        and type(row[6]) is int
        and type(row[8]) is str
        and (row[0], row[1], row[2], row[3], row[4], nonce, row[6], database_guid, row[8])
        == (
            stage.object_id,
            stage.schema_id,
            stage.table_name,
            stage.create_date,
            stage.owner_binding,
            stage.object_nonce,
            stage.database_id,
            stage.database_guid,
            stage.database_name,
        )
    )


__all__ = (
    "DROP_EXACT_STAGE_SQL",
    "OBSERVE_EXACT_STAGE_SQL",
    "drop_exact_parameters",
    "exact_stage_parameters",
    "observe_stage_parameters",
    "stage_catalog_status",
    "stage_is_exact",
)
