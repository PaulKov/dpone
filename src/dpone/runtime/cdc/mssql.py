"""SQL Server CDC and Change Tracking readers."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from dpone.contracts.mssql_object_name import MSSQLObjectName
from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.base import CDCBatch, CDCChange, CDCOperation


@dataclass(frozen=True, slots=True)
class MSSQLTableCDCReaderConfig:
    """Configuration for SQL Server CDC table functions."""

    source_schema: str
    source_table: str
    source_database: str | None = None
    capture_instance: str | None = None
    include_update_before: bool = False

    @property
    def resolved_capture_instance(self) -> str:
        name = MSSQLObjectName.from_parts(
            schema=self.source_schema,
            table=self.source_table,
            database=self.source_database,
        )
        return self.capture_instance or f"{name.schema}_{name.table}"


@dataclass(frozen=True, slots=True)
class MSSQLChangeTrackingReaderConfig:
    """Configuration for SQL Server Change Tracking."""

    source_schema: str
    source_table: str
    source_database: str | None = None
    primary_keys: tuple[str, ...] = field(default_factory=tuple)
    track_columns_updated: bool = True


class MSSQLCDCReader:
    """Reader for SQL Server native CDC ``cdc.fn_cdc_get_all_changes_*`` functions."""

    _capture_instance_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    _operation_map = {
        1: CDCOperation.DELETE,
        2: CDCOperation.INSERT,
        3: CDCOperation.UPDATE_BEFORE,
        4: CDCOperation.UPDATE,
    }

    def __init__(self, connector: Any, config: MSSQLTableCDCReaderConfig):
        self.connector = connector
        self.config = config

    def setup(self) -> None:
        """Enable database/table CDC idempotently."""

        db_rows = self.connector.get_records(
            "SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()",
            as_dict=True,
        )
        if not db_rows or int(db_rows[0].get("is_cdc_enabled", 0)) != 1:
            self.connector.execute_query("EXEC sys.sp_cdc_enable_db")

        table_rows = self.connector.get_records(
            "SELECT 1 AS enabled FROM cdc.change_tables WHERE capture_instance = ?",
            (self.config.resolved_capture_instance,),
            as_dict=True,
        )
        if table_rows:
            return
        self.connector.execute_query(
            "EXEC sys.sp_cdc_enable_table @source_schema = ?, @source_name = ?, @role_name = NULL, "
            "@capture_instance = ?, @supports_net_changes = 1",
            (self.config.source_schema, self.config.source_table, self.config.resolved_capture_instance),
        )

    def current_lsn(self) -> str:
        rows = self.connector.get_records("SELECT sys.fn_varbintohexstr(sys.fn_cdc_get_max_lsn()) AS lsn", as_dict=True)
        if not rows or rows[0].get("lsn") is None:
            return "0x00000000000000000000"
        return str(rows[0]["lsn"])

    def read_batch(self, *, start_offset: CDCOffset | None = None, max_changes: int = 10000) -> CDCBatch:
        columns = self._load_user_columns()
        if not columns:
            return CDCBatch(changes=(), next_offset=self._offset(start_offset.token if start_offset else None))

        capture_instance = self._safe_capture_instance()
        column_sql = ", ".join(self._quote(column) for column in columns)
        query = f"""
DECLARE @min_lsn binary(10) = sys.fn_cdc_get_min_lsn(?);
DECLARE @requested_from_lsn binary(10) = CASE
    WHEN ? IS NULL OR ? = '0x00000000000000000000' THEN @min_lsn
    ELSE sys.fn_cdc_increment_lsn(CONVERT(binary(10), ?, 1))
END;
DECLARE @from_lsn binary(10) = CASE
    WHEN @requested_from_lsn IS NULL OR @min_lsn IS NULL THEN NULL
    WHEN @requested_from_lsn < @min_lsn THEN @min_lsn
    ELSE @requested_from_lsn
END;
DECLARE @to_lsn binary(10) = sys.fn_cdc_get_max_lsn();
IF @from_lsn IS NULL OR @to_lsn IS NULL
BEGIN
    SELECT TOP (0)
        CAST(NULL AS varchar(24)) AS __dpone__lsn,
        CAST(NULL AS int) AS __dpone__operation,
        {column_sql}
    FROM {self._qualified_name()} WHERE 1 = 0;
END
ELSE
BEGIN
    SELECT TOP (?)
        sys.fn_varbintohexstr([__$start_lsn]) AS __dpone__lsn,
        [__$operation] AS __dpone__operation,
        {column_sql}
    FROM cdc.fn_cdc_get_all_changes_{capture_instance}(@from_lsn, @to_lsn, 'all update old')
    ORDER BY [__$start_lsn], [__$seqval];
END
"""
        token = start_offset.token if start_offset is not None else None
        rows = self.connector.get_records(
            query,
            (capture_instance, token, token, token, max_changes),
            as_dict=True,
        )
        changes: list[CDCChange] = []
        high_watermark = token
        for row in rows:
            lsn = str(row.get("__dpone__lsn"))
            if lsn and lsn != "None":
                high_watermark = lsn
            operation = self._operation_map.get(int(row.get("__dpone__operation", 0) or 0))
            if operation is None:
                continue
            if operation == CDCOperation.UPDATE_BEFORE and not self.config.include_update_before:
                continue
            data = {column: row.get(column) for column in columns}
            changes.append(
                CDCChange(
                    operation=operation,
                    data=data,
                    position=lsn,
                    source_schema=self.config.source_schema,
                    source_table=self.config.source_table,
                    metadata={"backend": "mssql_cdc"},
                )
            )
        return CDCBatch(changes=tuple(changes), next_offset=self._offset(high_watermark), high_watermark=high_watermark)

    def _load_user_columns(self) -> list[str]:
        rows = self.connector.get_records(
            f"""
SELECT COLUMN_NAME
FROM {self._object_name().information_schema_table("COLUMNS")}
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION
""",
            (self._object_name().schema, self._object_name().table),
            as_dict=True,
        )
        return [
            str(row.get("COLUMN_NAME") or row.get("name")) for row in rows if row.get("COLUMN_NAME") or row.get("name")
        ]

    def _safe_capture_instance(self) -> str:
        capture_instance = self.config.resolved_capture_instance
        if not self._capture_instance_re.match(capture_instance):
            raise ValueError(f"Unsafe SQL Server CDC capture instance: {capture_instance!r}")
        return capture_instance

    def _quote(self, name: str) -> str:
        return self.connector.quote_identifier(name)

    def _qualified_name(self) -> str:
        name = self._object_name()
        try:
            return self.connector.qualified_name(name.schema, name.table, database=name.database)
        except TypeError:
            return self.connector.qualified_name(name.schema_label, name.table)

    def _object_name(self) -> MSSQLObjectName:
        return MSSQLObjectName.from_parts(
            schema=self.config.source_schema,
            table=self.config.source_table,
            database=self.config.source_database,
        )

    def _offset(self, token: str | None) -> CDCOffset | None:
        if token is None:
            return None
        return CDCOffset(backend=CDCBackend.MSSQL_CDC, token=token, snapshot_complete=True)


class MSSQLChangeTrackingReader:
    """Reader for SQL Server Change Tracking row version streams."""

    _operation_map = {
        "I": CDCOperation.INSERT,
        "U": CDCOperation.UPDATE,
        "D": CDCOperation.DELETE,
    }

    def __init__(self, connector: Any, config: MSSQLChangeTrackingReaderConfig):
        self.connector = connector
        self.config = config

    def setup(self) -> None:
        """Enable database/table Change Tracking idempotently."""

        db_rows = self.connector.get_records(
            "SELECT 1 AS enabled FROM sys.change_tracking_databases WHERE database_id = DB_ID()",
            as_dict=True,
        )
        if not db_rows:
            self.connector.execute_query(
                "DECLARE @sql nvarchar(max) = N'ALTER DATABASE ' + QUOTENAME(DB_NAME()) + "
                "N' SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)'; EXEC(@sql);"
            )

        table_rows = self.connector.get_records(
            "SELECT 1 AS enabled FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID(?)",
            (self._qualified_name_literal(),),
            as_dict=True,
        )
        if table_rows:
            return
        track_columns = "ON" if self.config.track_columns_updated else "OFF"
        self.connector.execute_query(
            f"ALTER TABLE {self._qualified_name()} ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = {track_columns})"
        )

    def current_offset(self) -> CDCOffset:
        return CDCOffset(
            backend=CDCBackend.MSSQL_CHANGE_TRACKING, token=str(self.current_version()), snapshot_complete=True
        )

    def current_version(self) -> int:
        rows = self.connector.get_records("SELECT CHANGE_TRACKING_CURRENT_VERSION() AS version", as_dict=True)
        if not rows or rows[0].get("version") is None:
            return 0
        return int(rows[0]["version"])

    def read_batch(self, *, start_offset: CDCOffset | None = None, max_changes: int = 10000) -> CDCBatch:
        start_version = int(start_offset.token) if start_offset is not None and start_offset.token else 0
        high_watermark = self.current_version()
        primary_keys = self._primary_keys()
        columns = self._load_user_columns()
        select_sql = self._select_columns_sql(columns, primary_keys)
        join_sql = " AND ".join(f"T.{self._quote(key)} = CT.{self._quote(key)}" for key in primary_keys)
        query = f"""
SELECT TOP (?)
    CT.SYS_CHANGE_VERSION AS __dpone__version,
    CT.SYS_CHANGE_OPERATION AS __dpone__operation,
    {select_sql}
FROM CHANGETABLE(CHANGES {self._qualified_name()}, ?) AS CT
LEFT JOIN {self._qualified_name()} AS T ON {join_sql}
ORDER BY CT.SYS_CHANGE_VERSION;
"""
        rows = self.connector.get_records(query, (max_changes, start_version), as_dict=True)
        changes: list[CDCChange] = []
        for row in rows:
            operation = self._operation_map.get(str(row.get("__dpone__operation")))
            if operation is None:
                continue
            version = str(row.get("__dpone__version"))
            data = {column: row.get(column) for column in columns}
            changes.append(
                CDCChange(
                    operation=operation,
                    data=data,
                    position=version,
                    source_schema=self.config.source_schema,
                    source_table=self.config.source_table,
                    metadata={"backend": "mssql_change_tracking"},
                )
            )
        offset = CDCOffset(
            backend=CDCBackend.MSSQL_CHANGE_TRACKING,
            token=str(high_watermark),
            snapshot_complete=True,
        )
        return CDCBatch(changes=tuple(changes), next_offset=offset, high_watermark=str(high_watermark))

    def _primary_keys(self) -> tuple[str, ...]:
        if self.config.primary_keys:
            return self.config.primary_keys
        rows = self.connector.get_records(
            """
SELECT c.name
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.is_primary_key = 1 AND i.object_id = OBJECT_ID(?)
ORDER BY ic.key_ordinal
""",
            (self._qualified_name_literal(),),
            as_dict=True,
        )
        keys = tuple(
            str(row.get("name") or row.get("COLUMN_NAME")) for row in rows if row.get("name") or row.get("COLUMN_NAME")
        )
        if not keys:
            raise ValueError(f"SQL Server Change Tracking requires a primary key on {self._qualified_name_literal()}")
        return keys

    def _load_user_columns(self) -> list[str]:
        rows = self.connector.get_records(
            f"""
SELECT COLUMN_NAME
FROM {self._object_name().information_schema_table("COLUMNS")}
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION
""",
            (self._object_name().schema, self._object_name().table),
            as_dict=True,
        )
        columns = [
            str(row.get("COLUMN_NAME") or row.get("name")) for row in rows if row.get("COLUMN_NAME") or row.get("name")
        ]
        if columns:
            return columns
        return list(self._primary_keys())

    def _select_columns_sql(self, columns: list[str], primary_keys: tuple[str, ...]) -> str:
        expressions: list[str] = []
        pk_set = set(primary_keys)
        for column in columns:
            quoted = self._quote(column)
            if column in pk_set:
                expressions.append(f"COALESCE(T.{quoted}, CT.{quoted}) AS {quoted}")
            else:
                expressions.append(f"T.{quoted} AS {quoted}")
        return ",\n    ".join(expressions)

    def _quote(self, name: str) -> str:
        return self.connector.quote_identifier(name)

    def _qualified_name(self) -> str:
        name = self._object_name()
        try:
            return self.connector.qualified_name(name.schema, name.table, database=name.database)
        except TypeError:
            return self.connector.qualified_name(name.schema_label, name.table)

    def _qualified_name_literal(self) -> str:
        return self._object_name().quoted()

    def _object_name(self) -> MSSQLObjectName:
        return MSSQLObjectName.from_parts(
            schema=self.config.source_schema,
            table=self.config.source_table,
            database=self.config.source_database,
        )
