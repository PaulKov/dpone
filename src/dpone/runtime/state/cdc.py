"""CDC offset state storages for SQL-backed runtime checkpoints."""

from __future__ import annotations

from typing import Any

from dpone.readiness.cdc import CDCBackend, CDCOffset


class MSSQLCDCOffsetStorage:
    """Persist durable CDC offsets in SQL Server.

    The primary key includes ``pipeline_name`` and source table identity so the
    same physical source table can be consumed by multiple independent targets
    without offset collisions.
    """

    def __init__(self, connector, schema: str = "etl_state", table: str = "etl_cdc_offset") -> None:
        self.connector = connector
        self.schema = schema
        self.table = table
        self._table_created = False

    @property
    def fq_table(self) -> str:
        return self.connector.qualified_name(self.schema, self.table)

    def create_state_table(self) -> None:
        if self._table_created:
            return
        self.connector.execute_query(
            f"IF SCHEMA_ID(?) IS NULL EXEC('CREATE SCHEMA {self.connector.quote_identifier(self.schema)}')",
            (self.schema,),
        )
        self.connector.execute_query(
            f"""
            IF OBJECT_ID(N'{self.schema}.{self.table}', N'U') IS NULL
            CREATE TABLE {self.fq_table} (
                pipeline_name nvarchar(512) NOT NULL,
                source_schema nvarchar(256) NOT NULL,
                source_table nvarchar(256) NOT NULL,
                backend nvarchar(64) NOT NULL,
                token nvarchar(256) NOT NULL,
                snapshot_complete bit NOT NULL,
                __dpone__loaded_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                __dpone__updated_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                CONSTRAINT pk_{self.table}_pipeline_source PRIMARY KEY (pipeline_name, source_schema, source_table, backend)
            )
            """
        )
        self._table_created = True

    def save_offset(self, pipeline_name: str, source_schema: str, source_table: str, offset: CDCOffset) -> None:
        self.create_state_table()
        self.connector.execute_query(
            f"""
            MERGE {self.fq_table} AS target
            USING (SELECT ? AS pipeline_name, ? AS source_schema, ? AS source_table, ? AS backend) AS source
            ON target.pipeline_name = source.pipeline_name
                AND target.source_schema = source.source_schema
                AND target.source_table = source.source_table
                AND target.backend = source.backend
            WHEN MATCHED THEN UPDATE SET
                token = ?, snapshot_complete = ?, __dpone__updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT (
                pipeline_name, source_schema, source_table, backend, token, snapshot_complete, __dpone__loaded_at, __dpone__updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME());
            """,
            (
                pipeline_name,
                source_schema,
                source_table,
                offset.backend.value,
                offset.token,
                int(offset.snapshot_complete),
                pipeline_name,
                source_schema,
                source_table,
                offset.backend.value,
                offset.token,
                int(offset.snapshot_complete),
            ),
        )

    def load_offset(
        self,
        pipeline_name: str,
        source_schema: str,
        source_table: str,
        backend: CDCBackend,
    ) -> CDCOffset | None:
        self.create_state_table()
        rows = self.connector.get_records(
            f"""
            SELECT TOP (1) backend, token, snapshot_complete
            FROM {self.fq_table}
            WHERE pipeline_name = ? AND source_schema = ? AND source_table = ? AND backend = ?
            """,
            (pipeline_name, source_schema, source_table, backend.value),
            as_dict=True,
        )
        return self._row_to_offset(rows[0]) if rows else None

    def delete_offset(self, pipeline_name: str, source_schema: str, source_table: str, backend: CDCBackend) -> None:
        self.create_state_table()
        self.connector.execute_query(
            f"""
            DELETE FROM {self.fq_table}
            WHERE pipeline_name = ? AND source_schema = ? AND source_table = ? AND backend = ?
            """,
            (pipeline_name, source_schema, source_table, backend.value),
        )

    def _row_to_offset(self, row: dict[str, Any]) -> CDCOffset:
        return CDCOffset(
            backend=CDCBackend(str(row["backend"])),
            token=str(row["token"]),
            snapshot_complete=bool(row["snapshot_complete"]),
        )


class PostgresCDCOffsetStorage:
    """Persist durable CDC offsets in PostgreSQL."""

    def __init__(self, connector, schema: str = "etl_state", table: str = "etl_cdc_offset") -> None:
        self.connector = connector
        self.schema = schema
        self.table = table
        self._table_created = False

    @property
    def fq_table(self) -> Any:
        from psycopg import sql

        return sql.SQL("{}.{}").format(sql.Identifier(self.schema), sql.Identifier(self.table))

    def create_state_table(self) -> None:
        from psycopg import sql

        if self._table_created:
            return
        self.connector.execute_query(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema)))
        self.connector.execute_query(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    pipeline_name text NOT NULL,
                    source_schema text NOT NULL,
                    source_table text NOT NULL,
                    backend text NOT NULL,
                    token text NOT NULL,
                    snapshot_complete boolean NOT NULL,
                    __dpone__loaded_at timestamp NOT NULL DEFAULT timezone('utc', now()),
                    __dpone__updated_at timestamp NOT NULL DEFAULT timezone('utc', now()),
                    CONSTRAINT {} PRIMARY KEY (pipeline_name, source_schema, source_table, backend)
                )
                """
            ).format(self.fq_table, sql.Identifier(f"pk_{self.table}_pipeline_source"))
        )
        self._table_created = True

    def save_offset(self, pipeline_name: str, source_schema: str, source_table: str, offset: CDCOffset) -> None:
        from psycopg import sql

        self.create_state_table()
        self.connector.execute_query(
            sql.SQL(
                """
                INSERT INTO {} (
                    pipeline_name, source_schema, source_table, backend, token, snapshot_complete,
                    __dpone__loaded_at, __dpone__updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, timezone('utc', now()), timezone('utc', now()))
                ON CONFLICT (pipeline_name, source_schema, source_table, backend) DO UPDATE SET
                    token = EXCLUDED.token,
                    snapshot_complete = EXCLUDED.snapshot_complete,
                    __dpone__updated_at = timezone('utc', now())
                """
            ).format(self.fq_table),
            (
                pipeline_name,
                source_schema,
                source_table,
                offset.backend.value,
                offset.token,
                offset.snapshot_complete,
            ),
        )

    def load_offset(
        self,
        pipeline_name: str,
        source_schema: str,
        source_table: str,
        backend: CDCBackend,
    ) -> CDCOffset | None:
        from psycopg import sql

        self.create_state_table()
        rows = self.connector.get_records(
            sql.SQL(
                """
                SELECT backend, token, snapshot_complete
                FROM {}
                WHERE pipeline_name = %s AND source_schema = %s AND source_table = %s AND backend = %s
                LIMIT 1
                """
            ).format(self.fq_table),
            (pipeline_name, source_schema, source_table, backend.value),
            as_dict=True,
        )
        return self._row_to_offset(rows[0]) if rows else None

    def delete_offset(self, pipeline_name: str, source_schema: str, source_table: str, backend: CDCBackend) -> None:
        from psycopg import sql

        self.create_state_table()
        self.connector.execute_query(
            sql.SQL(
                """
                DELETE FROM {}
                WHERE pipeline_name = %s AND source_schema = %s AND source_table = %s AND backend = %s
                """
            ).format(self.fq_table),
            (pipeline_name, source_schema, source_table, backend.value),
        )

    def _row_to_offset(self, row: dict[str, Any]) -> CDCOffset:
        return CDCOffset(
            backend=CDCBackend(str(row["backend"])),
            token=str(row["token"]),
            snapshot_complete=bool(row["snapshot_complete"]),
        )
