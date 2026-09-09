"""Thin read-only SQL Server catalog observer for semantic-refresh proofs."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from dpone.contracts.dbt_semantic_refresh_catalog_proof import (
    RelationIdentity,
    SemanticRefreshCatalogAuthority,
    SemanticRefreshCatalogObservation,
    SqlServerDependencyLimits,
    build_sqlserver_catalog_observation,
)

_METADATA_SQL = """
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT DB_NAME(), HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION');
"""
_CATALOG_SQL = """
SET NOCOUNT ON;
DECLARE @maximum_rows int = ?;
DECLARE @maximum_nodes int = ?;
DECLARE @maximum_depth int = ?;
DECLARE @maximum_definition_bytes bigint = ?;
DECLARE @root_relations nvarchar(max) = ?;

DECLARE @objects TABLE (
    object_id int NOT NULL PRIMARY KEY,
    dependency_depth int NOT NULL
);
DECLARE @frontier TABLE (object_id int NOT NULL PRIMARY KEY);
DECLARE @next_frontier TABLE (object_id int NOT NULL PRIMARY KEY);
DECLARE @dependencies TABLE (
    referencing_id int NOT NULL,
    referenced_id int NULL,
    referenced_server_name nvarchar(128) NULL,
    referenced_database_name nvarchar(128) NULL,
    is_caller_dependent bit NOT NULL
);
DECLARE @observed_dependencies TABLE (
    referencing_id int NOT NULL,
    referenced_id int NULL,
    referenced_server_name nvarchar(128) NULL,
    referenced_database_name nvarchar(128) NULL,
    is_caller_dependent bit NOT NULL
);

;WITH requested_relations AS (
    SELECT DISTINCT database_name, schema_name, object_name
    FROM OPENJSON(@root_relations)
    WITH (
        database_name nvarchar(128) '$.database',
        schema_name nvarchar(128) '$.schema',
        object_name nvarchar(128) '$.name'
    )
)
INSERT INTO @frontier (object_id)
SELECT TOP (@maximum_nodes + 1) o.object_id
FROM requested_relations AS requested
JOIN sys.schemas AS s
  ON s.name COLLATE Latin1_General_100_CI_AS = requested.schema_name COLLATE Latin1_General_100_CI_AS
JOIN sys.objects AS o
  ON o.schema_id = s.schema_id
 AND o.name COLLATE Latin1_General_100_CI_AS = requested.object_name COLLATE Latin1_General_100_CI_AS
WHERE requested.database_name COLLATE Latin1_General_100_CI_AS
    = DB_NAME() COLLATE Latin1_General_100_CI_AS
  AND o.is_ms_shipped = 0
ORDER BY o.object_id;

IF (SELECT COUNT(*) FROM @frontier) > @maximum_nodes
    THROW 51000, 'DPONE_DBT_V2_CATALOG_NODE_BUDGET_EXCEEDED', 1;

INSERT INTO @objects (object_id, dependency_depth)
SELECT object_id, 0 FROM @frontier;

DECLARE @depth int = 0;
WHILE EXISTS (SELECT 1 FROM @frontier)
BEGIN
    DECLARE @remaining_edges int = @maximum_rows - @maximum_nodes
        - (SELECT COUNT(*) FROM @dependencies) - 1;
    DELETE FROM @observed_dependencies;
    INSERT INTO @observed_dependencies (
        referencing_id,
        referenced_id,
        referenced_server_name,
        referenced_database_name,
        is_caller_dependent
    )
    SELECT TOP (@remaining_edges + 1)
        dependency.referencing_id,
        dependency.referenced_id,
        dependency.referenced_server_name,
        dependency.referenced_database_name,
        COALESCE(dependency.is_caller_dependent, 0)
    FROM sys.sql_expression_dependencies AS dependency
    JOIN @frontier AS frontier ON frontier.object_id = dependency.referencing_id
    ORDER BY
        dependency.referencing_id,
        dependency.referenced_id,
        dependency.referenced_server_name,
        dependency.referenced_database_name;

    IF (SELECT COUNT(*) FROM @observed_dependencies) > @remaining_edges
        THROW 51000, 'DPONE_DBT_V2_CATALOG_EDGE_BUDGET_EXCEEDED', 1;

    INSERT INTO @dependencies
    SELECT * FROM @observed_dependencies;

    IF @depth >= @maximum_depth BREAK;

    DECLARE @remaining_nodes int = @maximum_nodes - (SELECT COUNT(*) FROM @objects);
    DELETE FROM @next_frontier;
    INSERT INTO @next_frontier (object_id)
    SELECT DISTINCT TOP (@remaining_nodes + 1) dependency.referenced_id
    FROM @observed_dependencies AS dependency
    WHERE dependency.referenced_id IS NOT NULL
      AND dependency.referenced_server_name IS NULL
      AND (
          dependency.referenced_database_name IS NULL
          OR dependency.referenced_database_name COLLATE Latin1_General_100_CI_AS
              = DB_NAME() COLLATE Latin1_General_100_CI_AS
      )
      AND NOT EXISTS (
          SELECT 1 FROM @objects AS observed
          WHERE observed.object_id = dependency.referenced_id
      )
    ORDER BY dependency.referenced_id;

    IF (SELECT COUNT(*) FROM @next_frontier) > @remaining_nodes
        THROW 51000, 'DPONE_DBT_V2_CATALOG_NODE_BUDGET_EXCEEDED', 1;

    SET @depth = @depth + 1;
    INSERT INTO @objects (object_id, dependency_depth)
    SELECT object_id, @depth FROM @next_frontier;
    DELETE FROM @frontier;
    INSERT INTO @frontier SELECT object_id FROM @next_frontier;
END;

;WITH catalog_objects AS (
    SELECT
        o.object_id,
        s.name AS schema_name,
        o.name AS object_name,
        o.type_desc,
        m.definition,
        COALESCE(m.is_schema_bound, 0) AS is_schema_bound,
        CASE WHEN m.object_id IS NOT NULL AND m.definition IS NULL THEN 1 ELSE 0 END AS is_encrypted,
        CASE WHEN EXISTS (
            SELECT 1 FROM sys.columns AS c
            WHERE c.object_id = o.object_id AND c.is_computed = 1
        ) THEN 1 ELSE 0 END AS has_computed_columns,
        COALESCE(
            DATALENGTH(CONVERT(varchar(max), m.definition COLLATE Latin1_General_100_BIN2_UTF8)),
            0
        ) AS definition_bytes
    FROM @objects AS selected
    JOIN sys.objects AS o ON o.object_id = selected.object_id
    JOIN sys.schemas AS s ON s.schema_id = o.schema_id
    LEFT JOIN sys.sql_modules AS m ON m.object_id = o.object_id
),
bounded_objects AS (
    SELECT
        catalog_objects.*,
        SUM(definition_bytes) OVER (
            ORDER BY object_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_definition_bytes
    FROM catalog_objects
),
catalog_rows AS (
    SELECT
        bounded_objects.*,
        dependency.referencing_id,
        dependency.referenced_id,
        dependency.referenced_server_name,
        dependency.referenced_database_name,
        COALESCE(dependency.is_caller_dependent, 0) AS is_caller_dependent,
        ROW_NUMBER() OVER (
            PARTITION BY bounded_objects.object_id
            ORDER BY
                dependency.referenced_id,
                dependency.referenced_server_name,
                dependency.referenced_database_name
        ) AS dependency_ordinal
    FROM bounded_objects
    LEFT JOIN @dependencies AS dependency
      ON dependency.referencing_id = bounded_objects.object_id
)
SELECT
    TOP (@maximum_rows)
    object_id,
    schema_name,
    object_name,
    type_desc,
    CASE
        WHEN dependency_ordinal = 1 AND cumulative_definition_bytes <= @maximum_definition_bytes
        THEN definition
        ELSE NULL
    END,
    is_schema_bound,
    is_encrypted,
    has_computed_columns,
    referencing_id,
    referenced_id,
    referenced_server_name,
    referenced_database_name,
    is_caller_dependent,
    CASE
        WHEN dependency_ordinal = 1 AND cumulative_definition_bytes > @maximum_definition_bytes
        THEN 1
        ELSE 0
    END AS definition_budget_exceeded
FROM catalog_rows
ORDER BY object_id, dependency_ordinal;
"""


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> Sequence[Any] | None: ...

    def fetchall(self) -> Sequence[Sequence[Any]]: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SemanticRefreshMssqlCatalogObservationError(RuntimeError):
    """Raised when exact live catalog metadata cannot be observed."""


class MssqlSemanticRefreshCatalogObserver:
    """Read sys catalogs in one transaction and resolve exact relation IDs."""

    def __init__(self, connection_factory: Callable[[], _Connection]) -> None:
        self._connection_factory = connection_factory

    def observe(
        self,
        *,
        authority: SemanticRefreshCatalogAuthority,
        read_relations: tuple[RelationIdentity, ...],
        target_relation: RelationIdentity,
        limits: SqlServerDependencyLimits,
    ) -> SemanticRefreshCatalogObservation:
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            metadata = cursor.execute(_METADATA_SQL).fetchone()
            roots = tuple(sorted(set((*read_relations, target_relation))))
            maximum_rows = limits.max_nodes + limits.max_edges + 1
            rows = cursor.execute(
                _CATALOG_SQL,
                maximum_rows,
                limits.max_nodes,
                limits.max_depth,
                limits.max_definition_bytes,
                json.dumps(
                    [{"database": database, "name": name, "schema": schema} for database, schema, name in roots],
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ).fetchall()
            _validate_observation_budget(rows, limits, maximum_rows=maximum_rows)
            connection.commit()
            return build_sqlserver_catalog_observation(
                authority=authority,
                read_relations=read_relations,
                target_relation=target_relation,
                metadata_row=metadata,
                catalog_rows=rows,
            )
        except Exception as exc:
            if connection is not None:
                try:
                    connection.rollback()
                except Exception:
                    pass
            if isinstance(exc, SemanticRefreshMssqlCatalogObservationError):
                raise
            raise SemanticRefreshMssqlCatalogObservationError("SQL Server catalog observation is unavailable") from exc
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()


def _validate_observation_budget(
    rows: Sequence[Sequence[Any]],
    limits: SqlServerDependencyLimits,
    *,
    maximum_rows: int,
) -> None:
    if len(rows) >= maximum_rows or any(len(row) != 14 for row in rows):
        raise SemanticRefreshMssqlCatalogObservationError("SQL Server catalog row budget was exceeded")
    object_ids = {row[0] for row in rows}
    edge_count = sum(row[8] is not None for row in rows)
    definitions = {row[0]: str(row[4]) for row in rows if row[4] is not None}
    if (
        len(object_ids) > limits.max_nodes
        or edge_count > limits.max_edges
        or sum(len(value.encode("utf-8")) for value in definitions.values()) > limits.max_definition_bytes
        or any(row[13] in {1, True} for row in rows)
    ):
        raise SemanticRefreshMssqlCatalogObservationError("SQL Server catalog proof budget was exceeded")


__all__ = [
    "MssqlSemanticRefreshCatalogObserver",
    "SemanticRefreshMssqlCatalogObservationError",
]
