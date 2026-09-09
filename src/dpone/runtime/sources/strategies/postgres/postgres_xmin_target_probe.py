"""Target-shape probes used before PostgreSQL XMin extraction."""

from __future__ import annotations

from typing import Any

from psycopg import sql


class PostgresXminTargetProbe:
    """Probe target existence and baseline safety without owning extraction."""

    def __init__(self, *, connector: Any, source_connector: Any, logger: Any) -> None:
        self._connector = connector
        self._source_connector = source_connector
        self._logger = logger

    def exists(self, load_config: Any) -> bool | None:
        """Return target existence, or ``None`` when it cannot be proven."""

        connector = self._connector
        try:
            if hasattr(connector, "table_exists"):
                database = getattr(load_config, "target_database", None)
                try:
                    exists = bool(
                        connector.table_exists(
                            load_config.target_schema,
                            load_config.target_table,
                            database=database,
                        )
                    )
                except TypeError:
                    schema = f"{database}.{load_config.target_schema}" if database else load_config.target_schema
                    exists = bool(connector.table_exists(schema, load_config.target_table))
                self._log_existence(load_config, exists)
                return exists

            if connector.__class__.__name__ == "PostgresConnector":
                check_sql = sql.SQL(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    )
                    """
                )
                result = connector.get_records(
                    check_sql,
                    [load_config.target_schema, load_config.target_table],
                )
            else:
                fq_table = f"{connector.project_id}.{load_config.target_schema}.{load_config.target_table}"
                client = connector.connection
                try:
                    client.get_table(fq_table)
                    result = [[True]]
                except Exception:
                    result = [[False]]

            exists = bool(result and result[0][0])
            self._log_existence(load_config, exists)
            return exists
        except Exception as exc:
            self._logger.log_xmin_state_info(
                "Не удалось проверить существование целевой таблицы",
                {
                    "Target Schema": load_config.target_schema,
                    "Target Table": load_config.target_table,
                    "Error": str(exc),
                },
            )
            return None

    def is_empty(self, load_config: Any) -> bool | None:
        """Return target emptiness, or ``None`` when it cannot be proven."""

        database = getattr(load_config, "target_database", None)
        try:
            try:
                target = self._connector.qualified_name(
                    load_config.target_schema,
                    load_config.target_table,
                    database=database,
                )
            except TypeError:
                schema = f"{database}.{load_config.target_schema}" if database else load_config.target_schema
                target = self._connector.qualified_name(schema, load_config.target_table)
            rows = self._connector.get_records(f"SELECT TOP (1) 1 FROM {target}")
            return not bool(rows)
        except Exception as exc:
            self._logger.log_xmin_state_info(
                "Не удалось проверить пустоту целевой таблицы",
                {
                    "Target Schema": load_config.target_schema,
                    "Target Table": load_config.target_table,
                    "Error": str(exc),
                },
            )
            return None

    def _log_existence(self, load_config: Any, exists: bool) -> None:
        self._logger.log_xmin_state_info(
            "Проверка существования целевой таблицы",
            {
                "Target Schema": load_config.target_schema,
                "Target Table": load_config.target_table,
                "Exists": exists,
                "Connector": "target" if self._connector is not self._source_connector else "source",
            },
        )


__all__ = ["PostgresXminTargetProbe"]
