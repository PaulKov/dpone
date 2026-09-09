"""Kafka offset state storage adapters."""

from __future__ import annotations

import json
from typing import Any

from dpone.runtime.kafka.offsets import KafkaOffsetState


class SQLKafkaOffsetStateStorage:
    """Small SQL-backed offset storage used by Postgres and MSSQL connectors."""

    def __init__(
        self, connector: Any, schema: str = "etl_state", table: str = "etl_kafka_offsets", dialect: str = "postgres"
    ):
        self.connector = connector
        self.schema = schema
        self.table = table
        self.dialect = dialect

    def load_state(self, topic: str, group_id: str) -> KafkaOffsetState | None:
        self.ensure_table()
        placeholder = "?" if self.dialect == "mssql" else "%s"
        rows = self.connector.get_records(
            f"SELECT state_json FROM {self._qualified()} WHERE topic = {placeholder} AND group_id = {placeholder}",
            (topic, group_id),
        )
        if not rows:
            return None
        payload = rows[0][0]
        if isinstance(payload, str):
            return KafkaOffsetState.from_dict(json.loads(payload))
        return KafkaOffsetState.from_dict(payload)

    def save_state(self, state: KafkaOffsetState) -> None:
        self.ensure_table()
        payload = json.dumps(state.to_dict(), sort_keys=True)
        if self.dialect == "mssql":
            self.connector.execute_query(
                f"DELETE FROM {self._qualified()} WHERE topic = ? AND group_id = ?",
                (state.topic, state.group_id),
            )
            self.connector.execute_query(
                f"INSERT INTO {self._qualified()} (topic, group_id, state_json) VALUES (?, ?, ?)",
                (state.topic, state.group_id, payload),
            )
            return
        self.connector.execute_query(
            f"""
            INSERT INTO {self._qualified()} (topic, group_id, state_json)
            VALUES (%s, %s, %s)
            ON CONFLICT (topic, group_id) DO UPDATE SET state_json = EXCLUDED.state_json
            """,
            (state.topic, state.group_id, payload),
        )

    def ensure_table(self) -> None:
        if self.dialect == "mssql":
            self.connector.execute_query(f"IF SCHEMA_ID('{self.schema}') IS NULL EXEC('CREATE SCHEMA [{self.schema}]')")
            self.connector.execute_query(
                f"""
                IF OBJECT_ID('{self.schema}.{self.table}', 'U') IS NULL
                CREATE TABLE {self._qualified()} (
                    topic NVARCHAR(512) NOT NULL,
                    group_id NVARCHAR(512) NOT NULL,
                    state_json NVARCHAR(MAX) NOT NULL,
                    CONSTRAINT PK_{self.table} PRIMARY KEY (topic, group_id)
                )
                """
            )
            return
        self.connector.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"')
        self.connector.execute_query(
            f"""
            CREATE TABLE IF NOT EXISTS {self._qualified()} (
                topic TEXT NOT NULL,
                group_id TEXT NOT NULL,
                state_json JSONB NOT NULL,
                PRIMARY KEY (topic, group_id)
            )
            """
        )

    def _qualified(self) -> str:
        if self.dialect == "mssql":
            return f"[{self.schema}].[{self.table}]"
        return f'"{self.schema}"."{self.table}"'


class BigQueryKafkaOffsetStateStorage:
    """BigQuery-backed Kafka offset storage."""

    def __init__(self, connector: Any, dataset: str = "etl_state", table: str = "etl_kafka_offsets"):
        self.connector = connector
        self.dataset = dataset
        self.table = table

    def load_state(self, topic: str, group_id: str) -> KafkaOffsetState | None:
        table_id = self._table_id()
        rows = self.connector.get_records(
            f"SELECT state_json FROM `{table_id}` WHERE topic = @topic AND group_id = @group_id LIMIT 1",
            {"topic": topic, "group_id": group_id},
        )
        if not rows:
            return None
        return KafkaOffsetState.from_dict(json.loads(rows[0][0]))

    def save_state(self, state: KafkaOffsetState) -> None:
        table_id = self._table_id()
        payload = json.dumps(state.to_dict(), sort_keys=True)
        self.connector.execute_query(
            f"""
            MERGE `{table_id}` T
            USING (SELECT @topic AS topic, @group_id AS group_id, @state_json AS state_json) S
            ON T.topic = S.topic AND T.group_id = S.group_id
            WHEN MATCHED THEN UPDATE SET state_json = S.state_json
            WHEN NOT MATCHED THEN INSERT (topic, group_id, state_json) VALUES (S.topic, S.group_id, S.state_json)
            """,
            {"topic": state.topic, "group_id": state.group_id, "state_json": payload},
        )

    def _table_id(self) -> str:
        project_id = getattr(self.connector, "project_id", None)
        return f"{project_id}.{self.dataset}.{self.table}" if project_id else f"{self.dataset}.{self.table}"
