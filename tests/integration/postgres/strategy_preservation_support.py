"""Isolated PostgreSQL fixtures and raw observations for sink strategy regressions.

Only synthetic fixture statements and values enter the operation journal. Connection
settings never enter evidence. The observer uses a separate autocommit connection,
so snapshots describe committed state rather than the sink's uncommitted writes.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InternalQueryArtifact
from dpone.runtime.connectors.postgres import PostgresConnector
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.postgres import PostgresSink

BUSINESS_SCHEMA = [("id", "integer"), ("name", "text"), ("event_day", "date")]


class RecordingPostgresConnector(PostgresConnector):
    """Observe production connector operations without replacing database behavior."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.events: list[dict[str, Any]] = []

    def execute_query(self, query: Any, params: Any = None) -> int:
        rendered = query.as_string(self.connection) if isinstance(query, sql.Composable) else str(query)
        event = {"operation": "execute", "sql": rendered, "params": params, "started_ns": time.monotonic_ns()}
        self.events.append(event)
        try:
            result = super().execute_query(query, params)
        except BaseException as exc:
            event.update(error_type=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None))
            raise
        event["rowcount"] = result
        return result

    def begin(self) -> None:
        super().begin()
        self.events.append({"operation": "begin"})

    def commit_transaction(self) -> None:
        super().commit_transaction()
        self.events.append({"operation": "commit"})

    def rollback(self) -> None:
        super().rollback()
        self.events.append({"operation": "rollback"})


class PreservationLab:
    """Own two fresh schemas, the sink session and an independent catalog observer."""

    def __init__(self, settings: Any) -> None:
        kwargs = {key: getattr(settings, key) for key in ("host", "port", "database", "user", "password")}
        self.connector = RecordingPostgresConnector(**kwargs, application_name="dpone-strategy-preservation")
        kwargs["dbname"] = kwargs.pop("database")
        self.observer = psycopg.connect(**kwargs, autocommit=True, row_factory=dict_row)
        self.schema = f"dp_preserve_{uuid.uuid4().hex[:16]}"
        self.stage_schema = self.schema + "_stage"
        self.observations: list[dict[str, Any]] = []
        self.loads: list[dict[str, Any]] = []
        self.sink = PostgresSink(self.connector, None)
        for schema in (self.schema, self.stage_schema):
            self.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        self.execute(f"CREATE TABLE {self.schema}.source (id integer, name text, event_day date)")

    def execute(self, query: Any, params: Any = None) -> None:
        self.observer.execute(query, params)

    def records(self, query: Any, params: Any = None) -> list[dict[str, Any]]:
        return self.observer.execute(query, params).fetchall()

    def config(self, *, technical: str = "forbidden", cross_schema: bool = False, **changes: Any) -> LoadConfig:
        values = dict(
            source_conn_id="synthetic-source",
            target_conn_id="synthetic-target",
            source_schema=self.schema,
            source_table="source",
            target_schema=self.schema,
            target_table="target",
            staging_schema=self.stage_schema if cross_schema else self.schema,
            load_strategy=LoadStrategy.FULL_REFRESH,
            log_sample_rows=0,
            options={"technical_columns": technical},
        )
        values.update(changes)
        return LoadConfig(**values)

    def source(self, rows: list[tuple[Any, ...]]) -> None:
        self.execute(f"DELETE FROM {self.schema}.source")
        for row in rows:
            self.execute(f"INSERT INTO {self.schema}.source VALUES (%s, %s, %s)", row)

    def constrained_target(self, *, technical: str = "forbidden", view: bool = True) -> None:
        self.execute(
            f"CREATE TABLE {self.schema}.target (id integer PRIMARY KEY, "
            "name text NOT NULL DEFAULT 'fallback' CHECK (name <> 'invalid'), event_day date)"
        )
        if technical == "required":
            self.execute(
                f"ALTER TABLE {self.schema}.target ADD __dpone__loaded_at timestamptz, "
                "ADD __dpone__deleted_at timestamptz"
            )
        self.execute(f"CREATE INDEX target_name_idx ON {self.schema}.target(name)")
        self.execute(f"GRANT SELECT ON {self.schema}.target TO PUBLIC")
        self.execute(f"CREATE TABLE {self.schema}.audit (id integer)")
        self.execute(
            f"CREATE FUNCTION {self.schema}.audit_insert() RETURNS trigger LANGUAGE plpgsql AS $$ "
            f"BEGIN INSERT INTO {self.schema}.audit VALUES (NEW.id); RETURN NEW; END $$"
        )
        self.execute(
            f"CREATE TRIGGER audit_insert AFTER INSERT ON {self.schema}.target "
            f"FOR EACH ROW EXECUTE FUNCTION {self.schema}.audit_insert()"
        )
        if view:
            self.execute(
                f"CREATE VIEW {self.schema}.target_view AS SELECT id, name, event_day FROM {self.schema}.target"
            )
        self.execute(f"INSERT INTO {self.schema}.target(id,name,event_day) VALUES (90,'previous','2026-01-01')")

    def load(self, config: LoadConfig | None = None, *, query: str | None = None, params: Any = None):
        artifact = InternalQueryArtifact(query or f"SELECT id,name,event_day FROM {self.schema}.source", params=params)
        start = len(self.connector.events)
        record: dict[str, Any] = {"event_start": start}
        self.loads.append(record)
        try:
            result = self.sink.load(config or self.config(), LoadPayload(artifact=artifact, schema=BUSINESS_SCHEMA))
        except BaseException as exc:
            record.update(error_type=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None))
            raise
        else:
            record["result"] = asdict(result)
            return result
        finally:
            record["event_end"] = len(self.connector.events)

    def rows(self, table: str = "target") -> list[tuple[Any, ...]]:
        query = sql.SQL("SELECT id,name,event_day::text FROM {}.{} ORDER BY id,name").format(
            sql.Identifier(self.schema), sql.Identifier(table)
        )
        return [tuple(row.values()) for row in self.records(query)]

    def snapshot(self, label: str) -> dict[str, Any]:
        relation = f"{self.schema}.target"
        snapshot = {"rows": self.rows(), "metadata": {}}
        queries = {
            "relation": "SELECT oid, relkind, relacl::text FROM pg_class WHERE oid=%s::regclass",
            "columns": "SELECT attname, attnotnull, format_type(atttypid,atttypmod) AS type, "
            "pg_get_expr(d.adbin,d.adrelid) AS default_expr FROM pg_attribute a LEFT JOIN pg_attrdef d "
            "ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=%s::regclass "
            "AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
            "constraints": "SELECT oid,conname,contype,pg_get_constraintdef(oid) AS definition "
            "FROM pg_constraint WHERE conrelid=%s::regclass ORDER BY conname",
            "indexes": "SELECT indexrelid,pg_get_indexdef(indexrelid) AS definition "
            "FROM pg_index WHERE indrelid=%s::regclass ORDER BY indexrelid",
            "triggers": "SELECT oid,tgname,pg_get_triggerdef(oid) AS definition FROM pg_trigger "
            "WHERE tgrelid=%s::regclass AND NOT tgisinternal ORDER BY tgname",
            "view_dependencies": "SELECT DISTINCT r.ev_class AS view_oid,d.refobjid AS target_oid "
            "FROM pg_depend d JOIN pg_rewrite r ON r.oid=d.objid JOIN pg_class c ON c.oid=r.ev_class "
            "WHERE d.refobjid=%s::regclass AND c.relkind='v' ORDER BY r.ev_class",
        }
        snapshot["metadata"] = {name: self.records(query, (relation,)) for name, query in queries.items()}
        self.observations.append({"label": label, **snapshot})
        return snapshot

    def last_events(self) -> list[dict[str, Any]]:
        record = self.loads[-1]
        return self.connector.events[record["event_start"] : record["event_end"]]

    def assert_transaction(self, *, committed: bool) -> None:
        operations = [event["operation"] for event in self.last_events() if event["operation"] != "execute"]
        assert operations == ["begin", "commit" if committed else "rollback"]

    def assert_no_staging(self) -> None:
        rows = self.records(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname IN (%s,%s) AND c.relname LIKE 'stg_%%'",
            (self.schema, self.stage_schema),
        )
        assert rows == []

    def write_evidence(self, test_name: str) -> None:
        directory = os.getenv("DPONE_PG_EVIDENCE_DIR")
        if not directory:
            return
        identity = {
            "git_head": os.getenv("DPONE_PG_SOURCE_COMMIT", "UNVERIFIED"),
            "source_tree_sha256": os.getenv("DPONE_PG_SOURCE_SHA256", "UNVERIFIED"),
            "postgres_version": self.records("SHOW server_version"),
            "schema": self.schema,
            "staging_schema": self.stage_schema,
        }
        path = Path(directory) / f"{test_name}-{self.schema}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "identity": identity,
                    "observations": self.observations,
                    "loads": self.loads,
                    "events": self.connector.events,
                },
                indent=2,
                default=str,
            )
            + "\n",
            encoding="utf-8",
        )

    def close(self) -> None:
        self.connector.close()
        try:
            for schema in (self.schema, self.stage_schema):
                relations = self.records(
                    "SELECT c.relname,c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=%s AND c.relkind IN ('v','r','p') ORDER BY "
                    "CASE WHEN c.relkind='v' THEN 0 WHEN c.relname='incoming' THEN 1 "
                    "WHEN c.relkind='p' THEN 2 ELSE 3 END",
                    (schema,),
                )
                for relation in relations:
                    kind = "VIEW" if relation["relkind"] == "v" else "TABLE"
                    self.execute(
                        sql.SQL("DROP {} IF EXISTS {}.{}").format(
                            sql.SQL(kind), sql.Identifier(schema), sql.Identifier(relation["relname"])
                        )
                    )
                if schema == self.schema:
                    self.execute(f"DROP FUNCTION IF EXISTS {schema}.audit_insert()")
                self.execute(sql.SQL("DROP SCHEMA {}").format(sql.Identifier(schema)))
        finally:
            self.observer.close()
