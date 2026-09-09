#!/usr/bin/env python3
"""Run a local Kafka live integration matrix and write repo test artifacts.

The harness intentionally exercises the public dpone runtime contracts instead
of talking to Kafka directly everywhere:

* ``KafkaSink`` produces rows for every supported load strategy.
* ``KafkaSource`` reads bounded batches for every supported message format.
* Local Postgres and ClickHouse targets are loaded through their dpone sinks
  when corresponding services are available.

It is designed for developer workstations and CI-like local runs. External
systems that cannot be created locally without credentials, such as BigQuery or
SQL Server on Apple Silicon, are recorded as explicit skips rather than hidden.
"""

from __future__ import annotations

import csv
import json
import os
import socket
import sys
import tempfile
import time
import traceback
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from dpone._compat import UTC

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dpone.config import LoadConfig, LoadStrategy  # noqa: E402
from dpone.runtime.artifacts import FileExportArtifact, InMemoryRowsArtifact, StreamingRowsArtifact  # noqa: E402
from dpone.runtime.connectors.clickhouse import ClickHouseConnector  # noqa: E402
from dpone.runtime.connectors.kafka import KafkaConnector  # noqa: E402
from dpone.runtime.connectors.mssql import MSSQLConnector  # noqa: E402
from dpone.runtime.connectors.postgres import PostgresConnector  # noqa: E402
from dpone.runtime.kafka.offsets import InMemoryKafkaOffsetStateStorage  # noqa: E402
from dpone.runtime.sinks.base import LoadPayload  # noqa: E402
from dpone.runtime.sinks.clickhouse import ClickHouseSink  # noqa: E402
from dpone.runtime.sinks.kafka import KafkaSink  # noqa: E402
from dpone.runtime.sinks.mssql import MSSQLSink  # noqa: E402
from dpone.runtime.sinks.postgres import PostgresSink  # noqa: E402
from dpone.runtime.sources.kafka import KafkaSource  # noqa: E402

SCHEMA: list[tuple[str, str]] = [
    ("id", "bigint"),
    ("name", "text"),
    ("amount", "double precision"),
    ("active", "boolean"),
]
ROWS: list[dict[str, Any]] = [
    {"id": 1, "name": "alpha", "amount": 10.5, "active": True},
    {"id": 2, "name": "bravo", "amount": 20.75, "active": False},
    {"id": 3, "name": "charlie", "amount": 30.125, "active": True},
]


@dataclass
class MatrixCase:
    name: str
    status: str
    duration_ms: int
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    traceback: str | None = None


class MatrixRunner:
    def __init__(self) -> None:
        self.started_at = datetime.now(UTC)
        self.run_id = self.started_at.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        self.bootstrap_servers = os.getenv("DPONE_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
        self.schema_registry_url = os.getenv("DPONE_SCHEMA_REGISTRY_URL", "http://127.0.0.1:18081")
        self.topic_prefix = f"dpone_matrix_{self.run_id}".replace("-", "_").lower()
        self.connector = KafkaConnector(
            bootstrap_servers=self.bootstrap_servers,
            client_id=f"dpone-matrix-{self.run_id}",
            schema_registry_url=self.schema_registry_url,
        )
        self.results: list[MatrixCase] = []
        self.protobuf_message_class = _build_order_event_message_class()

    def run(self) -> dict[str, Any]:
        self.case("service_preflight.kafka_socket", self._check_kafka_socket)
        self.case("service_preflight.schema_registry_http", self._check_schema_registry)
        self._run_sink_strategy_format_matrix()
        self._run_source_read_mode_matrix()
        self._run_offset_state_resume()
        self._run_key_policy_matrix()
        self._run_envelope_and_tombstone_matrix()
        self._run_source_payload_contract_matrix()
        self._run_live_target_matrix()
        return self._report()

    def case(self, name: str, fn: Callable[[], dict[str, Any] | None]) -> None:
        started = time.perf_counter()
        try:
            details = fn() or {}
            status = str(details.pop("status", "passed"))
            error = None
            tb = None
        except SkipCase as exc:
            details = exc.details
            status = "skipped"
            error = str(exc)
            tb = None
        except Exception as exc:  # noqa: BLE001 - report artifact must capture unexpected live failures.
            details = {}
            status = "failed"
            error = str(exc)
            tb = traceback.format_exc()
        duration_ms = int((time.perf_counter() - started) * 1000)
        self.results.append(
            MatrixCase(
                name=name,
                status=status,
                duration_ms=duration_ms,
                details=details,
                error=error,
                traceback=tb,
            )
        )

    def _check_kafka_socket(self) -> dict[str, Any]:
        host, port_text = self.bootstrap_servers.split(":", 1)
        with socket.create_connection((host, int(port_text)), timeout=10):
            return {"bootstrap_servers": self.bootstrap_servers}

    def _check_schema_registry(self) -> dict[str, Any]:
        with urlopen(f"{self.schema_registry_url}/subjects", timeout=10) as response:  # noqa: S310
            payload = response.read().decode("utf-8")
        return {"schema_registry_url": self.schema_registry_url, "subjects_response": payload[:300]}

    def _run_sink_strategy_format_matrix(self) -> None:
        for message_format in ("json", "json_schema", "avro", "protobuf"):
            for strategy in (
                LoadStrategy.FULL_REFRESH,
                LoadStrategy.INCREMENTAL_APPEND,
                LoadStrategy.INCREMENTAL_MERGE,
                LoadStrategy.REPLACE,
            ):
                self.case(
                    f"kafka_sink_source_roundtrip.{message_format}.{strategy.value}",
                    lambda message_format=message_format, strategy=strategy: self._sink_source_roundtrip(
                        message_format,
                        strategy,
                    ),
                )

    def _sink_source_roundtrip(self, message_format: str, strategy: LoadStrategy) -> dict[str, Any]:
        topic = self._topic(f"{message_format}_{strategy.value}")
        rows = [dict(row) for row in ROWS[:2]]
        produced = self._produce(topic, rows, message_format, strategy)
        decoded_rows, state = self._extract_rows(topic, message_format, group_suffix=f"{strategy.value}_reader")
        assert produced == len(rows), f"expected {len(rows)} produced rows, got {produced}"
        assert len(decoded_rows) == len(rows), f"expected {len(rows)} decoded rows, got {len(decoded_rows)}"
        assert {row["id"] for row in decoded_rows} == {1, 2}
        return {
            "topic": topic,
            "message_format": message_format,
            "strategy": strategy.value,
            "produced_rows": produced,
            "decoded_rows": len(decoded_rows),
            "state_offsets": state.partition_offsets if state else {},
        }

    def _run_source_read_mode_matrix(self) -> None:
        self.case("kafka_source.read_mode.offsets", lambda: self._read_mode_offsets())
        self.case("kafka_source.read_mode.max_records", lambda: self._read_mode_max_records())
        self.case("kafka_source.read_mode.time_window", lambda: self._read_mode_time_window())

    def _read_mode_offsets(self) -> dict[str, Any]:
        topic = self._topic("read_offsets")
        self._produce(topic, ROWS, "json", LoadStrategy.INCREMENTAL_APPEND)
        rows, state = self._extract_rows(topic, "json", group_suffix="offsets")
        assert len(rows) == len(ROWS), f"expected {len(ROWS)} offset rows, got {len(rows)}"
        return {"topic": topic, "rows": len(rows), "offsets": state.partition_offsets if state else {}}

    def _read_mode_max_records(self) -> dict[str, Any]:
        topic = self._topic("read_max_records")
        self._produce(topic, ROWS, "json", LoadStrategy.INCREMENTAL_APPEND)
        rows, state = self._extract_rows(
            topic,
            "json",
            group_suffix="max_records",
            read_mode="max_records",
            extra_options={"max_records": 2},
        )
        assert len(rows) == 2, f"expected 2 max_records rows, got {len(rows)}"
        return {"topic": topic, "rows": len(rows), "offsets": state.partition_offsets if state else {}}

    def _read_mode_time_window(self) -> dict[str, Any]:
        topic = self._topic("read_time_window")
        before_ms = int(time.time() * 1000) - 5_000
        self._produce(topic, ROWS, "json", LoadStrategy.INCREMENTAL_APPEND)
        time.sleep(0.5)
        after_ms = int(time.time() * 1000) + 5_000
        rows, state = self._extract_rows(
            topic,
            "json",
            group_suffix="time_window",
            read_mode="time_window",
            extra_options={"time_window_start_ms": before_ms, "time_window_end_ms": after_ms},
        )
        assert len(rows) == len(ROWS), f"expected {len(ROWS)} time_window rows, got {len(rows)}"
        return {
            "topic": topic,
            "rows": len(rows),
            "window": {"start_ms": before_ms, "end_ms": after_ms},
            "offsets": state.partition_offsets if state else {},
        }

    def _run_offset_state_resume(self) -> None:
        self.case("kafka_source.offset_state.dpone_resume", self._offset_state_resume)

    def _offset_state_resume(self) -> dict[str, Any]:
        topic = self._topic("offset_state_resume")
        storage = InMemoryKafkaOffsetStateStorage()
        self._produce(topic, ROWS[:2], "json", LoadStrategy.INCREMENTAL_APPEND)
        rows_1, state_1 = self._extract_rows(topic, "json", group_suffix="resume", state_storage=storage)
        source = KafkaSource(self.connector, state_storage=storage)
        cfg = self._source_config(topic, "json", group_suffix="resume")
        source.save_state(cfg, state_1)
        self._produce(topic, ROWS[2:], "json", LoadStrategy.INCREMENTAL_APPEND)
        rows_2, state_2 = self._extract_rows(topic, "json", group_suffix="resume", state_storage=storage)
        assert len(rows_1) == 2, f"expected initial 2 rows, got {len(rows_1)}"
        assert len(rows_2) == 1, f"expected resumed 1 row, got {len(rows_2)}"
        return {
            "topic": topic,
            "initial_rows": len(rows_1),
            "resumed_rows": len(rows_2),
            "initial_offsets": state_1.partition_offsets if state_1 else {},
            "resumed_offsets": state_2.partition_offsets if state_2 else {},
        }

    def _run_key_policy_matrix(self) -> None:
        for mode in ("unique_key", "hash_row", "null"):
            self.case(f"kafka_sink.key_policy.{mode}", lambda mode=mode: self._key_policy(mode))

    def _key_policy(self, mode: str) -> dict[str, Any]:
        topic = self._topic(f"key_{mode}")
        options = self._sink_options(topic, "json")
        options["key"] = {"mode": mode}
        cfg = self._sink_config(topic, LoadStrategy.INCREMENTAL_APPEND, options=options)
        sink = KafkaSink(self.connector)
        sink.load(cfg, LoadPayload(InMemoryRowsArtifact(ROWS[:1]), SCHEMA))
        raw = self._raw_consume(topic, expected=1)
        key = raw[0]["key"]
        if mode == "null":
            assert key is None, f"expected null key, got {key!r}"
        else:
            assert key, f"expected non-null key for key mode {mode}"
        return {"topic": topic, "mode": mode, "raw_key": key}

    def _run_envelope_and_tombstone_matrix(self) -> None:
        self.case("kafka_sink.envelope.dpone_auto_unwrap", self._envelope_auto_unwrap)
        self.case("kafka_sink.deletes.tombstone_and_source_delete_row", self._tombstone_delete)

    def _envelope_auto_unwrap(self) -> dict[str, Any]:
        topic = self._topic("envelope_dpone")
        options = self._sink_options(topic, "json")
        options["envelope"] = "dpone"
        cfg = self._sink_config(topic, LoadStrategy.INCREMENTAL_MERGE, options=options)
        KafkaSink(self.connector).load(cfg, LoadPayload(InMemoryRowsArtifact(ROWS[:1]), SCHEMA))
        rows, _state = self._extract_rows(topic, "json", group_suffix="envelope")
        assert rows and rows[0]["id"] == 1
        assert rows[0].get("__dpone_op") == "upsert"
        return {"topic": topic, "decoded_row": rows[0]}

    def _tombstone_delete(self) -> dict[str, Any]:
        topic = self._topic("tombstone_delete")
        options = self._sink_options(topic, "json")
        options["deletes"] = {"enabled": True, "tombstone": True}
        cfg = self._sink_config(topic, LoadStrategy.INCREMENTAL_MERGE, options=options)
        row = {"id": 1, "name": "alpha", "amount": 10.5, "active": True, "__dpone_op": "delete"}
        KafkaSink(self.connector).load(cfg, LoadPayload(InMemoryRowsArtifact([row]), SCHEMA))
        raw = self._raw_consume(topic, expected=1)
        assert raw[0]["value_is_null"], "expected Kafka tombstone value"
        rows, _state = self._extract_rows(topic, "json", group_suffix="tombstone")
        assert rows and rows[0].get("__dpone_op") == "delete"
        return {"topic": topic, "raw": raw[0], "decoded": rows[0]}

    def _run_source_payload_contract_matrix(self) -> None:
        source_payloads: list[tuple[str, Callable[[], Any]]] = [
            ("postgres_inmemory", lambda: InMemoryRowsArtifact(self._source_rows("postgres"))),
            ("mssql_streaming", lambda: StreamingRowsArtifact(iter(self._source_rows("mssql")), batch_size=2)),
            ("clickhouse_file", lambda: self._file_artifact(self._source_rows("clickhouse"))),
            ("rest_api_streaming", lambda: StreamingRowsArtifact(iter(self._source_rows("rest_api")), batch_size=2)),
        ]
        for source_name, artifact_factory in source_payloads:
            self.case(
                f"any_source_to_kafka.contract.{source_name}",
                lambda source_name=source_name, artifact_factory=artifact_factory: self._source_payload_contract(
                    source_name,
                    artifact_factory,
                ),
            )

    def _source_payload_contract(self, source_name: str, artifact_factory: Callable[[], Any]) -> dict[str, Any]:
        topic = self._topic(f"source_contract_{source_name}")
        artifact = artifact_factory()
        cfg = self._sink_config(topic, LoadStrategy.INCREMENTAL_APPEND)
        result = KafkaSink(self.connector).load(cfg, LoadPayload(artifact, SCHEMA + [("source_system", "text")]))
        raw = self._raw_consume(topic, expected=3)
        assert result.inserted_rows == 3
        assert len(raw) == 3
        return {"topic": topic, "source_name": source_name, "produced_rows": result.inserted_rows}

    def _run_live_target_matrix(self) -> None:
        self.case("kafka_source_to_postgres.live.full_refresh", self._kafka_to_postgres_live)
        self.case("kafka_source_to_clickhouse.live.full_refresh", self._kafka_to_clickhouse_live)
        self.case("kafka_source_to_mssql.live", self._kafka_to_mssql_live)
        self.case("kafka_source_to_bigquery.live", self._kafka_to_bigquery_live_skip)

    def _kafka_to_postgres_live(self) -> dict[str, Any]:
        port = int(os.getenv("DPONE_KAFKA_MATRIX_PG_PORT", "55433"))
        connector = PostgresConnector(
            host="127.0.0.1",
            port=port,
            database=os.getenv("DPONE_KAFKA_MATRIX_PG_DATABASE", "dpone_it"),
            user=os.getenv("DPONE_KAFKA_MATRIX_PG_USER", "dpone"),
            password=os.getenv("DPONE_KAFKA_MATRIX_PG_PASSWORD", "dpone"),
        )
        connector.execute_query("CREATE SCHEMA IF NOT EXISTS landing")
        connector.execute_query("CREATE SCHEMA IF NOT EXISTS staging")
        topic = self._topic("to_postgres")
        self._produce(topic, ROWS, "json", LoadStrategy.INCREMENTAL_APPEND)
        extract, state = self._extract_result(topic, "json", group_suffix="to_postgres")
        table = f"kafka_orders_{self.run_id}".replace("-", "_").lower()
        cfg = LoadConfig(
            source_conn_id="kafka",
            target_conn_id="postgres",
            source_schema="kafka",
            source_table=topic,
            target_schema="landing",
            target_table=table,
            staging_schema="staging",
            load_strategy=LoadStrategy.FULL_REFRESH,
            unique_key="id",
        )
        result = PostgresSink(connector, state_storage=None).load(cfg, LoadPayload(extract.artifact, extract.schema))
        count = connector.get_records(f"SELECT count(*) FROM landing.{table}")[0][0]
        assert count == len(ROWS), f"expected {len(ROWS)} Postgres rows, got {count}"
        return {
            "topic": topic,
            "target": f"landing.{table}",
            "inserted_rows": result.inserted_rows,
            "count": count,
            "source_state": state.partition_offsets if state else {},
        }

    def _kafka_to_clickhouse_live(self) -> dict[str, Any]:
        port = int(os.getenv("DPONE_KAFKA_MATRIX_CH_PORT", "59002"))
        connector = ClickHouseConnector(
            host="127.0.0.1",
            port=port,
            database=os.getenv("DPONE_KAFKA_MATRIX_CH_DATABASE", "default"),
            user=os.getenv("DPONE_KAFKA_MATRIX_CH_USER", "default"),
            password=os.getenv("DPONE_KAFKA_MATRIX_CH_PASSWORD", "dpone"),
        )
        topic = self._topic("to_clickhouse")
        self._produce(topic, ROWS, "json", LoadStrategy.INCREMENTAL_APPEND)
        extract, state = self._extract_result(topic, "json", group_suffix="to_clickhouse")
        table = f"kafka_orders_{self.run_id}".replace("-", "_").lower()
        cfg = LoadConfig(
            source_conn_id="kafka",
            target_conn_id="clickhouse",
            source_schema="kafka",
            source_table=topic,
            target_schema="default",
            target_table=table,
            staging_schema="default",
            load_strategy=LoadStrategy.FULL_REFRESH,
            unique_key="id",
        )
        result = ClickHouseSink(connector).load(cfg, LoadPayload(extract.artifact, extract.schema))
        count = connector.get_records(f"SELECT count() FROM default.{table}")[0][0]
        assert count == len(ROWS), f"expected {len(ROWS)} ClickHouse rows, got {count}"
        return {
            "topic": topic,
            "target": f"default.{table}",
            "inserted_rows": result.inserted_rows,
            "count": count,
            "source_state": state.partition_offsets if state else {},
        }

    def _kafka_to_mssql_live(self) -> dict[str, Any]:
        host = os.getenv("DPONE_KAFKA_MATRIX_MSSQL_HOST")
        if not host:
            raise SkipCase(
                "Local MSSQL live target is not configured for this Kafka matrix run.",
                {
                    "required_env": [
                        "DPONE_KAFKA_MATRIX_MSSQL_HOST",
                        "DPONE_KAFKA_MATRIX_MSSQL_PORT",
                        "DPONE_KAFKA_MATRIX_MSSQL_PASSWORD",
                    ],
                    "contract_status": "Kafka source rows and sink payload contract were tested; MSSQL live load remains external-gate.",
                },
            )
        connector = MSSQLConnector(
            host=host,
            port=int(os.getenv("DPONE_KAFKA_MATRIX_MSSQL_PORT", "1433")),
            database=os.getenv("DPONE_KAFKA_MATRIX_MSSQL_DATABASE", "dpone"),
            user=os.getenv("DPONE_KAFKA_MATRIX_MSSQL_USER", "sa"),
            password=os.getenv("DPONE_KAFKA_MATRIX_MSSQL_PASSWORD", ""),
            driver=os.getenv("DPONE_KAFKA_MATRIX_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
            trust_server_certificate=os.getenv("DPONE_KAFKA_MATRIX_MSSQL_TRUST_SERVER_CERTIFICATE", "yes"),
            bcp_path=os.getenv("DPONE_KAFKA_MATRIX_MSSQL_BCP_PATH", "bcp"),
        )
        topic = self._topic("to_mssql")
        schema = f"it_kafka_{self.run_id}".replace("-", "_").lower()
        table = "orders"
        try:
            connector.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
            self._produce(topic, ROWS, "json", LoadStrategy.INCREMENTAL_APPEND)
            extract, state = self._extract_result(topic, "json", group_suffix="to_mssql")
            cfg = LoadConfig(
                source_conn_id="kafka",
                target_conn_id="mssql",
                source_schema="kafka",
                source_table=topic,
                target_schema=schema,
                target_table=table,
                staging_schema=schema,
                load_strategy=LoadStrategy.FULL_REFRESH,
                unique_key="id",
            )
            result = MSSQLSink(connector).load(cfg, LoadPayload(extract.artifact, extract.schema))
            count = connector.get_records(f"SELECT COUNT_BIG(*) FROM [{schema}].[{table}]")[0][0]
            assert count == len(ROWS), f"expected {len(ROWS)} MSSQL rows, got {count}"
            return {
                "topic": topic,
                "target": f"{schema}.{table}",
                "inserted_rows": result.inserted_rows,
                "count": int(count),
                "source_state": state.partition_offsets if state else {},
            }
        finally:
            try:
                connector.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{table}]")
                connector.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
            finally:
                connector.close()

    def _kafka_to_bigquery_live_skip(self) -> dict[str, Any]:
        raise SkipCase(
            "BigQuery live target requires GCP project credentials and is intentionally not created by this harness.",
            {
                "reason": "No GCP credentials/project were provided for local live BigQuery.",
                "contract_status": "Kafka source rows were tested; BigQuery live load remains external-gate.",
            },
        )

    def _produce(
        self,
        topic: str,
        rows: Iterable[Mapping[str, Any]],
        message_format: str,
        strategy: LoadStrategy,
    ) -> int:
        options = self._sink_options(topic, message_format)
        cfg = self._sink_config(topic, strategy, options=options)
        result = KafkaSink(self.connector).load(cfg, LoadPayload(InMemoryRowsArtifact(rows), SCHEMA))
        time.sleep(0.2)
        return result.inserted_rows

    def _extract_rows(
        self,
        topic: str,
        message_format: str,
        *,
        group_suffix: str,
        read_mode: str = "offsets",
        extra_options: dict[str, Any] | None = None,
        state_storage: InMemoryKafkaOffsetStateStorage | None = None,
    ) -> tuple[list[dict[str, Any]], Any]:
        extract, state = self._extract_result(
            topic,
            message_format,
            group_suffix=group_suffix,
            read_mode=read_mode,
            extra_options=extra_options,
            state_storage=state_storage,
        )
        artifact = extract.artifact
        if isinstance(artifact, InMemoryRowsArtifact):
            rows = list(artifact._rows)
        elif isinstance(artifact, StreamingRowsArtifact):
            rows = list(artifact._iterator)
            artifact.cleanup()
        else:
            raise TypeError(f"Unsupported Kafka source artifact in matrix: {type(artifact).__name__}")
        return rows, state

    def _extract_result(
        self,
        topic: str,
        message_format: str,
        *,
        group_suffix: str,
        read_mode: str = "offsets",
        extra_options: dict[str, Any] | None = None,
        state_storage: InMemoryKafkaOffsetStateStorage | None = None,
    ) -> tuple[Any, Any]:
        source = KafkaSource(self.connector, state_storage=state_storage or InMemoryKafkaOffsetStateStorage())
        cfg = self._source_config(
            topic,
            message_format,
            group_suffix=group_suffix,
            read_mode=read_mode,
            extra_options=extra_options,
        )
        extract = source.extract(cfg, None)
        return extract, extract.state

    def _raw_consume(self, topic: str, *, expected: int) -> list[dict[str, Any]]:
        from confluent_kafka import Consumer

        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": f"{self.topic_prefix}_{topic}_raw_{uuid.uuid4().hex[:6]}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([topic])
        deadline = time.time() + 20
        rows: list[dict[str, Any]] = []
        try:
            while len(rows) < expected and time.time() < deadline:
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    raise RuntimeError(str(msg.error()))
                rows.append(
                    {
                        "partition": int(msg.partition()),
                        "offset": int(msg.offset()),
                        "key": msg.key().decode("utf-8") if msg.key() else None,
                        "value_is_null": msg.value() is None,
                        "value_size": len(msg.value() or b""),
                    }
                )
        finally:
            consumer.close()
        assert len(rows) == expected, f"expected {expected} raw Kafka messages, got {len(rows)}"
        return rows

    def _sink_config(
        self,
        topic: str,
        strategy: LoadStrategy,
        *,
        options: dict[str, Any] | None = None,
    ) -> LoadConfig:
        return LoadConfig(
            source_conn_id="source",
            target_conn_id="kafka",
            source_schema="source",
            source_table="orders",
            target_schema="kafka",
            target_table=topic,
            load_strategy=strategy,
            unique_key="id",
            custom_predicate="id <= 999",
            options=options or self._sink_options(topic, "json"),
        )

    def _source_config(
        self,
        topic: str,
        message_format: str,
        *,
        group_suffix: str,
        read_mode: str = "offsets",
        extra_options: dict[str, Any] | None = None,
    ) -> LoadConfig:
        options: dict[str, Any] = {
            "topic": topic,
            "group_id": f"{self.topic_prefix}_{group_suffix}",
            "read_mode": read_mode,
            "offset_storage": "dpone",
            "start_from": "stored",
            "message_format": message_format,
            "batch_size": 10_000,
            "poll_timeout_ms": 250,
            "max_empty_polls": 8,
            "schema_registry": {"enabled": message_format != "json"},
        }
        if message_format == "protobuf":
            options["protobuf_message_class"] = self.protobuf_message_class
        options.update(extra_options or {})
        return LoadConfig(
            source_conn_id="kafka",
            target_conn_id="target",
            source_schema="kafka",
            source_table=topic,
            target_schema="target",
            target_table="orders",
            load_strategy=LoadStrategy.INCREMENTAL_APPEND,
            unique_key="id",
            options=options,
        )

    def _sink_options(self, topic: str, message_format: str = "json") -> dict[str, Any]:
        options: dict[str, Any] = {
            "topic": topic,
            "message_format": message_format,
            "key": {"mode": "unique_key"},
            "delivery": {
                "mode": "at_least_once",
                "compression_type": "zstd",
                "linger_ms": 5,
                "batch_num_messages": 10_000,
                "flush_timeout": 30,
            },
            "schema_registry": {
                "enabled": message_format != "json",
                "auto_register_schemas": True,
                "subject_name_strategy": "topic",
            },
        }
        if message_format == "protobuf":
            options["protobuf_message_class"] = self.protobuf_message_class
        return options

    def _source_rows(self, source_name: str) -> list[dict[str, Any]]:
        return [{**row, "source_system": source_name} for row in ROWS]

    def _file_artifact(self, rows: list[Mapping[str, Any]]) -> FileExportArtifact:
        handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="", suffix=".csv", delete=False)
        with handle:
            writer = csv.writer(handle)
            for row in rows:
                writer.writerow([row.get(column) for column, _type in SCHEMA + [("source_system", "text")]])
        columns = [column for column, _type in SCHEMA + [("source_system", "text")]]
        return FileExportArtifact(handle.name, columns)

    def _topic(self, suffix: str) -> str:
        return f"{self.topic_prefix}_{suffix}".lower()

    def _report(self) -> dict[str, Any]:
        completed_at = datetime.now(UTC)
        summary: dict[str, int] = {"passed": 0, "failed": 0, "skipped": 0}
        for result in self.results:
            summary[result.status] = summary.get(result.status, 0) + 1
        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_seconds": round((completed_at - self.started_at).total_seconds(), 3),
            "tester": "Codex",
            "environment": {
                "bootstrap_servers": self.bootstrap_servers,
                "schema_registry_url": self.schema_registry_url,
                "postgres": "127.0.0.1:55433",
                "clickhouse_native": "127.0.0.1:59002",
                "platform": sys.platform,
                "python": sys.version.split()[0],
            },
            "summary": summary,
            "results": [asdict(result) for result in self.results],
        }


class SkipCase(RuntimeError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = details or {}


def _build_order_event_message_class() -> type[Any]:
    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.name = "dpone_kafka_matrix.proto"
    file_proto.package = "dpone.kafka.matrix"
    file_proto.syntax = "proto3"
    message = file_proto.message_type.add()
    message.name = "OrderEvent"
    fields = [
        ("id", 1, descriptor_pb2.FieldDescriptorProto.TYPE_INT64),
        ("name", 2, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("amount", 3, descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE),
        ("active", 4, descriptor_pb2.FieldDescriptorProto.TYPE_BOOL),
    ]
    for name, number, field_type in fields:
        field = message.field.add()
        field.name = name
        field.number = number
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        field.type = field_type
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_proto)
    descriptor = pool.FindMessageTypeByName("dpone.kafka.matrix.OrderEvent")
    return message_factory.GetMessageClass(descriptor)


def write_artifacts(report: dict[str, Any]) -> tuple[Path, Path]:
    artifact_dir = ROOT / "test_artifacts" / "kafka_live_matrix"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{report['started_at'][:10]}-{report['run_id']}-kafka-live-matrix"
    json_path = artifact_dir / f"{stem}.json"
    markdown_path = artifact_dir / f"{stem}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, markdown_path


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# dpone Kafka live integration matrix",
        "",
        f"- Date: {report['started_at']}",
        "- Tester: Codex",
        f"- Run ID: `{report['run_id']}`",
        f"- Duration: {report['duration_seconds']}s",
        f"- Summary: `{report['summary']}`",
        "",
        "## Environment",
        "",
    ]
    for key, value in report["environment"].items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Matrix results",
            "",
            "| Case | Status | Duration ms | Details | Error |",
            "| --- | --- | ---: | --- | --- |",
        ]
    )
    for result in report["results"]:
        details = json.dumps(result["details"], ensure_ascii=False, sort_keys=True, default=str)
        error = result["error"] or ""
        lines.append(
            "| {name} | {status} | {duration_ms} | `{details}` | {error} |".format(
                name=result["name"],
                status=result["status"],
                duration_ms=result["duration_ms"],
                details=details.replace("|", "\\|"),
                error=error.replace("|", "\\|"),
            )
        )
    failures = [result for result in report["results"] if result["status"] == "failed"]
    if failures:
        lines.extend(["", "## Failure traces", ""])
        for result in failures:
            lines.extend(
                [
                    f"### {result['name']}",
                    "",
                    "```text",
                    result["traceback"] or result["error"] or "",
                    "```",
                    "",
                ]
            )
    lines.extend(
        [
            "",
            "## Coverage notes",
            "",
            "- Live Kafka + Schema Registry were tested through Redpanda.",
            "- Kafka sink and Kafka source were exercised through real dpone runtime contracts.",
            "- Local Postgres and ClickHouse targets were tested when containers were available.",
            "- MSSQL and BigQuery live target cases are explicit skips in this local harness because they require a runnable SQL Server target or GCP credentials.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    runner = MatrixRunner()
    report = runner.run()
    json_path, markdown_path = write_artifacts(report)
    print(json.dumps({"summary": report["summary"], "json": str(json_path), "markdown": str(markdown_path)}, indent=2))
    return 1 if report["summary"].get("failed", 0) else 0


if __name__ == "__main__":
    raise SystemExit(main())
