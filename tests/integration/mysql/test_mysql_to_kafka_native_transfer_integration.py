"""Docker MySQL → Kafka CSV produce evidence (SKIP when services unavailable)."""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Callable
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.connectors.kafka import KafkaConnector
from dpone.runtime.connectors.mysql import MySQLConnector
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.kafka import KafkaSink
from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy

pytestmark = [pytest.mark.integration, pytest.mark.integration_mysql, pytest.mark.integration_kafka]


def _enabled() -> bool:
    return bool(
        os.environ.get("DPONE_RUN_INTEGRATION") == "1"
        or os.environ.get("DPONE_IT_MYSQL_HOST")
        or os.environ.get("DPONE_IT_MYSQL_PORT_FORWARD")
        or os.environ.get("DPONE_IT_KAFKA_BOOTSTRAP")
        or os.environ.get("DPONE_KAFKA_BOOTSTRAP_SERVERS")
        or os.environ.get("DPONE_IT_KAFKA_PORT_FORWARD")
    )


def _mysql() -> MySQLConnector:
    return MySQLConnector(
        host=os.environ.get("DPONE_IT_MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("DPONE_IT_MYSQL_PORT_FORWARD", "53306")),
        database=os.environ.get("DPONE_IT_MYSQL_DATABASE", "dpone_it"),
        user=os.environ.get("DPONE_IT_MYSQL_USER", "dpone"),
        password=os.environ.get("DPONE_IT_MYSQL_PASSWORD", "dpone"),
    )


def _kafka() -> KafkaConnector:
    bootstrap = (
        os.environ.get("DPONE_IT_KAFKA_BOOTSTRAP")
        or os.environ.get("DPONE_KAFKA_BOOTSTRAP_SERVERS")
        or f"127.0.0.1:{os.environ.get('DPONE_IT_KAFKA_PORT_FORWARD', '59092')}"
    )
    return KafkaConnector(bootstrap_servers=bootstrap, client_id="dpone-mysql-kafka-it")


class _Logger:
    def log_etl_progress(self, event: str, payload: dict[str, object]) -> None:
        del event, payload

    def log_data_sample(self, kind: str, rows: list[object], max_rows: int) -> None:
        del kind, rows, max_rows


def _wait_until_ready(label: str, action: Callable[[], object], *, timeout_seconds: int = 90) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            action()
            return
        except Exception as exc:  # noqa: BLE001 - readiness loop
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"{label} was not ready within {timeout_seconds}s") from last_error


def _consume_ids(kafka: KafkaConnector, topic: str, *, expected: int, group_id: str) -> set[int]:
    consumer = kafka.create_consumer(
        group_id=group_id,
        options={"auto.offset.reset": "earliest", "enable.partition.eof": True},
    )
    consumer.subscribe([topic])
    ids: set[int] = set()
    empty_polls = 0
    while len(ids) < expected and empty_polls < 40:
        message = consumer.poll(0.25)
        if message is None or message.error():
            empty_polls += 1
            continue
        empty_polls = 0
        value = json.loads(message.value())
        payload = value.get("data") if isinstance(value.get("data"), dict) else value
        ids.add(int(payload["id"]))
    consumer.close()
    return ids


def _prepare_mysql(mysql: MySQLConnector) -> None:
    database = mysql.database
    mysql.execute_query(f"DROP TABLE IF EXISTS `{database}`.`mysql_to_kafka_orders`")
    mysql.execute_query(
        f"""
        CREATE TABLE `{database}`.`mysql_to_kafka_orders` (
            id INT PRIMARY KEY,
            name VARCHAR(64) NOT NULL,
            updated_at DATETIME NOT NULL
        )
        """
    )
    mysql.execute_query(
        f"""
        INSERT INTO `{database}`.`mysql_to_kafka_orders` (id, name, updated_at) VALUES
            (1, 'alpha', '2026-07-22 10:00:00'),
            (2, 'beta,comma', '2026-07-22 11:00:00')
        """
    )


@pytest.mark.skipif(not _enabled(), reason="MySQL/Kafka integration host not configured")
def test_mysql_to_kafka_full_refresh_csv_produce(tmp_path: Path) -> None:
    mysql = _mysql()
    _wait_until_ready("mysql", lambda: mysql.get_records("SELECT 1"))
    kafka = _kafka()
    topic = f"dpone_mysql_kafka_{uuid.uuid4().hex[:10]}"
    _prepare_mysql(mysql)

    config = LoadConfig(
        source_conn_id="mysql_source",
        target_conn_id="kafka_sink",
        source_schema=mysql.database,
        source_table="mysql_to_kafka_orders",
        source_database=mysql.database,
        target_schema="kafka",
        target_table=topic,
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="csv",
        compress_export=False,
        unique_key=["id"],
        options={
            "sink_type": "kafka",
            "topic": topic,
            "message_format": "json",
            "envelope": "dpone",
            "partition_tmp_dir": str(tmp_path),
            "key": {"mode": "unique_key"},
            "delivery": {"mode": "at_least_once"},
        },
    )
    extract = MySQLFullExtractStrategy(mysql, logger=_Logger(), sink_connector=kafka).extract(config, None)
    assert getattr(extract.artifact, "format", None) == "csv"
    assert getattr(extract.artifact, "rows_exported", None) == 2
    result = KafkaSink(kafka, state_storage=None, logger=None).load(
        config, LoadPayload(artifact=extract.artifact, schema=extract.schema)
    )
    assert result.inserted_rows == 2
    ids = _consume_ids(
        kafka,
        topic,
        expected=2,
        group_id=f"dpone-mysql-kafka-it-{uuid.uuid4().hex[:8]}",
    )
    assert ids == {1, 2}
