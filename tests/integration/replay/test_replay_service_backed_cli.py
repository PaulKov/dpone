from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from dpone.cli.main import main as cli_main

pytestmark = [pytest.mark.integration, pytest.mark.integration_replay, pytest.mark.integration_replay_services]


def _enabled() -> bool:
    return os.getenv("DPONE_RUN_INTEGRATION_REPLAY_SERVICES") == "1"


@pytest.mark.skipif(
    not _enabled(),
    reason="Set DPONE_RUN_INTEGRATION_REPLAY_SERVICES=1 and start docker/docker-compose.integration.yml.",
)
def test_postgres_live_replay_cli_executes_against_local_service(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    psycopg = pytest.importorskip("psycopg")
    conninfo = _postgres_conninfo()
    run_id = "01JREPLAYSERVICEPG000000001"

    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS replay_target")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS replay_stage")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS etl_state")
            cursor.execute("DROP TABLE IF EXISTS replay_target.orders")
            cursor.execute("DROP TABLE IF EXISTS replay_stage.orders__replay_staging")
            cursor.execute("DROP TABLE IF EXISTS etl_state.__dpone__loads")
            cursor.execute("CREATE TABLE replay_target.orders (id integer primary key, value text)")
            cursor.execute("CREATE TABLE replay_stage.orders__replay_staging (id integer primary key, value text)")
            cursor.execute("CREATE TABLE etl_state.__dpone__loads (run_id text primary key, status text)")
            cursor.execute("INSERT INTO replay_target.orders VALUES (1, 'old'), (3, 'keep')")
            cursor.execute("INSERT INTO replay_stage.orders__replay_staging VALUES (1, 'new'), (2, 'inserted')")
            cursor.execute("INSERT INTO etl_state.__dpone__loads VALUES (%s, 'staged')", (run_id,))

    with pytest.raises(SystemExit) as exc:
        cli_main(
            [
                "resync",
                "--run-id",
                run_id,
                "--source-type",
                "postgres",
                "--sink-type",
                "postgres",
                "--strategy",
                "incremental_merge",
                "--artifact-dir",
                str(tmp_path),
                "--yes",
                "--live-backend",
                "--connection-type",
                "params",
                "--connection-id",
                _postgres_params_json(),
                "--target-schema",
                "replay_target",
                "--target-table",
                "orders",
                "--staging-schema",
                "replay_stage",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["executed"] is True
    assert payload["state_committed"] is True

    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, value FROM replay_target.orders ORDER BY id")
            rows = cursor.fetchall()
            cursor.execute("SELECT status FROM etl_state.__dpone__loads WHERE run_id = %s", (run_id,))
            status = cursor.fetchone()[0]

    assert rows == [(1, "new"), (2, "inserted")]
    assert status == "committed"


@pytest.mark.skipif(
    not _enabled(),
    reason="Set DPONE_RUN_INTEGRATION_REPLAY_SERVICES=1 and start docker/docker-compose.integration.yml.",
)
def test_kafka_live_replay_cli_produces_replay_event_against_local_service(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    confluent_kafka = pytest.importorskip("confluent_kafka")
    topic = f"dpone.replay.service.{os.getpid()}"
    run_id = "01JREPLAYSERVICEKF000000001"

    with pytest.raises(SystemExit) as exc:
        cli_main(
            [
                "resume",
                run_id,
                "--from-stage",
                "finalize",
                "--source-type",
                "postgres",
                "--sink-type",
                "kafka",
                "--strategy",
                "incremental_merge",
                "--artifact-dir",
                str(tmp_path),
                "--yes",
                "--live-backend",
                "--connection-type",
                "params",
                "--connection-id",
                _kafka_params_json(),
                "--target-table",
                topic,
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["executed"] is True

    consumer = confluent_kafka.Consumer(
        {
            "bootstrap.servers": _kafka_bootstrap_servers(),
            "group.id": f"dpone-replay-service-{os.getpid()}",
            "auto.offset.reset": "earliest",
        }
    )
    try:
        consumer.subscribe([topic])
        message = consumer.poll(10)
    finally:
        consumer.close()

    assert message is not None
    assert message.error() is None
    payload = json.loads(message.value().decode("utf-8"))
    assert payload["op"] == "replay"
    assert payload["run_id"] == run_id
    assert payload["strategy"] == "incremental_merge"


@pytest.mark.skipif(
    not _enabled(),
    reason="Set DPONE_RUN_INTEGRATION_REPLAY_SERVICES=1 and start docker/docker-compose.integration.yml.",
)
def test_clickhouse_live_replay_cli_executes_against_local_service(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    clickhouse_driver = pytest.importorskip("clickhouse_driver")
    client = clickhouse_driver.Client(
        host=os.getenv("DPONE_IT_CH_HOST", "127.0.0.1"),
        port=int(os.getenv("DPONE_IT_CH_PORT_FORWARD", "59000")),
        user=os.getenv("DPONE_IT_CH_USER", "default"),
        password=os.getenv("DPONE_IT_CH_PASSWORD", "dpone"),
        database=os.getenv("DPONE_IT_CH_DATABASE", "dpone_it"),
    )
    run_id = "01JREPLAYSERVICECH000000001"

    client.execute("CREATE DATABASE IF NOT EXISTS replay_target")
    client.execute("CREATE DATABASE IF NOT EXISTS replay_stage")
    client.execute("CREATE DATABASE IF NOT EXISTS etl_state")
    client.execute("DROP TABLE IF EXISTS replay_target.orders")
    client.execute("DROP TABLE IF EXISTS replay_stage.orders__replay_staging")
    client.execute("DROP TABLE IF EXISTS etl_state.__dpone__loads")
    client.execute("CREATE TABLE replay_target.orders (id Int32, value String) ENGINE = MergeTree ORDER BY id")
    client.execute(
        "CREATE TABLE replay_stage.orders__replay_staging (id Int32, value String) ENGINE = MergeTree ORDER BY id"
    )
    client.execute(
        "CREATE TABLE etl_state.__dpone__loads (run_id String, status String) ENGINE = MergeTree ORDER BY run_id"
    )
    client.execute("INSERT INTO replay_target.orders VALUES", [(1, "old"), (3, "keep")])
    client.execute("INSERT INTO replay_stage.orders__replay_staging VALUES", [(1, "new"), (2, "inserted")])
    client.execute("INSERT INTO etl_state.__dpone__loads VALUES", [(run_id, "staged")])

    with pytest.raises(SystemExit) as exc:
        cli_main(
            [
                "resync",
                "--run-id",
                run_id,
                "--source-type",
                "postgres",
                "--sink-type",
                "clickhouse",
                "--strategy",
                "replace",
                "--artifact-dir",
                str(tmp_path),
                "--yes",
                "--live-backend",
                "--connection-type",
                "params",
                "--connection-id",
                _clickhouse_params_json(),
                "--target-schema",
                "replay_target",
                "--target-table",
                "orders",
                "--staging-schema",
                "replay_stage",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["executed"] is True
    assert payload["state_committed"] is True
    assert client.execute("SELECT id, value FROM replay_target.orders ORDER BY id") == [(1, "new"), (2, "inserted")]
    assert (
        client.execute("SELECT status FROM etl_state.__dpone__loads WHERE run_id = %(run_id)s", {"run_id": run_id})[0][
            0
        ]
        == "committed"
    )


@pytest.mark.skipif(
    not _enabled(),
    reason="Set DPONE_RUN_INTEGRATION_REPLAY_SERVICES=1 and start docker/docker-compose.integration.yml.",
)
def test_mssql_live_replay_cli_executes_against_local_service(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pyodbc = pytest.importorskip("pyodbc")
    if "ODBC Driver 18 for SQL Server" not in pyodbc.drivers():
        pytest.skip("ODBC Driver 18 for SQL Server is required for MSSQL replay service test.")
    run_id = "01JREPLAYSERVICEMS000000001"
    connection_string = _mssql_connection_string()

    _wait_for_mssql(pyodbc, connection_string)
    conn = pyodbc.connect(connection_string, autocommit=True)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'replay_target') EXEC('CREATE SCHEMA replay_target')"
        )
        cursor.execute(
            "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'replay_stage') EXEC('CREATE SCHEMA replay_stage')"
        )
        cursor.execute(
            "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl_state') EXEC('CREATE SCHEMA etl_state')"
        )
        cursor.execute("DROP TABLE IF EXISTS replay_target.orders")
        cursor.execute("DROP TABLE IF EXISTS replay_stage.orders__replay_staging")
        cursor.execute("DROP TABLE IF EXISTS etl_state.__dpone__loads")
        cursor.execute("CREATE TABLE replay_target.orders (id int NOT NULL PRIMARY KEY, value nvarchar(100) NOT NULL)")
        cursor.execute(
            "CREATE TABLE replay_stage.orders__replay_staging "
            "(id int NOT NULL PRIMARY KEY, value nvarchar(100) NOT NULL)"
        )
        cursor.execute(
            "CREATE TABLE etl_state.__dpone__loads (run_id nvarchar(64) NOT NULL PRIMARY KEY, status nvarchar(32) NOT NULL)"
        )
        cursor.execute("INSERT INTO replay_target.orders VALUES (1, N'old'), (3, N'keep')")
        cursor.execute("INSERT INTO replay_stage.orders__replay_staging VALUES (1, N'new'), (2, N'inserted')")
        cursor.execute("INSERT INTO etl_state.__dpone__loads VALUES (?, N'staged')", run_id)
        cursor.close()
    finally:
        conn.close()

    with pytest.raises(SystemExit) as exc:
        cli_main(
            [
                "resync",
                "--run-id",
                run_id,
                "--source-type",
                "postgres",
                "--sink-type",
                "mssql",
                "--strategy",
                "incremental_merge",
                "--artifact-dir",
                str(tmp_path),
                "--yes",
                "--live-backend",
                "--connection-type",
                "params",
                "--connection-id",
                _mssql_params_json(),
                "--target-schema",
                "replay_target",
                "--target-table",
                "orders",
                "--staging-schema",
                "replay_stage",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["executed"] is True
    assert payload["state_committed"] is True
    conn = pyodbc.connect(connection_string, autocommit=True)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, value FROM replay_target.orders ORDER BY id")
        rows = [tuple(row) for row in cursor.fetchall()]
        cursor.execute("SELECT status FROM etl_state.__dpone__loads WHERE run_id = ?", run_id)
        status = cursor.fetchone()[0]
        cursor.close()
    finally:
        conn.close()

    assert rows == [(1, "new"), (2, "inserted")]
    assert status == "committed"


def _postgres_conninfo() -> str:
    return (
        f"host={os.getenv('DPONE_IT_PG_HOST', '127.0.0.1')} "
        f"port={os.getenv('DPONE_IT_PG_PORT_FORWARD', '55432')} "
        f"dbname={os.getenv('DPONE_IT_PG_DATABASE', 'dpone_it')} "
        f"user={os.getenv('DPONE_IT_PG_USER', 'dpone')} "
        f"password={os.getenv('DPONE_IT_PG_PASSWORD', 'dpone')}"
    )


def _postgres_params_json() -> str:
    return json.dumps(
        {
            "host": os.getenv("DPONE_IT_PG_HOST", "127.0.0.1"),
            "port": int(os.getenv("DPONE_IT_PG_PORT_FORWARD", "55432")),
            "database": os.getenv("DPONE_IT_PG_DATABASE", "dpone_it"),
            "username": os.getenv("DPONE_IT_PG_USER", "dpone"),
            "password": os.getenv("DPONE_IT_PG_PASSWORD", "dpone"),
        }
    )


def _kafka_bootstrap_servers() -> str:
    return os.getenv("DPONE_IT_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:59092")


def _kafka_params_json() -> str:
    return json.dumps({"bootstrap_servers": _kafka_bootstrap_servers()})


def _clickhouse_params_json() -> str:
    return json.dumps(
        {
            "host": os.getenv("DPONE_IT_CH_HOST", "127.0.0.1"),
            "port": int(os.getenv("DPONE_IT_CH_PORT_FORWARD", "59000")),
            "database": os.getenv("DPONE_IT_CH_DATABASE", "dpone_it"),
            "username": os.getenv("DPONE_IT_CH_USER", "default"),
            "password": os.getenv("DPONE_IT_CH_PASSWORD", "dpone"),
        }
    )


def _mssql_params_json() -> str:
    return json.dumps(
        {
            "host": os.getenv("DPONE_IT_MSSQL_HOST", "127.0.0.1"),
            "port": int(os.getenv("DPONE_IT_MSSQL_PORT_FORWARD", "51433")),
            "database": os.getenv("DPONE_IT_MSSQL_DATABASE", "master"),
            "username": os.getenv("DPONE_IT_MSSQL_USER", "sa"),
            "password": os.getenv("DPONE_IT_MSSQL_PASSWORD", "Dp0ne.Strong.Pw.2026!"),
            "driver": "ODBC Driver 18 for SQL Server",
            "encrypt": "yes",
            "trust_server_certificate": "yes",
        }
    )


def _mssql_connection_string() -> str:
    return (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.getenv('DPONE_IT_MSSQL_HOST', '127.0.0.1')},{os.getenv('DPONE_IT_MSSQL_PORT_FORWARD', '51433')};"
        f"DATABASE={os.getenv('DPONE_IT_MSSQL_DATABASE', 'master')};"
        f"UID={os.getenv('DPONE_IT_MSSQL_USER', 'sa')};"
        f"PWD={os.getenv('DPONE_IT_MSSQL_PASSWORD', 'Dp0ne.Strong.Pw.2026!')};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )


def _wait_for_mssql(pyodbc_module, connection_string: str) -> None:
    import time

    deadline = time.monotonic() + 90
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = pyodbc_module.connect(connection_string, timeout=5, autocommit=True)
            conn.close()
            return
        except Exception as exc:  # pragma: no cover - service readiness loop
            last_exc = exc
            time.sleep(2)
    raise RuntimeError(f"MSSQL service did not become ready: {last_exc}")
