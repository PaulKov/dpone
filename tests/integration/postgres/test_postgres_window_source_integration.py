"""Real PostgreSQL exported-snapshot consistency using synthetic local fixtures."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from dpone.contracts.bounded_window import WindowContractError, WindowPlan
from dpone.runtime.sources.postgres_window_source import PostgresWindowSource

pytestmark = [pytest.mark.integration, pytest.mark.integration_postgres]


def test_shared_snapshot_survives_source_updates(postgres_settings, postgres_schema):
    import psycopg
    from psycopg import sql

    def connect():
        return psycopg.connect(
            host=postgres_settings.host,
            port=postgres_settings.port,
            dbname=postgres_settings.database,
            user=postgres_settings.user,
            password=postgres_settings.password,
        )

    relation = sql.Identifier(postgres_schema, "window_source")
    start = datetime(2026, 1, 1, tzinfo=UTC)
    middle = start + timedelta(days=1)
    end = start + timedelta(days=2)
    with connect() as admin:
        admin.execute(sql.SQL("CREATE TABLE {} (id int, at timestamptz, amount numeric(8,2))").format(relation))
        with admin.cursor() as cursor:
            cursor.executemany(
                sql.SQL("INSERT INTO {} VALUES (%s,%s,%s)").format(relation),
                [
                    (1, start, "1.25"),
                    (1, start, "1.25"),
                    (2, middle, None),
                    (3, end, "3.00"),
                    (4, None, "4.00"),
                ],
            )

    def make_source():
        return PostgresWindowSource(
            connection_factory=connect,
            schema_name=postgres_schema,
            table_name="window_source",
            columns=("id", "at", "amount"),
            window_column="at",
            schema_fingerprint="synthetic_target",
            batch_rows=1,
        )

    with make_source() as reader:
        frozen = WindowPlan(
            "synthetic_route",
            "synthetic_target",
            start,
            end,
            (start, middle, end),
            reader.schema_fingerprint,
            reader.source_version,
            reader.parameters_fingerprint,
            2,
            window_column="at",
        )
        # Another transaction changes rows after the snapshot has been fixed.
        with connect() as writer:
            writer.execute(sql.SQL("DELETE FROM {} WHERE id=1").format(relation))
            writer.execute(sql.SQL("INSERT INTO {} VALUES (5,%s,5.00)").format(relation), (start,))
        with ThreadPoolExecutor(max_workers=2) as pool:
            outputs = list(pool.map(lambda chunk: list(reader.read(frozen, chunk)), frozen.chunks))
        assert [row[0] for row in outputs[0]] == [1, 1]
        assert [row[0] for row in outputs[1]] == [2]
        assert str(outputs[0][0][2]) == "1.25"
        assert outputs[1][0][2] is None
        empty = replace(frozen, start=end, end=end + timedelta(days=1), boundaries=(end, end + timedelta(days=1)))
        assert [row[0] for row in reader.read(empty, empty.chunks[0])] == [3]
    with pytest.raises(WindowContractError, match="expired"):
        reader.validate(frozen)
    with make_source() as restarted:
        assert restarted.source_version != frozen.source_version
        with pytest.raises(WindowContractError, match="identity"):
            restarted.validate(frozen)


def test_terminated_keeper_rejects_new_worker(postgres_settings, postgres_schema):
    import psycopg
    from psycopg import sql

    connections = []

    def connect():
        connection = psycopg.connect(
            host=postgres_settings.host,
            port=postgres_settings.port,
            dbname=postgres_settings.database,
            user=postgres_settings.user,
            password=postgres_settings.password,
        )
        connections.append(connection)
        return connection

    with connect() as admin:
        admin.execute(sql.SQL("CREATE TABLE {} (at timestamptz)").format(sql.Identifier(postgres_schema, "terminated")))
    with PostgresWindowSource(
        connection_factory=connect,
        schema_name=postgres_schema,
        table_name="terminated",
        columns=("at",),
        window_column="at",
        schema_fingerprint="target",
    ) as reader:
        keeper = connections[-1]
        start = datetime(2026, 1, 1, tzinfo=UTC)
        end = start + timedelta(days=1)
        frozen = WindowPlan(
            "route",
            "target",
            start,
            end,
            (start, end),
            reader.schema_fingerprint,
            reader.source_version,
            reader.parameters_fingerprint,
            window_column="at",
        )
        with connect() as admin:
            assert admin.execute("SELECT pg_terminate_backend(%s)", (keeper.info.backend_pid,)).fetchone()[0]
        with pytest.raises(WindowContractError, match="unavailable"):
            list(reader.read(frozen, frozen.chunks[0]))
