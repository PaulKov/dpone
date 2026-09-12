"""Real server cancellation of COPY must leave a reusable prepared source."""

from __future__ import annotations

import csv
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path

import pytest
from psycopg import errors, sql
from psycopg.pq import TransactionStatus

from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.postgres_mssql_source_schema_runtime import PostgresMssqlSourceSchemaRuntimeV1
from dpone.runtime.sources.postgres_mssql_source_schema_issuer import PostgresMssqlRelationSchemaAuthorityIssuerV1
from dpone.runtime.sources.postgres_mssql_source_schema_projection import PostgresMssqlSourceSchemaProjectionAdapterV1
from dpone.runtime.sources.postgres_source_authority import PostgresSourceAuthorityVerifier
from dpone.runtime.sources.postgres_verified_relation_snapshot import PostgresVerifiedRelationSnapshotIssuerV1
from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy
from dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary import build_r1_postgres_fetched_schema
from dpone.runtime.sources.strategies.postgres.postgres_whole_file_export_service import PostgresWholeFileExportService
from tests.integration.postgres.postgres_live_support import postgres_connector, postgres_enabled
from tests.integration.postgres.test_postgres_catalog_source_authority_live import _resolved_connection
from tests.integration.postgres.test_postgres_exported_snapshot_lifecycle_live import NoopLogger, _config
from tests.test_postgres_mssql_r1_source_schema_runtime import _policy

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.skipif(not postgres_enabled(), reason="local PostgreSQL vendor environment is disabled"),
]


def _wait_for_sleeping_copy(admin, backend_pid: int, future: Future) -> None:
    """Synchronize against observed server execution, with a bounded deadline."""
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        rows = admin.get_records(
            "SELECT state, wait_event, query FROM pg_catalog.pg_stat_activity WHERE pid=%s",
            (backend_pid,),
            as_dict=True,
        )
        if (
            len(rows) == 1
            and rows[0]["state"] == "active"
            and rows[0]["wait_event"] == "PgSleep"
            and rows[0]["query"].lstrip().upper().startswith("COPY (")
        ):
            return
        if future.done():
            future.result()
            pytest.fail("COPY completed before the server cancellation barrier")
        time.sleep(0.02)
    pytest.fail("active COPY did not reach the server cancellation barrier")


def test_real_prepared_copy_server_cancel_cleans_up_and_reuses_connection(tmp_path: Path) -> None:
    with ExitStack() as cleanup:
        admin = postgres_connector()
        cleanup.callback(admin.close)
        source = postgres_connector()
        cleanup.callback(source.close)
        table = "server_cancel_" + uuid.uuid4().hex[:12]
        relation = sql.Identifier("public", table)
        admin.execute_query(sql.SQL("CREATE TABLE {} (id bigint PRIMARY KEY)").format(relation))
        cleanup.callback(admin.execute_query, sql.SQL("DROP TABLE IF EXISTS {}").format(relation))
        admin.execute_query(sql.SQL("INSERT INTO {} VALUES (1), (2)").format(relation))
        facts = admin.get_records(
            """SELECT d.oid::bigint AS database_oid, r.oid::bigint AS principal_oid,
                n.oid::bigint AS namespace_oid, c.oid::bigint AS relation_oid
                FROM pg_catalog.pg_database d CROSS JOIN pg_catalog.pg_roles r
                CROSS JOIN pg_catalog.pg_namespace n JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid
                WHERE d.datname=current_database() AND r.rolname=current_user
                AND n.nspname='public' AND c.relname=%s""",
            (table,),
            as_dict=True,
        )
        assert len(facts) == 1
        verifier = PostgresSourceAuthorityVerifier.from_connection(
            _resolved_connection(database=admin.database, role=admin.user, table=table, fact=facts[0])
        )
        runtime = PostgresMssqlSourceSchemaRuntimeV1(
            verifier=verifier,
            snapshot_scope_issuer=PostgresVerifiedRelationSnapshotIssuerV1(),
            schema_authority_issuer=PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy()),
            projection_adapter=PostgresMssqlSourceSchemaProjectionAdapterV1(
                fetched_schema_factory=build_r1_postgres_fetched_schema
            ),
        )
        # This deadline is an emergency backstop; the assertion requires an observed
        # PgSleep and a successful explicit admin cancellation well before it.
        source.execute_query("SET statement_timeout='15000ms'")
        backend_pid = source.get_records("SELECT pg_backend_pid() AS pid", as_dict=True)[0]["pid"]
        physical = source.connection
        config = _config(table, tmp_path)
        config.source_schema = "public"
        lifecycle = ExtractionLifecycleAuthority()
        boundary = runtime.prepare_boundary(connector=source, lifecycle=lifecycle, load_config=config)
        cleanup.callback(boundary.close_if_active)
        service = PostgresWholeFileExportService(PostgresFullExtractStrategy(source, NoopLogger()))
        query = sql.SQL("SELECT id FROM ONLY {} WHERE pg_sleep(30) IS NULL").format(relation)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                service.export_full,
                query,
                [("id", "bigint")],
                config,
                prepared_boundary=boundary,
                relation_schema=[("id", "bigint")],
            )
            try:
                _wait_for_sleeping_copy(admin, backend_pid, future)
                assert (
                    admin.get_records("SELECT pg_cancel_backend(%s) AS cancelled", (backend_pid,), as_dict=True)[0][
                        "cancelled"
                    ]
                    is True
                )
                with pytest.raises(errors.QueryCanceled) as caught:
                    future.result(timeout=5)
                assert caught.value.sqlstate == "57014"
            finally:
                if not future.done():
                    admin.get_records("SELECT pg_cancel_backend(%s)", (backend_pid,))
        terminal = boundary.terminal_receipt
        assert terminal is not None
        assert terminal.outcome == "aborted"
        assert terminal.cleanup_attempted and terminal.cleanup_succeeded
        assert terminal.cleanup_error_reason is None
        assert not terminal.connection_quarantined
        assert not lifecycle.receipt.complete
        assert not [path for path in tmp_path.rglob("*") if path.is_file()]
        assert source.connection is physical
        assert physical.info.transaction_status == TransactionStatus.IDLE
        boundary.close_if_active()
        assert boundary.terminal_receipt is terminal
        assert physical.info.transaction_status == TransactionStatus.IDLE
        # ACCESS EXCLUSIVE conflicts with the prepared relation's ACCESS SHARE lock.
        admin.begin()
        try:
            admin.execute_query(sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE NOWAIT").format(relation))
        finally:
            admin.rollback()
        fresh_lifecycle = ExtractionLifecycleAuthority()
        fresh = runtime.prepare_boundary(connector=source, lifecycle=fresh_lifecycle, load_config=config)
        cleanup.callback(fresh.close_if_active)
        artifact = service.export_full(
            sql.SQL("SELECT id FROM ONLY {} ORDER BY id").format(relation),
            [("id", "bigint")],
            config,
            prepared_boundary=fresh,
            relation_schema=[("id", "bigint")],
        )
        cleanup.callback(Path(artifact.file_path).unlink, missing_ok=True)
        with Path(artifact.file_path).open(newline="") as stream:
            assert list(csv.reader(stream)) == [["1"], ["2"]]
        assert artifact.require_integrity_receipt().rows_exported == 2
        assert fresh.terminal_receipt.outcome == "completed"
        assert fresh_lifecycle.receipt.complete
        assert source.connection is physical
        assert physical.info.transaction_status == TransactionStatus.IDLE
