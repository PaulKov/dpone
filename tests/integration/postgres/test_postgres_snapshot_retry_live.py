"""Vendor-live proof that snapshot retry replaces the PostgreSQL session."""

from __future__ import annotations

import pytest
from psycopg import errors

from dpone.runtime.sources.strategies.postgres.postgres_snapshot_failure import (
    add_redacted_secondary_note,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_retry import (
    PostgresSnapshotRetryRunner,
)
from tests.integration.postgres.postgres_live_support import (
    NoopLogger,
    postgres_connector,
    postgres_enabled,
)

pytestmark = [pytest.mark.integration, pytest.mark.integration_live, pytest.mark.integration_postgres]


def test_snapshot_retry_replaces_real_postgres_backend() -> None:
    """Inject one 40001 after a real RR read and prove reconnect isolation."""

    if not postgres_enabled():
        pytest.skip("PostgreSQL vendor service is disabled")

    connector = postgres_connector()
    backend_pids: list[int] = []
    attempt = 0

    def source_snapshot_attempt() -> str:
        nonlocal attempt
        attempt += 1
        connector.begin()
        row = connector.get_records(
            "SELECT pg_backend_pid() AS backend_pid, txid_current_snapshot()::text AS snapshot_token",
            as_dict=True,
        )[0]
        backend_pids.append(int(row["backend_pid"]))
        if attempt == 1:
            raise errors.SerializationFailure("injected transient snapshot conflict")
        connector.rollback()
        return str(row["snapshot_token"])

    try:
        token = PostgresSnapshotRetryRunner(
            sleeper=lambda _delay: None,
            random_unit=lambda: 0.0,
            secondary_failure_recorder=add_redacted_secondary_note,
        ).run(
            source_snapshot_attempt,
            connector=connector,
            logger=NoopLogger(),
        )
    finally:
        connector.close()

    assert token
    assert len(backend_pids) == 2
    assert backend_pids[0] != backend_pids[1]
