"""Lifecycle for generated ClickHouse cluster evidence."""

from __future__ import annotations

import os
import time
from urllib.request import Request, urlopen

import pytest

from tests.integration.clickhouse_cluster import evidence


@pytest.fixture(scope="session", autouse=True)
def _fresh_cluster_publication_receipt() -> None:
    if os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") == "1":
        _prepare_cluster_publication_session()


def _prepare_cluster_publication_session() -> None:
    evidence.reset_receipt()
    evidence.reset_external_receipt()
    evidence.reset_external_performance_receipt()
    _wait_for_distributed_ddl()


def _wait_for_distributed_ddl() -> None:
    """Wait beyond the image HTTP health check for Keeper-backed DDL readiness."""

    statement = "DROP TABLE IF EXISTS default.__dpone_cluster_readiness ON CLUSTER publication_cluster"
    deadline = time.monotonic() + 30
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            request = Request("http://127.0.0.1:18123/", data=statement.encode(), method="POST")
            with urlopen(request, timeout=10):  # noqa: S310 - fixed local Docker endpoint
                return
        except Exception as exc:  # pragma: no cover - exercised only during container startup
            last_error = exc
            time.sleep(0.5)
    raise AssertionError("ClickHouse distributed DDL did not become ready") from last_error
