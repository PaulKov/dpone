"""Lifecycle for generated ClickHouse cluster evidence."""

from __future__ import annotations

import os

import pytest

from tests.integration.clickhouse_cluster.evidence import reset_receipt


@pytest.fixture(scope="session", autouse=True)
def _fresh_cluster_publication_receipt() -> None:
    if os.getenv("DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION") == "1":
        reset_receipt()
