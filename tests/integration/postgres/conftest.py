"""PostgreSQL integration-suite composition."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from tests.integration.postgres.postgres_live_support import (
    ensure_mssql_database_and_schemas,
    mssql_connector,
    wait_until_ready,
)
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    GovernedMssqlCampaign,
    governed_mssql_campaign,
)


@pytest.fixture
def governed_mssql_live_campaign() -> Iterator[GovernedMssqlCampaign]:
    """Provide one disposable external governance authority per live matrix."""

    database = os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it")
    ensure_mssql_database_and_schemas(database=database)
    target = mssql_connector(database=database)
    wait_until_ready("mssql", lambda: target.get_records("SELECT 1"))
    try:
        with governed_mssql_campaign(target, target_database=database) as campaign:
            yield campaign
    finally:
        target.close()
