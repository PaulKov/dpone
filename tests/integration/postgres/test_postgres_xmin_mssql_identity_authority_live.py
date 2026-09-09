"""Frozen target-identity and repair-authority vendor-live certification."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from tests.integration.postgres.postgres_live_support import postgres_mssql_enabled
from tests.integration.postgres.postgres_xmin_mssql_identity_live_certification import (
    REQUIRED_CASES,
    run_identity_authority_certification,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
    pytest.mark.route_live_wide,
]


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_target_identity_and_repair_authority_via_real_vendors(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Require complete real-vendor execution; skipped or partial is never PASS."""

    completed = run_identity_authority_certification(
        tmp_path / "identity-authority",
        route_live_recorder=route_live_recorder,
    )

    assert completed == REQUIRED_CASES
