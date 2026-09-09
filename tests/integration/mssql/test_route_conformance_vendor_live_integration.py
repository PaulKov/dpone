from __future__ import annotations

import os
from pathlib import Path

import pytest

from dpone.ops.route_conformance_live import RouteConformanceLiveService
from dpone.ops.routes import RouteConformanceDatasetProfile, RouteConformanceLiveConfig

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
    pytest.mark.integration_clickhouse,
]

_TRUTHY = {"1", "true", "yes", "on"}
_ROWS = 10_000
_COLUMNS = 200


def _truthy_env(name: str) -> bool:
    return str(os.getenv(name, "0")).strip().lower() in _TRUTHY


pytestmark = [
    *pytestmark,
    pytest.mark.skipif(
        not _truthy_env("DPONE_RUN_INTEGRATION"),
        reason="Integration tests are disabled. Set DPONE_RUN_INTEGRATION=1 to enable them.",
    ),
    pytest.mark.skipif(
        not _truthy_env("DPONE_VENDOR_LIVE"),
        reason="Vendor-live route conformance is disabled. Set DPONE_VENDOR_LIVE=1 to enable it.",
    ),
]


@pytest.mark.parametrize(
    ("source", "sink"),
    [
        ("postgres", "mssql"),
        ("mssql", "clickhouse"),
    ],
)
def test_vendor_live_route_conformance_verifies_wide_exact_contracts(
    source: str,
    sink: str,
    tmp_path: Path,
) -> None:
    report = RouteConformanceLiveService().run(
        output_dir=tmp_path / f"{source}_to_{sink}",
        source=source,
        sink=sink,
        strategy="incremental_merge",
        config=RouteConformanceLiveConfig(
            adapter="vendor_live",
            dataset=RouteConformanceDatasetProfile(
                name="wide_vendor_contract",
                row_count=_ROWS,
                column_count=_COLUMNS,
                chunk_size=1_000,
                include_nested=True,
                include_schema_evolution=True,
            ),
            min_rows=_ROWS,
            min_columns=_COLUMNS,
            require_schema_evolution=True,
        ),
    )

    assert report.passed is True
    assert report.verification.source_rows == _ROWS
    assert report.verification.sink_rows == _ROWS
    assert report.verification.chunk_count == 10
    assert report.schema_evolution["status"] == "verified"
    assert report.blockers == ()
