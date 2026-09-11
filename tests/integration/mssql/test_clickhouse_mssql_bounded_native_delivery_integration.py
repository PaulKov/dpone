"""Real source/BCP/target fidelity and durable recovery, only after explicit approval."""

from __future__ import annotations

import pytest
from tests.integration.mssql.clickhouse_mssql_delivery_support import assert_live_report, live_factory, redacted_live
from tools.native_delivery_live_support.artifacts import ArtifactStore
from tools.native_delivery_live_support.execution import ExecutionAdapter
from tools.native_delivery_live_support.profiles import PROFILES, Dataset
from tools.native_delivery_live_support.runner import run_benchmark

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql, pytest.mark.integration_clickhouse]


@pytest.mark.parametrize("profile", PROFILES)
@pytest.mark.parametrize("strategy", ["full_refresh", "partition_replace"])
@redacted_live
def test_real_bounded_delivery_fidelity_and_recovery(tmp_path, profile, strategy):
    factory, config, route = live_factory(strategy, "bounded_native")
    report = run_benchmark(
        adapter=ExecutionAdapter(factory, "candidate"),
        dataset=Dataset(profile, 64),
        config=config,
        route=route,
        store=ArtifactStore(tmp_path / "run.json"),
    )
    assert_live_report(report, execution=factory.execution)
