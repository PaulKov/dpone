"""Real two-worker proof for the generic MSSQL target transaction fence."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.postgres.postgres_live_support import postgres_mssql_enabled
from tests.integration.postgres.postgres_mssql_backfill_orchestration_live_support import (
    apply_runtime_environment,
    backfill_load_config,
    invoke_public_process,
    operational_image,
    reviewed_backfill_cases,
    seed_target_with_public_runner,
    semantic_image,
)
from tests.integration.postgres.postgres_mssql_production_hydration_live_support import (
    production_hydration_live_fixture,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
]


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_postgres_mssql_parallel_target_fence_serializes_real_mutation_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Release two staged workers together and prove one-at-a-time target DML."""

    case = next(item for item in reviewed_backfill_cases() if item.case_id == "incremental_merge__workers_2__success")
    case_root = tmp_path / "parallel-target-fence"
    with production_hydration_live_fixture(case_root) as live:
        apply_runtime_environment(monkeypatch, live)
        baseline = seed_target_with_public_runner(live)
        assert baseline.status == "success"
        before = semantic_image(live)
        load_config = backfill_load_config(
            live,
            case,
            state_dir=case_root / "backfill-state",
        )
        invocation = invoke_public_process(
            live,
            load_config,
            invocation_id="backfill-target-fence-barrier",
        )
        after = semantic_image(live)
        operational = operational_image(live, live.transfer_root)

        assert invocation.error is None, invocation.traceback
        assert invocation.result is not None
        assert invocation.result.status == "success"
        chunks = invocation.result.details["backfill"]["chunks"]
        fences = [chunk["execution_evidence"]["mssql_target_fence_evidence"] for chunk in chunks]
        assert len(fences) == 3, fences
        assert {fence["schema"] for fence in fences} == {"dpone.mssql.target-fence-evidence.v1"}
        assert {fence["mode"] for fence in fences} == {"transaction_exclusive_applock"}
        assert {fence["lock_owner"] for fence in fences} == {"transaction"}
        assert {fence["lock_mode"] for fence in fences} == {"exclusive"}
        assert len({fence["resource_sha256"] for fence in fences}) == 1
        assert len({fence["session_id"] for fence in fences[:2]}) == 2, fences
        assert len(after["committed_receipts"]) == len(before["committed_receipts"]) + 3
        assert after["committed_chunks"] == (1, 2, 3)
        assert operational["staging_objects"] == ()
        assert operational["transfer_files"] == ()
        worker_spids = {int(fence["session_id"]) for fence in fences}
        live_spids = {int(row["session_id"]) for row in operational["target_sessions"]}
        assert worker_spids.isdisjoint(live_spids), (fences, operational["target_sessions"])
