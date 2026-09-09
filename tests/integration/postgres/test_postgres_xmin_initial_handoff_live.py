"""Real PostgreSQL/MSSQL proof for the initial-to-incremental XMin handoff."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from dpone.backfill.state import BackfillChunkRecord, BackfillLedger, FileBackfillStateStore
from dpone.backfill.xmin_handoff import PostgresXminInitialHandoffLifecycle
from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.state.mssql_xmin_handoff import MssqlXminHandoffCommitter
from tests.integration.postgres.postgres_live_support import postgres_mssql_enabled
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    STATE_SCHEMA,
    TARGET_SCHEMA,
    TARGET_TABLE,
    provision_snapshot_route,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
]


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_chunked_initial_handoff_seeds_and_admits_separate_incremental_live(tmp_path: Path) -> None:
    """Use real vendor identities, XMin state CAS, receipt, and admission."""

    with provision_snapshot_route(tmp_path / "artifacts") as route:
        initial = _phase_config(route.load_config, mode="initial")
        state_storage = route.processor.sink.state_storage
        source = route.processor.source.build_xmin_initial_handoff_source(state_storage)
        lifecycle = PostgresXminInitialHandoffLifecycle(
            source=source,
            committer=MssqlXminHandoffCommitter(
                target_connector=route.target,
                state_storage=state_storage,
                load_config=initial,
            ),
        )
        store = FileBackfillStateStore(tmp_path / "ledger")
        ledger = _ledger()

        lifecycle.before_chunks(initial, ledger, store)
        assert ledger.xmin_handoff is not None
        assert ledger.xmin_handoff.status == "anchored"

        incremental = _phase_config(route.load_config, mode="incremental")
        with pytest.raises(RuntimeError, match="postgres_xmin_handoff.not_committed"):
            route.processor.source.get_incremental_state(incremental)

        for chunk in ledger.chunks:
            chunk.status = "success"
        store.save(ledger)

        evidence = lifecycle.after_chunks(initial, ledger, store)
        admitted = route.processor.source.get_incremental_state(incremental)

        assert evidence["status"] == "committed"
        assert evidence["candidate_revision"] == 1
        assert admitted.revision == 1
        assert admitted.xmin_value == evidence["anchor_xmin"]
        assert route.state.get_records(
            f"SELECT COUNT_BIG(*) AS n FROM [{route.state_database}].[{STATE_SCHEMA}].[dpone_source_state]",
            as_dict=True,
        ) == [{"n": 1}]
        assert route.state.get_records(
            f"SELECT COUNT_BIG(*) AS n FROM [{route.state_database}].[{STATE_SCHEMA}].[dpone_commit_receipt]",
            as_dict=True,
        ) == [{"n": 1}]

        replay = lifecycle.after_chunks(initial, ledger, store)
        assert replay == evidence


def _phase_config(base, *, mode: str):
    options = {
        **base.options,
        "xmin_execution": {"mode": mode, "handoff_id": "orders_history_v1"},
    }
    if mode == "initial":
        options["backfill"] = {
            "inner_mode": "incremental_merge",
            "parallel_workers": 1,
            "state": {"backend": "audit_schema", "require_distributed_lock": True},
            "chunk": {"column": "metric_code", "from": 1, "to": 2, "step": 1},
        }
        return replace(base, load_strategy=LoadStrategy.BACKFILL, options=options)
    return replace(base, load_strategy=LoadStrategy.INCREMENTAL_MERGE, options=options)


def _ledger() -> BackfillLedger:
    return BackfillLedger(
        run_key="orders-history-v1",
        dataset=f"{TARGET_SCHEMA}.{TARGET_TABLE}",
        inner_mode="incremental_merge",
        plan_hash="sha256:vendor-plan",
        config_hash="sha256:vendor-config",
        chunk_config={"column": "metric_code"},
        chunks=[
            BackfillChunkRecord(
                index=index,
                start=str(index),
                end=str(index),
                idempotency_key=f"chunk-{index}",
            )
            for index in (1, 2)
        ],
    )
