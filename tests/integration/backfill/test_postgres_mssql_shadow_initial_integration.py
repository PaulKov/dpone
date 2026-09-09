"""Bounded local certification for resumable MSSQL shadow initial loads.

The test deliberately uses ten one-row chunks and four fixed worker lanes.  It
injects a crash after the first target receipt but before the campaign-ledger
CAS, then starts a fresh public process invocation.  The retry must recover the
receipt without reading PostgreSQL again, complete the remaining chunks, and
publish the fully validated shadow in one SQL Server transaction.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import time
from contextlib import suppress
from dataclasses import replace
from datetime import UTC, datetime
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import pytest
from tests.integration.postgres.postgres_mssql_backfill_lifecycle_live_support import (
    ReviewedBackfillLifecycleController,
    ReviewedBackfillOrchestratorFactory,
    ReviewedHardDeathParentPayload,
    reviewed_short_campaign_lease,
    run_reviewed_hard_death_parent,
)
from tests.integration.postgres.postgres_mssql_backfill_orchestration_live_support import (
    apply_runtime_environment,
    invoke_public_process,
)
from tests.integration.postgres.postgres_mssql_production_hydration_live_support import (
    SOURCE_SCHEMA,
    STATE_SCHEMA,
    TARGET_SCHEMA,
    production_hydration_live_fixture,
)

from dpone.config import LoadStrategy

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_backfill,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
]

if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {
    "1",
    "true",
    "yes",
    "on",
}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)


def test_ten_chunk_four_lane_shadow_initial_resumes_and_publishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove the pre-deploy smoke contract on disposable vendor databases."""

    _require_vendor_drivers()
    with production_hydration_live_fixture(tmp_path) as live:
        apply_runtime_environment(monkeypatch, live)
        _seed_ten_rows(live)
        _create_reviewed_live_target(live)
        _create_retained_legacy_shadow(live)
        _provision_xmin_state(live)
        runtime_config = _xmin_runtime_config(live)
        load_config = replace(
            _shadow_initial_config(live, state_dir=tmp_path / "backfill-state"),
            source_schema=SOURCE_SCHEMA.upper(),
            source_table=live.source_table.upper(),
        )
        crash_marker = tmp_path / "native-crash-after-receipt.marker"
        crash_events = tmp_path / "native-crash-events.jsonl"
        lifecycle = ReviewedBackfillLifecycleController(
            "retry_resume",
            native_crash_marker=crash_marker,
            native_crash_events=crash_events,
        )
        factory = ReviewedBackfillOrchestratorFactory(lifecycle)

        first = invoke_public_process(
            live,
            load_config,
            invocation_id="shadow-initial-smoke",
            backfill_orchestrator_factory=factory,
            runtime_config=runtime_config,
        )

        assert first.error is not None
        assert "DPONE_BACKFILL_PROCESS_LANE_NATIVE_EXIT" in str(first.error)
        assert "SIGSEGV" in str(first.error)
        assert _count(live, live.target_table) == 0
        assert _count(live, _shadow_name(live.target_table)) == 1
        assert _count(live, _legacy_shadow_name(live.target_table)) == 0
        first_receipts = _shadow_receipts(live)
        _assert_shadow_receipts(live, first_receipts, expected_count=1)
        first_receipt_id = str(first_receipts[0]["receipt_id"])

        lifecycle.begin_retry()
        resumed = invoke_public_process(
            live,
            load_config,
            invocation_id="shadow-initial-smoke",
            scheduler_run_id="shadow-initial-smoke-retry",
            backfill_orchestrator_factory=factory,
            runtime_config=runtime_config,
        )

        assert resumed.error is None, resumed.traceback
        assert resumed.result is not None
        assert resumed.result.status == "success"
        assert _count(live, live.target_table) == 10
        assert _count(live, _backup_name(live.target_table)) == 0
        assert not _table_exists(live, _shadow_name(live.target_table))
        assert _table_exists(live, _legacy_shadow_name(live.target_table))
        assert _count(live, _legacy_shadow_name(live.target_table)) == 0
        assert _duplicate_ids(live) == 0
        receipts = _shadow_receipts(live)
        _assert_shadow_receipts(live, receipts, expected_count=10)
        assert first_receipt_id in {str(receipt["receipt_id"]) for receipt in receipts}
        details = resumed.result.details["backfill"]
        assert details["chunks_total"] == 10
        assert details["chunks_committed"] == 10
        assert details["progress"]["committed"] == 10
        assert details["progress"]["rows_loaded"] == 10
        assert details["publication"] == {
            "mode": "shadow_swap",
            "phase": "published",
            "validation": "passed",
            "expected_rows": 10,
            "actual_rows": 10,
            "duplicate_keys": 0,
            "receipt_id": details["publication"]["receipt_id"],
            "backup_retained": True,
        }
        chunks = details["chunks"]
        recovered = chunks[0]["execution_evidence"]["mssql_receipt_recovery"]
        assert recovered == {
            "schema": "dpone.mssql.receipt-recovery-evidence.v1",
            "status": "committed",
            "source_io_replayed": False,
            "rows": 1,
        }
        typed = [chunk["execution_evidence"]["mssql_staging_evidence"] for chunk in chunks[1:]]
        assert len(typed) == 9
        assert {evidence["typed_file_ingestion"] for evidence in typed} == {True}
        assert {evidence["direct_native_staging"] for evidence in typed} == {True}
        assert {evidence["transport"] for evidence in typed} == {"mssql.bcp.length_prefixed_utf8.v1"}
        events = [json.loads(line) for line in crash_events.read_text(encoding="utf-8").splitlines()]
        assert len([event for event in events if event["event"] == "sigsegv_after_receipt"]) == 1
        assert (
            len([event for event in events if event["event"] == "production_result" and event["chunk_index"] == 1]) == 1
        )


def test_parent_sigkill_immediately_takes_over_and_recovers_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove whole-parent death resumes before any durable lease expires."""

    if os.name != "posix" or not hasattr(os, "killpg") or not Path("/proc/self/stat").is_file():
        pytest.skip("parent SIGKILL certification requires a Linux container")
    _require_vendor_drivers()
    with production_hydration_live_fixture(tmp_path) as live:
        apply_runtime_environment(monkeypatch, live)
        _seed_ten_rows(live)
        _create_reviewed_live_target(live)
        _create_retained_legacy_shadow(live)
        _provision_xmin_state(live)
        runtime_config = _xmin_runtime_config(live)
        load_config = _shadow_initial_config(live, state_dir=tmp_path / "hard-death-state")
        crash_marker = tmp_path / "parent-sigkill-after-receipt.marker"
        crash_events = tmp_path / "parent-sigkill-events.jsonl"
        payload = ReviewedHardDeathParentPayload(
            load_config=load_config,
            runtime_config=runtime_config,
            crash_marker=str(crash_marker),
            events_path=str(crash_events),
            invocation_id="shadow-initial-parent-sigkill",
        )
        process = get_context("spawn").Process(
            target=run_reviewed_hard_death_parent,
            args=(payload,),
            name="dpone-reviewed-kpo-parent",
            daemon=False,
        )
        lane_pids: set[int] = set()
        process.start()
        try:
            events = _wait_for_parent_kill_marker(
                crash_marker,
                crash_events,
                process=process,
            )
            lane_pids = {int(event["pid"]) for event in events if event["event"] == "lane_opened"}
            assert len(lane_pids) == 4, events
            assert (
                len([event for event in events if event["event"] == "production_result" and event["chunk_index"] == 1])
                == 1
            )
            old_campaign = _latest_campaign_revision(live)
            old_chunk = _latest_chunk_revision(live, index=1)
            observed_at = datetime.now(UTC)
            old_campaign_expiry = datetime.fromisoformat(str(old_campaign["lock_expires_at"]))
            old_chunk_expiry = datetime.fromisoformat(str(old_chunk["lease_expires_at"]))
            assert old_campaign_expiry > observed_at
            assert old_chunk_expiry > observed_at
            assert old_chunk["status"] == "running"
            assert old_chunk["lease_owner"]
            assert _count(live, _shadow_name(live.target_table)) == 1

            os.kill(int(process.pid), signal.SIGKILL)
            process.join(timeout=5.0)
            assert process.exitcode == -signal.SIGKILL
            _kill_isolated_lane_groups(lane_pids)

            retry_lifecycle = ReviewedBackfillLifecycleController(
                "retry_resume",
                native_crash_marker=crash_marker,
                native_crash_events=crash_events,
                native_crash_mode="parent_sigkill",
            )
            retry_lifecycle.begin_retry()
            resumed = invoke_public_process(
                live,
                load_config,
                invocation_id="shadow-initial-parent-sigkill",
                scheduler_run_id="shadow-initial-parent-sigkill-retry",
                backfill_orchestrator_factory=ReviewedBackfillOrchestratorFactory(
                    retry_lifecycle,
                    campaign_lease_factory=reviewed_short_campaign_lease,
                ),
                runtime_config=runtime_config,
            )

            assert resumed.error is None, resumed.traceback
            assert resumed.result is not None and resumed.result.status == "success"
            takeover = _first_campaign_takeover(
                live,
                after_journal_id=int(old_campaign["journal_id"]),
                previous_owner=str(old_campaign["lock_owner"]),
            )
            assert takeover["loaded_at"] < old_campaign_expiry
            assert takeover["lock_owner"] != old_campaign["lock_owner"]
            assert _orphaned_chunk_revision_exists(
                live,
                after_journal_id=int(old_chunk["journal_id"]),
                index=1,
            )
            details = resumed.result.details["backfill"]
            assert details["chunks_committed"] == 10
            assert details["chunks"][0]["execution_evidence"]["mssql_transaction_replay_suppressed"] is True
            final_events = [json.loads(line) for line in crash_events.read_text(encoding="utf-8").splitlines()]
            chunk_one_results = [
                event for event in final_events if event["event"] == "production_result" and event["chunk_index"] == 1
            ]
            assert len(chunk_one_results) == 2
            assert len([event for event in chunk_one_results if event["replay_suppressed"] is False]) == 1
            assert len([event for event in chunk_one_results if event["replay_suppressed"] is True]) == 1
            assert _count(live, live.target_table) == 10
            assert _duplicate_ids(live) == 0
        finally:
            if process.is_alive():
                with suppress(ProcessLookupError):
                    os.kill(int(process.pid), signal.SIGKILL)
                process.join(timeout=5.0)
            _kill_isolated_lane_groups(lane_pids)
            if process.exitcode is not None:
                process.close()


def _wait_for_parent_kill_marker(
    marker: Path,
    events_path: Path,
    *,
    process: Any,
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + 60.0
    latest_events: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        if process.exitcode is not None:
            raise AssertionError(f"reviewed KPO parent exited before SIGKILL: {process.exitcode}")
        if events_path.is_file():
            latest_events = [
                json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines() if line.strip()
            ]
        opened = {int(event["worker_id"]) for event in latest_events if event["event"] == "lane_opened"}
        ready = any(event["event"] == "parent_sigkill_ready" for event in latest_events)
        if marker.is_file() and opened == {0, 1, 2, 3} and ready:
            return latest_events
        time.sleep(0.02)
    raise AssertionError(f"reviewed KPO parent did not reach the post-receipt SIGKILL window: {latest_events}")


def _kill_isolated_lane_groups(lane_pids: set[int]) -> None:
    for pid in sorted(lane_pids):
        try:
            process_group = os.getpgid(pid)
            session = os.getsid(pid)
        except ProcessLookupError:
            continue
        if process_group != pid or session != pid:
            raise AssertionError(
                f"refusing to signal an unauthenticated process tree: pid={pid} pgid={process_group} sid={session}"
            )
        with suppress(ProcessLookupError):
            os.killpg(process_group, signal.SIGKILL)

    deadline = time.monotonic() + 5.0
    executable_members = _executable_process_group_members(lane_pids)
    while executable_members and time.monotonic() < deadline:
        executable_members = _executable_process_group_members(lane_pids)
        if executable_members:
            time.sleep(0.02)
    if executable_members:
        raise AssertionError(f"reviewed KPO lane process groups survived SIGKILL: {executable_members}")


def _executable_process_group_members(process_groups: set[int]) -> dict[int, list[int]]:
    """Ignore unreaped zombies but reject every process that could still mutate."""

    members: dict[int, list[int]] = {}
    for stat_path in Path("/proc").glob("[0-9]*/stat"):
        with suppress(OSError, ValueError):
            stat = stat_path.read_text(encoding="utf-8")
            fields = stat[stat.rfind(")") + 2 :].split()
            state = fields[0]
            process_group = int(fields[2])
            if process_group in process_groups and state not in {"Z", "X"}:
                members.setdefault(process_group, []).append(int(stat_path.parent.name))
    return members


def _latest_campaign_revision(live: object) -> dict[str, Any]:
    rows = live.target.get_records(
        f"SELECT TOP (1) journal_id, details_json, __dpone__loaded_at "
        f"FROM [{live.target_database}].[{TARGET_SCHEMA}].[__dpone__backfill_campaigns] "
        "WHERE run_key = ? ORDER BY journal_id DESC",
        (_campaign_id(live.target_table),),
        as_dict=True,
    )
    assert len(rows) == 1
    return _journal_revision(rows[0])


def _latest_chunk_revision(live: object, *, index: int) -> dict[str, Any]:
    rows = live.target.get_records(
        f"SELECT TOP (1) journal_id, status, details_json, __dpone__loaded_at "
        f"FROM [{live.target_database}].[{TARGET_SCHEMA}].[__dpone__backfill_chunks] "
        "WHERE run_key = ? AND chunk_index = ? ORDER BY journal_id DESC",
        (_campaign_id(live.target_table), index),
        as_dict=True,
    )
    assert len(rows) == 1
    return _journal_revision(rows[0])


def _first_campaign_takeover(
    live: object,
    *,
    after_journal_id: int,
    previous_owner: str,
) -> dict[str, Any]:
    rows = live.target.get_records(
        f"SELECT journal_id, details_json, __dpone__loaded_at "
        f"FROM [{live.target_database}].[{TARGET_SCHEMA}].[__dpone__backfill_campaigns] "
        "WHERE run_key = ? AND journal_id > ? ORDER BY journal_id ASC",
        (_campaign_id(live.target_table), after_journal_id),
        as_dict=True,
    )
    for row in rows:
        revision = _journal_revision(row)
        if revision.get("lock_owner") and revision["lock_owner"] != previous_owner:
            return revision
    raise AssertionError("fresh invocation never acquired the orphaned campaign session fence")


def _orphaned_chunk_revision_exists(
    live: object,
    *,
    after_journal_id: int,
    index: int,
) -> bool:
    rows = live.target.get_records(
        f"SELECT journal_id, details_json, __dpone__loaded_at "
        f"FROM [{live.target_database}].[{TARGET_SCHEMA}].[__dpone__backfill_chunks] "
        "WHERE run_key = ? AND chunk_index = ? AND journal_id > ? ORDER BY journal_id ASC",
        (_campaign_id(live.target_table), index, after_journal_id),
        as_dict=True,
    )
    return any(_journal_revision(row).get("error") == "campaign_session_fence_orphaned" for row in rows)


def _journal_revision(row: dict[str, Any]) -> dict[str, Any]:
    details = json.loads(str(row["details_json"]))
    loaded_at = row["__dpone__loaded_at"]
    if isinstance(loaded_at, datetime) and (loaded_at.tzinfo is None or loaded_at.utcoffset() is None):
        loaded_at = loaded_at.replace(tzinfo=UTC)
    return {
        **details,
        "journal_id": int(row["journal_id"]),
        "loaded_at": loaded_at,
        **({"status": str(row["status"])} if "status" in row else {}),
    }


def _shadow_receipts(live: object) -> tuple[dict[str, Any], ...]:
    """Read immutable chunk receipts for the campaign-owned shadow target."""

    rows = live.state.get_records(
        f"""
        SELECT r.receipt_id, r.operation_key, r.scope_hash,
               r.payload_manifest_sha256, r.native_contract_sha256,
               r.declared_rows, r.actual_raw_rows, r.actual_native_rows,
               r.inserted_rows, r.updated_rows, r.total_rows, r.staging_rows,
               a.target_identity, a.target_database, a.target_schema,
               a.target_table, a.strategy
        FROM [{live.state_database}].[{STATE_SCHEMA}].[dpone_load_receipt] AS r
        INNER JOIN [{live.state_database}].[{STATE_SCHEMA}].[dpone_load_attempt] AS a
          ON a.attempt_key = r.attempt_key
        WHERE a.target_database = ?
          AND a.target_schema = ?
          AND a.target_table = ?
          AND a.strategy = ?
        ORDER BY r.receipt_id
        """,
        (
            live.target_database,
            TARGET_SCHEMA,
            _shadow_name(live.target_table),
            LoadStrategy.BACKFILL.value,
        ),
        as_dict=True,
    )
    return tuple(dict(row) for row in rows)


def _assert_shadow_receipts(
    live: object,
    receipts: tuple[dict[str, Any], ...],
    *,
    expected_count: int,
) -> None:
    """Require exact native row parity and one immutable identity per chunk."""

    assert len(receipts) == expected_count
    for receipt in receipts:
        assert (
            int(receipt["declared_rows"]),
            int(receipt["actual_raw_rows"]),
            int(receipt["actual_native_rows"]),
            int(receipt["staging_rows"]),
            int(receipt["inserted_rows"]),
            int(receipt["total_rows"]),
        ) == (1, 1, 1, 1, 1, 1)
        assert int(receipt["updated_rows"]) == 0
        assert receipt["target_database"] == live.target_database
        assert receipt["target_schema"] == TARGET_SCHEMA
        assert receipt["target_table"] == _shadow_name(live.target_table)
        assert receipt["strategy"] == LoadStrategy.BACKFILL.value
        for field in (
            "operation_key",
            "scope_hash",
            "payload_manifest_sha256",
            "native_contract_sha256",
            "target_identity",
        ):
            assert len(bytes(receipt[field])) == 32

    assert len({bytes(receipt["operation_key"]) for receipt in receipts}) == expected_count
    assert len({bytes(receipt["scope_hash"]) for receipt in receipts}) == expected_count
    assert len({bytes(receipt["payload_manifest_sha256"]) for receipt in receipts}) == expected_count
    assert len({bytes(receipt["native_contract_sha256"]) for receipt in receipts}) == 1
    assert len({bytes(receipt["target_identity"]) for receipt in receipts}) == 1


def _seed_ten_rows(live: object) -> None:
    for identifier in range(4, 11):
        live.postgres.execute_query(
            f'INSERT INTO "{SOURCE_SCHEMA}"."{live.source_table}" '
            "(id, metric_code, metric_value, note) VALUES (%s, %s, %s, %s)",
            (identifier, f"metric.{identifier}", float(identifier), f"row-{identifier}"),
        )


def _require_vendor_drivers() -> None:
    """Fail immediately when a requested live smoke lacks native client tools."""

    import psycopg

    if not callable(getattr(psycopg, "connect", None)):
        pytest.fail("live shadow smoke requires the postgres extra; run `uv sync --extra postgres --extra mssql`")
    try:
        import pyodbc
    except (ImportError, OSError) as exc:
        pytest.fail(f"live shadow smoke requires a loadable unixODBC/pyodbc driver: {exc}")
    if not callable(getattr(pyodbc, "connect", None)):
        pytest.fail("live shadow smoke requires a loadable pyodbc.connect")
    bcp = os.getenv("DPONE_IT_MSSQL_BCP_PATH", "bcp")
    if not (Path(bcp).is_file() if Path(bcp).is_absolute() else shutil.which(bcp)):
        pytest.fail("live shadow smoke requires bcp; set DPONE_IT_MSSQL_BCP_PATH to mssql-tools18/bcp")


def _create_reviewed_live_target(live: object) -> None:
    baseline_config = live.load_config()
    baseline_options = dict(baseline_config.options)
    baseline_options.update(
        {
            "technical_columns": "required",
            "soft_delete": {"mode": "timestamp_only"},
        }
    )
    baseline = invoke_public_process(
        live,
        replace(baseline_config, options=baseline_options),
        invocation_id="shadow-initial-target-contract",
    )
    assert baseline.error is None, baseline.traceback
    assert baseline.result is not None and baseline.result.status == "success"
    target = f"[{live.target_database}].[{TARGET_SCHEMA}].[{live.target_table}]"
    live.target.execute_query(f"TRUNCATE TABLE {target}")
    live.target.execute_query(
        f"ALTER TABLE {target} ADD [__dpone__row_hash] varchar(64) NOT NULL, [__dpone__deleted_at] datetime2(7) NULL"
    )
    live.target.execute_query(f"CREATE CLUSTERED COLUMNSTORE INDEX [cci_shadow_initial] ON {target}")
    live.target.execute_query(f"CREATE UNIQUE NONCLUSTERED INDEX [ux_shadow_initial_id] ON {target} ([id])")


def _create_retained_legacy_shadow(live: object) -> None:
    """Leave an older campaign artifact in place to certify safe supersession."""

    legacy = _legacy_shadow_name(live.target_table)
    qualified = f"[{live.target_database}].[{TARGET_SCHEMA}].[{legacy}]"
    live.target.execute_query(
        f"SELECT TOP (0) * INTO {qualified} FROM [{live.target_database}].[{TARGET_SCHEMA}].[{live.target_table}]"
    )
    live.target.execute_query(
        f"EXEC [{live.target_database}].sys.sp_addextendedproperty "
        "@name=?, @value=?, @level0type=N'SCHEMA', @level0name=?, "
        "@level1type=N'TABLE', @level1name=?",
        ("dpone_backfill_run_key", "legacy-raw-generation", TARGET_SCHEMA, legacy),
    )


def _shadow_initial_config(live: object, *, state_dir: Path) -> object:
    baseline = live.load_config()
    options = dict(baseline.options)
    options.update(
        {
            "incremental_strategy": "xmin",
            "xmin_execution": {
                "mode": "initial",
                "handoff_id": "shadow_initial_live_v1",
            },
            "technical_columns": "required",
            "soft_delete": {"mode": "timestamp_only"},
        }
    )
    options["backfill"] = {
        "inner_mode": "incremental_append",
        "parallel_workers": 4,
        "max_chunks": 10,
        "retry_policy": "non_committed",
        "lease_ttl_minutes": 60,
        "state_dir": str(state_dir),
        "backfill_id": _campaign_id(live.target_table),
        "state": {
            "backend": "audit_schema",
            "schema": TARGET_SCHEMA,
            "require_distributed_lock": True,
        },
        "publication": {
            "mode": "shadow_swap",
            "retain_backup": True,
            "artifact_scope": "campaign",
        },
        "chunk": {
            "column": "id",
            "kind": "integer",
            "from": "1",
            "to": "10",
            "step": "1",
        },
    }
    return replace(
        baseline,
        load_strategy=LoadStrategy.BACKFILL,
        unique_key=["id"],
        only_new_rows=False,
        options=options,
    )


def _provision_xmin_state(live: object) -> None:
    fixture = Path(__file__).parents[1] / "postgres" / "sql" / "postgres_xmin_mssql_external_state.sql"
    rendered = fixture.read_text(encoding="utf-8")
    rendered = rendered.replace("__DATABASE__", str(live.state_database))
    rendered = rendered.replace("[system]", f"[{STATE_SCHEMA}]")
    for statement in rendered.split("-- dpone:statement"):
        if statement := statement.strip():
            live.state.execute_query(statement)


def _xmin_runtime_config(live: object) -> dict[str, Any]:
    config = dict(live.runtime_config)
    state = dict(config["state"])
    state.update(
        {
            "table": {"name": "dpone_source_state"},
            "run_table": {"name": "dpone_run_state"},
            "receipt_table": {"name": "dpone_commit_receipt"},
            "repair_authority_table": {"name": "dpone_repair_authority"},
            "repair_consumption_table": {"name": "dpone_repair_authority_consumption"},
            "audit_table": {"name": "dpone_load_audit"},
        }
    )
    config["state"] = state
    return config


def _count(live: object, table: str) -> int:
    rows = live.target.get_records(
        f"SELECT COUNT_BIG(*) AS row_count FROM [{live.target_database}].[{TARGET_SCHEMA}].[{table}]",
        as_dict=True,
    )
    return int(rows[0]["row_count"])


def _duplicate_ids(live: object) -> int:
    rows = live.target.get_records(
        f"SELECT COUNT_BIG(*) AS duplicate_groups FROM ("
        f"SELECT [id] FROM [{live.target_database}].[{TARGET_SCHEMA}].[{live.target_table}] "
        "GROUP BY [id] HAVING COUNT_BIG(*) > 1) AS duplicates",
        as_dict=True,
    )
    return int(rows[0]["duplicate_groups"])


def _table_exists(live: object, table: str) -> bool:
    return bool(
        live.target.table_exists(
            TARGET_SCHEMA,
            table,
            database=live.target_database,
        )
    )


def _shadow_name(table: str) -> str:
    return _campaign_artifact_name(table, kind="shadow")


def _backup_name(table: str) -> str:
    return _campaign_artifact_name(table, kind="backup")


def _legacy_shadow_name(table: str) -> str:
    return f"{table}__dpone_initial_shadow"


def _campaign_id(table: str) -> str:
    return f"shadow-initial-{table}"


def _campaign_artifact_name(table: str, *, kind: str) -> str:
    token = hashlib.sha256(_campaign_id(table).encode("utf-8")).hexdigest()[:12]
    return f"{table}__dpone_{token}_{kind}"
