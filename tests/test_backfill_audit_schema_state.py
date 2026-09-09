from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from dpone.backfill.campaign_lease import (
    LEGACY_SESSION_CAMPAIGN_OWNER_PREFIX,
    RECOVERABLE_CAMPAIGN_OWNER_PREFIX,
)
from dpone.backfill.sql_state import (
    ClickHouseBackfillStateStore,
    MSSQLBackfillStateStore,
    PostgresBackfillStateStore,
)
from dpone.backfill.sql_state_journal import (
    BackfillJournalOrderError,
    latest_campaign_details,
    latest_chunk_records,
)
from dpone.backfill.sql_state_mssql_invariants import merge_xmin_handoff
from dpone.backfill.sql_state_schema import BackfillJournalSchemaError
from dpone.backfill.state import (
    BACKFILL_LEDGER_MIGRATION_ERROR,
    BackfillChunkRecord,
    BackfillLedger,
    BackfillPublicationRecord,
)
from dpone.backfill.state_factory import BackfillStateStoreFactory
from dpone.backfill.xmin_handoff_models import BackfillXminHandoffRecord


def _ledger() -> BackfillLedger:
    return BackfillLedger(
        run_key="campaign-a",
        dataset="analytics.orders",
        inner_mode="partition_replace",
        chunk_config={"column": "business_date"},
        plan_hash="plan-a",
        config_hash="config-a",
        chunks=[
            BackfillChunkRecord(index=1, start="2025-01-01", end="2025-01-02", idempotency_key="k:1"),
        ],
    )


@pytest.mark.parametrize("status", ("anchored", "committing", "committed"))
def test_mssql_xmin_handoff_allows_exact_publication_authority_enrichment(status: str) -> None:
    current = BackfillXminHandoffRecord(
        handoff_id="orders-v1",
        status=status,
        anchor_xmin=42,
        snapshot_token="sha256:" + "a" * 64,
        state_key_sha256="b" * 64,
        source_authority_sha256="c" * 64,
        plan_hash="plan-a",
        seed_load_id="seed-a",
        receipt_id="xmin-receipt-a" if status == "committed" else None,
        candidate_revision=1 if status == "committed" else None,
    )
    incoming = deepcopy(current)
    incoming.publication_receipt_id = "publication-a"

    assert merge_xmin_handoff(current, incoming) == incoming

    conflicting = deepcopy(incoming)
    conflicting.publication_receipt_id = "publication-b"
    with pytest.raises(ValueError, match="publication authority changed"):
        merge_xmin_handoff(incoming, conflicting)


def test_clickhouse_backfill_state_store_mirrors_campaign_and_chunks_to_audit_schema(tmp_path) -> None:
    connector = _Connector()
    store = ClickHouseBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)

    store.save(_ledger())
    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert "CREATE TABLE IF NOT EXISTS `DWH_Tech`.`__dpone__backfill_campaigns`" in rendered
    assert "CREATE TABLE IF NOT EXISTS `DWH_Tech`.`__dpone__backfill_chunks`" in rendered
    assert "journal_id UInt64 DEFAULT generateSnowflakeID()" in rendered
    assert "ReplacingMergeTree(journal_id)" in rendered
    assert "`DWH_Tech`.`__dpone__backfill_campaigns`" in rendered
    statement, params = connector.calls[-1]
    assert str(statement).rstrip().endswith("VALUES")
    assert isinstance(params, list)
    assert params[0][0] == "campaign-a"


def test_clickhouse_backfill_state_store_reads_latest_campaign_from_audit_schema(tmp_path) -> None:
    connector = _ReadConnector(json.dumps(_ledger().to_jsonable(), ensure_ascii=False))
    store = ClickHouseBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)

    loaded = store.load("campaign-a")

    assert loaded is not None
    assert loaded.run_key == "campaign-a"
    assert loaded.chunks[0].idempotency_key == "k:1"
    assert not store.path_for("campaign-a").exists()
    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert "SELECT journal_id, details_json" in rendered
    assert "`DWH_Tech`.`__dpone__backfill_campaigns` FINAL" in rendered


def test_postgres_backfill_state_store_uses_quoted_audit_tables(tmp_path) -> None:
    connector = _Connector()
    store = PostgresBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)

    store.save(_ledger())

    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert 'CREATE TABLE IF NOT EXISTS "DWH_Tech"."__dpone__backfill_campaigns"' in rendered
    assert '"DWH_Tech"."__dpone__backfill_chunks"' in rendered
    assert "journal_id bigint GENERATED ALWAYS AS IDENTITY" in rendered
    assert "ix_dpone_backfill_campaign_run_key_journal" in rendered
    assert "ix_dpone_backfill_chunk_run_key_index_journal" in rendered
    statement, params = connector.calls[-1]
    assert str(statement).count("%s") == 10
    assert isinstance(params, tuple)
    assert params[0] == "campaign-a"
    assert str(params[-1]).startswith("{")


def test_postgres_backfill_state_store_reads_campaign_with_escaped_key(tmp_path) -> None:
    connector = _ReadConnector(_ledger_json())
    store = PostgresBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)

    loaded = store.load("campaign'a")

    assert loaded is not None
    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert '"DWH_Tech"."__dpone__backfill_campaigns"' in rendered
    assert "run_key = 'campaign''a'" in rendered
    assert "ORDER BY journal_id DESC LIMIT 1" in rendered
    assert "SELECT DISTINCT ON (chunk_index)" in rendered


def test_postgres_load_merges_latest_chunk_transition_over_campaign_snapshot(tmp_path) -> None:
    ledger = _ledger()
    completed = BackfillChunkRecord.from_dict(ledger.chunk(1).to_jsonable())
    completed.status = "success"
    completed.rows_loaded = 42
    connector = _MergedReadConnector(ledger, [completed])
    store = PostgresBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)

    loaded = store.load("campaign-a")

    assert loaded is not None
    assert loaded.chunk(1).status == "success"
    assert loaded.chunk(1).rows_loaded == 42
    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert "__dpone__backfill_chunks" in rendered
    assert "ORDER BY chunk_index ASC, journal_id DESC" in rendered


def test_postgres_backfill_state_store_uses_advisory_campaign_lock(tmp_path) -> None:
    connector = _AdvisoryLockConnector(locked=True)
    store = PostgresBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())

    acquired = store.acquire_campaign_lock(
        "campaign-a",
        owner="worker-a",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    store.release_campaign_lock("campaign-a", owner="worker-a")

    assert acquired is True
    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert "SELECT pg_try_advisory_lock(" in rendered
    assert "SELECT pg_advisory_unlock(" in rendered
    assert store.state_capabilities()["distributed_lock"] is True
    assert store.state_capabilities()["lock_scope"] == "postgres_advisory_campaign"
    assert store.state_capabilities()["distributed_chunk_lease"] is True
    assert store.state_capabilities()["compare_and_set_completion"] is True


def test_postgres_chunk_completion_rejects_stale_lease_owner(tmp_path) -> None:
    connector = _AdvisoryLockConnector(locked=True)
    store = PostgresBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())
    assert store.acquire_chunk_lease(
        "campaign-a",
        1,
        owner="worker-current",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    completion = store.load("campaign-a").chunk(1)
    completion.status = "success"

    assert store.complete_chunk_if_owned("campaign-a", completion, owner="worker-stale") is False
    assert store.load("campaign-a").chunk(1).status == "running"
    assert store.complete_chunk_if_owned("campaign-a", completion, owner="worker-current") is True
    assert store.load("campaign-a").chunk(1).status == "success"
    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert rendered.count("pg_try_advisory_lock") >= 3


def test_postgres_backfill_state_store_does_not_mutate_ledger_when_advisory_lock_is_busy(tmp_path) -> None:
    connector = _AdvisoryLockConnector(locked=False)
    store = PostgresBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())

    acquired = store.acquire_campaign_lock(
        "campaign-a",
        owner="worker-a",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )

    assert acquired is False
    assert store.load("campaign-a").lock_owner is None


def test_mssql_backfill_state_store_uses_parameterized_json(tmp_path) -> None:
    connector = _Connector()
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)

    state_path = store.save(_ledger())

    assert state_path.exists()
    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert "[DWH_Tech].[__dpone__backfill_campaigns]" in rendered
    assert "[DWH_Tech].[__dpone__backfill_chunks]" in rendered
    assert "journal_id bigint IDENTITY(1,1) NOT NULL" in rendered
    assert "ix_dpone_backfill_campaign_run_key_journal" in rendered
    assert "ix_dpone_backfill_chunk_run_key_index_journal" in rendered
    statement, params = connector.calls[-1]
    assert str(statement).count("?") == 10
    assert isinstance(params, tuple)
    assert params[0] == "campaign-a"
    assert str(params[-1]).startswith("{")


def test_mssql_backfill_state_store_reads_latest_campaign_by_journal_id(tmp_path) -> None:
    connector = _ReadConnector(_ledger_json())
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)

    loaded = store.load("campaign-a")

    assert loaded is not None
    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert "SELECT TOP (1) journal_id, details_json FROM [DWH_Tech].[__dpone__backfill_campaigns]" in rendered
    assert "ROW_NUMBER() OVER (PARTITION BY chunk_index ORDER BY journal_id DESC)" in rendered
    assert "ORDER BY journal_id DESC" in rendered


@pytest.mark.parametrize(
    "rows",
    [
        [
            {"journal_id": 7, "details_json": {}},
            {"journal_id": 7, "details_json": {}},
        ],
        [
            {"journal_id": 6, "details_json": {}},
            {"journal_id": 7, "details_json": {}},
        ],
    ],
)
def test_campaign_journal_rejects_duplicate_or_nonmonotonic_ids(rows) -> None:
    with pytest.raises(BackfillJournalOrderError, match="strictly descending"):
        latest_campaign_details(rows)


@pytest.mark.parametrize(
    "rows",
    [
        [
            {"chunk_index": 1, "journal_id": 7, "details_json": json.dumps(_ledger().chunk(1).to_jsonable())},
            {"chunk_index": 1, "journal_id": 7, "details_json": json.dumps(_ledger().chunk(1).to_jsonable())},
        ],
        [
            {"chunk_index": 1, "journal_id": 6, "details_json": json.dumps(_ledger().chunk(1).to_jsonable())},
            {"chunk_index": 1, "journal_id": 7, "details_json": json.dumps(_ledger().chunk(1).to_jsonable())},
        ],
    ],
)
def test_chunk_journal_rejects_duplicate_or_nonmonotonic_ids(rows) -> None:
    with pytest.raises(BackfillJournalOrderError, match="strictly descending"):
        latest_chunk_records(rows, BackfillChunkRecord.from_dict)


@pytest.mark.parametrize(
    "store_type",
    [ClickHouseBackfillStateStore, PostgresBackfillStateStore, MSSQLBackfillStateStore],
)
def test_sql_backfill_state_rejects_legacy_journal_shape(tmp_path, store_type) -> None:
    store = store_type(_LegacyShapeConnector(), schema="DWH_Tech", cache_dir=tmp_path)

    with pytest.raises(BackfillJournalSchemaError, match="automatic migration is forbidden"):
        store.save(_ledger())


def test_backfill_ledger_rejects_v1_without_explicit_operator_migration() -> None:
    legacy = _ledger().to_jsonable()
    legacy["schema_version"] = "1"

    with pytest.raises(ValueError, match=BACKFILL_LEDGER_MIGRATION_ERROR):
        BackfillLedger.from_dict(legacy)


def test_mssql_backfill_state_store_uses_application_campaign_lock(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())

    acquired = store.acquire_campaign_lock(
        "campaign-a",
        owner="worker-a",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    store.release_campaign_lock("campaign-a", owner="worker-a")

    assert acquired is True
    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert "sp_getapplock" in rendered
    assert "sp_releaseapplock" in rendered
    assert store.state_capabilities()["distributed_lock"] is True
    assert store.state_capabilities()["lock_scope"] == "mssql_application_campaign"
    assert store.state_capabilities()["distributed_chunk_lease"] is True
    assert store.state_capabilities()["compare_and_set_completion"] is True


def test_mssql_backfill_state_store_does_not_mutate_ledger_when_application_lock_is_busy(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=-1)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())

    acquired = store.acquire_campaign_lock(
        "campaign-a",
        owner="worker-a",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )

    assert acquired is False
    assert store.load("campaign-a").lock_owner is None


def test_mssql_recoverable_lease_reclaims_only_an_orphaned_legacy_session_owner(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())
    orphaned = store.load("campaign-a")
    assert orphaned is not None
    orphaned.lock_owner = f"{LEGACY_SESSION_CAMPAIGN_OWNER_PREFIX}dead-pod"
    orphaned.lock_expires_at = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    store.save(orphaned)
    new_owner = f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}retry-pod"

    assert store.acquire_campaign_lock(
        "campaign-a",
        owner=new_owner,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
    )

    recovered = store.load("campaign-a")
    assert recovered is not None
    assert recovered.lock_owner == new_owner
    assert connector.sessions[0].lock_held is True
    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert rendered.count("sp_getapplock") >= 1
    assert "sp_releaseapplock" not in rendered

    # A blocking target phase can outlive the durable TTL.  The same SQL
    # session's application lock remains the authoritative continuity fence,
    # so its exact owner may renew instead of abandoning an already-running
    # idempotent publication.
    recovered.lock_expires_at = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    store.save(recovered)
    renewed_until = datetime.now(UTC) + timedelta(seconds=90)
    assert store.renew_campaign_lock(
        "campaign-a",
        owner=new_owner,
        lease_expires_at=renewed_until,
    )
    renewed = store.load("campaign-a")
    assert renewed is not None
    assert datetime.fromisoformat(str(renewed.lock_expires_at)) >= renewed_until
    assert store.state_capabilities()["continuous_campaign_session_fence"] is True
    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert "APPLOCK_MODE(N'public', ?, 'Session')" in rendered
    assert "DB_PRINCIPAL_ID" not in rendered

    store.release_campaign_lock("campaign-a", owner=new_owner)
    released = store.load("campaign-a")
    assert released is not None
    assert released.lock_owner is None
    assert connector.sessions[0].lock_held is False
    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert rendered.count("sp_releaseapplock") == 1


def test_mssql_recoverable_lease_fails_closed_after_session_lock_loss(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())
    owner = f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}worker-a"
    assert store.acquire_campaign_lock(
        "campaign-a",
        owner=owner,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
    )

    connector.sessions[0].lock_held = False
    assert not store.renew_campaign_lock(
        "campaign-a",
        owner=owner,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
    )
    current = store.load("campaign-a")
    assert current is not None
    assert current.lock_owner == owner


def test_mssql_stale_campaign_save_cannot_erase_concurrent_cancel(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())
    stale = store.load("campaign-a")
    assert stale is not None

    store.request_cancel("campaign-a", reason="operator stop", requested_by="qa")
    stale.lock_owner = "stale-owner"
    stale.lock_expires_at = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
    store.save(stale)

    current = store.load("campaign-a")
    assert current is not None
    assert current.status == "cancel_requested"
    assert current.cancel_reason == "operator stop"
    assert current.cancel_requested_by == "qa"
    assert current.lock_owner is None
    assert stale.status == "cancel_requested"


def test_mssql_campaign_fence_survives_target_connector_reconnect(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())
    owner = f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}worker-a"
    assert store.acquire_campaign_lock(
        "campaign-a",
        owner=owner,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
    )

    connector.close()

    assert connector.sessions[0].lock_held is True
    assert store.renew_campaign_lock(
        "campaign-a",
        owner=owner,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=120),
    )
    assert store.state_capabilities()["continuous_campaign_session_fence"] is True


def test_mssql_campaign_takeover_invalidates_unexpired_orphan_chunk_lease(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    ledger = _ledger()
    ledger.chunk(1).status = "running"
    ledger.chunk(1).lease_owner = "dead-parent-chunk-owner"
    ledger.chunk(1).lease_expires_at = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    store.save(ledger)
    owner = f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}replacement-parent"

    assert store.acquire_campaign_lock(
        ledger.run_key,
        owner=owner,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
    )

    recovered = store.load(ledger.run_key)
    assert recovered is not None
    assert recovered.lock_owner == owner
    assert recovered.chunk(1).status == "failed"
    assert recovered.chunk(1).error == "campaign_session_fence_orphaned"
    assert recovered.chunk(1).lease_owner is None
    assert recovered.chunk(1).lease_expires_at is None
    assert store.retryable_chunk_indexes(ledger.run_key) == [1]


def test_mssql_heartbeat_is_constant_cost_for_two_thousand_chunks(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    ledger = _ledger()
    ledger.chunks = [
        BackfillChunkRecord(index=index, start=str(index), end=str(index + 1), idempotency_key=f"k:{index}")
        for index in range(1, 2001)
    ]
    store.save(ledger)
    assert len(connector.chunk_rows) == 2000
    owner = f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}worker-a"
    assert store.acquire_campaign_lock(
        ledger.run_key,
        owner=owner,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
    )
    connector.calls.clear()

    assert store.renew_campaign_lock(
        ledger.run_key,
        owner=owner,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=120),
    )

    rendered = [str(sql) for sql, _ in connector.calls]
    campaign_payloads = [str(row["details_json"]) for row in connector.campaign_rows]
    assert campaign_payloads
    assert all("chunks" not in json.loads(payload) for payload in campaign_payloads)
    assert max(len(payload.encode("utf-8")) for payload in campaign_payloads) < 4096
    assert not any("INSERT INTO" in sql and "__dpone__backfill_chunks" in sql for sql in rendered)
    assert sum("INSERT INTO" in sql and "__dpone__backfill_campaigns" in sql for sql in rendered) == 1
    assert len(rendered) <= 8


def test_mssql_first_compact_mutation_atomically_materializes_legacy_inline_chunks(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    legacy = _ledger()
    legacy.chunks = [
        BackfillChunkRecord(index=index, start=str(index), end=str(index + 1), idempotency_key=f"k:{index}")
        for index in range(1, 4)
    ]
    connector.campaign_rows.append(
        {
            "journal_id": 1,
            "run_key": legacy.run_key,
            "details_json": json.dumps(legacy.to_jsonable(), ensure_ascii=False),
        }
    )
    completed = BackfillChunkRecord.from_dict(legacy.chunk(2).to_jsonable())
    completed.status = "success"
    completed.rows_loaded = 41
    connector.chunk_rows.append(
        {
            "chunk_index": 2,
            "journal_id": 1,
            "run_key": legacy.run_key,
            "details_json": json.dumps(completed.to_jsonable(), ensure_ascii=False),
        }
    )
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    before = store.load(legacy.run_key)
    assert before is not None
    assert [chunk.index for chunk in before.chunks] == [1, 2, 3]
    assert before.chunk(2).status == "success"
    assert before.chunk(2).rows_loaded == 41
    owner = f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}migration-worker"

    assert store.acquire_campaign_lock(
        legacy.run_key,
        owner=owner,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
    )

    latest_campaign = json.loads(str(connector.campaign_rows[-1]["details_json"]))
    assert "chunks" not in latest_campaign
    assert {int(row["chunk_index"]) for row in connector.chunk_rows} == {1, 2, 3}
    after = store.load(legacy.run_key)
    assert after is not None
    assert [chunk.index for chunk in after.chunks] == [1, 2, 3]
    assert after.chunk(2).status == "success"
    assert after.chunk(2).rows_loaded == 41


def test_mssql_legacy_campaign_persists_portable_scope_contract_once(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())
    contract = {
        "schema": "dpone.backfill.portable-scope-columns.v1",
        "identity_bound": False,
        "source": {"name": "id", "type": "bigint", "collation": None},
        "target": {"name": "id", "type": "bigint", "collation": None},
    }
    legacy = store.load("campaign-a")
    assert legacy is not None
    legacy.portable_scope_column_contract = contract

    store.save(legacy)

    persisted = store.load("campaign-a")
    assert persisted is not None
    assert persisted.portable_scope_column_contract == contract
    changed = deepcopy(persisted)
    assert changed.portable_scope_column_contract is not None
    changed.portable_scope_column_contract["target"] = {
        "name": "id",
        "type": "nvarchar",
        "collation": "Latin1_General_100_CI_AS",
    }
    with pytest.raises(ValueError, match="portable scope campaign contract changed"):
        store.save(changed)


def test_mssql_chunk_lease_reads_and_writes_only_the_requested_chunk(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    ledger = _ledger()
    ledger.chunks = [
        BackfillChunkRecord(index=index, start=str(index), end=str(index + 1), idempotency_key=f"k:{index}")
        for index in range(1, 2001)
    ]
    store.save(ledger)
    initial_chunk_revisions = len(connector.chunk_rows)
    connector.calls.clear()

    assert store.acquire_chunk_lease(
        ledger.run_key,
        1733,
        owner="chunk-worker-a",
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
    )

    point_reads = [
        (str(sql), params)
        for sql, params in connector.calls
        if "__dpone__backfill_chunks" in str(sql) and "SELECT" in str(sql)
    ]
    assert len(point_reads) == 1
    point_sql, point_params = point_reads[0]
    assert "chunk_index IN (?)" in point_sql
    assert point_params == (ledger.run_key, 1733)
    assert len(connector.chunk_rows) == initial_chunk_revisions + 1
    assert json.loads(str(connector.chunk_rows[-1]["details_json"]))["index"] == 1733


def test_mssql_cancel_is_closed_after_irreversible_target_publication(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    ledger = _ledger()
    ledger.publication = BackfillPublicationRecord(
        mode="shadow_swap",
        target_table="analytics.orders",
        shadow_table="analytics.orders__shadow",
        backup_table="analytics.orders__backup",
        phase="published",
        receipt_id="receipt-a",
    )
    store.save(ledger)
    revision_count = len(connector.campaign_rows)

    with pytest.raises(RuntimeError, match="cancellation is closed after target publication"):
        store.request_cancel(ledger.run_key, reason="too late", requested_by="qa")

    current = store.load(ledger.run_key)
    assert current is not None
    assert current.status == "active"
    assert current.cancel_reason is None
    assert len(connector.campaign_rows) == revision_count


def test_mssql_campaign_acquisition_self_heals_legacy_cancel_after_publication(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    ledger = _ledger()
    ledger.status = "cancel_requested"
    ledger.cancel_reason = "legacy race"
    ledger.cancel_requested_by = "qa"
    ledger.publication = BackfillPublicationRecord(
        mode="shadow_swap",
        target_table="analytics.orders",
        shadow_table="analytics.orders__shadow",
        backup_table="analytics.orders__backup",
        phase="published",
        receipt_id="receipt-a",
    )
    store.save(ledger)
    owner = f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}resume-worker"

    assert store.acquire_campaign_lock(
        ledger.run_key,
        owner=owner,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
    )

    current = store.load(ledger.run_key)
    assert current is not None
    assert current.status == "active"
    assert current.cancel_reason is None
    assert current.cancel_requested_by is None
    assert current.lock_owner == owner
    assert current.publication is not None
    assert current.publication.phase == "published"


def test_mssql_campaign_handoff_gate_runs_after_begin_and_before_row_lock(tmp_path) -> None:
    connector = _OrderedMSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())
    connector.events.clear()
    owner = f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}worker-a"

    assert store.acquire_campaign_lock(
        "campaign-a",
        owner=owner,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
    )

    assert connector.events.index("transaction_begin") < connector.events.index("publication_gate")
    assert connector.events.index("publication_gate") < connector.events.index("campaign_row_lock")


def test_mssql_publication_transaction_fence_rejects_lost_session_before_row_lock(tmp_path) -> None:
    connector = _OrderedMSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())
    owner = f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}worker-a"
    assert store.acquire_campaign_lock(
        "campaign-a",
        owner=owner,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
    )
    connector.sessions[0].lock_held = False
    connector.events.clear()
    connector.begin()

    with pytest.raises(RuntimeError, match="campaign_session_fence_lost"):
        store.fence_publication_in_transaction(
            "campaign-a",
            owner=owner,
            connector=connector,
        )

    assert connector.events == ["transaction_begin", "publication_gate", "session_fence_probe"]
    connector.rollback()


def test_mssql_transactional_publication_rejects_using_the_dedicated_fence_session(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())
    owner = f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}worker-a"
    assert store.acquire_campaign_lock(
        "campaign-a",
        owner=owner,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
    )
    current = store.load("campaign-a")
    assert current is not None

    with pytest.raises(RuntimeError, match="campaign_session_mismatch"):
        store.persist_campaign_in_transaction(
            current,
            connector=connector.sessions[0],
            campaign_owner=owner,
        )


def test_mssql_publication_cas_preserves_current_campaign_state(tmp_path) -> None:
    owner = f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}worker-a"
    current = _ledger()
    current.lock_owner = owner
    current.lock_expires_at = (datetime.now(UTC) + timedelta(seconds=90)).isoformat()
    current.chunk(1).status = "success"
    current.chunk(1).rows_loaded = 42
    candidate = BackfillLedger.from_dict(_ledger().to_jsonable())
    candidate.publication = BackfillPublicationRecord(
        mode="shadow_swap",
        target_table="analytics.orders",
        shadow_table="analytics.orders__shadow",
        backup_table="analytics.orders__backup",
        phase="published",
        receipt_id="receipt-a",
    )
    connector = _ReadConnector(json.dumps(current.to_jsonable(), ensure_ascii=False))
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)

    committed = store._publication_candidate_if_owned(  # noqa: SLF001 - focused CAS contract test.
        candidate,
        connector=connector,
        campaign_owner=owner,
    )

    assert committed.lock_owner == owner
    assert committed.chunk(1).status == "success"
    assert committed.chunk(1).rows_loaded == 42
    assert committed.publication is not None
    assert committed.publication.receipt_id == "receipt-a"
    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert "WITH (UPDLOCK, HOLDLOCK)" in rendered


@pytest.mark.parametrize("cancelled", [False, True])
def test_mssql_publication_cas_rejects_owner_replacement_or_cancel(
    tmp_path,
    *,
    cancelled: bool,
) -> None:
    owner = f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}worker-a"
    current = _ledger()
    current.lock_owner = owner if cancelled else f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}worker-b"
    current.lock_expires_at = (datetime.now(UTC) + timedelta(seconds=90)).isoformat()
    if cancelled:
        current.status = "cancel_requested"
    candidate = BackfillLedger.from_dict(_ledger().to_jsonable())
    connector = _ReadConnector(json.dumps(current.to_jsonable(), ensure_ascii=False))
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)

    with pytest.raises(RuntimeError, match="campaign_fence_lost"):
        store._publication_candidate_if_owned(  # noqa: SLF001 - focused CAS contract test.
            candidate,
            connector=connector,
            campaign_owner=owner,
        )


def test_mssql_recoverable_lease_never_steals_a_legacy_owner_held_by_this_session(tmp_path) -> None:
    connector = _MSSQLAppLockConnector(lock_result=0)
    store = MSSQLBackfillStateStore(connector, schema="DWH_Tech", cache_dir=tmp_path)
    store.save(_ledger())
    legacy_owner = f"{LEGACY_SESSION_CAMPAIGN_OWNER_PREFIX}live-pod"
    assert store.acquire_campaign_lock(
        "campaign-a",
        owner=legacy_owner,
        lease_expires_at=datetime.now(UTC) + timedelta(hours=1),
    )

    assert not store.acquire_campaign_lock(
        "campaign-a",
        owner=f"{RECOVERABLE_CAMPAIGN_OWNER_PREFIX}retry-pod",
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
    )
    current = store.load("campaign-a")
    assert current is not None
    assert current.lock_owner == legacy_owner


def test_backfill_state_store_factory_builds_local_file_by_default(tmp_path) -> None:
    store = BackfillStateStoreFactory().build({"state_dir": tmp_path}, sink_type="clickhouse")

    assert store.root_dir == tmp_path


def test_backfill_package_facade_exports_state_store_factory() -> None:
    from dpone.backfill import BackfillStateStoreFactory as FacadeFactory

    assert FacadeFactory is BackfillStateStoreFactory


def test_backfill_state_store_factory_builds_clickhouse_audit_backend(tmp_path) -> None:
    connector = _Connector()

    store = BackfillStateStoreFactory().build(
        {"state_dir": tmp_path, "state": {"backend": "audit_schema", "schema": "DWH_Tech"}},
        sink_type="clickhouse",
        sink_connector=connector,
    )
    store.save(_ledger())

    assert isinstance(store, ClickHouseBackfillStateStore)
    assert any("`DWH_Tech`.`__dpone__backfill_campaigns`" in str(sql) for sql, _ in connector.calls)
    assert store.state_capabilities()["lock_scope"] == "local_cache"
    assert store.state_capabilities()["distributed_lock"] is False
    assert store.state_capabilities()["distributed_chunk_lease"] is False
    assert store.state_capabilities()["compare_and_set_completion"] is False


def test_backfill_state_store_factory_requires_connector_for_audit_schema(tmp_path) -> None:
    try:
        BackfillStateStoreFactory().build(
            {"state_dir": tmp_path, "state": {"backend": "audit_schema"}},
            sink_type="clickhouse",
        )
    except ValueError as exc:
        assert "requires sink_connector" in str(exc)
    else:
        raise AssertionError("audit_schema backend without connector must fail")


def test_backfill_state_store_factory_blocks_required_distributed_lock_for_uncertified_backend(tmp_path) -> None:
    try:
        BackfillStateStoreFactory().build(
            {
                "state_dir": tmp_path,
                "state": {
                    "backend": "audit_schema",
                    "schema": "DWH_Tech",
                    "require_distributed_lock": True,
                },
            },
            sink_type="clickhouse",
            sink_connector=_Connector(),
        )
    except ValueError as exc:
        assert "distributed backfill lock is required" in str(exc)
        assert "lock_scope=local_cache" in str(exc)
    else:
        raise AssertionError("uncertified distributed lock backend must fail")


def test_backfill_state_store_factory_allows_required_distributed_lock_for_postgres(tmp_path) -> None:
    store = BackfillStateStoreFactory().build(
        {
            "state_dir": tmp_path,
            "state": {
                "backend": "audit_schema",
                "schema": "DWH_Tech",
                "require_distributed_lock": True,
            },
        },
        sink_type="postgres",
        sink_connector=_AdvisoryLockConnector(),
    )

    assert isinstance(store, PostgresBackfillStateStore)


@pytest.mark.parametrize(
    "sink_type",
    (
        "mssql",
        "MSSQL",
        "microsoft mssql",
        "microsoft_mssql",
        "odbc",
        "sqlserver",
        "sql_server",
        "sql-server",
    ),
)
def test_backfill_state_store_factory_allows_required_distributed_lock_for_mssql_aliases(
    tmp_path,
    sink_type: str,
) -> None:
    store = BackfillStateStoreFactory().build(
        {
            "state_dir": tmp_path,
            "state": {
                "backend": "audit_schema",
                "schema": "DWH_Tech",
                "require_distributed_lock": True,
            },
        },
        sink_type=sink_type,
        sink_connector=_MSSQLAppLockConnector(),
    )

    assert isinstance(store, MSSQLBackfillStateStore)


class _Connector:
    def __init__(self) -> None:
        self.calls: list[tuple[object, object | None]] = []
        self.campaign_rows: list[dict[str, object]] = []
        self.chunk_rows: list[dict[str, object]] = []
        self._transaction_snapshot: tuple[list[dict[str, object]], list[dict[str, object]]] | None = None

    def execute_query(self, sql, params=None):
        self.calls.append((sql, params))
        rendered = str(sql)
        row = params[0] if isinstance(params, list) and params and isinstance(params[0], tuple) else params
        if "INSERT INTO" in rendered and "__dpone__backfill_campaigns" in rendered:
            self.campaign_rows.append(
                {
                    "journal_id": len(self.campaign_rows) + 1,
                    "run_key": row[0],
                    "details_json": row[-1],
                }
            )
        elif "INSERT INTO" in rendered and "__dpone__backfill_chunks" in rendered:
            self.chunk_rows.append(
                {
                    "chunk_index": row[1],
                    "journal_id": len(self.chunk_rows) + 1,
                    "run_key": row[0],
                    "details_json": row[-1],
                }
            )

    def begin(self):
        self._transaction_snapshot = (list(self.campaign_rows), list(self.chunk_rows))

    def commit_transaction(self):
        self._transaction_snapshot = None

    def rollback(self):
        if self._transaction_snapshot is not None:
            self.campaign_rows, self.chunk_rows = self._transaction_snapshot
        self._transaction_snapshot = None

    def close(self):
        return None

    def get_records(self, sql, params=None, as_dict=False):
        self.calls.append((sql, params))
        rendered = str(sql)
        if "dpone_backfill_journal_shape:mssql" in rendered:
            rows = [
                {
                    "column_name": "journal_id",
                    "data_type": "bigint",
                    "is_nullable": False,
                    "is_identity": 1,
                }
            ]
        elif "dpone_backfill_journal_shape:postgres" in rendered:
            rows = [
                {
                    "column_name": "journal_id",
                    "data_type": "bigint",
                    "is_nullable": "NO",
                    "is_identity": "YES",
                    "identity_generation": "ALWAYS",
                }
            ]
        elif "dpone_backfill_journal_shape:clickhouse" in rendered:
            rows = [
                {
                    "column_name": "journal_id",
                    "data_type": "UInt64",
                    "default_kind": "DEFAULT",
                    "default_expression": "generateSnowflakeID()",
                    "engine_full": "ReplacingMergeTree(journal_id) ORDER BY run_key",
                }
            ]
        elif "__dpone__backfill_campaigns" in rendered and "SELECT" in rendered:
            run_key = params[0] if params else None
            rows = [row for row in self.campaign_rows if run_key is None or row["run_key"] == run_key]
            rows = sorted(rows, key=lambda row: int(row["journal_id"]), reverse=True)[:1]
        elif "__dpone__backfill_chunks" in rendered and "SELECT" in rendered:
            run_key = params[0] if params else None
            candidates = [row for row in self.chunk_rows if run_key is None or row["run_key"] == run_key]
            if "chunk_index IN (" in rendered and params:
                requested_indexes = {int(index) for index in params[1:]}
                candidates = [row for row in candidates if int(row["chunk_index"]) in requested_indexes]
            latest = {}
            for row in candidates:
                latest.setdefault(int(row["chunk_index"]), row)
                if int(row["journal_id"]) > int(latest[int(row["chunk_index"])]["journal_id"]):
                    latest[int(row["chunk_index"])] = row
            rows = [latest[index] for index in sorted(latest)]
        else:
            rows = []
        return rows if as_dict else [tuple(row.values()) for row in rows]

    def quote_identifier(self, value: str) -> str:
        return f"[{value}]"

    def qualified_name(self, schema: str, table: str) -> str:
        return f"[{schema}].[{table}]"


class _ReadConnector(_Connector):
    def __init__(self, details_json: str) -> None:
        super().__init__()
        self.details_json = details_json

    def get_records(self, sql, params=None, as_dict=False):
        if "dpone_backfill_journal_shape" in str(sql):
            return super().get_records(sql, params=params, as_dict=as_dict)
        self.calls.append((sql, params))
        if "__dpone__backfill_chunks" in str(sql):
            return []
        rows = [{"journal_id": 1, "details_json": self.details_json}]
        return rows if as_dict else [(1, self.details_json)]


class _AdvisoryLockConnector(_Connector):
    def __init__(self, locked: bool = True) -> None:
        super().__init__()
        self.locked = locked

    def get_records(self, sql, params=None, as_dict=False):
        self.calls.append((sql, params))
        if "pg_try_advisory_lock" in str(sql):
            return [{"locked": self.locked}] if as_dict else [(self.locked,)]
        if "pg_advisory_unlock" in str(sql):
            return [{"unlocked": True}] if as_dict else [(True,)]
        return super().get_records(sql, params=params, as_dict=as_dict)


class _MSSQLAppLockConnector(_Connector):
    def __init__(self, lock_result: int = 0, *, shared_calls=None) -> None:
        super().__init__()
        if shared_calls is not None:
            self.calls = shared_calls
        self.lock_result = lock_result
        self.lock_held = False
        self.sessions: list[_MSSQLAppLockConnector] = []

    def open_session(self, *, application_name: str):
        assert application_name == "dpone-backfill-campaign-fence"
        session = _MSSQLAppLockConnector(lock_result=self.lock_result, shared_calls=self.calls)
        self.sessions.append(session)
        return session

    def get_records(self, sql, params=None, as_dict=False):
        if "sp_getapplock" in str(sql):
            self.calls.append((sql, params))
            self.lock_held = self.lock_result >= 0
            return [{"lock_result": self.lock_result}] if as_dict else [(self.lock_result,)]
        if "APPLOCK_MODE" in str(sql):
            self.calls.append((sql, params))
            mode = "Exclusive" if self.lock_held else "NoLock"
            return [{"lock_mode": mode}] if as_dict else [(mode,)]
        if "sp_releaseapplock" in str(sql):
            self.calls.append((sql, params))
            self.lock_held = False
            return [{"release_result": 0}] if as_dict else [(0,)]
        return super().get_records(sql, params=params, as_dict=as_dict)


class _OrderedMSSQLAppLockConnector(_MSSQLAppLockConnector):
    def __init__(self, lock_result: int = 0, *, shared_calls=None, events=None) -> None:
        super().__init__(lock_result=lock_result, shared_calls=shared_calls)
        self.events = events if events is not None else []

    def open_session(self, *, application_name: str):
        assert application_name == "dpone-backfill-campaign-fence"
        session = _OrderedMSSQLAppLockConnector(
            lock_result=self.lock_result,
            shared_calls=self.calls,
            events=self.events,
        )
        self.sessions.append(session)
        return session

    def begin(self):
        self.events.append("transaction_begin")
        return super().begin()

    def get_records(self, sql, params=None, as_dict=False):
        rendered = str(sql)
        if "sp_getapplock" in rendered and "@LockOwner = 'Transaction'" in rendered:
            self.events.append("publication_gate")
        elif "APPLOCK_MODE" in rendered:
            self.events.append("session_fence_probe")
        elif "__dpone__backfill_campaigns" in rendered and "WITH (UPDLOCK, HOLDLOCK)" in rendered:
            self.events.append("campaign_row_lock")
        return super().get_records(sql, params=params, as_dict=as_dict)


class _MergedReadConnector(_Connector):
    def __init__(self, ledger: BackfillLedger, chunks: list[BackfillChunkRecord]) -> None:
        super().__init__()
        self.ledger = ledger
        self.chunks = chunks

    def get_records(self, sql, params=None, as_dict=False):
        if "dpone_backfill_journal_shape" in str(sql):
            return super().get_records(sql, params=params, as_dict=as_dict)
        self.calls.append((sql, params))
        if "__dpone__backfill_chunks" in str(sql):
            rows = [
                {
                    "chunk_index": chunk.index,
                    "journal_id": position,
                    "details_json": json.dumps(chunk.to_jsonable(), ensure_ascii=False),
                }
                for position, chunk in enumerate(self.chunks, start=1)
            ]
            return rows if as_dict else [(row["chunk_index"], row["journal_id"], row["details_json"]) for row in rows]
        if "__dpone__backfill_campaigns" in str(sql):
            details = json.dumps(self.ledger.to_jsonable(), ensure_ascii=False)
            return [{"journal_id": 1, "details_json": details}] if as_dict else [(1, details)]
        if "pg_try_advisory_lock" in str(sql):
            return [{"locked": True}] if as_dict else [(True,)]
        if "pg_advisory_unlock" in str(sql):
            return [{"unlocked": True}] if as_dict else [(True,)]
        return []


class _LegacyShapeConnector(_Connector):
    def get_records(self, sql, params=None, as_dict=False):
        self.calls.append((sql, params))
        return []


def _ledger_json() -> str:
    return json.dumps(_ledger().to_jsonable(), ensure_ascii=False)
