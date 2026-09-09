from __future__ import annotations

import re
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.backfill.shadow_append_authority import (
    SHADOW_APPEND_AUTHORITY_OPTION,
    issue_shadow_append_authority,
)
from dpone.backfill.state import (
    CHUNK_STATUS_SUCCESS,
    BackfillChunkRecord,
    BackfillLedger,
)
from dpone.backfill.xmin_handoff_models import BackfillXminHandoffRecord
from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.sinks.mssql_backfill_cutover import acquire_cutover_target_lock
from dpone.runtime.sinks.mssql_backfill_publication import MssqlBackfillShadowPublisher
from dpone.runtime.sinks.mssql_backfill_publication_catalog import publication_names
from dpone.runtime.sinks.mssql_shadow_append_target import (
    assert_shadow_append_target_identity,
)
from dpone.runtime.sinks.strategies.mssql.mssql_concrete_load_strategies import (
    MSSQLIncrementAppendStrategy,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_lock import (
    acquire_mutation_locks,
    acquire_target_lock,
    target_lock_resource,
    transaction_lock_timeout_ms,
)

LIVE = "reg_important_entity_change_log"
SHADOW = LIVE + "__dpone_initial_shadow"
BACKUP = LIVE + "__dpone_initial_backup"


def _config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="postgres",
        target_conn_id="mssql",
        source_schema="public",
        source_table=LIVE,
        target_database="DWH_Dev",
        target_schema="crm_archive",
        target_table=LIVE,
        load_strategy=LoadStrategy.BACKFILL,
        unique_key=["id"],
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "backfill": {
                "inner_mode": "incremental_append",
                "parallel_workers": 4,
                "max_chunks": 2,
                "chunk": {"column": "id", "kind": "uuid", "buckets": 2},
                "state": {
                    "backend": "audit_schema",
                    "schema": "system",
                    "require_distributed_lock": True,
                },
                "publication": {"mode": "shadow_swap", "retain_backup": True},
            },
        },
    )


def _ledger() -> BackfillLedger:
    return BackfillLedger(
        run_key="campaign-a",
        dataset=f"crm_archive.{LIVE}",
        inner_mode="incremental_append",
        chunk_config={"column": "id", "kind": "uuid", "buckets": 2},
        plan_hash="plan-a",
        config_hash="config-a",
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


def _handoff(status: str) -> BackfillXminHandoffRecord:
    committed = status == "committed"
    return BackfillXminHandoffRecord(
        handoff_id="orders_v1",
        status=status,
        anchor_xmin=100,
        snapshot_token="sha256:" + "a" * 64,
        state_key_sha256="b" * 64,
        source_authority_sha256="c" * 64,
        plan_hash="plan-a",
        seed_load_id="xmin-seed-1",
        receipt_id="xmin-receipt-1" if committed else None,
        candidate_revision=1 if committed else None,
    )


def _authorized_append_config() -> LoadConfig:
    authored = _config()
    authority = issue_shadow_append_authority(
        authored,
        run_key="campaign-a",
        live_table=LIVE,
        shadow_table=SHADOW,
    )
    options = {key: value for key, value in authored.options.items() if key != "backfill"}
    options[SHADOW_APPEND_AUTHORITY_OPTION] = authority
    return replace(
        authored,
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        target_table=SHADOW,
        options=options,
    )


class _Store:
    dialect = "mssql"

    def __init__(self, tmp_path: Path) -> None:
        self.root_dir = tmp_path
        self.current: BackfillLedger | None = None
        self.persisted_campaign_owner: str | None = None
        self.transaction_events: list[str] = []
        self.load_transform = None

    def save(self, ledger: BackfillLedger) -> Path:
        self.current = deepcopy(ledger)
        return self.root_dir / "campaign-a.json"

    def persist_campaign_in_transaction(
        self,
        ledger: BackfillLedger,
        *,
        connector: object,
        campaign_owner: str | None = None,
    ) -> BackfillLedger:
        assert connector is not None
        assert getattr(connector, "transaction_active", False) is True
        self.transaction_events.append("persist")
        self.persisted_campaign_owner = campaign_owner
        self.current = deepcopy(ledger)
        return deepcopy(ledger)

    def fence_publication_in_transaction(
        self,
        run_key: str,
        *,
        owner: str,
        connector: object,
    ) -> BackfillLedger:
        assert run_key == "campaign-a"
        assert owner
        assert getattr(connector, "transaction_active", False) is True
        self.transaction_events.append("fence")
        assert self.current is not None
        if self.current.status == "cancel_requested":
            raise RuntimeError("mssql_backfill_publication.campaign_fence_lost")
        return deepcopy(self.current)

    @staticmethod
    def state_capabilities() -> dict[str, object]:
        return {"continuous_campaign_session_fence": True}

    def sync_local_cache(self, ledger: BackfillLedger) -> Path:
        self.current = deepcopy(ledger)
        return self.root_dir / "campaign-a.json"

    def load(self, run_key: str) -> BackfillLedger | None:
        assert run_key == "campaign-a"
        current = deepcopy(self.current)
        return self.load_transform(current) if self.load_transform is not None else current


class _Connector:
    def __init__(self, *, fail_publish_commit_ack: bool = False) -> None:
        self.tables = {LIVE}
        self.object_ids = {LIVE: 100}
        self.next_object_id = 101
        self.owners: dict[str, str] = {}
        self.properties: dict[str, dict[str, str]] = {}
        self.rows = {LIVE: 7, SHADOW: 0}
        self.indexes = {
            LIVE: [
                {
                    "index_id": 1,
                    "index_name": f"CCI_crm_archive_{LIVE}",
                    "type_desc": "CLUSTERED COLUMNSTORE",
                    "is_unique": False,
                    "is_primary_key": False,
                    "is_unique_constraint": False,
                    "is_disabled": False,
                    "is_hypothetical": False,
                    "has_filter": False,
                    "filter_definition": None,
                    "key_ordinal": 0,
                    "is_included_column": True,
                    "is_descending_key": False,
                    "column_name": "id",
                },
                {
                    "index_id": 2,
                    "index_name": f"ux_dpone_{LIVE}_id",
                    "type_desc": "NONCLUSTERED",
                    "is_unique": True,
                    "is_primary_key": False,
                    "is_unique_constraint": False,
                    "is_disabled": False,
                    "is_hypothetical": False,
                    "has_filter": False,
                    "filter_definition": None,
                    "key_ordinal": 1,
                    "is_included_column": False,
                    "is_descending_key": False,
                    "column_name": "id",
                },
            ]
        }
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0
        self.fail_publish_commit_ack = fail_publish_commit_ack
        self.queries: list[str] = []
        self.transaction_active = False
        self.lock_resources: list[str] = []
        self.publish_lock_hook = None
        self.cutover_active = False

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    def table_exists(self, schema: str, table: str, *, database: str | None = None) -> bool:
        assert (database, schema) == ("DWH_Dev", "crm_archive")
        return table in self.tables

    def begin(self) -> None:
        assert self.transaction_active is False
        self.transaction_active = True

    def commit_transaction(self) -> None:
        assert self.transaction_active is True
        self.transaction_active = False
        self.cutover_active = False
        self.commits += 1
        if self.fail_publish_commit_ack and self.commits == 3:
            raise OSError("ack lost")

    def rollback(self) -> None:
        self.transaction_active = False
        self.cutover_active = False
        self.rollbacks += 1

    def close(self) -> None:
        self.closed += 1

    def execute_query(self, sql: str, params: tuple[object, ...] | None = None) -> int:
        if "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE" in sql:
            self.cutover_active = True
        elif "SELECT TOP (0) * INTO" in sql:
            self.tables.add(SHADOW)
            self.object_ids[SHADOW] = self.next_object_id
            self.next_object_id += 1
            self.indexes[SHADOW] = []
        elif "sp_addextendedproperty" in sql:
            assert params is not None
            table = str(params[3])
            name = str(params[0])
            value = str(params[1])
            self.properties.setdefault(table, {})[name] = value
            if name == "dpone_backfill_run_key":
                self.owners[table] = value
        elif "CREATE CLUSTERED COLUMNSTORE INDEX" in sql:
            self.indexes[SHADOW].append(deepcopy(self.indexes[LIVE][0]))
        elif "CREATE UNIQUE NONCLUSTERED INDEX" in sql:
            self.indexes[SHADOW].append(deepcopy(self.indexes[LIVE][1]))
        elif "sp_rename" in sql:
            names = re.findall(r"N'crm_archive\.([^']+)'|N'([^']+)'", sql)
            flattened = [left or right for left, right in names]
            old, new = flattened[-2:]
            self.tables.remove(old)
            self.tables.add(new)
            self.rows[new] = self.rows.pop(old)
            self.object_ids[new] = self.object_ids.pop(old)
            self.indexes[new] = self.indexes.pop(old)
            if old in self.owners:
                self.owners[new] = self.owners.pop(old)
            if old in self.properties:
                self.properties[new] = self.properties.pop(old)
        return 0

    def get_records(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
        as_dict: bool = False,
    ) -> list[dict[str, object]]:
        assert as_dict
        self.queries.append(sql)
        if "dpone_target_identity" in sql:
            return [{"binding_id": "11111111-1111-1111-1111-111111111111"}]
        if "sp_getapplock" in sql:
            assert params is not None
            resource = str(params[0])
            self.lock_resources.append(resource)
            if self.publish_lock_hook is not None and self.cutover_active:
                hook = self.publish_lock_hook
                self.publish_lock_hook = None
                hook()
            return [{"lock_result": 0}]
        if "AS publication_receipt_id" in sql:
            assert params is not None
            table = str(params[-1])
            if table not in self.tables:
                return []
            properties = self.properties.get(table, {})
            return [
                {
                    "object_id": self.object_ids[table],
                    "create_token": properties.get("dpone_backfill_generation_id"),
                    "publication_receipt_id": properties.get("dpone_backfill_publication_receipt"),
                    "xmin_state_key_sha256": properties.get("dpone_backfill_xmin_state_key_sha256"),
                    "xmin_seed_load_id": properties.get("dpone_backfill_xmin_seed_load_id"),
                    "xmin_receipt_id": properties.get("dpone_backfill_xmin_receipt_id"),
                }
            ]
        if "sys.extended_properties" in sql:
            assert params is not None
            owner = self.owners.get(str(params[2]))
            return [{"run_key": owner}] if owner is not None else []
        if "COUNT_BIG(*) AS row_count" in sql:
            table = SHADOW if SHADOW in sql else LIVE
            return [{"row_count": self.rows[table]}]
        if "HAVING COUNT_BIG(*) > 1" in sql:
            return []
        if "FROM [DWH_Dev].sys.indexes" in sql:
            assert params is not None
            return deepcopy(self.indexes.get(str(params[1]), []))
        if "FROM [DWH_Dev].sys.columns" in sql:
            assert params is not None
            if str(params[1]) not in self.tables:
                return []
            return [
                {
                    "column_id": 1,
                    "name": "id",
                    "type_name": "uniqueidentifier",
                    "max_length": 16,
                    "precision": 0,
                    "scale": 0,
                    "is_nullable": False,
                    "is_computed": False,
                    "is_identity": False,
                    "collation_name": None,
                    "default_object_id": 0,
                }
            ]
        raise AssertionError(sql)


_OBJECT_CONTRACT_SHA256 = "sha256:" + "c" * 64


class _ObjectContractGuard:
    def __init__(
        self,
        connector: _Connector,
        *,
        fail_under_cutover: bool = False,
        drift_under_cutover: bool = False,
        fail_shadow_under_cutover: bool = False,
    ) -> None:
        self.connector = connector
        self.fail_under_cutover = fail_under_cutover
        self.drift_under_cutover = drift_under_cutover
        self.fail_shadow_under_cutover = fail_shadow_under_cutover
        self.calls: list[tuple[str, str, bool]] = []

    def require_supported(self, target):
        self.calls.append(("supported", target.table, self.connector.cutover_active))
        if self.fail_under_cutover and self.connector.cutover_active:
            raise RuntimeError("mssql_backfill_publication.object_contract_residual_surface_unsupported")
        digest = (
            "sha256:" + "d" * 64
            if self.drift_under_cutover and self.connector.cutover_active
            else _OBJECT_CONTRACT_SHA256
        )
        return SimpleNamespace(sha256=digest)

    def require_loadable_shadow(self, live, shadow):
        self.calls.append(("loadable", shadow.table, self.connector.cutover_active))
        return live

    def require_matching_shadow(self, live, shadow):
        self.calls.append(("matching", shadow.table, self.connector.cutover_active))
        if self.fail_shadow_under_cutover:
            raise RuntimeError("mssql_backfill_publication.object_contract_trigger_unsupported")
        return live


_TEST_TARGET_IDENTITY = b"t" * 32


def test_cutover_target_lock_uses_canonical_physical_binding(monkeypatch) -> None:
    connector = object()
    session = object()
    resolved: list[dict[str, object]] = []
    acquired: list[tuple[str, str, int]] = []
    live = publication_names(
        _config(),
        run_key="campaign-a",
        artifact_scope="stable",
    )[0]
    physical = SimpleNamespace(
        digest=_TEST_TARGET_IDENTITY,
        database_name="DWH_Dev",
    )

    monkeypatch.setattr(
        "dpone.runtime.sinks.mssql_backfill_cutover.MssqlSessionIdentity",
        SimpleNamespace(read=lambda actual: session if actual is connector else None),
    )

    def resolve(actual: object, **kwargs: object) -> object:
        assert actual is connector
        resolved.append(kwargs)
        return physical

    def acquire(
        actual: object,
        resource: str,
        *,
        database: str,
        timeout_ms: int,
    ) -> None:
        assert actual is connector
        acquired.append((resource, database, timeout_ms))

    monkeypatch.setattr(
        "dpone.runtime.sinks.mssql_backfill_cutover.resolve_mssql_physical_target_identity",
        resolve,
    )
    monkeypatch.setattr(
        "dpone.runtime.sinks.mssql_backfill_cutover.acquire_target_lock",
        acquire,
    )

    result = acquire_cutover_target_lock(connector, live, _config())

    assert result == _TEST_TARGET_IDENTITY
    assert resolved == [
        {
            "session": session,
            "database": "DWH_Dev",
            "schema": "crm_archive",
            "table": LIVE,
        }
    ]
    assert acquired == [
        (
            target_lock_resource(_TEST_TARGET_IDENTITY),
            "DWH_Dev",
            300_000,
        )
    ]


def _acquire_test_cutover_target_lock(
    connector: _Connector,
    live: Any,
    load_config: LoadConfig,
) -> bytes:
    acquire_target_lock(
        connector,
        target_lock_resource(_TEST_TARGET_IDENTITY),
        database=live.database,
        timeout_ms=transaction_lock_timeout_ms(load_config),
    )
    return _TEST_TARGET_IDENTITY


def _assert_test_cutover_target_identity(
    _connector: _Connector,
    *,
    database: str,
    schema: str,
    table: str,
    expected: bytes,
) -> None:
    assert (database, schema, table, expected) == (
        "DWH_Dev",
        "crm_archive",
        LIVE,
        _TEST_TARGET_IDENTITY,
    )


def _publisher(
    connector: _Connector,
    *,
    guard: _ObjectContractGuard | None = None,
    target_identity_assertion: Any = _assert_test_cutover_target_identity,
):
    return MssqlBackfillShadowPublisher(
        connector,
        object_contract_guard=guard or _ObjectContractGuard(connector),
        cutover_target_lock_acquirer=_acquire_test_cutover_target_lock,
        cutover_target_identity_assertion=target_identity_assertion,
    )


@pytest.mark.parametrize("ack_lost", (False, True))
def test_shadow_publication_prepares_loads_validates_and_recovers_commit_ack(
    tmp_path: Path,
    ack_lost: bool,
) -> None:
    connector = _Connector(fail_publish_commit_ack=ack_lost)
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector)

    chunk_config = publisher.before_chunks(_config(), ledger, store)

    assert chunk_config.target_table == SHADOW
    assert connector.tables == {LIVE, SHADOW}
    assert not any("INSERT INTO" in query and "dpone_target_identity" in query for query in connector.queries)
    assert [index["type_desc"] for index in connector.indexes[SHADOW]] == ["CLUSTERED COLUMNSTORE"]
    for record in ledger.chunks:
        record.status = CHUNK_STATUS_SUCCESS
        record.rows_extracted = record.rows_loaded = 10
    connector.rows[SHADOW] = 20

    evidence = publisher.after_chunks(_config(), ledger, store)

    assert evidence["phase"] == "published"
    assert evidence["expected_rows"] == evidence["actual_rows"] == 20
    assert connector.tables == {LIVE, BACKUP}
    assert [index["type_desc"] for index in connector.indexes[LIVE]] == [
        "CLUSTERED COLUMNSTORE",
        "NONCLUSTERED",
    ]
    assert store.current is not None
    assert store.current.publication is not None
    assert store.current.publication.phase == "published"
    assert connector.closed == int(ack_lost)
    assert connector.lock_resources[-2].startswith("dpone:target:")
    assert connector.lock_resources[-1].startswith("dpone:backfill-publication:")


def test_cutover_reproves_target_identity_inside_target_and_publication_locks(
    tmp_path: Path,
) -> None:
    connector = _Connector()
    store = _Store(tmp_path)
    ledger = _ledger()
    identity_proofs: list[tuple[str, ...]] = []

    def assert_identity(_connector: _Connector, **_kwargs: object) -> None:
        identity_proofs.append(tuple(connector.lock_resources[-2:]))

    publisher = _publisher(connector, target_identity_assertion=assert_identity)
    publisher.before_chunks(_config(), ledger, store)
    for record in ledger.chunks:
        record.status = CHUNK_STATUS_SUCCESS
        record.rows_extracted = record.rows_loaded = 10
    connector.rows[SHADOW] = 20

    publisher.after_chunks(_config(), ledger, store)

    assert len(identity_proofs) == 1
    target_resource, publication_resource = identity_proofs[0]
    assert target_resource.startswith("dpone:target:")
    assert publication_resource.startswith("dpone:backfill-publication:")


def test_cutover_rejects_changed_physical_binding_inside_both_locks(
    tmp_path: Path,
) -> None:
    connector = _Connector()
    store = _Store(tmp_path)
    ledger = _ledger()

    def reject_changed_identity(_connector: _Connector, **_kwargs: object) -> None:
        assert connector.lock_resources[-2].startswith("dpone:target:")
        assert connector.lock_resources[-1].startswith("dpone:backfill-publication:")
        raise RuntimeError("mssql_physical_target_binding_changed")

    publisher = _publisher(
        connector,
        target_identity_assertion=reject_changed_identity,
    )
    publisher.before_chunks(_config(), ledger, store)
    for record in ledger.chunks:
        record.status = CHUNK_STATUS_SUCCESS
        record.rows_extracted = record.rows_loaded = 10
    connector.rows[SHADOW] = 20

    with pytest.raises(RuntimeError, match="mssql_physical_target_binding_changed"):
        publisher.after_chunks(_config(), ledger, store)

    assert connector.tables == {LIVE, SHADOW}
    assert BACKUP not in connector.tables


def test_shadow_publication_binds_atomic_receipt_to_campaign_owner(tmp_path: Path) -> None:
    connector = _Connector()
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector)
    publisher.before_chunks(_config(), ledger, store)
    for record in ledger.chunks:
        record.status = CHUNK_STATUS_SUCCESS
        record.rows_extracted = record.rows_loaded = 10
    connector.rows[SHADOW] = 20

    evidence = publisher.after_chunks_fenced(
        _config(),
        ledger,
        store,
        campaign_owner="dpone-backfill-campaign-lease-v2:worker-a",
    )

    assert evidence["phase"] == "published"
    assert store.persisted_campaign_owner == "dpone-backfill-campaign-lease-v2:worker-a"
    assert store.transaction_events == ["fence", "persist"]


def test_cutover_ack_loss_rejects_forged_generation_receipt(tmp_path: Path) -> None:
    connector = _Connector(fail_publish_commit_ack=True)
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector)
    publisher.before_chunks(_config(), ledger, store)
    for record in ledger.chunks:
        record.status = CHUNK_STATUS_SUCCESS
        record.rows_extracted = record.rows_loaded = 10
    connector.rows[SHADOW] = 20

    def forge_generation(current: BackfillLedger | None) -> BackfillLedger | None:
        assert current is not None and current.publication is not None
        current.publication.predecessor_object_id = 777
        current.publication.predecessor_create_token = "77777777-7777-4777-8777-777777777777"
        current.publication.shadow_object_id = 888
        current.publication.shadow_create_token = "88888888-8888-4888-8888-888888888888"
        return current

    store.load_transform = forge_generation

    with pytest.raises(RuntimeError, match="commit_outcome_unknown"):
        publisher.after_chunks(_config(), ledger, store)


def test_cutover_ack_loss_normalizes_unavailable_recovery_probe(tmp_path: Path) -> None:
    connector = _Connector(fail_publish_commit_ack=True)
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector)
    publisher.before_chunks(_config(), ledger, store)
    for record in ledger.chunks:
        record.status = CHUNK_STATUS_SUCCESS
        record.rows_extracted = record.rows_loaded = 10
    connector.rows[SHADOW] = 20

    def unavailable_probe(_current: BackfillLedger | None) -> BackfillLedger | None:
        raise OSError("recovery state unavailable")

    store.load_transform = unavailable_probe

    with pytest.raises(RuntimeError, match="^mssql_backfill_publication.commit_outcome_unknown$"):
        publisher.after_chunks(_config(), ledger, store)


def test_cutover_rejects_stale_live_generation_after_waiting_for_target_lock(tmp_path: Path) -> None:
    connector = _Connector()
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector)
    publisher.before_chunks(_config(), ledger, store)
    for record in ledger.chunks:
        record.status = CHUNK_STATUS_SUCCESS
        record.rows_extracted = record.rows_loaded = 10
    connector.rows[SHADOW] = 20

    def publish_newer_generation() -> None:
        assert connector.lock_resources[-1].startswith("dpone:target:")
        connector.object_ids[LIVE] = 900
        connector.properties.setdefault(LIVE, {})["dpone_backfill_publication_receipt"] = "newer-receipt"

    connector.publish_lock_hook = publish_newer_generation

    with pytest.raises(RuntimeError, match="stale_live_generation"):
        publisher.after_chunks(_config(), ledger, store)

    assert BACKUP not in connector.tables
    assert SHADOW in connector.tables


@pytest.mark.parametrize(
    ("table", "expected_error"),
    ((LIVE, "stale_live_generation"), (SHADOW, "shadow_generation_changed")),
)
def test_cutover_rejects_object_id_aba_after_waiting_for_target_lock(
    tmp_path: Path,
    table: str,
    expected_error: str,
) -> None:
    connector = _Connector()
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector)
    publisher.before_chunks(_config(), ledger, store)
    for record in ledger.chunks:
        record.status = CHUNK_STATUS_SUCCESS
        record.rows_extracted = record.rows_loaded = 10
    connector.rows[SHADOW] = 20
    original_object_id = connector.object_ids[table]

    def replace_same_object_id() -> None:
        assert connector.lock_resources[-1].startswith("dpone:target:")
        connector.properties[table]["dpone_backfill_generation_id"] = "99999999-9999-4999-8999-999999999999"
        assert connector.object_ids[table] == original_object_id

    connector.publish_lock_hook = replace_same_object_id

    with pytest.raises(RuntimeError, match=expected_error):
        publisher.after_chunks(_config(), ledger, store)

    assert BACKUP not in connector.tables
    assert SHADOW in connector.tables


def test_cutover_reproves_shadow_rows_after_waiting_for_target_lock(tmp_path: Path) -> None:
    connector = _Connector()
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector)
    publisher.before_chunks(_config(), ledger, store)
    for record in ledger.chunks:
        record.status = CHUNK_STATUS_SUCCESS
        record.rows_extracted = record.rows_loaded = 10
    connector.rows[SHADOW] = 20

    def mutate_shadow_after_target_lock() -> None:
        assert connector.lock_resources[-1].startswith("dpone:target:")
        connector.rows[SHADOW] = 19

    connector.publish_lock_hook = mutate_shadow_after_target_lock

    with pytest.raises(RuntimeError, match="shadow_changed_after_validation"):
        publisher.after_chunks(_config(), ledger, store)

    assert connector.tables == {LIVE, SHADOW}


@pytest.mark.parametrize(
    ("guard_options", "expected_error"),
    (
        ({"fail_under_cutover": True}, "object_contract_residual_surface_unsupported"),
        ({"drift_under_cutover": True}, "object_contract_changed"),
        ({"fail_shadow_under_cutover": True}, "object_contract_trigger_unsupported"),
    ),
)
def test_cutover_rejects_live_object_contract_race_before_rename(
    tmp_path: Path,
    guard_options: dict[str, bool],
    expected_error: str,
) -> None:
    connector = _Connector()
    guard = _ObjectContractGuard(connector, **guard_options)
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector, guard=guard)
    publisher.before_chunks(_config(), ledger, store)
    for record in ledger.chunks:
        record.status = CHUNK_STATUS_SUCCESS
        record.rows_extracted = record.rows_loaded = 10
    connector.rows[SHADOW] = 20

    with pytest.raises(RuntimeError, match=expected_error):
        publisher.after_chunks(_config(), ledger, store)

    assert guard.calls[:2] == [("supported", LIVE, False), ("loadable", SHADOW, False)]
    assert ("supported", LIVE, True) in guard.calls
    if guard_options.get("fail_shadow_under_cutover"):
        assert guard.calls[-1] == ("matching", SHADOW, True)
    assert connector.tables == {LIVE, SHADOW}
    assert BACKUP not in connector.tables


def test_published_replay_rejects_replaced_backup_generation(tmp_path: Path) -> None:
    connector = _Connector()
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector)
    publisher.before_chunks(_config(), ledger, store)
    for record in ledger.chunks:
        record.status = CHUNK_STATUS_SUCCESS
        record.rows_extracted = record.rows_loaded = 10
    connector.rows[SHADOW] = 20
    publisher.after_chunks(_config(), ledger, store)
    connector.properties[BACKUP]["dpone_backfill_generation_id"] = "99999999-9999-4999-8999-999999999999"
    connector.properties.setdefault(BACKUP, {})["dpone_backfill_publication_receipt"] = "replacement-receipt"

    with pytest.raises(RuntimeError, match="published_backup_generation_changed"):
        publisher.before_chunks(_config(), ledger, store)


def test_legacy_campaign_scoped_prepared_record_is_rebound_under_target_lock(
    tmp_path: Path,
    monkeypatch,
) -> None:
    connector = _Connector()
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector)
    publisher.before_chunks(_config(), ledger, store)
    assert ledger.publication is not None
    ledger.publication.predecessor_object_id = None
    ledger.publication.predecessor_create_token = None
    ledger.publication.predecessor_receipt_id = None
    ledger.publication.shadow_object_id = None
    ledger.publication.shadow_create_token = None
    store.current = deepcopy(ledger)
    campaign_config = _config()
    campaign_config.options["backfill"]["publication"]["artifact_scope"] = "campaign"
    stable_names = publication_names(_config(), run_key=ledger.run_key, artifact_scope="stable")
    monkeypatch.setattr(
        "dpone.runtime.sinks.mssql_backfill_publication.publication_names",
        lambda *_args, **_kwargs: stable_names,
    )

    publisher.before_chunks_fenced(
        campaign_config,
        ledger,
        store,
        campaign_owner="dpone-backfill-campaign-lease-v2:worker-a",
    )

    assert ledger.publication.predecessor_object_id == connector.object_ids[LIVE]
    assert ledger.publication.shadow_object_id == connector.object_ids[SHADOW]
    assert ledger.publication.object_contract_sha256 == _OBJECT_CONTRACT_SHA256
    assert store.current is not None
    assert store.current.publication == ledger.publication
    assert len(set(connector.lock_resources)) == 1


def test_legacy_rebind_rejects_concurrent_campaign_cancellation(tmp_path: Path) -> None:
    connector = _Connector()
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector)
    publisher.before_chunks(_config(), ledger, store)
    assert ledger.publication is not None
    ledger.publication.predecessor_object_id = None
    ledger.publication.predecessor_create_token = None
    ledger.publication.predecessor_receipt_id = None
    ledger.publication.shadow_object_id = None
    ledger.publication.shadow_create_token = None
    store.current = deepcopy(ledger)
    store.current.status = "cancel_requested"

    with pytest.raises(RuntimeError, match="campaign_fence_lost"):
        publisher.before_chunks_fenced(
            _config(),
            ledger,
            store,
            campaign_owner="dpone-backfill-campaign-lease-v2:worker-a",
        )

    assert store.current.status == "cancel_requested"
    assert store.current.publication.predecessor_object_id is None


def test_legacy_prepared_rebind_recovers_lost_commit_ack(tmp_path: Path) -> None:
    connector = _Connector(fail_publish_commit_ack=True)
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector)
    publisher.before_chunks(_config(), ledger, store)
    assert ledger.publication is not None
    ledger.publication.predecessor_object_id = None
    ledger.publication.predecessor_create_token = None
    ledger.publication.predecessor_receipt_id = None
    ledger.publication.shadow_object_id = None
    ledger.publication.shadow_create_token = None
    store.current = deepcopy(ledger)
    connector.commits = 2

    publisher.before_chunks_fenced(
        _config(),
        ledger,
        store,
        campaign_owner="dpone-backfill-campaign-lease-v2:worker-a",
    )

    assert ledger.publication.predecessor_object_id == connector.object_ids[LIVE]
    assert connector.closed == 1


@pytest.mark.parametrize("handoff_status", ("anchored", "committed"))
def test_legacy_published_generation_migrates_pending_or_committed_handoff(
    tmp_path: Path,
    handoff_status: str,
) -> None:
    connector = _Connector()
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector)
    publisher.before_chunks(_config(), ledger, store)
    for record in ledger.chunks:
        record.status = CHUNK_STATUS_SUCCESS
        record.rows_extracted = record.rows_loaded = 10
    ledger.xmin_handoff = _handoff(handoff_status)
    connector.rows[SHADOW] = 20
    publisher.after_chunks(_config(), ledger, store)
    assert ledger.publication is not None
    publication_receipt = ledger.publication.receipt_id
    owner = connector.properties[LIVE]["dpone_backfill_run_key"]
    connector.properties[LIVE] = {"dpone_backfill_run_key": owner}
    ledger.publication.predecessor_object_id = None
    ledger.publication.predecessor_create_token = None
    ledger.publication.predecessor_receipt_id = None
    ledger.publication.shadow_object_id = None
    ledger.publication.shadow_create_token = None
    store.current = deepcopy(ledger)

    publisher.before_chunks_fenced(
        _config(),
        ledger,
        store,
        campaign_owner="dpone-backfill-campaign-lease-v2:worker-a",
    )

    assert ledger.publication.predecessor_object_id == connector.object_ids[BACKUP]
    assert ledger.publication.shadow_object_id == connector.object_ids[LIVE]
    assert connector.properties[LIVE]["dpone_backfill_publication_receipt"] == publication_receipt
    assert connector.properties[LIVE]["dpone_backfill_xmin_state_key_sha256"] == "b" * 64
    expected_xmin_receipt = "xmin-receipt-1" if handoff_status == "committed" else None
    assert connector.properties[LIVE].get("dpone_backfill_xmin_receipt_id") == expected_xmin_receipt


def test_new_campaign_is_blocked_while_predecessor_xmin_handoff_is_pending(tmp_path: Path) -> None:
    connector = _Connector()
    connector.properties[LIVE] = {
        "dpone_backfill_publication_receipt": "publication-a",
        "dpone_backfill_xmin_state_key_sha256": "a" * 64,
        "dpone_backfill_xmin_seed_load_id": "seed-a",
    }

    with pytest.raises(RuntimeError, match="predecessor_handoff_pending"):
        _publisher(connector).before_chunks(
            _config(),
            _ledger(),
            _Store(tmp_path),
        )

    assert connector.tables == {LIVE}


def test_shadow_publication_rejects_count_mismatch_without_touching_live(tmp_path: Path) -> None:
    connector = _Connector()
    store = _Store(tmp_path)
    ledger = _ledger()
    publisher = _publisher(connector)
    publisher.before_chunks(_config(), ledger, store)
    for record in ledger.chunks:
        record.status = CHUNK_STATUS_SUCCESS
        record.rows_extracted = record.rows_loaded = 10
    connector.rows[SHADOW] = 19

    with pytest.raises(RuntimeError, match="row_count_mismatch"):
        publisher.after_chunks(_config(), ledger, store)

    assert connector.tables == {LIVE, SHADOW}
    assert BACKUP not in connector.tables


def test_shadow_publication_requires_live_unique_key_authority_before_shadow_creation(
    tmp_path: Path,
) -> None:
    connector = _Connector()
    connector.indexes[LIVE] = [
        index for index in connector.indexes[LIVE] if index["type_desc"] == "CLUSTERED COLUMNSTORE"
    ]

    with pytest.raises(RuntimeError, match="live_unique_key_index_missing"):
        _publisher(connector).before_chunks(
            _config(),
            _ledger(),
            _Store(tmp_path),
        )

    assert connector.tables == {LIVE}


def test_campaign_scoped_artifacts_are_deterministic_disjoint_and_bounded() -> None:
    first = _config()
    first.options["backfill"]["publication"]["artifact_scope"] = "campaign"
    live, first_shadow, first_backup = publication_names(
        first,
        run_key="typed-generation-v1",
        artifact_scope="campaign",
    )
    repeated = publication_names(first, run_key="typed-generation-v1", artifact_scope="campaign")
    _, second_shadow, second_backup = publication_names(
        first,
        run_key="typed-generation-v2",
        artifact_scope="campaign",
    )

    assert repeated == (live, first_shadow, first_backup)
    assert first_shadow.table != second_shadow.table
    assert first_backup.table != second_backup.table
    assert first_shadow.table.endswith("_shadow")
    assert first_backup.table.endswith("_backup")
    assert len(first_shadow.table) <= 128
    assert len(first_backup.table) <= 128
    assert SHADOW not in {first_shadow.table, second_shadow.table}
    assert BACKUP not in {first_backup.table, second_backup.table}


def test_stable_artifact_scope_preserves_legacy_campaign_contract() -> None:
    assert _publisher(_Connector()).campaign_contract(_config()) == {
        "kind": "dpone.mssql_backfill_shadow_publication.v1",
        "mode": "shadow_swap",
        "retain_backup": True,
        "target": f"DWH_Dev.crm_archive.{LIVE}",
        "shadow": f"DWH_Dev.crm_archive.{SHADOW}",
        "backup": f"DWH_Dev.crm_archive.{BACKUP}",
    }


def test_shadow_append_avoids_table_lock_and_never_scans_growing_target() -> None:
    calls: list[tuple[str, bool]] = []

    class Strategy(MSSQLIncrementAppendStrategy):
        def _consume_with_staging(self, load_config, payload, handler):
            return handler(SimpleNamespace(columns=["id"]))

        def _insert_from_staging_to_table(
            self,
            load_config,
            staging,
            destination_table,
            *,
            table_lock=False,
        ):
            calls.append((destination_table, table_lock))
            return 970_000

        def _count_target(self, load_config):
            raise AssertionError("shadow chunk must not COUNT_BIG the growing target")

    strategy = object.__new__(Strategy)

    result = strategy.load(_authorized_append_config(), SimpleNamespace())

    assert calls == [(SHADOW, False)]
    assert result.inserted_rows == result.total_rows == 970_000


def test_disjoint_shadow_chunks_take_only_operation_locks() -> None:
    target_locks: list[bytes] = []
    operation_locks: list[bytes] = []
    operation = SimpleNamespace(
        operation_key=b"o" * 32,
        attempt=SimpleNamespace(
            target_identity=b"t" * 32,
            request=SimpleNamespace(target_database="DWH_Dev"),
        ),
    )

    acquire_mutation_locks(
        object(),
        _authorized_append_config(),
        operation,
        target_lock_acquirer=lambda *_args, **kwargs: target_locks.append(kwargs["target_identity"]),
        operation_lock_acquirer=lambda *_args, **kwargs: operation_locks.append(kwargs["operation_key"]),
    )

    assert target_locks == []
    assert operation_locks == [b"o" * 32]


def test_shadow_chunk_revalidates_live_binding_and_campaign_owner() -> None:
    connector = _Connector()
    connector.tables.add(SHADOW)
    connector.owners[SHADOW] = "campaign-a"
    assertions: list[tuple[str, str, str, bytes]] = []
    operation = SimpleNamespace(
        attempt=SimpleNamespace(
            target_identity=b"t" * 32,
            request=SimpleNamespace(
                target_database="DWH_Dev",
                target_schema="crm_archive",
                target_table=SHADOW,
            ),
        )
    )

    assert_shadow_append_target_identity(
        connector,
        _authorized_append_config(),
        operation,
        identity_assertion=lambda _connector, **kwargs: assertions.append(
            (kwargs["database"], kwargs["schema"], kwargs["table"], kwargs["expected"])
        ),
    )

    assert assertions == [("DWH_Dev", "crm_archive", LIVE, b"t" * 32)]

    connector.owners[SHADOW] = "other-campaign"
    with pytest.raises(RuntimeError, match="shadow_owner_mismatch"):
        assert_shadow_append_target_identity(
            connector,
            _authorized_append_config(),
            operation,
            identity_assertion=lambda *_args, **_kwargs: None,
        )
