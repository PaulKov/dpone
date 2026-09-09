from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.backfill import config_hash, normalize_backfill_execution_policy, plan_chunks, plan_hash
from dpone.backfill.state import (
    BackfillChunkRecord,
    BackfillLedger,
    BackfillPublicationRecord,
    FileBackfillStateStore,
)
from dpone.backfill.xmin_handoff import (
    BackfillXminHandoffRecord,
    PostgresXminHandoffProof,
    PostgresXminInitialHandoffLifecycle,
)
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.ports.source_state_storage import CheckpointCommitOutcome, SourceStateKey
from dpone.runtime.etl.backfill_orchestrator import BackfillOrchestrator
from dpone.runtime.sources.strategies.postgres.postgres_xmin_handoff_source import (
    PostgresXminHandoffSource,
)
from dpone.runtime.state.mssql_xmin_handoff import (
    MssqlXminHandoffCommitOutcomeUnknown,
    MssqlXminHandoffCommitter,
)
from dpone.runtime.state.xmin_storage import XMinState


def _state_key() -> SourceStateKey:
    return SourceStateKey(
        environment="dev",
        process="xmin-handoff:orders_v1",
        source_connection="pg_orders",
        source_database="orders",
        source_schema="public",
        source_table="orders",
        target_database="DWH_Dev",
        target_schema="landing",
        target_table="orders",
        target_identity=b"t" * 32,
        unique_key=("id",),
        schema_hash="sha256:schema",
        scope_hash="sha256:scope",
    )


def _proof(*, status: str = "anchored", plan_hash: str = "plan") -> PostgresXminHandoffProof:
    key = _state_key()
    record = BackfillXminHandoffRecord(
        handoff_id="orders_v1",
        status=status,
        anchor_xmin=100,
        snapshot_token="sha256:" + "a" * 64,
        state_key_sha256=key.digest.hex(),
        source_authority_sha256="b" * 64,
        plan_hash=plan_hash,
        seed_load_id="xmin-seed-" + "c" * 40,
    )
    return PostgresXminHandoffProof(
        record=record,
        state_key=key,
        candidate=XMinState(
            xmin_value=100,
            timestamp=datetime(2026, 8, 21, tzinfo=UTC),
            is_initial=False,
        ),
    )


def _ledger(*, succeeded: int = 0) -> BackfillLedger:
    return BackfillLedger(
        run_key="orders-v1",
        dataset="landing.orders",
        inner_mode="incremental_merge",
        plan_hash="plan",
        config_hash="config",
        chunk_config={"column": "id"},
        chunks=[
            BackfillChunkRecord(
                index=index,
                start=str(index),
                end=str(index),
                idempotency_key=str(index),
                status=("success" if index <= succeeded else "pending"),
            )
            for index in range(1, 4)
        ],
    )


class _Store:
    def __init__(self) -> None:
        self.saved: list[dict[str, object]] = []

    def save(self, ledger: BackfillLedger) -> None:
        self.saved.append(ledger.to_jsonable())


class _Source:
    def __init__(self) -> None:
        self.events: list[str] = []

    def capture_anchor(self, load_config: object, *, handoff_id: str, plan_hash: str) -> PostgresXminHandoffProof:
        del load_config
        self.events.append(f"capture:{handoff_id}:{plan_hash}")
        return _proof(plan_hash=plan_hash)

    def revalidate_anchor(self, load_config: object, record: BackfillXminHandoffRecord) -> PostgresXminHandoffProof:
        del load_config
        self.events.append(f"revalidate:{record.anchor_xmin}")
        proof = _proof()
        return PostgresXminHandoffProof(
            record=record,
            state_key=proof.state_key,
            candidate=proof.candidate,
        )


class _Committer:
    def __init__(self, *, existing: CheckpointCommitOutcome | None = None) -> None:
        self.existing = existing
        self.events: list[str] = []

    def probe_seed(self, proof: PostgresXminHandoffProof) -> CheckpointCommitOutcome | None:
        self.events.append(f"probe:{proof.record.seed_load_id}")
        return self.existing

    def commit_seed(self, proof: PostgresXminHandoffProof) -> CheckpointCommitOutcome:
        self.events.append(f"commit:{proof.candidate.xmin_value}")
        return CheckpointCommitOutcome(
            receipt_id="receipt-1",
            candidate_xmin=proof.candidate.xmin_value,
            candidate_revision=1,
        )


def _config() -> SimpleNamespace:
    return SimpleNamespace(
        source_schema="public",
        source_table="orders",
        target_database="DWH_Dev",
        target_schema="landing",
        target_table="orders",
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "incremental_strategy": "xmin",
            "xmin_execution": {"mode": "initial", "handoff_id": "orders_v1"},
            "backfill": {
                "inner_mode": "incremental_merge",
                "chunk": {"column": "id", "from": 1, "to": 3, "step": 1},
                "state": {"backend": "audit_schema", "require_distributed_lock": True},
            },
            "unique_key": ["id"],
        },
        load_strategy=SimpleNamespace(value="backfill"),
    )


def _shadow_config() -> SimpleNamespace:
    config = _config()
    config.options["backfill"]["publication"] = {
        "mode": "shadow_swap",
        "retain_backup": True,
    }
    return config


def _publication_bound_proof() -> PostgresXminHandoffProof:
    proof = _proof()
    proof.record.publication_receipt_id = "publication-receipt-1"
    return proof


def _pending_publication(target: _TargetConnector, proof: PostgresXminHandoffProof) -> None:
    target.properties.update(
        {
            "dpone_backfill_publication_receipt": str(proof.record.publication_receipt_id),
            "dpone_backfill_xmin_state_key_sha256": proof.record.state_key_sha256,
            "dpone_backfill_xmin_seed_load_id": proof.record.seed_load_id,
        }
    )


def test_ledger_roundtrip_preserves_additive_handoff_contract() -> None:
    ledger = _ledger()
    ledger.xmin_handoff = _proof().record

    restored = BackfillLedger.from_dict(ledger.to_jsonable())

    assert restored.xmin_handoff == ledger.xmin_handoff
    assert restored.xmin_handoff is not None
    assert restored.xmin_handoff.anchor_xmin == 100


def test_lifecycle_persists_anchor_before_chunks_and_seeds_only_after_all_success() -> None:
    source = _Source()
    committer = _Committer()
    lifecycle = PostgresXminInitialHandoffLifecycle(source=source, committer=committer)
    store = _Store()
    ledger = _ledger()

    prepared = lifecycle.before_chunks(_config(), ledger, store)

    assert prepared.xmin_handoff is not None
    assert prepared.xmin_handoff.status == "anchored"
    assert source.events == ["capture:orders_v1:plan"]
    assert committer.events == ["probe:xmin-seed-" + "c" * 40]
    assert store.saved[-1]["xmin_handoff"]["status"] == "anchored"  # type: ignore[index]

    with pytest.raises(RuntimeError, match="postgres_xmin_handoff.chunks_incomplete"):
        lifecycle.after_chunks(_config(), prepared, store)
    assert not any(event.startswith("commit:") for event in committer.events)

    for chunk in prepared.chunks:
        chunk.status = "success"
    evidence = lifecycle.after_chunks(_config(), prepared, store)

    assert evidence["status"] == "committed"
    assert evidence["receipt_id"] == "receipt-1"
    assert prepared.xmin_handoff.status == "committed"
    assert [entry["xmin_handoff"]["status"] for entry in store.saved[-2:]] == ["committing", "committed"]  # type: ignore[index]


def test_retry_repairs_commit_ack_from_exact_receipt_without_second_seed() -> None:
    outcome = CheckpointCommitOutcome(
        receipt_id="receipt-existing",
        candidate_xmin=100,
        candidate_revision=1,
    )
    source = _Source()
    committer = _Committer(existing=outcome)
    lifecycle = PostgresXminInitialHandoffLifecycle(source=source, committer=committer)
    store = _Store()
    ledger = _ledger(succeeded=3)
    ledger.xmin_handoff = _proof(status="committing").record

    prepared = lifecycle.before_chunks(_config(), ledger, store)
    evidence = lifecycle.after_chunks(_config(), prepared, store)

    assert evidence["status"] == "committed"
    assert evidence["receipt_id"] == "receipt-existing"
    assert not any(event.startswith("commit:") for event in committer.events)


def test_lifecycle_durably_binds_seed_to_published_generation() -> None:
    source = _Source()
    committer = _Committer()
    lifecycle = PostgresXminInitialHandoffLifecycle(source=source, committer=committer)
    store = _Store()
    ledger = _ledger(succeeded=3)
    ledger.xmin_handoff = _proof().record
    ledger.publication = BackfillPublicationRecord(
        mode="shadow_swap",
        target_table="landing.orders",
        shadow_table="landing.orders_shadow",
        backup_table="landing.orders_backup",
        phase="published",
        receipt_id="publication-receipt-1",
    )

    lifecycle.after_chunks(_config(), ledger, store)

    assert ledger.xmin_handoff.publication_receipt_id == "publication-receipt-1"
    assert store.saved[-2]["xmin_handoff"]["publication_receipt_id"] == "publication-receipt-1"  # type: ignore[index]


def test_mapped_execution_is_rejected_for_initial_handoff_v1() -> None:
    lifecycle = PostgresXminInitialHandoffLifecycle(source=_Source(), committer=_Committer())

    with pytest.raises(RuntimeError, match="postgres_xmin_handoff.airflow_mapping_unsupported"):
        lifecycle.require_unmapped(selection=object())


def test_handoff_identity_participates_in_backfill_plan_and_config_hashes() -> None:
    config = _config()
    config.options["backfill"]["chunk"].update({"from": 1, "to": 2, "step": 1, "kind": "integer"})
    policy = normalize_backfill_execution_policy(config.options["backfill"])
    chunks = plan_chunks(policy.chunk, run_key="orders")
    first = PostgresXminInitialHandoffLifecycle.campaign_contract(config)
    config.options["xmin_execution"]["handoff_id"] = "orders_v2"
    second = PostgresXminInitialHandoffLifecycle.campaign_contract(config)

    assert plan_hash(chunks, execution_policy=policy, campaign_contract=first) != plan_hash(
        chunks,
        execution_policy=policy,
        campaign_contract=second,
    )
    assert config_hash(dataset="landing.orders", execution_policy=policy, campaign_contract=first) != config_hash(
        dataset="landing.orders",
        execution_policy=policy,
        campaign_contract=second,
    )


def test_backfill_orchestrator_projects_handoff_only_after_chunk_success(tmp_path) -> None:
    config = LoadConfig(
        source_conn_id="pg",
        target_conn_id="mssql",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.BACKFILL,
        unique_key=["id"],
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "incremental_strategy": "xmin",
            "xmin_execution": {"mode": "initial", "handoff_id": "orders_v1"},
            "backfill": {
                "inner_mode": "incremental_merge",
                "chunk": {"column": "id", "from": 1, "to": 2, "step": 1, "kind": "integer"},
                "state": {"backend": "audit_schema", "require_distributed_lock": True},
            },
        },
    )
    source = _Source()
    committer = _Committer()
    lifecycle = PostgresXminInitialHandoffLifecycle(source=source, committer=committer)

    result = BackfillOrchestrator(
        chunk_runner=lambda chunk: {
            "extracted_rows": 1,
            "loaded_rows": 1,
            "load_id": f"load-{chunk.options['backfill']['chunk_context']['index']}",
        },
        state_store=FileBackfillStateStore(tmp_path / "state"),
        campaign_lifecycle=lifecycle,
    ).run(config)

    assert result["status"] == "success"
    assert result["backfill"]["chunks_committed"] == 2
    assert result["xmin_handoff"]["status"] == "committed"
    assert source.events[0].startswith("capture:orders_v1:")
    assert source.events[-1] == "revalidate:100"


class _Lifecycle:
    def __init__(self) -> None:
        self.completed = False

    def complete(self) -> None:
        self.completed = True


class _PgConnector:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit_transaction(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def get_records(self, *_args, **_kwargs):
        return [{"checkpoint_frozen": False, "relation_frozen_xid": 20, "database_frozen_xid": 10}]


class _SnapshotExtractor:
    def state_key(self, *_args, **_kwargs) -> SourceStateKey:
        return _state_key()


class _Strategy:
    def __init__(self) -> None:
        self.connector = _PgConnector()
        self._snapshot_extractor = _SnapshotExtractor()
        self.xmin_manager = SimpleNamespace(get_snapshot_xmin_anchor=lambda: 100)
        self.lifecycle = _Lifecycle()
        self.authority = "sha256:" + "b" * 64

    def _preflight_atomic_route(self, _load_config) -> None:
        return None

    def _new_extraction_lifecycle(self) -> _Lifecycle:
        self.lifecycle = _Lifecycle()
        return self.lifecycle

    def _begin_repeatable_read_snapshot(self, _lifecycle):
        return SimpleNamespace(snapshot_token_digest="sha256:" + "a" * 64)

    def _verify_postgres_source_authority(self, _lease, _load_config):
        return SimpleNamespace(authority_sha256=self.authority)

    def fetch_schema_projection(self, _load_config):
        return SimpleNamespace(
            projected_schema=(("id", "integer"),),
            relation_schema=(("id", "integer"),),
            relation_metadata=(),
        )


def test_concrete_source_captures_and_revalidates_same_signed_anchor() -> None:
    strategy = _Strategy()
    source = PostgresXminHandoffSource(strategy)

    proof = source.capture_anchor(_config(), handoff_id="orders_v1", plan_hash="plan")
    replay = source.revalidate_anchor(_config(), proof.record)

    assert proof.record.anchor_xmin == 100
    assert proof.record.seed_load_id.startswith("xmin-seed-")
    assert proof.candidate.is_initial is False
    assert replay.state_key == proof.state_key
    assert strategy.connector.commits == 2


def test_concrete_source_rejects_signed_authority_rotation() -> None:
    strategy = _Strategy()
    source = PostgresXminHandoffSource(strategy)
    proof = source.capture_anchor(_config(), handoff_id="orders_v1", plan_hash="plan")
    strategy.authority = "sha256:" + "d" * 64

    with pytest.raises(RuntimeError, match="postgres_xmin_handoff.source_identity_changed"):
        source.revalidate_anchor(_config(), proof.record)


class _TargetConnector:
    def __init__(
        self,
        *,
        commit_error: bool = False,
        fail_xmin_property: bool = False,
        tamper_xmin_head_on_commit_error: bool = False,
    ) -> None:
        self.commit_error = commit_error
        self.fail_xmin_property = fail_xmin_property
        self.tamper_xmin_head_on_commit_error = tamper_xmin_head_on_commit_error
        self.events: list[str] = []
        self.properties: dict[str, str] = {}
        self.publication_locks: list[str] = []

    def begin(self) -> None:
        self.events.append("begin")

    def execute_query(self, query: str, params=None) -> None:
        self.events.append(query)
        if "sp_addextendedproperty" in query:
            assert params is not None
            if self.fail_xmin_property and str(params[0]) == "dpone_backfill_xmin_receipt_id":
                raise OSError("property write failed")
            self.properties[str(params[0])] = str(params[1])

    def get_records(self, query: str, params=None, as_dict: bool = False):
        assert as_dict
        if "dpone_target_identity" in query:
            return [{"binding_id": "11111111-1111-1111-1111-111111111111"}]
        if "sp_getapplock" in query:
            assert params is not None
            self.publication_locks.append(str(params[0]))
            return [{"lock_result": 0}]
        if "AS publication_receipt_id" in query:
            return [
                {
                    "object_id": 101,
                    "create_token": "11111111-1111-4111-8111-111111111111",
                    "publication_receipt_id": self.properties.get("dpone_backfill_publication_receipt"),
                    "xmin_state_key_sha256": self.properties.get("dpone_backfill_xmin_state_key_sha256"),
                    "xmin_seed_load_id": self.properties.get("dpone_backfill_xmin_seed_load_id"),
                    "xmin_receipt_id": self.properties.get("dpone_backfill_xmin_receipt_id"),
                }
            ]
        raise AssertionError(query)

    def commit_transaction(self) -> None:
        self.events.append("commit")
        if self.commit_error:
            if self.tamper_xmin_head_on_commit_error:
                self.properties["dpone_backfill_xmin_receipt_id"] = "other-receipt"
            raise OSError("lost ack")

    def rollback(self) -> None:
        self.events.append("rollback")

    def close(self) -> None:
        self.events.append("close")


class _XminStateStorage:
    atomicity = "target_atomic"

    def __init__(self, *, receipt_after_cas: bool = False) -> None:
        self.receipt_after_cas = receipt_after_cas
        self.cas_called = False
        self.publication_receipt_id: str | None = None
        self.publication_bindings: list[str] = []

    def probe_receipt(self, **_kwargs):
        if self.receipt_after_cas and self.cas_called:
            return CheckpointCommitOutcome(
                "receipt-1",
                100,
                candidate_revision=1,
                publication_receipt_id=self.publication_receipt_id,
            )
        return None

    def load_state_by_key(self, _key):
        if self.receipt_after_cas and self.cas_called:
            return SimpleNamespace(xmin_value=100, revision=1)
        return None

    def assert_physical_target_identity(self, **_kwargs) -> None:
        return None

    def assert_target_authority(self, **_kwargs) -> None:
        return None

    def compare_and_set_with_receipt(self, **kwargs):
        self.cas_called = True
        self.publication_receipt_id = kwargs.get("publication_receipt_id")
        return CheckpointCommitOutcome(
            "receipt-1",
            100,
            candidate_revision=1,
            publication_receipt_id=self.publication_receipt_id,
        )

    def bind_seed_publication_receipt(self, **kwargs):
        self.publication_receipt_id = kwargs["publication_receipt_id"]
        self.publication_bindings.append(self.publication_receipt_id)
        return CheckpointCommitOutcome(
            "receipt-1",
            100,
            candidate_revision=1,
            publication_receipt_id=self.publication_receipt_id,
        )


def test_concrete_committer_seeds_state_and_receipt_under_target_lock(monkeypatch) -> None:
    target = _TargetConnector()
    state = _XminStateStorage()
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.require_atomic_mssql_route",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.acquire_target_lock",
        lambda *_args, **_kwargs: target.events.append("target-lock"),
    )

    outcome = MssqlXminHandoffCommitter(
        target_connector=target,
        state_storage=state,
        load_config=_config(),
    ).commit_seed(_proof())

    assert outcome.receipt_id == "receipt-1"
    assert state.cas_called is True
    assert target.events == [
        "begin",
        "SET XACT_ABORT ON",
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "target-lock",
        "commit",
    ]


def test_concrete_committer_repairs_lost_commit_ack_from_receipt(monkeypatch) -> None:
    target = _TargetConnector(commit_error=True)
    state = _XminStateStorage(receipt_after_cas=True)
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.require_atomic_mssql_route",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.acquire_target_lock",
        lambda *_args, **_kwargs: None,
    )

    outcome = MssqlXminHandoffCommitter(
        target_connector=target,
        state_storage=state,
        load_config=_config(),
    ).commit_seed(_proof())

    assert outcome.receipt_id == "receipt-1"
    assert target.events[-1] == "close"
    assert "rollback" not in target.events


def test_shadow_seed_closes_exact_pending_publication_head_atomically(monkeypatch) -> None:
    target = _TargetConnector()
    state = _XminStateStorage()
    proof = _publication_bound_proof()
    _pending_publication(target, proof)
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.require_atomic_mssql_route",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.acquire_target_lock",
        lambda *_args, **_kwargs: target.events.append("target-lock"),
    )

    outcome = MssqlXminHandoffCommitter(
        target_connector=target,
        state_storage=state,
        load_config=_shadow_config(),
    ).commit_seed(proof)

    assert outcome.receipt_id == "receipt-1"
    assert target.properties["dpone_backfill_xmin_receipt_id"] == "receipt-1"
    assert state.cas_called is True
    assert state.publication_receipt_id == "publication-receipt-1"
    assert len(target.publication_locks) == 1
    assert target.publication_locks[0].startswith("dpone:backfill-publication:")


def test_legacy_shadow_seed_receipt_is_bound_under_target_and_publication_locks(monkeypatch) -> None:
    target = _TargetConnector()
    state = _XminStateStorage(receipt_after_cas=True)
    state.cas_called = True
    proof = _publication_bound_proof()
    _pending_publication(target, proof)
    target.properties["dpone_backfill_xmin_receipt_id"] = "receipt-1"
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.require_atomic_mssql_route",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.acquire_target_lock",
        lambda *_args, **_kwargs: target.events.append("target-lock"),
    )

    outcome = MssqlXminHandoffCommitter(
        target_connector=target,
        state_storage=state,
        load_config=_shadow_config(),
    ).probe_seed(proof)

    assert outcome is not None
    assert outcome.publication_receipt_id == "publication-receipt-1"
    assert state.publication_bindings == ["publication-receipt-1"]
    assert target.events[:4] == [
        "begin",
        "SET XACT_ABORT ON",
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "target-lock",
    ]
    assert target.events[-1] == "commit"
    assert len(target.publication_locks) == 1


def test_legacy_committed_seed_repairs_pending_publication_head_atomically(monkeypatch) -> None:
    target = _TargetConnector()
    state = _XminStateStorage(receipt_after_cas=True)
    state.cas_called = True
    proof = _publication_bound_proof()
    _pending_publication(target, proof)
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.require_atomic_mssql_route",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.acquire_target_lock",
        lambda *_args, **_kwargs: target.events.append("target-lock"),
    )

    outcome = MssqlXminHandoffCommitter(
        target_connector=target,
        state_storage=state,
        load_config=_shadow_config(),
    ).probe_seed(proof)

    assert outcome is not None
    assert outcome.publication_receipt_id == "publication-receipt-1"
    assert state.publication_bindings == ["publication-receipt-1"]
    assert target.properties["dpone_backfill_xmin_receipt_id"] == "receipt-1"
    assert target.events[-1] == "commit"
    assert len(target.publication_locks) == 1


def test_legacy_seed_binding_ack_recovery_rejects_tampered_publication_head(monkeypatch) -> None:
    target = _TargetConnector(commit_error=True, tamper_xmin_head_on_commit_error=True)
    state = _XminStateStorage(receipt_after_cas=True)
    state.cas_called = True
    proof = _publication_bound_proof()
    _pending_publication(target, proof)
    target.properties["dpone_backfill_xmin_receipt_id"] = "receipt-1"
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.require_atomic_mssql_route",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.acquire_target_lock",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(MssqlXminHandoffCommitOutcomeUnknown):
        MssqlXminHandoffCommitter(
            target_connector=target,
            state_storage=state,
            load_config=_shadow_config(),
        ).probe_seed(proof)

    assert target.properties["dpone_backfill_xmin_receipt_id"] == "other-receipt"


def test_shadow_seed_rejects_stale_publication_before_checkpoint_cas(monkeypatch) -> None:
    target = _TargetConnector()
    state = _XminStateStorage()
    proof = _publication_bound_proof()
    _pending_publication(target, proof)
    target.properties["dpone_backfill_publication_receipt"] = "newer-publication"
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.require_atomic_mssql_route",
        lambda *_args: None,
    )

    with pytest.raises(RuntimeError, match="publication_authority_changed"):
        MssqlXminHandoffCommitter(
            target_connector=target,
            state_storage=state,
            load_config=_shadow_config(),
        ).commit_seed(proof)

    assert state.cas_called is False
    assert "begin" not in target.events


def test_shadow_seed_commit_ack_recovery_keeps_head_and_receipt_coupled(monkeypatch) -> None:
    target = _TargetConnector(commit_error=True)
    state = _XminStateStorage(receipt_after_cas=True)
    proof = _publication_bound_proof()
    _pending_publication(target, proof)
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.require_atomic_mssql_route",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.acquire_target_lock",
        lambda *_args, **_kwargs: None,
    )

    outcome = MssqlXminHandoffCommitter(
        target_connector=target,
        state_storage=state,
        load_config=_shadow_config(),
    ).commit_seed(proof)

    assert outcome.receipt_id == "receipt-1"
    assert target.properties["dpone_backfill_xmin_receipt_id"] == outcome.receipt_id
    assert target.events[-1] == "close"
    assert "rollback" not in target.events


def test_shadow_seed_ack_recovery_rejects_tampered_publication_head(monkeypatch) -> None:
    target = _TargetConnector(
        commit_error=True,
        tamper_xmin_head_on_commit_error=True,
    )
    state = _XminStateStorage(receipt_after_cas=True)
    proof = _publication_bound_proof()
    _pending_publication(target, proof)
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.require_atomic_mssql_route",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.acquire_target_lock",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(MssqlXminHandoffCommitOutcomeUnknown):
        MssqlXminHandoffCommitter(
            target_connector=target,
            state_storage=state,
            load_config=_shadow_config(),
        ).commit_seed(proof)

    assert target.properties["dpone_backfill_xmin_receipt_id"] == "other-receipt"


def test_shadow_seed_rolls_back_when_head_cannot_close_before_commit(monkeypatch) -> None:
    target = _TargetConnector(fail_xmin_property=True)
    state = _XminStateStorage()
    proof = _publication_bound_proof()
    _pending_publication(target, proof)
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.require_atomic_mssql_route",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_xmin_handoff.acquire_target_lock",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(OSError, match="property write failed"):
        MssqlXminHandoffCommitter(
            target_connector=target,
            state_storage=state,
            load_config=_shadow_config(),
        ).commit_seed(proof)

    assert target.events[-1] == "rollback"
    assert "close" not in target.events
