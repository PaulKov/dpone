"""Durable publication-generation admission for future XMin loads."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.ports.source_state_storage import CheckpointCommitOutcome, SourceStateKey
from dpone.runtime.postgres_xmin_execution import xmin_handoff_seed_load_id
from dpone.runtime.sinks.strategies.mssql.mssql_incremental_publication_head import (
    MssqlIncrementalPublicationHeadGuard,
)
from dpone.runtime.state.xmin_storage import XMinState


def _key() -> SourceStateKey:
    return SourceStateKey(
        environment="prod",
        process="orders",
        source_connection="postgres-source",
        source_database="source",
        source_schema="public",
        source_table="orders",
        target_database="DWH_Prod",
        target_schema="landing",
        target_table="orders",
        target_identity=b"t" * 32,
        unique_key=("id",),
        schema_hash="sha256:" + "a" * 64,
        scope_hash="sha256:" + "b" * 64,
    )


def _config(*, mode: str = "incremental") -> SimpleNamespace:
    return SimpleNamespace(
        target_database="wrong-config-database",
        target_schema="wrong-config-schema",
        target_table="wrong-config-table",
        options={"xmin_execution": {"mode": mode, "handoff_id": "orders_v1"}},
    )


def _envelope(*, previous: XMinState | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        state_key=_key(),
        previous_checkpoint=previous or XMinState(101, datetime.now(UTC), revision=2),
    )


class _State:
    def __init__(self, receipt: CheckpointCommitOutcome | None) -> None:
        self.receipt = receipt
        self.probes: list[tuple[SourceStateKey, str, object]] = []

    def probe_receipt(self, *, key, load_id, executor=None):
        self.probes.append((key, load_id, executor))
        return self.receipt


def test_shadow_seed_requires_exact_live_head_under_publication_lock(monkeypatch) -> None:
    connector = object()
    receipt = CheckpointCommitOutcome(
        "xmin-seed-receipt",
        100,
        candidate_revision=1,
        publication_receipt_id="publication-receipt",
    )
    state = _State(receipt)
    events: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        "dpone.runtime.sinks.strategies.mssql.mssql_incremental_publication_head.acquire_publication_lock",
        lambda _connector, target, **kwargs: events.append(("lock", target, kwargs)),
    )
    monkeypatch.setattr(
        "dpone.runtime.sinks.strategies.mssql.mssql_incremental_publication_head.require_xmin_publication_head",
        lambda _connector, target, **kwargs: events.append(("head", target, kwargs)),
    )
    envelope = _envelope()
    guard = MssqlIncrementalPublicationHeadGuard(connector=connector, state_storage=state)

    authority = guard.acquire(_config(), envelope, timeout_ms=1234)
    guard.require_locked(authority, envelope)

    expected_seed = xmin_handoff_seed_load_id(
        handoff_id="orders_v1",
        state_key_digest=envelope.state_key.digest,
    )
    assert authority is not None
    assert authority.target.database == "DWH_Prod"
    assert authority.target.schema == "landing"
    assert authority.target.table == "orders"
    assert state.probes == [(envelope.state_key, expected_seed, connector)]
    assert events[0][0] == "lock"
    assert events[0][2] == {"phase": "incremental", "timeout_ms": 1234}
    assert events[1] == (
        "head",
        authority.target,
        {
            "publication_receipt_id": "publication-receipt",
            "state_key_sha256": envelope.state_key.digest.hex(),
            "seed_load_id": expected_seed,
            "xmin_receipt_id": "xmin-seed-receipt",
        },
    )


def test_direct_seed_is_durably_compatible_without_live_head(monkeypatch) -> None:
    receipt = CheckpointCommitOutcome("xmin-seed-receipt", 100, candidate_revision=1)
    state = _State(receipt)
    head_calls: list[object] = []
    monkeypatch.setattr(
        "dpone.runtime.sinks.strategies.mssql.mssql_incremental_publication_head.acquire_publication_lock",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.sinks.strategies.mssql.mssql_incremental_publication_head.require_xmin_publication_head",
        lambda *_args, **_kwargs: head_calls.append(object()),
    )
    monkeypatch.setattr(
        "dpone.runtime.sinks.strategies.mssql.mssql_incremental_publication_head.read_shadow_owner",
        lambda *_args, **_kwargs: None,
    )
    envelope = _envelope()
    guard = MssqlIncrementalPublicationHeadGuard(connector=object(), state_storage=state)

    authority = guard.acquire(_config(), envelope, timeout_ms=100)
    guard.require_locked(authority, envelope)

    assert len(state.probes) == 1
    assert head_calls == []


def test_legacy_shadow_seed_without_publication_binding_fails_closed(monkeypatch) -> None:
    receipt = CheckpointCommitOutcome("xmin-seed-receipt", 100, candidate_revision=1)
    state = _State(receipt)
    monkeypatch.setattr(
        "dpone.runtime.sinks.strategies.mssql.mssql_incremental_publication_head.acquire_publication_lock",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.sinks.strategies.mssql.mssql_incremental_publication_head.read_shadow_owner",
        lambda *_args, **_kwargs: "legacy-initial-campaign",
    )
    envelope = _envelope()
    guard = MssqlIncrementalPublicationHeadGuard(connector=object(), state_storage=state)

    authority = guard.acquire(_config(), envelope, timeout_ms=100)

    with pytest.raises(RuntimeError, match="legacy_publication_migration_required"):
        guard.require_locked(authority, envelope)


@pytest.mark.parametrize(
    "receipt,previous",
    [
        (None, XMinState(101, datetime.now(UTC), revision=2)),
        (CheckpointCommitOutcome("seed", 100, candidate_revision=2), XMinState(101, datetime.now(UTC), revision=2)),
        (CheckpointCommitOutcome("seed", 100, candidate_revision=1), None),
        (CheckpointCommitOutcome("seed", 100, candidate_revision=1), XMinState(99, datetime.now(UTC), revision=2)),
    ],
)
def test_missing_or_incoherent_seed_fails_before_head_and_dml(monkeypatch, receipt, previous) -> None:
    monkeypatch.setattr(
        "dpone.runtime.sinks.strategies.mssql.mssql_incremental_publication_head.acquire_publication_lock",
        lambda *_args, **_kwargs: None,
    )
    envelope = _envelope(previous=previous)
    if previous is None:
        envelope.previous_checkpoint = None
    guard = MssqlIncrementalPublicationHeadGuard(connector=object(), state_storage=_State(receipt))
    authority = guard.acquire(_config(), envelope, timeout_ms=100)

    with pytest.raises(RuntimeError, match="postgres_xmin_handoff.not_committed"):
        guard.require_locked(authority, envelope)


def test_auto_mode_does_not_claim_publication_authority(monkeypatch) -> None:
    state = _State(None)
    lock_calls: list[object] = []
    monkeypatch.setattr(
        "dpone.runtime.sinks.strategies.mssql.mssql_incremental_publication_head.acquire_publication_lock",
        lambda *_args, **_kwargs: lock_calls.append(object()),
    )
    config = SimpleNamespace(options={})
    guard = MssqlIncrementalPublicationHeadGuard(connector=object(), state_storage=state)

    authority = guard.acquire(config, _envelope(), timeout_ms=100)
    guard.require_locked(authority, _envelope())

    assert authority is None
    assert state.probes == []
    assert lock_calls == []
