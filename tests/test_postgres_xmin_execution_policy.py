from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.config.postgres_xmin_execution import require_postgres_xmin_execution_route
from dpone.contracts.postgres_xmin_execution import (
    PostgresXminExecutionMode,
    checkpoint_process_identity,
    postgres_xmin_execution_policy,
    xmin_handoff_seed_load_id,
)
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.ports.source_state_storage import CheckpointCommitOutcome, SourceStateKey
from dpone.runtime.sources.strategies.postgres.postgres_xmin_handoff_admission import (
    require_committed_xmin_handoff,
)
from dpone.runtime.state.xmin_storage import XMinState


def _config(*, mode: str, strategy: str, key_snapshot: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        load_strategy=SimpleNamespace(value=strategy),
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "incremental_strategy": "xmin",
            "xmin_execution": {
                "mode": mode,
                "handoff_id": "crm_archive_reg_important_entity_change_log_v1",
            },
            "backfill": {
                "inner_mode": "incremental_merge",
                "chunk": {"column": "id", "from": 1, "to": 10, "step": 1},
                "state": {"backend": "audit_schema", "require_distributed_lock": True},
            },
            "unique_key": ["id"],
            "reconciliation_policy": ({"enabled": True, "mode": "key_snapshot"} if key_snapshot else None),
        },
        reconciliation_policy={"enabled": True, "mode": "key_snapshot"} if key_snapshot else None,
    )


def test_absent_policy_preserves_auto_compatibility() -> None:
    policy = postgres_xmin_execution_policy({})

    assert policy.mode is PostgresXminExecutionMode.AUTO
    assert policy.handoff_id is None
    assert checkpoint_process_identity(policy, authored_process="orders") == "orders"


@pytest.mark.parametrize("mode", ("initial", "incremental"))
def test_explicit_modes_bind_one_stable_checkpoint_owner(mode: str) -> None:
    policy = postgres_xmin_execution_policy({"xmin_execution": {"mode": mode, "handoff_id": "orders_history_v1"}})

    assert policy.mode.value == mode
    assert policy.to_contract() == {"version": 1, "mode": mode, "handoff_id": "orders_history_v1"}
    assert (
        checkpoint_process_identity(policy, authored_process="different_manifest") == "xmin-handoff:orders_history_v1"
    )


@pytest.mark.parametrize(
    "raw",
    (
        None,
        [],
        "initial",
        {"mode": "unknown", "handoff_id": "orders_v1"},
        {"mode": "initial"},
        {"mode": "auto", "handoff_id": "orders_v1"},
        {"mode": "initial", "handoff_id": "Orders"},
        {"mode": "initial", "handoff_id": "orders v1"},
        {"mode": "initial", "handoff_id": "orders_v1", "extra": True},
    ),
)
def test_policy_rejects_ambiguous_authoring(raw: object) -> None:
    with pytest.raises(ValueError, match="postgres_xmin_handoff"):
        postgres_xmin_execution_policy({"xmin_execution": raw})


def test_initial_accepts_legacy_chunked_incremental_merge_backfill() -> None:
    config = _config(mode="initial", strategy="backfill")

    policy = require_postgres_xmin_execution_route(config)

    assert policy.mode is PostgresXminExecutionMode.INITIAL


@pytest.mark.parametrize(
    ("source_type", "sink_type"),
    (
        ("postgres", "MSSQL"),
        ("postgres", "microsoft mssql"),
        ("postgres", "microsoft_mssql"),
        ("postgres", "odbc"),
        ("postgres", "sqlserver"),
        ("postgres", "sql_server"),
        ("postgres", "sql-server"),
        ("postgresql", "mssql"),
        ("PostgreSQL", "mssql"),
    ),
)
def test_initial_accepts_every_canonical_route_alias(source_type: str, sink_type: str) -> None:
    config = _config(mode="initial", strategy="backfill")
    config.options.update(source_type=source_type, sink_type=sink_type)

    assert require_postgres_xmin_execution_route(config).mode is PostgresXminExecutionMode.INITIAL


def test_initial_accepts_resumable_shadow_append_backfill() -> None:
    config = _config(mode="initial", strategy="backfill")
    config.options["backfill"].update(
        {
            "inner_mode": "incremental_append",
            "publication": {"mode": "shadow_swap", "retain_backup": True},
        }
    )
    config.options["backfill"]["chunk"] = {"column": "id", "kind": "uuid", "buckets": 10}
    config.only_new_rows = False

    policy = require_postgres_xmin_execution_route(config)

    assert policy.mode is PostgresXminExecutionMode.INITIAL


@pytest.mark.parametrize(
    "publication",
    (
        None,
        {"mode": "direct"},
        {"mode": "shadow_swap", "retain_backup": False},
    ),
)
def test_initial_rejects_incomplete_shadow_append_publication(publication: object) -> None:
    config = _config(mode="initial", strategy="backfill")
    config.options["backfill"]["inner_mode"] = "incremental_append"
    if publication is not None:
        config.options["backfill"]["publication"] = publication

    with pytest.raises(ValueError, match=r"postgres_xmin_handoff\.initial_contract_invalid"):
        require_postgres_xmin_execution_route(config)


def test_initial_rejects_only_new_rows_shadow_append() -> None:
    config = _config(mode="initial", strategy="backfill")
    config.options["backfill"].update(
        {
            "inner_mode": "incremental_append",
            "publication": {"mode": "shadow_swap", "retain_backup": True},
        }
    )
    config.only_new_rows = True

    with pytest.raises(ValueError, match=r"postgres_xmin_handoff\.initial_contract_invalid"):
        require_postgres_xmin_execution_route(config)


def test_initial_accepts_closed_uuid_bucket_plan() -> None:
    config = _config(mode="initial", strategy="backfill")
    config.options["backfill"]["chunk"] = {"column": "id", "kind": "uuid", "buckets": 64}

    policy = require_postgres_xmin_execution_route(config)

    assert policy.mode is PostgresXminExecutionMode.INITIAL


def test_load_config_builder_preserves_initial_phase_and_backfill_contract() -> None:
    config = LoadConfigBuilder().build(
        {
            "source": {
                "type": "postgres",
                "connection_id": "pg",
                "table": {"schema": "public", "name": "orders"},
                "options": {
                    "incremental_strategy": "xmin",
                    "xmin_execution": {"mode": "initial", "handoff_id": "orders_v1"},
                },
            },
            "sink": {
                "type": "mssql",
                "connection_id": "mssql",
                "table": {"schema": "landing", "name": "orders"},
                "strategy": {
                    "mode": "backfill",
                    "unique_key": ["id"],
                    "backfill": {
                        "inner_mode": "incremental_merge",
                        "state": {"backend": "audit_schema", "require_distributed_lock": True},
                        "chunk": {"column": "id", "from": 1, "to": 100, "step": 10, "kind": "integer"},
                    },
                },
            },
        }
    )

    assert config.load_strategy.value == "backfill"
    assert config.options["xmin_execution"] == {"mode": "initial", "handoff_id": "orders_v1"}
    assert config.options["backfill"]["chunk"]["step"] == 10
    assert require_postgres_xmin_execution_route(config).mode is PostgresXminExecutionMode.INITIAL


@pytest.mark.parametrize(
    ("mutate", "error"),
    (
        (lambda cfg: setattr(cfg.load_strategy, "value", "incremental_merge"), "initial_contract_invalid"),
        (lambda cfg: cfg.options["backfill"].pop("chunk"), "initial_contract_invalid"),
        (lambda cfg: cfg.options["backfill"].update({"inner_mode": "replace"}), "initial_contract_invalid"),
        (lambda cfg: cfg.options["backfill"].update({"state": {"backend": "file"}}), "initial_contract_invalid"),
        (
            lambda cfg: cfg.options["backfill"].update(
                {"state": {"backend": "audit_schema", "require_distributed_lock": False}}
            ),
            "initial_contract_invalid",
        ),
        (lambda cfg: cfg.options.update({"unique_key": []}), "initial_contract_invalid"),
        (lambda cfg: cfg.options.update({"incremental_strategy": "column"}), "initial_contract_invalid"),
        (lambda cfg: cfg.options.update({"source_type": "mysql"}), "route_unsupported"),
        (lambda cfg: cfg.options.update({"sink_type": "postgres"}), "route_unsupported"),
    ),
)
def test_initial_rejects_unsafe_route_before_io(mutate: object, error: str) -> None:
    config = _config(mode="initial", strategy="backfill")
    mutate(config)  # type: ignore[operator]

    with pytest.raises(ValueError, match=rf"postgres_xmin_handoff\.{error}"):
        require_postgres_xmin_execution_route(config)


def test_incremental_requires_xmin_key_snapshot_merge() -> None:
    config = _config(mode="incremental", strategy="incremental_merge", key_snapshot=True)

    policy = require_postgres_xmin_execution_route(config)

    assert policy.mode is PostgresXminExecutionMode.INCREMENTAL


def test_load_config_builder_preserves_incremental_phase_and_key_snapshot_contract() -> None:
    config = LoadConfigBuilder().build(
        {
            "source": {
                "type": "postgres",
                "connection_id": "pg",
                "table": {"schema": "public", "name": "orders"},
                "options": {
                    "incremental_strategy": "xmin",
                    "xmin_execution": {"mode": "incremental", "handoff_id": "orders_v1"},
                },
            },
            "sink": {
                "type": "mssql",
                "connection_id": "mssql",
                "table": {"schema": "landing", "name": "orders"},
                "strategy": {"mode": "incremental_merge", "unique_key": ["id"]},
            },
            "reconciliation": {
                "enabled": True,
                "mode": "key_snapshot",
                "cadence": "every_run",
                "consistency": "same_source_snapshot",
                "delete_policy": "soft_delete",
            },
        }
    )

    assert config.load_strategy.value == "incremental_merge"
    assert config.options["xmin_execution"] == {"mode": "incremental", "handoff_id": "orders_v1"}
    assert config.reconciliation_policy is not None
    assert require_postgres_xmin_execution_route(config).mode is PostgresXminExecutionMode.INCREMENTAL


@pytest.mark.parametrize(
    "mutate",
    (
        lambda cfg: setattr(cfg.load_strategy, "value", "backfill"),
        lambda cfg: cfg.options.update({"incremental_strategy": "column"}),
        lambda cfg: setattr(cfg, "reconciliation_policy", None),
    ),
)
def test_incremental_rejects_missing_handoff_preconditions(mutate: object) -> None:
    config = _config(mode="incremental", strategy="incremental_merge", key_snapshot=True)
    mutate(config)  # type: ignore[operator]

    with pytest.raises(ValueError, match="postgres_xmin_handoff.incremental_contract_invalid"):
        require_postgres_xmin_execution_route(config)


def _admission_key() -> SourceStateKey:
    return SourceStateKey(
        environment="dev",
        process="xmin-handoff:orders_v1",
        source_connection="pg",
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


def test_incremental_admission_requires_deterministic_seed_receipt() -> None:
    key = _admission_key()
    policy = postgres_xmin_execution_policy({"xmin_execution": {"mode": "incremental", "handoff_id": "orders_v1"}})
    state = XMinState(xmin_value=120, timestamp=datetime.now(UTC), revision=2)
    expected_load_id = xmin_handoff_seed_load_id(handoff_id="orders_v1", state_key_digest=key.digest)

    class Storage:
        def probe_receipt(self, *, key: SourceStateKey, load_id: str):
            assert key == _admission_key()
            assert load_id == expected_load_id
            return CheckpointCommitOutcome("receipt", 100, candidate_revision=1)

    assert (
        require_committed_xmin_handoff(
            state_storage=Storage(),
            state_key=key,
            state=state,
            policy=policy,
        )
        is state
    )


@pytest.mark.parametrize("state", [None, XMinState(xmin_value=100, timestamp=datetime.now(UTC), revision=1)])
def test_incremental_admission_fails_closed_without_seed_receipt(state) -> None:
    policy = postgres_xmin_execution_policy({"xmin_execution": {"mode": "incremental", "handoff_id": "orders_v1"}})
    storage = SimpleNamespace(probe_receipt=lambda **_kwargs: None)

    with pytest.raises(RuntimeError, match="postgres_xmin_handoff.not_committed"):
        require_committed_xmin_handoff(
            state_storage=storage,
            state_key=_admission_key(),
            state=state,
            policy=policy,
        )
