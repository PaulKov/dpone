"""Hermetic contracts for ClickHouse snapshot_diff / scd2 finalizers."""

from __future__ import annotations

from copy import copy
from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.governance.ports import StagedLoadHandle, StagedLoadPostCommitCleanupError
from dpone.runtime.sinks.clickhouse_production_finalize import ClickHouseProductionFinalizer
from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink
from dpone.runtime.sinks.clickhouse_staged_load import ClickHouseStagedLoadService
from dpone.runtime.sinks.clickhouse_staging_finalizer import (
    CLICKHOUSE_STAGED_VALIDATION_RECEIPT_INVALID,
    ClickHouseStagingFinalizer,
)
from dpone.runtime.sinks.merge_policy import MergePolicy


class FakeConnector:
    def __init__(self, *, null_keys: int = 0, duplicate_keys: int = 0) -> None:
        self.queries: list[str] = []
        self.null_keys = null_keys
        self.duplicate_keys = duplicate_keys

    def execute_query(self, query: str, params=None) -> int:
        self.queries.append(query)
        return 1

    def get_records(self, query: str, as_dict: bool = False):
        del as_dict
        self.queries.append(query)
        if " IS NULL" in query:
            return [(self.null_keys,)]
        if "HAVING count() > 1" in query:
            return [(self.duplicate_keys,)]
        if query.startswith("SELECT count()"):
            return [(1,)]
        return [(1,)]


class UncopyableRuntimeConnector:
    """Model a live DB client that must stay identity-bound during snapshots."""

    def __deepcopy__(self, memo):  # noqa: ANN001
        del memo
        raise TypeError("runtime connector cannot be copied")


class FakeSink:
    def __init__(self) -> None:
        self.connector = FakeConnector()
        self._staging_finalizer = ClickHouseStagingFinalizer(
            connector=self.connector,
            table_name=lambda cfg: f"`{cfg.target_schema}`.`{cfg.target_table}`",
            count_rows=lambda cfg: 1,
            mutations_sync=lambda cfg: 1,
        )
        self.swapped = False
        self._exists = True

    def _table(self, config) -> str:
        return f"`{config.target_schema}`.`{config.target_table}`"

    def _table_exists(self, load_config) -> bool:
        return self._exists

    def _count(self, load_config) -> int:
        return 2

    def _count_where(self, load_config, predicate: str) -> int:
        del load_config, predicate
        return 1

    def _swap_table_into_target(self, load_config, replacement) -> None:
        del load_config, replacement
        self.swapped = True

    def _insert_from_table(self, source, target) -> int:
        del source, target
        return 2

    def _create_shadow_table(self, load_config):
        return SimpleNamespace(
            target_schema=load_config.target_schema,
            target_table=f"{load_config.target_table}__shadow",
        )

    def _drop_table(self, table, config) -> None:
        del table, config


def _handle() -> StagedLoadHandle:
    staging = SimpleNamespace(target_schema="dpone_it", target_table="stg")
    return StagedLoadHandle(
        staging_config=staging,
        payload_schema=(("id", "Int64"), ("__dpone__row_hash", "String")),
        staged_rows=2,
        finalization_config=staging,
    )


def _config(strategy: LoadStrategy, **options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="dst",
        source_schema="public",
        source_table="orders",
        target_schema="dpone_it",
        target_table="orders",
        load_strategy=strategy,
        unique_key=["id"],
        options=options,
    )


def test_staged_load_dispatches_snapshot_diff_and_scd2() -> None:
    sink = FakeSink()
    service = ClickHouseStagedLoadService(sink)
    handle = _handle()
    result = service.finalize(_config(LoadStrategy.SNAPSHOT_DIFF, diff={"delete_policy": "hard_delete"}), handle)
    assert result.total_rows == 2
    assert any("DELETE FROM" in q for q in sink.connector.queries)

    sink2 = FakeSink()
    service2 = ClickHouseStagedLoadService(sink2)
    result2 = service2.finalize(
        _config(
            LoadStrategy.SCD2,
            scd2={"delete_policy": "expire"},
        ),
        _handle(),
    )
    assert result2.total_rows == 2
    assert any("ALTER TABLE" in q and "UPDATE" in q for q in sink2.connector.queries)


def test_projected_staging_validation_precedes_target_preparation_and_lookup() -> None:
    events: list[str] = []

    class OrderedSink(FakeSink):
        def __init__(self) -> None:
            super().__init__()
            self._exists = False
            validation_token = object()

            def validate(_load_config, staging):  # noqa: ANN001
                events.append(f"validate:{staging.target_table}")
                return validation_token

            def require(token, _load_config, _staging):  # noqa: ANN001
                if token is not validation_token:
                    raise AssertionError("wrong validation token")

            self._staging_finalizer = SimpleNamespace(
                validate_strategy_staging_key_integrity=validate,
                require_strategy_staging_validation=require,
            )

        def _prepare_staged_finalization(self, load_config, handle):  # noqa: ANN001
            del load_config, handle
            events.append("prepare_target")

        def _table_exists(self, load_config):  # noqa: ANN001
            del load_config
            events.append("target_lookup")
            return False

        def _swap_table_into_target(self, load_config, replacement):  # noqa: ANN001
            del load_config, replacement
            events.append("target_swap")

    projected = SimpleNamespace(target_schema="dpone_it", target_table="orders__projected")
    handle = replace(_handle(), finalization_config=projected)

    ClickHouseStagedLoadService(OrderedSink()).finalize(
        _config(LoadStrategy.INCREMENTAL_MERGE),
        handle,
    )

    assert events == [
        "validate:orders__projected",
        "prepare_target",
        "target_lookup",
        "target_swap",
    ]


def test_validated_finalizer_rejects_forged_or_drifted_token_before_target() -> None:
    sink = FakeSink()
    service = ClickHouseStagedLoadService(sink)
    load_config = _config(LoadStrategy.FULL_REFRESH)
    handle = _handle()

    with pytest.raises(ValueError, match=CLICKHOUSE_STAGED_VALIDATION_RECEIPT_INVALID):
        service.finalize_validated(load_config, handle, object())
    assert sink.swapped is False

    validation_token = service.validate(load_config, handle)
    load_config.target_table = "other_target"
    with pytest.raises(ValueError, match=CLICKHOUSE_STAGED_VALIDATION_RECEIPT_INVALID):
        service.finalize_validated(load_config, handle, validation_token)
    assert sink.swapped is False


def test_clickhouse_sink_rejects_lookalike_validation_receipt() -> None:
    staged_sink = FakeSink()
    facade = object.__new__(ClickHouseSink)
    facade._staged_load = ClickHouseStagedLoadService(staged_sink)
    load_config = _config(LoadStrategy.FULL_REFRESH)
    handle = _handle()

    class LookalikeReceipt:
        @staticmethod
        def frozen_inputs(*, sink, load_config):  # noqa: ANN001
            del sink
            return object(), load_config, handle

    with pytest.raises(ValueError, match=CLICKHOUSE_STAGED_VALIDATION_RECEIPT_INVALID):
        facade.finalize_staged_load(load_config, LookalikeReceipt())
    assert staged_sink.swapped is False


def test_clickhouse_validation_token_is_single_use() -> None:
    sink = FakeSink()
    sink._exists = False
    service = ClickHouseStagedLoadService(sink)
    load_config = _config(LoadStrategy.FULL_REFRESH)
    handle = _handle()
    validation_token = service.validate(load_config, handle)

    copied_token = copy(validation_token)
    assert copied_token is validation_token

    service.finalize_validated(load_config, handle, validation_token)
    assert sink.swapped is True

    sink.swapped = False
    with pytest.raises(ValueError, match=CLICKHOUSE_STAGED_VALIDATION_RECEIPT_INVALID):
        service.finalize_validated(load_config, handle, copied_token)
    assert sink.swapped is False

    with pytest.raises(AttributeError):
        setattr(validation_token, "load_config", load_config)


def test_clickhouse_validation_snapshot_preserves_runtime_connector_identity() -> None:
    sink = FakeSink()
    sink._exists = False
    service = ClickHouseStagedLoadService(sink)
    source_connector = UncopyableRuntimeConnector()
    load_config = _config(
        LoadStrategy.FULL_REFRESH,
        _source_connector=source_connector,
        semantic_marker={"version": 1},
    )
    handle = _handle()

    validation_token = service.validate(load_config, handle)
    service.finalize_validated(load_config, handle, validation_token)

    assert sink.swapped is True


def test_clickhouse_abort_retires_unused_validation_token() -> None:
    sink = FakeSink()
    service = ClickHouseStagedLoadService(sink)
    load_config = _config(LoadStrategy.FULL_REFRESH)
    handle = _handle()
    validation_token = service.validate(load_config, handle)

    service.abort(handle)

    with pytest.raises(ValueError, match=CLICKHOUSE_STAGED_VALIDATION_RECEIPT_INVALID):
        service.finalize_validated(load_config, handle, validation_token)


def test_snapshot_diff_soft_delete_uses_alter_update() -> None:
    sink = FakeSink()
    finalizer = ClickHouseProductionFinalizer(sink)
    finalizer.snapshot_diff(
        _config(LoadStrategy.SNAPSHOT_DIFF, diff={"delete_policy": "soft_delete"}),
        _handle(),
    )
    assert any("UPDATE" in q and "__dpone__deleted_at" in q for q in sink.connector.queries)


def test_snapshot_diff_new_target_swaps_staging() -> None:
    sink = FakeSink()
    sink._exists = False
    result = ClickHouseProductionFinalizer(sink).snapshot_diff(
        _config(LoadStrategy.SNAPSHOT_DIFF),
        _handle(),
    )
    assert sink.swapped is True
    assert result.inserted_rows == 2


def test_scd2_rejects_unknown_delete_policy() -> None:
    sink = FakeSink()
    with pytest.raises(ValueError, match="expire, ignore"):
        ClickHouseProductionFinalizer(sink).scd2(
            _config(LoadStrategy.SCD2, scd2={"delete_policy": "hard_delete"}),
            _handle(),
        )


def test_merge_policy_mutation_delete_path() -> None:
    sink = FakeSink()
    config = replace(
        _config(LoadStrategy.SNAPSHOT_DIFF, diff={"delete_policy": "hard_delete"}),
        merge_policy=MergePolicy.MUTATION_DELETE_INSERT,
        allow_non_recommended_policy=True,
    )
    ClickHouseProductionFinalizer(sink).snapshot_diff(config, _handle())
    assert any("ALTER TABLE" in q and "DELETE" in q for q in sink.connector.queries)


@pytest.mark.parametrize(
    ("null_keys", "duplicate_keys", "expected_code"),
    [
        (1, 0, "DPONE_CLICKHOUSE_STAGING_UNIQUE_KEY_NULL"),
        (0, 1, "DPONE_CLICKHOUSE_STAGING_UNIQUE_KEY_DUPLICATE"),
    ],
)
def test_staging_key_integrity_fails_closed_in_null_then_duplicate_order(
    null_keys: int,
    duplicate_keys: int,
    expected_code: str,
) -> None:
    connector = FakeConnector(
        null_keys=null_keys,
        duplicate_keys=duplicate_keys,
    )
    finalizer = ClickHouseStagingFinalizer(
        connector=connector,
        table_name=lambda cfg: f"`{cfg.target_schema}`.`{cfg.target_table}`",
        count_rows=lambda cfg: 1,
        mutations_sync=lambda cfg: 1,
    )

    with pytest.raises(ValueError) as error:
        finalizer.validate_staging_key_integrity(
            _config(LoadStrategy.INCREMENTAL_MERGE),
            _handle().staging_config,
            ("id", "valid_at"),
        )

    assert getattr(error.value, "code", None) == expected_code
    assert " IS NULL OR " in connector.queries[0]
    if null_keys:
        assert not any("HAVING count() > 1" in query for query in connector.queries)
    else:
        assert "HAVING count() > 1" in connector.queries[1]


@pytest.mark.parametrize(
    "strategy",
    [
        LoadStrategy.INCREMENTAL_MERGE,
        LoadStrategy.SNAPSHOT_DIFF,
        LoadStrategy.SCD2,
    ],
)
def test_every_keyed_strategy_uses_the_same_staging_integrity_gate(
    strategy: LoadStrategy,
) -> None:
    connector = FakeConnector(null_keys=1)
    finalizer = ClickHouseStagingFinalizer(
        connector=connector,
        table_name=lambda cfg: f"`{cfg.target_schema}`.`{cfg.target_table}`",
        count_rows=lambda cfg: 1,
        mutations_sync=lambda cfg: 1,
    )

    with pytest.raises(ValueError) as error:
        finalizer.validate_strategy_staging_key_integrity(
            _config(strategy),
            _handle().staging_config,
        )

    assert getattr(error.value, "code", None) == ("DPONE_CLICKHOUSE_STAGING_UNIQUE_KEY_NULL")


def test_staging_integrity_probe_fails_closed_on_malformed_result() -> None:
    connector = FakeConnector()
    connector.get_records = lambda query, as_dict=False: []  # type: ignore[method-assign]
    finalizer = ClickHouseStagingFinalizer(
        connector=connector,
        table_name=lambda cfg: f"`{cfg.target_schema}`.`{cfg.target_table}`",
        count_rows=lambda cfg: 1,
        mutations_sync=lambda cfg: 1,
    )

    with pytest.raises(ValueError) as error:
        finalizer.validate_strategy_staging_key_integrity(
            _config(LoadStrategy.SNAPSHOT_DIFF),
            _handle().staging_config,
        )

    assert getattr(error.value, "code", None) == ("DPONE_CLICKHOUSE_STAGING_UNIQUE_KEY_NULL")


def test_direct_load_preserves_primary_failure_when_abort_cleanup_fails() -> None:
    class FailingDirectService(ClickHouseStagedLoadService):
        def __init__(self) -> None:
            self.cleanup_calls = 0

        @staticmethod
        def stage(load_config, payload):  # noqa: ANN001
            del load_config, payload
            return _handle()

        @staticmethod
        def validate(load_config, handle):  # noqa: ANN001
            del load_config, handle
            raise RuntimeError("primary finalization failure")

        def abort(self, handle):  # noqa: ANN001
            del handle
            self.cleanup_calls += 1
            raise RuntimeError("cleanup failure")

        def cleanup(self, handle):  # noqa: ANN001
            del handle
            self.cleanup_calls += 1

    service = FailingDirectService()
    with pytest.raises(RuntimeError, match="primary finalization failure") as raised:
        service.load(_config(LoadStrategy.INCREMENTAL_MERGE), object())

    assert service.cleanup_calls == 1
    assert raised.value.__notes__ == ["staged cleanup failed: RuntimeError"]


def test_direct_load_retains_staging_when_target_mutates_then_raises() -> None:
    class MutatingSink(FakeSink):
        def _swap_table_into_target(self, load_config, replacement) -> None:
            del load_config, replacement
            self.swapped = True
            raise RuntimeError("target mutation failed")

    class DirectService(ClickHouseStagedLoadService):
        def __init__(self, sink) -> None:  # noqa: ANN001
            super().__init__(sink)
            self.cleanup_calls = 0

        @staticmethod
        def stage(load_config, payload):  # noqa: ANN001
            del load_config, payload
            return _handle()

        def cleanup(self, handle):  # noqa: ANN001
            del handle
            self.cleanup_calls += 1

    sink = MutatingSink()
    sink._exists = False
    service = DirectService(sink)

    with pytest.raises(RuntimeError, match="target mutation failed") as raised:
        service.load(_config(LoadStrategy.FULL_REFRESH), object())

    assert sink.swapped is True
    assert service.cleanup_calls == 0
    assert raised.value.details["target_outcome"] == "commit_unknown"
    assert raised.value.details["cleanup_status"] == "retained_for_reconciliation"


def test_direct_load_cleanup_failure_is_confirmed_commit_and_not_retryable() -> None:
    class CountingSink(FakeSink):
        insert_calls = 0

        def _insert_from_table(self, source, target) -> int:
            del source, target
            self.insert_calls += 1
            return 2

    class CleanupFailureService(ClickHouseStagedLoadService):
        @staticmethod
        def stage(load_config, payload):  # noqa: ANN001
            del load_config, payload
            return _handle()

        @staticmethod
        def cleanup(handle):  # noqa: ANN001
            del handle
            raise RuntimeError("cleanup failure")

    sink = CountingSink()
    service = CleanupFailureService(sink)

    with pytest.raises(StagedLoadPostCommitCleanupError) as raised:
        service.load(_config(LoadStrategy.INCREMENTAL_APPEND), object())

    assert sink.insert_calls == 1
    assert raised.value.details["target_outcome"] == "committed"
    assert raised.value.details["cleanup_status"] == "failed"
    assert raised.value.details["safe_to_retry"] is False
