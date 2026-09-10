"""Native staged lifecycle preserves ownership and publication outcome authority."""

from types import SimpleNamespace

import pytest

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.mssql_native_staged_load import (
    MssqlNativeStagedLoadService,
    NativePreparedStage,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalizer import MssqlGenericCommitOutcomeUnknown


def fixture_service(*, failure=None):
    events = []
    staging = SimpleNamespace(schema="stage", table="owned", database="db", row_count=2)
    admission = SimpleNamespace(
        operation=SimpleNamespace(attempt=SimpleNamespace(request=SimpleNamespace(load_id="load")))
    )
    prepared = NativePreparedStage(staging, admission, "lifecycle", None, None)

    class Preparer:
        def stage(self, config, payload):
            events.append("stage")
            return prepared

        def reverify(self, value):
            assert value is prepared
            events.append("verify")

        def cleanup(self, value):
            events.append("cleanup")

    class Finalizer:
        def __init__(self, strategy, state):
            pass

        def finalize(self, *args, **kwargs):
            events.append("finalize")
            if failure:
                raise failure
            return LoadResult(inserted_rows=2, updated_rows=0, total_rows=2)

    strategy = SimpleNamespace(transaction_finalizer_factory=Finalizer, state_storage=object())
    sink = SimpleNamespace(_strategy_map={LoadStrategy.FULL_REFRESH: strategy})
    service = MssqlNativeStagedLoadService(sink, Preparer())
    config = SimpleNamespace(load_strategy=LoadStrategy.FULL_REFRESH, options={})
    return service, config, events


def test_stage_defers_publication_and_finalize_reverifies():
    service, config, events = fixture_service()
    handle = service.stage(config, object())
    assert events == ["stage"]
    result = service.finalize(config, handle)
    assert result.inserted_rows == 2
    assert events == ["stage", "verify", "finalize"]
    with pytest.raises(RuntimeError, match="already_published"):
        service.finalize(config, handle)


def test_unknown_commit_preserves_stages_and_blocks_abort():
    service, config, events = fixture_service(failure=MssqlGenericCommitOutcomeUnknown())
    handle = service.stage(config, object())
    with pytest.raises(MssqlGenericCommitOutcomeUnknown):
        service.finalize(config, handle)
    with pytest.raises(RuntimeError, match="publication_unresolved"):
        service.abort(handle)
    assert "cleanup" not in events


def test_abort_cleans_only_owned_unpublished_handle():
    service, config, events = fixture_service()
    other, _, _ = fixture_service()
    handle = service.stage(config, object())
    with pytest.raises(ValueError, match="handle_not_owned"):
        other.abort(handle)
    service.abort(handle)
    assert events == ["stage", "cleanup"]


def test_mutated_config_fails_before_publication():
    service, config, events = fixture_service()
    handle = service.stage(config, object())
    config.options["changed"] = True
    with pytest.raises(ValueError, match="configuration_changed"):
        service.finalize(config, handle)
    assert events == ["stage"]


def test_direct_load_uses_same_staging_and_finalization():
    service, config, events = fixture_service()
    assert service.load(config, object()).inserted_rows == 2
    assert events == ["stage", "verify", "finalize", "cleanup"]


@pytest.mark.parametrize("replacement", [[], [("new", 2), ("new", 2)]])
def test_authored_interval_preserves_null_and_outside_rows_including_empty_source(replacement):
    from datetime import UTC, datetime

    from dpone.contracts.rolling_window import FrozenRollingWindow

    start, end = datetime(2026, 1, 2, tzinfo=UTC), datetime(2026, 1, 3, tzinfo=UTC)
    target = [("before", datetime(2026, 1, 1, tzinfo=UTC)), ("inside", start), ("after", end), ("null", None)]
    statements = []

    class Connector:
        def quote_identifier(self, column):
            return f"[{column}]"

        def execute_query(self, sql, params=()):
            statements.append((sql, params))
            target[:] = [row for row in target if row[1] is None or not params[0] <= row[1] < params[1]]

    class Strategy:
        connector = Connector()

        def _target_name(self, config):
            return "[db].[dbo].[events]"

        def _count_target_matching_sql(self, config, predicate, params):
            return sum(row[1] is not None and params[0] <= row[1] < params[1] for row in target)

        def _insert_from_staging_to_table(self, config, stage, table, **kwargs):
            target.extend(replacement)
            return len(replacement)

        def _count_target(self, config):
            return len(target)

    config = SimpleNamespace(load_strategy=LoadStrategy.PARTITION_REPLACE, target_table="events")
    prepared = NativePreparedStage(
        object(), None, None, None, None, FrozenRollingWindow("observed_at", start, end, (start, end))
    )
    result = MssqlNativeStagedLoadService._handler(Strategy(), config, prepared)(prepared.staging)
    assert result.replaced_rows == 1
    assert target[:3] == [("before", datetime(2026, 1, 1, tzinfo=UTC)), ("after", end), ("null", None)]
    assert target[3:] == replacement
    assert statements == [
        ("DELETE FROM [db].[dbo].[events] WHERE [observed_at] >= ? AND [observed_at] < ?", (start, end))
    ]
