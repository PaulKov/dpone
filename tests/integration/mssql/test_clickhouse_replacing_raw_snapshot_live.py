"""Opt-in raw snapshot route diagnostics on disposable, explicitly configured TLS.

Set DPONE_RUN_RAW_SNAPSHOT_LIVE=1, DPONE_IT_CH_HOST, DPONE_IT_CH_PORT,
DPONE_IT_CH_CA_CERT and the existing DPONE_IT_MSSQL_* authority. Both importers
are required once enabled; select separate profiles with pytest -k. The bad-CA
cell additionally requires DPONE_IT_CH_BAD_CA_CERT (a valid unrelated CA PEM).
These correctness checks do not certify a platform or cluster-wide freshness.
"""

from __future__ import annotations

import json
import os
import uuid
from collections import Counter
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pytest
from tests.integration.mssql.clickhouse_raw_snapshot_live_support import (
    changed_source_rows,
    commit_source_change,
    preserve_on_failure,
    preserving_governed_route,
    raw_case,
    require_causal_source_rejection,
    route_process,
    tls_source,
)
from tests.integration.mssql.mssql_live_support import mssql_connector

from dpone.adapters.mssql_native_recovery_journal import MssqlNativeRecoveryJournalReader
from dpone.app.mssql_native_recovery_application import MssqlNativeRecoveryApplication
from dpone.runtime.mssql_native_application import MssqlNativeApplicationRuntime
from dpone.runtime.mssql_native_runtime import NativeMssqlRuntime
from dpone.runtime.process_logging import etl_logger
from dpone.runtime.sources.clickhouse_native_source import NativeQueryArtifact

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.integration_mssql,
    pytest.mark.integration_clickhouse,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_RAW_SNAPSHOT_LIVE") != "1", reason="explicit raw snapshot live opt-in required"
    ),
]


@contextmanager
def synthetic_route(tmp_path, backend, width=7, window="full", *, replicated=False, scope=None, mutable=False):
    """Create/drop only uniquely named synthetic objects owned by this test."""
    source, target = tls_source(), mssql_connector()
    suffix = uuid.uuid4().hex[:16]
    source_table, target_table = f"dpone_raw_src_{suffix}", f"dpone_raw_dst_{suffix}"
    case = raw_case(width)
    try:
        with preserve_on_failure(
            [
                lambda: target.execute_query(f"DROP TABLE IF EXISTS [dbo].[{target_table}]"),
                lambda: source.execute_query(f"DROP TABLE IF EXISTS `{source_table}`"),
            ],
            f"source={source_table}; target={target_table}; journal_root={tmp_path / 'state'}",
        ):
            version = str(source.get_records("SELECT version()")[0][0])
            assert tuple(int(part) for part in version.split(".")[:2]) >= (25, 8), "requires ClickHouse >=25.8"
            engine = (
                f"ReplicatedReplacingMergeTree('/dpone_synthetic/{suffix}', 'only_replica', version, is_deleted)"
                if replicated
                else "ReplacingMergeTree(version, is_deleted)"
            )
            mutation_settings = (
                " SETTINGS enable_block_number_column=1,enable_block_offset_column=1,min_bytes_for_wide_part=0,"
                "max_bytes_to_merge_at_max_space_in_pool=1,max_bytes_to_merge_at_min_space_in_pool=1"
                if mutable
                else ""
            )
            source.execute_query(
                f"CREATE TABLE `{source_table}` ({case.source_ddl()}) ENGINE={engine} PARTITION BY toYYYYMMDD(happened_at) ORDER BY row_key{mutation_settings}"
            )
            # ReplacingMergeTree applies replacement inside an INSERT block when
            # optimize_on_insert=1. Disable only this fixture write optimization;
            # the production SELECT still uses its unchanged frozen read profile.
            source.execute_query(f"INSERT INTO `{source_table}` SETTINGS optimize_on_insert=0 VALUES", case.rows)
            # Fixture oracle is hand-authored, not a second source SELECT used as truth.
            assert source.get_records(f"SELECT count() FROM `{source_table}` SETTINGS final=0") == [(6,)]
            assert source.get_records(
                f"SELECT version, is_deleted, count() FROM `{source_table}` "
                "GROUP BY version, is_deleted ORDER BY version, is_deleted SETTINGS final=0"
            ) == [(1, 0, 3), (2, 0, 1), (3, 1, 2)]
            target.execute_query(f"CREATE TABLE [dbo].[{target_table}] ({case.target_ddl()})")
            sentinels = []
            for date in (datetime(2023, 12, 31), datetime(2024, 1, 2), datetime(2024, 1, 3), datetime(2030, 1, 1)):
                row = (-99, 1, 0, date, "old target", None, None) + (None,) * (width - 7)
                target.execute_query(f"INSERT INTO [dbo].[{target_table}] VALUES ({','.join('?' for _ in row)})", row)
                sentinels.append(row)
            with preserving_governed_route(
                target, target_database=target.database, target_schema="dbo", target_table=target_table
            ) as route:
                process, config = route_process(
                    tmp_path,
                    source,
                    target,
                    route.sink(logger=etl_logger),
                    source_table,
                    target_table,
                    backend,
                    window,
                    scope or ("connected_replica" if replicated else "single_server"),
                )
                yield process, config, case, tuple(sentinels)
                for journal in (tmp_path / "state" / "mssql-native").glob("*.sqlite"):
                    assert MssqlNativeRecoveryJournalReader(journal).inspect_all()["retained_custody_count"] == 0
    finally:
        try:
            target.close()
        finally:
            source.close()


def target_rows(process, config, case):
    return tuple(
        tuple(row)
        for row in process.sink_obj.connector.get_records(
            f"SELECT {case.target_projection()} FROM [dbo].[{config.target_table}]"
        )
    )


def expected_rows(case, sentinels, window):
    if window == "full":
        return case.rows
    start, end = (
        (datetime(2024, 1, 2), datetime(2024, 1, 3))
        if window == "nonempty"
        else (datetime(2030, 1, 1), datetime(2030, 1, 2))
    )
    return case.window_rows(start, end) + tuple(row for row in sentinels if not start <= row[3] < end)


@pytest.mark.parametrize("backend", ["bcp", "mssql_sqlclient"])
@pytest.mark.parametrize("width", [7, 100], ids=["narrow", "wide100"])
@pytest.mark.parametrize("window", ["full", "nonempty", "empty"])
def test_raw_snapshot_preserves_typed_multiset_and_outside_window(tmp_path, backend, width, window):
    with synthetic_route(tmp_path, backend, width, window) as (process, config, case, sentinels):
        result = MssqlNativeApplicationRuntime(process).run(config, owner="synthetic-raw")
        assert result.status == "success"
        assert result.extracted_rows == {"full": 6, "nonempty": 4, "empty": 0}[window]
        assert Counter(target_rows(process, config, case)) == Counter(expected_rows(case, sentinels, window))


@pytest.mark.parametrize("backend", ["bcp", "mssql_sqlclient"])
def test_raw_snapshot_connected_replica_is_only_the_pinned_endpoint(tmp_path, backend):
    with synthetic_route(tmp_path, backend, replicated=True) as (process, config, case, _):
        result = MssqlNativeApplicationRuntime(process).run(config, owner="synthetic-replica")
        assert result.status == "success"
        assert result.extracted_rows == 6
        assert Counter(target_rows(process, config, case)) == Counter(case.rows)


@pytest.mark.parametrize("backend", ["bcp", "mssql_sqlclient"])
@pytest.mark.parametrize("fault", ["pre_eof", "post_eof"])
def test_raw_snapshot_failure_never_partially_publishes_and_recovery_is_source_free(
    tmp_path, monkeypatch, backend, fault
):
    with synthetic_route(tmp_path, backend) as (process, config, case, sentinels):
        original_rows = NativeQueryArtifact.iter_native_rows

        def interrupt_rows(artifact):
            for ordinal, row in enumerate(original_rows(artifact)):
                if ordinal == 3:
                    raise RuntimeError("synthetic_pre_eof")
                yield row

        def interrupt_publish(*_args, **_kwargs):
            raise RuntimeError("synthetic_post_eof")

        with monkeypatch.context() as injected:
            if fault == "pre_eof":
                injected.setattr(NativeQueryArtifact, "iter_native_rows", interrupt_rows)
            else:
                injected.setattr(NativeMssqlRuntime, "_publish", interrupt_publish)
            with pytest.raises(RuntimeError, match=f"synthetic_{fault}"):
                MssqlNativeApplicationRuntime(process).run(config, owner="synthetic-fault")
        assert Counter(target_rows(process, config, case)) == Counter(sentinels)
        journals = list((tmp_path / "state" / "mssql-native").glob("*.sqlite"))
        assert len(journals) == 1
        reader = MssqlNativeRecoveryJournalReader(journals[0])
        inventory = reader.inspect_all()["items"]
        assert len(inventory) == 1
        invocation_id = inventory[0]["invocation_id"]
        if fault == "post_eof":
            assert inventory[0]["state"] == "VERIFIED_EOF"
        else:
            assert inventory[0]["state"] != "PUBLISHED"

        class SourceForbidden:
            def __getattr__(self, _name):
                pytest.fail("recovery attempted source access")

        process.source_obj.connector = SourceForbidden()
        recovered = MssqlNativeRecoveryApplication().execute(
            process,
            journal_root=journals[0],
            invocation_id=invocation_id,
            action="retire" if fault == "pre_eof" else "resume",
            owner="synthetic-recovery",
            confirmed=True,
        )
        if fault == "pre_eof":
            assert recovered["state"] in {"EMPTY_STAGING", "RETIRED"}
            snapshot = reader.load(invocation_id)
            assert snapshot.custody is not None and snapshot.custody.state == "clear"
            assert snapshot.custody.release_reason == "nonpublication_all_stages_retired"
            assert reader.inspect_all()["retained_custody_count"] == 0
            assert snapshot.projection["publication"] is None
            assert recovered["attempts"]
            assert all(attempt["terminal_event"] == "RETIRED" for attempt in recovered["attempts"])
            from dpone.runtime.sinks.mssql_native_import import native_attempt_table_name

            for attempt_id in snapshot.projection["events"]:
                table = native_attempt_table_name(snapshot.identity.plan, attempt_id)
                assert process.sink_obj.connector.get_records("SELECT OBJECT_ID(?, 'U')", (f"dbo.{table}",)) == [
                    (None,)
                ]
            assert Counter(target_rows(process, config, case)) == Counter(sentinels)
        else:
            assert recovered["state"] == "CUSTODY_RELEASED"
            assert Counter(target_rows(process, config, case)) == Counter(case.rows)


def test_raw_snapshot_wrong_replica_scope_fails_before_publish(tmp_path):
    with synthetic_route(tmp_path, "bcp", scope="connected_replica") as (process, config, case, sentinels):
        with pytest.raises(ValueError, match="mssql_native.replica_scope_unproven"):
            MssqlNativeApplicationRuntime(process).run(config, owner="synthetic-scope")
        assert Counter(target_rows(process, config, case)) == Counter(sentinels)


def test_native_tls_rejects_untrusted_ca():
    """A real handshake must reject a valid CA that did not sign the server."""
    from clickhouse_driver.errors import NetworkError

    bad_ca = os.environ.get("DPONE_IT_CH_BAD_CA_CERT")
    assert bad_ca and Path(bad_ca).is_file(), "explicit unrelated CA PEM required"
    source = tls_source()
    source.ca_cert = bad_ca
    try:
        with pytest.raises(RuntimeError, match="CERTIFICATE_VERIFY_FAILED|certificate verify failed") as caught:
            source.get_records("SELECT 1")
        assert isinstance(caught.value.__cause__, NetworkError)
    finally:
        source.close()


@pytest.mark.parametrize("change", ["patch", "mask", "insert", "merge"])
def test_committed_source_change_at_select_barrier_is_visible_or_fail_closed(tmp_path, monkeypatch, change):
    """Synchronize a real commit after physical preflight, without timing sleeps."""
    from dpone.runtime.sources.clickhouse_native_source import ClickHouseNativeSource

    with synthetic_route(tmp_path, "bcp", mutable=True) as (process, config, case, sentinels):
        source = ClickHouseNativeSource(process.source_obj.connector)
        profile = source.snapshot_profile(config)
        assert profile is not None
        artifact = source.extract(
            config,
            expected_profile=profile,
            source_query_binding="clickhouse.raw-query.v1:" + "a" * 64,
        ).artifact
        client = process.source_obj.connector.connection
        original_iter = client.execute_iter
        committed = []
        native_errors = []
        observations = {}
        control = tls_source()

        def after_preflight_before_select(*args, **kwargs):
            assert not committed, "exactly one data SELECT is permitted"
            observations.update(commit_source_change(control, config.source_table, case, change))
            committed.append(change)

            def observe_stream():
                try:
                    yield from original_iter(*args, **kwargs)
                except Exception as error:
                    native_errors.append(type(error).__name__)
                    raise

            return observe_stream()

        try:
            with monkeypatch.context() as barrier:
                barrier.setattr(client, "execute_iter", after_preflight_before_select)
                try:
                    rows = tuple(artifact.iter_native_rows())
                except ValueError as error:
                    require_causal_source_rejection(error)
                    assert not hasattr(artifact, "raw_snapshot_eof")
                    observations.update(outcome="fail_closed", diagnostic_code=str(error))
                else:
                    actual = tuple(tuple(row[name] for name, _, _ in case.columns) for row in rows)
                    # Native wire strings are bytes until the target encoder consumes them.
                    actual = tuple(
                        tuple(value.decode("utf-8") if isinstance(value, bytes) else value for value in row)
                        for row in actual
                    )
                    assert Counter(actual) == Counter(changed_source_rows(case, change))
                    eof = artifact.raw_snapshot_eof
                    assert eof.rows == len(actual)
                    assert eof.endpoint_authority_agreement is False
                    observations.update(
                        outcome="exact_snapshot",
                        rows=eof.rows,
                        endpoint_authority_agreement=eof.endpoint_authority_agreement,
                    )
            assert not native_errors, "vendor stream errors are not a successful fail-closed observation"
            assert committed == [change], "the source mutation itself must succeed before fail-closed is accepted"
            assert Counter(target_rows(process, config, case)) == Counter(sentinels)
            observations.update(
                certification="not_claimed",
                change=change,
                kind="synthetic-source-acquisition-diagnostic",
                profile_sha256=profile.sha256,
            )
            (tmp_path / "raw-source-change.json").write_text(json.dumps(observations, sort_keys=True), encoding="utf-8")
        finally:
            control.close()
