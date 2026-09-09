"""Production-hydrated target, staging, and state database authority matrix."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from dpone.contracts.mssql_transaction_governance import InvocationIdentity, MssqlAttemptRequest
from dpone.contracts.run_context import RunContext
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from dpone.runtime.state.mssql_database_authority import (
    MSSQL_DATABASE_AUTHORITY_SHA256_OPTION,
)
from dpone.runtime.state.mssql_generic_transaction import MssqlGenericTransactionState
from dpone.runtime.state.mssql_generic_transaction_contract import require_generic_transaction_catalog
from dpone.runtime.state.mssql_generic_transaction_ddl import (
    render_generic_transaction_catalog_ddl,
    render_generic_transaction_catalog_v1_to_v2_ddl,
)
from tests.integration.postgres.postgres_live_support import (
    mssql_connector,
    postgres_mssql_enabled,
)
from tests.integration.postgres.postgres_mssql_production_hydration_live_support import (
    STAGING_SCHEMA,
    TARGET_SCHEMA,
    VendorCoordinates,
    close_runtime_bindings,
    database_identity,
    generic_state_row_counts,
    production_hydration_live_fixture,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
    pytest.mark.route_live_wide,
]

_DAG_ID = "DAG__integration__postgres_mssql__target_database_authority"
_LIMITED_PASSWORD = "Dp0ne.Metadata.Probe.2026!"


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_target_and_state_distinct_databases_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Commit through distinct target/state DBIDs using production hydration."""

    with production_hydration_live_fixture(tmp_path) as live:
        master = mssql_connector(database="master")
        bindings = None
        try:
            _apply_environment(monkeypatch, live.runtime_environment)
            before = _route_image(master, live, target=live.target, state=live.state)
            load_config = live.load_config()
            bindings = DefaultRuntimeHydrator().build(
                config=live.runtime_config,
                load_config=load_config,
            )
            result = _processor(bindings).run(
                load_config,
                run_context=_run_context("target-and-state-distinct"),
                dag_id=_DAG_ID,
            )
            assert result["commit_outcome"] == AtomicCommitOutcome.COMMITTED
            assert result["loaded_rows"] == result["final_rows"] == 3
            after = _route_image(
                master,
                live,
                target=bindings.sink_obj.connector,
                state=bindings.sink_obj.state_storage.connector,
            )
            target_id = _database_fact(after, live.target_database)["database_id"]
            state_id = _database_fact(after, live.state_database)["database_id"]
            assert target_id != state_id
            route_live_recorder.observe_case(
                "target_database_authority",
                "target_and_state_distinct_databases",
                before_image=before,
                after_image=after,
                observations={
                    "runtime_hydrator": "DefaultRuntimeHydrator",
                    "target_database_id": target_id,
                    "state_database_id": state_id,
                    "commit_outcome": str(result["commit_outcome"]),
                    "loaded_rows": result["loaded_rows"],
                },
            )
        finally:
            close_runtime_bindings(bindings)
            master.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_target_database_missing_rejects_before_endpoint_and_source_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        live.target.close()
        master = mssql_connector(database="master")
        try:
            _drop_database(master, live.target_database)
            _apply_environment(monkeypatch, live.runtime_environment)
            before = _route_image(master, live, target=None, state=live.state)
            with pytest.raises(RuntimeError, match="mssql_transaction.target_database_missing") as captured:
                DefaultRuntimeHydrator().build(
                    config=live.runtime_config,
                    load_config=live.load_config(),
                )
            after = _route_image(master, live, target=None, state=live.state)
            assert after == before
            _observe_rejection(
                route_live_recorder,
                "target_database_missing",
                before,
                after,
                captured,
            )
        finally:
            master.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_state_database_missing_rejects_before_endpoint_and_source_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        live.state.close()
        master = mssql_connector(database="master")
        try:
            _drop_database(master, live.state_database)
            _apply_environment(monkeypatch, live.runtime_environment)
            before = _route_image(master, live, target=live.target, state=None)
            with pytest.raises(RuntimeError, match="mssql_transaction.state_database_missing") as captured:
                DefaultRuntimeHydrator().build(
                    config=live.runtime_config,
                    load_config=live.load_config(),
                )
            after = _route_image(master, live, target=live.target, state=None)
            assert after == before
            _observe_rejection(
                route_live_recorder,
                "state_database_missing",
                before,
                after,
                captured,
            )
        finally:
            master.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_same_name_target_recreate_and_authority_rotation_reject_replay_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Reject the old pin and bind a reviewed rotation into replay identity."""

    with production_hydration_live_fixture(tmp_path) as live:
        master = mssql_connector(database="master")
        replacement = None
        baseline_bindings = None
        rotated_bindings = None
        try:
            _apply_environment(monkeypatch, live.runtime_environment)
            baseline_config = live.load_config()
            baseline_bindings = DefaultRuntimeHydrator().build(
                config=live.runtime_config,
                load_config=baseline_config,
            )
            invocation = _run_context("target-database-id-changed")
            baseline_result = _processor(baseline_bindings).run(
                baseline_config,
                run_context=invocation,
                dag_id=_DAG_ID,
            )
            assert baseline_result["commit_outcome"] == AtomicCommitOutcome.COMMITTED
            old_digest = baseline_config.options[MSSQL_DATABASE_AUTHORITY_SHA256_OPTION]
            close_runtime_bindings(baseline_bindings)
            baseline_bindings = None

            original = dict(live.target_database_authorities[live.target_database])
            live.target.close()
            _drop_database(master, live.target_database)
            master.execute_query(f"CREATE DATABASE [{live.target_database}]")
            replacement = mssql_connector(database=live.target_database)
            live.install_target_authority(replacement)
            recreated = database_identity(replacement, live.target_database)
            assert _pin_payload(recreated) != original

            before = _route_image(master, live, target=replacement, state=live.state)
            _apply_environment(monkeypatch, live.runtime_environment)
            with pytest.raises(
                RuntimeError,
                match="mssql_transaction.target_database_identity_mismatch",
            ) as stale_pin:
                DefaultRuntimeHydrator().build(
                    config=live.runtime_config,
                    load_config=live.load_config(),
                )

            rotated_authorities = {live.target_database: _pin_payload(recreated)}
            rotated_environment = live.runtime_environment_for(
                tmp_path / "rotated-context",
                target_database_authorities=rotated_authorities,
            )
            _apply_environment(monkeypatch, rotated_environment)
            rotated_config = live.load_config()
            rotated_bindings = DefaultRuntimeHydrator().build(
                config=live.runtime_config,
                load_config=rotated_config,
            )
            new_digest = rotated_config.options[MSSQL_DATABASE_AUTHORITY_SHA256_OPTION]
            assert new_digest != old_digest
            with pytest.raises(RuntimeError, match="mssql_transaction.attempt_identity_collision") as replay_drift:
                _processor(rotated_bindings).run(
                    rotated_config,
                    run_context=invocation,
                    dag_id=_DAG_ID,
                )
            after = _route_image(master, live, target=replacement, state=live.state)
            assert after == before
            route_live_recorder.observe_case(
                "target_database_authority",
                "target_database_id_changed",
                before_image=before,
                after_image=after,
                observations={
                    "stale_pin_blocker": str(stale_pin.value),
                    "rotated_replay_blocker": str(replay_drift.value),
                    "old_authority_sha256": old_digest,
                    "new_authority_sha256": new_digest,
                    "same_invocation_reused": True,
                    "target_and_state_unchanged": True,
                },
            )
        finally:
            close_runtime_bindings(rotated_bindings)
            close_runtime_bindings(baseline_bindings)
            if replacement is not None:
                replacement.close()
            master.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_same_name_state_database_recreate_rejects_pinned_identity_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        master = mssql_connector(database="master")
        replacement = None
        try:
            original = dict(live.state_database_authorities[live.state_database])
            live.state.close()
            _drop_database(master, live.state_database)
            master.execute_query(f"CREATE DATABASE [{live.state_database}]")
            replacement = mssql_connector(database=live.state_database)
            live.install_state_authority(replacement)
            recreated = database_identity(replacement, live.state_database)
            assert _pin_payload(recreated) != original

            _apply_environment(monkeypatch, live.runtime_environment)
            before = _route_image(master, live, target=live.target, state=replacement)
            with pytest.raises(
                RuntimeError,
                match="mssql_transaction.state_database_identity_mismatch",
            ) as captured:
                DefaultRuntimeHydrator().build(
                    config=live.runtime_config,
                    load_config=live.load_config(),
                )
            after = _route_image(master, live, target=live.target, state=replacement)
            assert after == before
            _observe_rejection(
                route_live_recorder,
                "state_database_id_changed",
                before,
                after,
                captured,
            )
        finally:
            if replacement is not None:
                replacement.close()
            master.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_target_single_user_and_offline_reject_before_source_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        master = mssql_connector(database="master")
        before = _route_image(master, live, target=live.target, state=live.state)
        live.target.close()
        single_user_blocker = ""
        offline_blocker = ""
        restored = None
        try:
            _apply_environment(monkeypatch, live.runtime_environment)
            master.execute_query(f"ALTER DATABASE [{live.target_database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
            with pytest.raises(
                RuntimeError,
                match="mssql_transaction.target_database_not_multi_user",
            ) as single_user:
                DefaultRuntimeHydrator().build(
                    config=live.runtime_config,
                    load_config=live.load_config(),
                )
            single_user_blocker = str(single_user.value)
            master.execute_query(f"ALTER DATABASE [{live.target_database}] SET MULTI_USER")
            master.execute_query(f"ALTER DATABASE [{live.target_database}] SET OFFLINE WITH ROLLBACK IMMEDIATE")
            with pytest.raises(
                RuntimeError,
                match="mssql_transaction.target_database_offline",
            ) as offline:
                DefaultRuntimeHydrator().build(
                    config=live.runtime_config,
                    load_config=live.load_config(),
                )
            offline_blocker = str(offline.value)
        finally:
            with suppress(Exception):
                master.execute_query(f"ALTER DATABASE [{live.target_database}] SET ONLINE")
            with suppress(Exception):
                master.execute_query(f"ALTER DATABASE [{live.target_database}] SET MULTI_USER")
        try:
            restored = mssql_connector(database=live.target_database)
            after = _route_image(master, live, target=restored, state=live.state)
            assert after == before
            route_live_recorder.observe_case(
                "target_database_authority",
                "target_offline",
                before_image=before,
                after_image=after,
                observations={
                    "single_user_blocker": single_user_blocker,
                    "offline_blocker": offline_blocker,
                    "source_export_started": False,
                    "owned_artifacts": [],
                },
            )
        finally:
            if restored is not None:
                restored.close()
            master.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_state_database_offline_rejects_before_source_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        master = mssql_connector(database="master")
        before = _route_image(master, live, target=live.target, state=live.state)
        live.state.close()
        restored = None
        captured: pytest.ExceptionInfo[BaseException] | None = None
        try:
            _apply_environment(monkeypatch, live.runtime_environment)
            master.execute_query(f"ALTER DATABASE [{live.state_database}] SET OFFLINE WITH ROLLBACK IMMEDIATE")
            with pytest.raises(
                RuntimeError,
                match="mssql_transaction.state_database_offline",
            ) as captured:
                DefaultRuntimeHydrator().build(
                    config=live.runtime_config,
                    load_config=live.load_config(),
                )
        finally:
            with suppress(Exception):
                master.execute_query(f"ALTER DATABASE [{live.state_database}] SET ONLINE")
        try:
            assert captured is not None
            restored = mssql_connector(database=live.state_database)
            after = _route_image(master, live, target=live.target, state=restored)
            assert after == before
            _observe_rejection(
                route_live_recorder,
                "state_offline",
                before,
                after,
                captured,
            )
        finally:
            if restored is not None:
                restored.close()
            master.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_cross_database_metadata_permission_deny_and_grant_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Prove explicit DENY=0, GRANT=1, no VIEW SERVER STATE, and cleanup."""

    with production_hydration_live_fixture(tmp_path) as live:
        login = f"dpone_authority_{uuid.uuid4().hex[:12]}"
        master = mssql_connector(database="master")
        limited_master = None
        granted_bindings = None
        try:
            _install_limited_login(master, live, login)
            limited_coordinates = replace(
                live.coordinates,
                mssql_username=login,
                mssql_password=_LIMITED_PASSWORD,
            )
            environment = live.runtime_environment_for(
                tmp_path / "limited-context",
                coordinates=limited_coordinates,
            )
            limited_master = _coordinate_connector(limited_coordinates, database="master")
            denied = _permission_image(limited_master)
            assert denied == {"is_sysadmin": 0, "view_any_database": 0}

            _apply_environment(monkeypatch, environment)
            before = _route_image(master, live, target=live.target, state=live.state)
            with pytest.raises(
                RuntimeError,
                match="mssql_transaction.target_database_metadata_permission_denied",
            ) as captured:
                DefaultRuntimeHydrator().build(
                    config=live.runtime_config,
                    load_config=live.load_config(),
                )
            after_denied = _route_image(master, live, target=live.target, state=live.state)
            assert after_denied == before

            master.execute_query(f"GRANT VIEW ANY DATABASE TO [{login}]")
            granted = _permission_image(limited_master)
            assert granted == {"is_sysadmin": 0, "view_any_database": 1}
            guid_rows = limited_master.get_records(
                "SELECT CONVERT(nvarchar(36), r.database_guid) AS database_guid "
                "FROM sys.databases AS d INNER JOIN sys.database_recovery_status AS r "
                "ON r.database_id=d.database_id WHERE d.name=?",
                (live.target_database,),
                as_dict=True,
            )
            assert len(guid_rows) == 1 and str(guid_rows[0]["database_guid"])
            granted_bindings = DefaultRuntimeHydrator().build(
                config=live.runtime_config,
                load_config=live.load_config(),
            )
            after_granted = _route_image(master, live, target=live.target, state=live.state)
            assert after_granted == before
            route_live_recorder.observe_case(
                "target_database_authority",
                "cross_database_permission_denied",
                before_image=before,
                after_image=after_granted,
                observations={
                    "deny_blocker": str(captured.value),
                    "denied_permission": denied,
                    "granted_permission": granted,
                    "view_server_state_required": False,
                    "database_guid_rows_after_grant": len(guid_rows),
                    "source_export_started": False,
                },
            )
        finally:
            close_runtime_bindings(granted_bindings)
            if limited_master is not None:
                limited_master.close()
            _drop_limited_login(master, live, login)
            assert not master.get_records(
                "SELECT 1 AS present FROM sys.server_principals WHERE name=?",
                (login,),
                as_dict=True,
            )
            master.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_three_part_name_case_alias_converges_to_canonical_identity_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        master = mssql_connector(database="master")
        bindings = None
        alias_target = live.target_database.upper()
        alias_state = live.state_database.upper()
        try:
            environment = live.runtime_environment_for(
                tmp_path / "alias-context",
                target_database=alias_target,
                state_database=alias_state,
            )
            _apply_environment(monkeypatch, environment)
            before = _route_image(master, live, target=live.target, state=live.state)
            load_config = live.load_config(target_database=alias_target)
            bindings = DefaultRuntimeHydrator().build(
                config=live.runtime_config,
                load_config=load_config,
            )
            result = _processor(bindings).run(
                load_config,
                run_context=_run_context("three-part-name-case-alias"),
                dag_id=_DAG_ID,
            )
            assert result["commit_outcome"] == AtomicCommitOutcome.COMMITTED
            after = _route_image(
                master,
                live,
                target=bindings.sink_obj.connector,
                state=bindings.sink_obj.state_storage.connector,
            )
            assert _database_fact(after, live.target_database)["database_name"] == live.target_database
            assert _database_fact(after, live.state_database)["database_name"] == live.state_database
            route_live_recorder.observe_case(
                "target_database_authority",
                "three_part_name_case_alias",
                before_image=before,
                after_image=after,
                observations={
                    "authored_target_alias": alias_target,
                    "authored_state_alias": alias_state,
                    "canonical_target": live.target_database,
                    "canonical_state": live.state_database,
                    "commit_outcome": str(result["commit_outcome"]),
                },
            )
        finally:
            close_runtime_bindings(bindings)
            master.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_same_name_staging_database_recreate_rejects_separate_pin_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: a cross-database staging target cannot bypass authority."""

    with production_hydration_live_fixture(tmp_path) as live:
        staging_database = f"dpone_ph_stage_{uuid.uuid4().hex[:12]}"
        master = mssql_connector(database="master")
        staging = None
        replacement = None
        try:
            master.execute_query(f"CREATE DATABASE [{staging_database}]")
            staging = mssql_connector(database=staging_database)
            staging.execute_query(f"CREATE SCHEMA [{STAGING_SCHEMA}] AUTHORIZATION [dbo]")
            original = _pin_payload(database_identity(staging, staging_database))
            authorities = {
                **live.target_database_authorities,
                staging_database: original,
            }
            environment = live.runtime_environment_for(
                tmp_path / "staging-context",
                target_database_authorities=authorities,
            )
            staging.close()
            staging = None
            _drop_database(master, staging_database)
            master.execute_query(f"CREATE DATABASE [{staging_database}]")
            replacement = mssql_connector(database=staging_database)
            replacement.execute_query(f"CREATE SCHEMA [{STAGING_SCHEMA}] AUTHORIZATION [dbo]")
            assert _pin_payload(database_identity(replacement, staging_database)) != original

            _apply_environment(monkeypatch, environment)
            before = _route_image(master, live, target=live.target, state=live.state)
            with pytest.raises(
                RuntimeError,
                match="mssql_transaction.staging_database_identity_mismatch",
            ):
                DefaultRuntimeHydrator().build(
                    config=live.runtime_config,
                    load_config=live.load_config(staging_database=staging_database),
                )
            after = _route_image(master, live, target=live.target, state=live.state)
            assert after == before
        finally:
            for connector in (replacement, staging):
                if connector is not None:
                    connector.close()
            with suppress(Exception):
                _drop_database(master, staging_database)
            master.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_staging_database_lease_blocks_post_preflight_substitution_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hold the pinned staging DB through CREATE/DML, then reject rotation."""

    with production_hydration_live_fixture(tmp_path) as live:
        staging_database = f"dpone_ph_stage_lease_{uuid.uuid4().hex[:10]}"
        master = mssql_connector(database="master")
        staging = None
        replacement = None
        bindings = None
        artifact = None
        try:
            master.execute_query(f"CREATE DATABASE [{staging_database}]")
            staging = mssql_connector(database=staging_database)
            staging.execute_query(f"CREATE SCHEMA [{STAGING_SCHEMA}] AUTHORIZATION [dbo]")
            pin = _pin_payload(database_identity(staging, staging_database))
            authorities = {
                **live.target_database_authorities,
                staging_database: pin,
            }
            environment = live.runtime_environment_for(
                tmp_path / "staging-lease-context",
                target_database_authorities=authorities,
            )
            _apply_environment(monkeypatch, environment)
            load_config = live.load_config(staging_database=staging_database)
            bindings = DefaultRuntimeHydrator().build(
                config=live.runtime_config,
                load_config=load_config,
            )
            manager = bindings.sink_obj.staging_manager
            artifact = manager.create(load_config, [("id", "int")])

            with pytest.raises(Exception, match="(?i)(currently in use|cannot drop)"):
                master.execute_query(f"DROP DATABASE [{staging_database}]")
            inserted = manager.insert_from_query(
                artifact,
                "SELECT CAST(1 AS int) AS [id]",
                [("id", "int")],
            )
            assert inserted in (0, 1, -1)
            rows = live.target.get_records(
                f"SELECT COUNT_BIG(*) AS row_count FROM [{staging_database}].[{STAGING_SCHEMA}].[{artifact.table}]",
                as_dict=True,
            )
            assert int(rows[0]["row_count"]) == 1

            artifact.cleanup()
            artifact = None
            staging.close()
            staging = None
            _drop_database(master, staging_database)
            master.execute_query(f"CREATE DATABASE [{staging_database}]")
            replacement = mssql_connector(database=staging_database)
            assert _pin_payload(database_identity(replacement, staging_database)) != pin
            with pytest.raises(
                RuntimeError,
                match="mssql_transaction.staging_database_identity_mismatch",
            ):
                manager.create(load_config, [("id", "int")])
            assert (
                replacement.get_records(
                    "SELECT COUNT_BIG(*) AS table_count FROM sys.tables",
                    as_dict=True,
                )[0]["table_count"]
                == 0
            )
        finally:
            if artifact is not None:
                with suppress(Exception):
                    artifact.cleanup()
            close_runtime_bindings(bindings)
            for connector in (replacement, staging):
                if connector is not None:
                    connector.close()
            with suppress(Exception):
                _drop_database(master, staging_database)
            master.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_generic_transaction_catalog_v1_to_v2_migration_is_lossless_and_fail_closed_live() -> None:
    """Prove row-compatible upgrade, rotation rejection, and duplicate abort."""

    master = mssql_connector(database="master")
    replay_database = f"dpone_v2_replay_{uuid.uuid4().hex[:12]}"
    duplicate_database = f"dpone_v2_duplicate_{uuid.uuid4().hex[:12]}"
    replay = None
    duplicate = None
    try:
        master.execute_query(f"CREATE DATABASE [{replay_database}]")
        replay = mssql_connector(database=replay_database)
        replay.execute_query(f"CREATE SCHEMA [{STAGING_SCHEMA}] AUTHORIZATION [dbo]")
        _execute_ddl_batches(replay, _v1_catalog_ddl(replay_database, STAGING_SCHEMA))
        state = MssqlGenericTransactionState(
            replay,
            database=replay_database,
            schema=STAGING_SCHEMA,
        )
        original_request = _migration_attempt_request(target_identity=b"a" * 32)
        original = state.attempts.allocate(original_request)
        before_rows = _attempt_rows(replay, replay_database)

        migration = render_generic_transaction_catalog_v1_to_v2_ddl(
            database=replay_database,
            schema=STAGING_SCHEMA,
        )
        _execute_ddl_batches(replay, migration)
        require_generic_transaction_catalog(
            replay,
            database=replay_database,
            schema=STAGING_SCHEMA,
        )
        replayed = state.attempts.allocate(original_request)
        assert replayed.attempt_key == original.attempt_key
        assert replayed.generation == original.generation == 1
        assert _attempt_rows(replay, replay_database) == before_rows

        rotated_request = _migration_attempt_request(target_identity=b"b" * 32)
        assert rotated_request.attempt_key != original_request.attempt_key
        with pytest.raises(RuntimeError, match="mssql_transaction.attempt_identity_collision"):
            state.attempts.allocate(rotated_request)
        assert _attempt_rows(replay, replay_database) == before_rows
        _execute_ddl_batches(replay, migration)
        assert _attempt_rows(replay, replay_database) == before_rows

        master.execute_query(f"CREATE DATABASE [{duplicate_database}]")
        duplicate = mssql_connector(database=duplicate_database)
        duplicate.execute_query(f"CREATE SCHEMA [{STAGING_SCHEMA}] AUTHORIZATION [dbo]")
        _execute_ddl_batches(duplicate, _v1_catalog_ddl(duplicate_database, STAGING_SCHEMA))
        _seed_duplicate_invocation_rows(duplicate, duplicate_database)
        duplicate_before = _attempt_catalog_image(duplicate, duplicate_database)
        with pytest.raises(Exception, match="DPONE_GENERIC_TRANSACTION_V2_DUPLICATE_INVOCATION"):
            _execute_ddl_batches(
                duplicate,
                render_generic_transaction_catalog_v1_to_v2_ddl(
                    database=duplicate_database,
                    schema=STAGING_SCHEMA,
                ),
            )
        assert _attempt_catalog_image(duplicate, duplicate_database) == duplicate_before
        for drift in (
            "foreign_key",
            "check",
            "default",
            "attempt_trigger_not_for_replication",
            "receipt_trigger_not_for_replication",
        ):
            _assert_v1_migration_drift_rolls_back(master, drift)
    finally:
        for connector in (duplicate, replay):
            if connector is not None:
                connector.close()
        for database in (duplicate_database, replay_database):
            with suppress(Exception):
                _drop_database(master, database)
        master.close()


def _v1_catalog_ddl(database: str, schema: str) -> str:
    ddl = render_generic_transaction_catalog_ddl(database=database, schema=schema)
    return ddl.replace(
        "-- dpone generic MSSQL transaction catalog v2",
        "-- dpone generic MSSQL transaction catalog v1",
        1,
    ).replace(
        "    CONSTRAINT [uq_dpone_load_attempt_invocation] UNIQUE NONCLUSTERED (invocation_digest),\n",
        "",
        1,
    )


def _execute_ddl_batches(connector: Any, ddl: str) -> None:
    for batch in ddl.replace("\r\n", "\n").split("\nGO\n"):
        if batch.strip():
            connector.execute_query(batch)


def _migration_attempt_request(*, target_identity: bytes) -> MssqlAttemptRequest:
    return MssqlAttemptRequest(
        invocation=InvocationIdentity(
            run_id="route-live-catalog-v1-replay",
            process="catalog-migration",
            task_partition="catalog-migration:target",
        ),
        target_identity=target_identity,
        route_fingerprint=(b"r" if target_identity == b"a" * 32 else b"s") * 32,
        load_id="catalog-migration-load",
        target_database="DWH",
        target_schema="dbo",
        target_table="events",
        strategy="full_refresh",
    )


def _attempt_rows(connector: Any, database: str) -> tuple[dict[str, Any], ...]:
    return tuple(
        connector.get_records(
            f"SELECT attempt_key, target_identity, generation, invocation_digest, "
            f"route_fingerprint, first_load_id, target_database, target_schema, "
            f"target_table, strategy FROM [{database}].[{STAGING_SCHEMA}].[dpone_load_attempt] "
            "ORDER BY attempt_key",
            as_dict=True,
        )
    )


def _seed_duplicate_invocation_rows(connector: Any, database: str) -> None:
    first = _migration_attempt_request(target_identity=b"c" * 32)
    second = _migration_attempt_request(target_identity=b"d" * 32)
    assert first.invocation.invocation_digest == second.invocation.invocation_digest
    for request in (first, second):
        connector.execute_query(
            f"INSERT INTO [{database}].[{STAGING_SCHEMA}].[dpone_load_attempt] "
            "(attempt_key,target_identity,generation,invocation_digest,route_fingerprint,"
            "first_load_id,target_database,target_schema,target_table,strategy) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                request.attempt_key,
                request.target_identity,
                1,
                request.invocation.invocation_digest,
                request.route_fingerprint,
                request.load_id,
                request.target_database,
                request.target_schema,
                request.target_table,
                request.strategy,
            ),
        )


def _attempt_catalog_image(connector: Any, database: str) -> dict[str, Any]:
    return {
        "rows": _attempt_rows(connector, database),
        "indexes": tuple(
            connector.get_records(
                f"SELECT i.name, i.is_unique, i.is_unique_constraint "
                f"FROM [{database}].sys.indexes AS i "
                f"INNER JOIN [{database}].sys.tables AS t ON t.object_id=i.object_id "
                f"INNER JOIN [{database}].sys.schemas AS s ON s.schema_id=t.schema_id "
                "WHERE s.name=? AND t.name=N'dpone_load_attempt' AND i.index_id>0 "
                "ORDER BY i.name",
                (STAGING_SCHEMA,),
                as_dict=True,
            )
        ),
    }


def _assert_v1_migration_drift_rolls_back(master: Any, drift: str) -> None:
    database = f"dpone_v2_drift_{uuid.uuid4().hex[:12]}"
    connector = None
    try:
        master.execute_query(f"CREATE DATABASE [{database}]")
        connector = mssql_connector(database=database)
        connector.execute_query(f"CREATE SCHEMA [{STAGING_SCHEMA}] AUTHORIZATION [dbo]")
        ddl = _v1_catalog_ddl(database, STAGING_SCHEMA)
        if drift == "attempt_trigger_not_for_replication":
            ddl = ddl.replace(
                "INSTEAD OF UPDATE, DELETE\nAS\n    THROW 51000, 'DPONE_LOAD_ATTEMPT_IMMUTABLE'",
                "INSTEAD OF UPDATE, DELETE\nNOT FOR REPLICATION\nAS\n    THROW 51000, 'DPONE_LOAD_ATTEMPT_IMMUTABLE'",
                1,
            )
        elif drift == "receipt_trigger_not_for_replication":
            ddl = ddl.replace(
                "INSTEAD OF UPDATE, DELETE\nAS\n    THROW 51000, 'DPONE_LOAD_RECEIPT_IMMUTABLE'",
                "INSTEAD OF UPDATE, DELETE\nNOT FOR REPLICATION\nAS\n    THROW 51000, 'DPONE_LOAD_RECEIPT_IMMUTABLE'",
                1,
            )
        _execute_ddl_batches(connector, ddl)
        expected = {
            "foreign_key": "DPONE_GENERIC_TRANSACTION_V1_FOREIGN_KEY_CONTRACT_MISMATCH",
            "check": "DPONE_GENERIC_TRANSACTION_V1_CHECK_CONTRACT_MISMATCH",
            "default": "DPONE_GENERIC_TRANSACTION_V1_DEFAULT_CONTRACT_MISMATCH",
            "attempt_trigger_not_for_replication": "DPONE_GENERIC_TRANSACTION_V1_TRIGGER_CONTRACT_MISMATCH",
            "receipt_trigger_not_for_replication": "DPONE_GENERIC_TRANSACTION_V1_TRIGGER_CONTRACT_MISMATCH",
        }[drift]
        if drift == "foreign_key":
            connector.execute_query(
                f"ALTER TABLE [{database}].[{STAGING_SCHEMA}].[dpone_target_fence] "
                "NOCHECK CONSTRAINT [fk_dpone_target_fence_attempt]"
            )
        elif drift == "check":
            connector.execute_query(
                f"ALTER TABLE [{database}].[{STAGING_SCHEMA}].[dpone_load_attempt] "
                "NOCHECK CONSTRAINT [ck_dpone_load_attempt_generation]"
            )
        elif drift == "default":
            connector.execute_query(
                f"ALTER TABLE [{database}].[{STAGING_SCHEMA}].[dpone_load_attempt] "
                "DROP CONSTRAINT [df_dpone_load_attempt_created]"
            )
        before = _migration_catalog_image(connector, database)
        with pytest.raises(Exception, match=expected):
            _execute_ddl_batches(
                connector,
                render_generic_transaction_catalog_v1_to_v2_ddl(
                    database=database,
                    schema=STAGING_SCHEMA,
                ),
            )
        assert _migration_catalog_image(connector, database) == before
        assert "uq_dpone_load_attempt_invocation" not in {str(row["name"]) for row in before["indexes"]}
    finally:
        if connector is not None:
            connector.close()
        with suppress(Exception):
            _drop_database(master, database)


def _migration_catalog_image(connector: Any, database: str) -> dict[str, Any]:
    prefix = f"[{database}]."
    table_filter = (
        "s.name=? AND t.name IN "
        "(N'dpone_target_fence',N'dpone_load_attempt',N'dpone_load_operation',N'dpone_load_receipt')"
    )
    return {
        "indexes": tuple(
            connector.get_records(
                f"SELECT t.name AS table_name,i.name,i.is_unique,i.is_disabled,i.is_hypothetical,"
                f"i.has_filter,i.ignore_dup_key FROM {prefix}sys.indexes AS i "
                f"INNER JOIN {prefix}sys.tables AS t ON t.object_id=i.object_id "
                f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id=t.schema_id "
                f"WHERE {table_filter} AND i.index_id>0 ORDER BY t.name,i.name",
                (STAGING_SCHEMA,),
                as_dict=True,
            )
        ),
        "foreign_keys": tuple(
            connector.get_records(
                f"SELECT t.name AS table_name,fk.name,fk.is_disabled,fk.is_not_trusted,"
                f"fk.is_not_for_replication FROM {prefix}sys.foreign_keys AS fk "
                f"INNER JOIN {prefix}sys.tables AS t ON t.object_id=fk.parent_object_id "
                f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id=t.schema_id "
                f"WHERE {table_filter} ORDER BY t.name,fk.name",
                (STAGING_SCHEMA,),
                as_dict=True,
            )
        ),
        "checks": tuple(
            connector.get_records(
                f"SELECT t.name AS table_name,ck.name,ck.is_disabled,ck.is_not_trusted,"
                f"ck.is_not_for_replication,ck.definition FROM {prefix}sys.check_constraints AS ck "
                f"INNER JOIN {prefix}sys.tables AS t ON t.object_id=ck.parent_object_id "
                f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id=t.schema_id "
                f"WHERE {table_filter} ORDER BY t.name,ck.name",
                (STAGING_SCHEMA,),
                as_dict=True,
            )
        ),
        "defaults": tuple(
            connector.get_records(
                f"SELECT t.name AS table_name,dc.name,dc.definition "
                f"FROM {prefix}sys.default_constraints AS dc "
                f"INNER JOIN {prefix}sys.tables AS t ON t.object_id=dc.parent_object_id "
                f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id=t.schema_id "
                f"WHERE {table_filter} ORDER BY t.name,dc.name",
                (STAGING_SCHEMA,),
                as_dict=True,
            )
        ),
        "triggers": tuple(
            connector.get_records(
                f"SELECT t.name AS table_name,tr.name,tr.is_disabled,tr.is_not_for_replication,"
                f"OBJECT_DEFINITION(tr.object_id) AS definition FROM {prefix}sys.triggers AS tr "
                f"INNER JOIN {prefix}sys.tables AS t ON t.object_id=tr.parent_id "
                f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id=t.schema_id "
                f"WHERE {table_filter} ORDER BY t.name,tr.name",
                (STAGING_SCHEMA,),
                as_dict=True,
            )
        ),
    }


def _observe_rejection(
    recorder: RouteLiveObservationRecorder,
    case_id: str,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    captured: pytest.ExceptionInfo[BaseException],
) -> None:
    recorder.observe_case(
        "target_database_authority",
        case_id,
        before_image=before,
        after_image=after,
        observations={
            "blocker": str(captured.value),
            "source_export_started": False,
            "staging_mutated": False,
            "owned_artifacts": [],
        },
    )


def _processor(bindings: Any) -> ETLProcessor:
    return ETLProcessor(
        source=bindings.source_obj,
        sink=bindings.sink_obj,
        etl_logger=bindings.etl_logger,
        run_state_storage=bindings.run_state_storage,
        load_identity_service=bindings.load_identity_service,
    )


def _run_context(case_id: str) -> RunContext:
    return RunContext(
        run_id=f"route-live-{case_id}",
        config={"pipeline_id": "target-database-authority", "task_id": case_id},
    )


def _apply_environment(
    monkeypatch: pytest.MonkeyPatch,
    environment: Mapping[str, str],
) -> None:
    for name, value in environment.items():
        monkeypatch.setenv(name, value)


def _route_image(
    master: Any,
    live: Any,
    *,
    target: Any | None,
    state: Any | None,
) -> dict[str, Any]:
    return {
        "databases": tuple(
            master.get_records(
                "SELECT d.name AS database_name, d.database_id, d.state_desc, "
                "d.user_access_desc, CONVERT(nvarchar(33),d.create_date,126) AS create_token, "
                "LOWER(CONVERT(nvarchar(36),r.database_guid)) AS database_guid "
                "FROM sys.databases AS d LEFT JOIN sys.database_recovery_status AS r "
                "ON r.database_id=d.database_id WHERE d.name IN (?,?) ORDER BY d.name",
                (live.target_database, live.state_database),
                as_dict=True,
            )
        ),
        "target": None if target is None else _target_image(target, live),
        "state_counts": None if state is None else generic_state_row_counts(state),
        "owned_artifacts": _artifact_image(live.transfer_root),
    }


def _target_image(target: Any, live: Any) -> dict[str, Any]:
    exists = bool(
        target.get_records(
            "SELECT CASE WHEN OBJECT_ID(?, N'U') IS NULL THEN 0 ELSE 1 END AS exists_flag",
            (f"{TARGET_SCHEMA}.{live.target_table}",),
            as_dict=True,
        )[0]["exists_flag"]
    )
    rows: list[dict[str, Any]] = []
    if exists:
        rows = list(
            target.get_records(
                f"SELECT [id], [metric_code], [metric_value], [note] "
                f"FROM [{TARGET_SCHEMA}].[{live.target_table}] ORDER BY [id]",
                as_dict=True,
            )
        )
    staging = tuple(
        target.get_records(
            "SELECT t.name AS table_name, SUM(p.rows) AS row_count "
            "FROM sys.tables AS t INNER JOIN sys.schemas AS s ON s.schema_id=t.schema_id "
            "INNER JOIN sys.partitions AS p ON p.object_id=t.object_id AND p.index_id IN (0,1) "
            "WHERE s.name=? GROUP BY t.name ORDER BY t.name",
            (STAGING_SCHEMA,),
            as_dict=True,
        )
    )
    return {"exists": exists, "rows": rows, "staging": staging}


def _artifact_image(root: Path) -> tuple[dict[str, Any], ...]:
    if not root.exists():
        return ()
    return tuple(
        {"path": path.relative_to(root).as_posix(), "bytes": path.stat().st_size}
        for path in sorted(root.rglob("*"))
        if path.is_file()
    )


def _database_fact(image: Mapping[str, Any], database: str) -> Mapping[str, Any]:
    return next(row for row in image["databases"] if str(row["database_name"]).casefold() == database.casefold())


def _drop_database(master: Any, database: str) -> None:
    master.execute_query(f"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
    master.execute_query(f"DROP DATABASE [{database}]")


def _pin_payload(identity: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "database_id": int(identity["database_id"]),
        "create_token": str(identity["create_token"]),
        "database_guid": str(identity["database_guid"]).lower(),
    }


def _install_limited_login(master: Any, live: Any, login: str) -> None:
    master.execute_query(f"CREATE LOGIN [{login}] WITH PASSWORD=N'{_LIMITED_PASSWORD}', CHECK_POLICY=OFF")
    master.execute_query(f"CREATE USER [{login}] FOR LOGIN [{login}]")
    live.target.execute_query(f"CREATE USER [{login}] FOR LOGIN [{login}]")
    live.state.execute_query(f"CREATE USER [{login}] FOR LOGIN [{login}]")
    master.execute_query(f"DENY VIEW ANY DATABASE TO [{login}]")


def _drop_limited_login(master: Any, live: Any, login: str) -> None:
    with suppress(Exception):
        live.target.execute_query(f"DROP USER IF EXISTS [{login}]")
    with suppress(Exception):
        live.state.execute_query(f"DROP USER IF EXISTS [{login}]")
    with suppress(Exception):
        master.execute_query(f"DROP USER IF EXISTS [{login}]")
    with suppress(Exception):
        master.execute_query(f"DROP LOGIN [{login}]")


def _coordinate_connector(
    coordinates: VendorCoordinates,
    *,
    database: str,
) -> MSSQLConnector:
    return MSSQLConnector(
        host=coordinates.mssql_host,
        port=coordinates.mssql_port,
        database=database,
        user=coordinates.mssql_username,
        password=coordinates.mssql_password,
        trust_server_certificate="yes",
        bcp_path=coordinates.mssql_bcp_path,
    )


def _permission_image(connector: Any) -> dict[str, int]:
    return connector.get_records(
        "SELECT CONVERT(int,IS_SRVROLEMEMBER('sysadmin')) AS is_sysadmin, "
        "CONVERT(int,HAS_PERMS_BY_NAME(NULL,NULL,'VIEW ANY DATABASE')) AS view_any_database",
        as_dict=True,
    )[0]
