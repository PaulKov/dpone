"""Production-hydrated PostgreSQL physical source-authority certification."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from tests.integration.postgres.postgres_live_support import postgres_mssql_enabled
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    factual_postgres_source_authority,
)
from tests.integration.postgres.postgres_mssql_production_hydration_live_support import (
    SOURCE_SCHEMA,
    VendorCoordinates,
    close_runtime_bindings,
    production_hydration_live_fixture,
)
from tests.integration.postgres.postgres_mssql_source_identity_live_support import (
    AUTHORITY_DATABASE,
    AUTHORITY_SCHEMA,
    apply_environment,
    authority_coordinates,
    drop_role,
    install_source,
    postgis_coordinates,
    postgres_at,
    processor,
    route_image,
    run_context,
    source_coordinates,
    wait_for_replicated_source,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
    pytest.mark.route_live_wide,
]

_DAG_ID = "DAG__integration__postgres_mssql__source_identity_authority"
_ROLE_PASSWORD = "Dp0ne.Source.Authority.2026!"


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_stable_same_cluster_commits_with_signed_rr_identity_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        load_config = live.load_config()
        actual = factual_postgres_source_authority(live.postgres, load_config=load_config)
        assert actual == live.postgres_source_authority
        _run_success(
            live,
            tmp_path,
            monkeypatch,
            route_live_recorder,
            case_id="stable_same_cluster",
            coordinates=live.coordinates,
            authority=live.postgres_source_authority,
            load_config=load_config,
            facts={"actual_authority": actual},
        )


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_database_oid_change_rejects_before_copy_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        database = f"dpone_source_oid_{uuid.uuid4().hex[:10]}"
        admin_coordinates = source_coordinates(
            live.coordinates,
            host=live.coordinates.postgres_host,
            port=live.coordinates.postgres_port,
            database="postgres",
            username=live.coordinates.postgres_username,
            password=live.coordinates.postgres_password,
        )
        route_coordinates = source_coordinates(
            live.coordinates,
            host=live.coordinates.postgres_host,
            port=live.coordinates.postgres_port,
            database=database,
            username=live.coordinates.postgres_username,
            password=live.coordinates.postgres_password,
        )
        admin = postgres_at(admin_coordinates)
        source = None
        try:
            admin.execute_query(f'CREATE DATABASE "{database}"')
            source = postgres_at(route_coordinates)
            install_source(source, schema=SOURCE_SCHEMA, table=live.source_table)
            expected = factual_postgres_source_authority(
                source,
                load_config=live.load_config(source_database=database),
            )
            source.close()
            source = None
            admin.execute_query(f'DROP DATABASE "{database}"')
            admin.execute_query(f'CREATE DATABASE "{database}"')
            source = postgres_at(route_coordinates)
            install_source(source, schema=SOURCE_SCHEMA, table=live.source_table)
            actual = factual_postgres_source_authority(
                source,
                load_config=live.load_config(source_database=database),
            )
            assert actual["database"]["oid"] != expected["database"]["oid"]
            _run_rejection(
                live,
                tmp_path,
                monkeypatch,
                route_live_recorder,
                case_id="database_oid_change",
                coordinates=route_coordinates,
                authority=expected,
                load_config=live.load_config(source_database=database),
                blocker="postgres_source_authority.database_identity_mismatch",
                facts={"expected_authority": expected, "actual_authority": actual},
            )
        finally:
            if source is not None:
                source.close()
            with suppress(Exception):
                admin.execute_query(f'DROP DATABASE IF EXISTS "{database}"')
            admin.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_system_identifier_change_uses_independent_postgis_cluster_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        coordinates = postgis_coordinates(live.coordinates)
        source = postgres_at(coordinates)
        try:
            install_source(source, schema=SOURCE_SCHEMA, table=live.source_table)
            actual = factual_postgres_source_authority(source, load_config=live.load_config())
            expected = live.postgres_source_authority
            assert actual["system_identifier"] != expected["system_identifier"]
            _run_rejection(
                live,
                tmp_path,
                monkeypatch,
                route_live_recorder,
                case_id="system_identifier_change",
                coordinates=coordinates,
                authority=expected,
                load_config=live.load_config(),
                blocker="postgres_source_authority.system_identifier_mismatch",
                facts={"expected_authority": expected, "actual_authority": actual},
            )
        finally:
            with suppress(Exception):
                source.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{live.source_table}" CASCADE')
            source.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_real_standby_promotion_increments_timeline_and_rejects_pin_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        primary_coordinates = authority_coordinates(live.coordinates, standby=False)
        standby_coordinates = authority_coordinates(live.coordinates, standby=True)
        primary = postgres_at(primary_coordinates)
        standby = postgres_at(standby_coordinates)
        try:
            install_source(primary, schema=AUTHORITY_SCHEMA, table=live.source_table)
            primary.get_records("SELECT pg_switch_wal()", as_dict=True)
            wait_for_replicated_source(standby, schema=AUTHORITY_SCHEMA, table=live.source_table)
            load_config = live.load_config(
                source_database=AUTHORITY_DATABASE,
                source_schema=AUTHORITY_SCHEMA,
            )
            expected = factual_postgres_source_authority(standby, load_config=load_config)
            assert expected["topology_role"] == "standby"
            promoted = standby.get_records("SELECT pg_promote(true, 30) AS promoted", as_dict=True)
            assert promoted == [{"promoted": True}]
            actual = factual_postgres_source_authority(standby, load_config=load_config)
            assert actual["topology_role"] == "primary"
            assert int(actual["timeline_id"]) > int(expected["timeline_id"])
            assert actual["system_identifier"] == expected["system_identifier"]
            _run_rejection(
                live,
                tmp_path,
                monkeypatch,
                route_live_recorder,
                case_id="timeline_change",
                coordinates=standby_coordinates,
                authority=expected,
                load_config=load_config,
                blocker="postgres_source_authority.timeline_id_mismatch",
                facts={
                    "expected_authority": expected,
                    "actual_authority": actual,
                    "promotion": "pg_promote",
                },
            )
        finally:
            standby.close()
            primary.close()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_principal_change_rejects_real_login_role_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        role = f"dpone_source_principal_{uuid.uuid4().hex[:8]}"
        limited = None
        try:
            _install_source_role(live.postgres, role, live.source_table)
            coordinates = _role_coordinates(live.coordinates, role)
            limited = postgres_at(coordinates)
            actual = factual_postgres_source_authority(limited, load_config=live.load_config())
            expected = live.postgres_source_authority
            assert actual["principals"] != expected["principals"]
            _run_rejection(
                live,
                tmp_path,
                monkeypatch,
                route_live_recorder,
                case_id="principal_change",
                coordinates=coordinates,
                authority=expected,
                load_config=live.load_config(),
                blocker="postgres_source_authority.effective_principal_identity_mismatch",
                facts={"expected_authority": expected, "actual_authority": actual},
            )
        finally:
            if limited is not None:
                limited.close()
            drop_role(live.postgres, role)


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_schema_oid_recreate_rejects_same_name_scope_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        expected = live.postgres_source_authority
        live.postgres.execute_query(f'DROP SCHEMA "{SOURCE_SCHEMA}" CASCADE')
        install_source(live.postgres, schema=SOURCE_SCHEMA, table=live.source_table)
        actual = factual_postgres_source_authority(live.postgres, load_config=live.load_config())
        expected_relation = expected["relations"][f"{SOURCE_SCHEMA}.{live.source_table}"]
        actual_relation = actual["relations"][f"{SOURCE_SCHEMA}.{live.source_table}"]
        assert actual_relation["namespace_oid"] != expected_relation["namespace_oid"]
        _run_rejection(
            live,
            tmp_path,
            monkeypatch,
            route_live_recorder,
            case_id="schema_oid_recreate",
            coordinates=live.coordinates,
            authority=expected,
            load_config=live.load_config(),
            blocker="postgres_source_authority.relation_identity_mismatch",
            facts={"expected_authority": expected, "actual_authority": actual},
        )


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_table_oid_recreate_rejects_same_name_relation_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        expected = live.postgres_source_authority
        install_source(live.postgres, schema=SOURCE_SCHEMA, table=live.source_table)
        actual = factual_postgres_source_authority(live.postgres, load_config=live.load_config())
        expected_relation = expected["relations"][f"{SOURCE_SCHEMA}.{live.source_table}"]
        actual_relation = actual["relations"][f"{SOURCE_SCHEMA}.{live.source_table}"]
        assert actual_relation["relation_oid"] != expected_relation["relation_oid"]
        _run_rejection(
            live,
            tmp_path,
            monkeypatch,
            route_live_recorder,
            case_id="table_oid_recreate",
            coordinates=live.coordinates,
            authority=expected,
            load_config=live.load_config(),
            blocker="postgres_source_authority.relation_identity_mismatch",
            facts={"expected_authority": expected, "actual_authority": actual},
        )


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_ascii_case_alias_resolves_canonical_oid_and_commits_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        load_config = live.load_config(
            source_schema=SOURCE_SCHEMA.upper(),
            source_table=live.source_table.upper(),
        )
        _run_success(
            live,
            tmp_path,
            monkeypatch,
            route_live_recorder,
            case_id="case_alias_same_oid",
            coordinates=live.coordinates,
            authority=live.postgres_source_authority,
            load_config=load_config,
            facts={
                "authored_schema": SOURCE_SCHEMA.upper(),
                "authored_relation": live.source_table.upper(),
                "canonical_schema": SOURCE_SCHEMA,
                "canonical_relation": live.source_table,
            },
        )


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_metadata_permission_denied_is_typed_before_copy_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with production_hydration_live_fixture(tmp_path) as live:
        role = f"dpone_source_denied_{uuid.uuid4().hex[:8]}"
        limited = None
        public_revoked = False
        try:
            _install_source_role(live.postgres, role, live.source_table)
            coordinates = _role_coordinates(live.coordinates, role)
            limited = postgres_at(coordinates)
            expected = factual_postgres_source_authority(limited, load_config=live.load_config())
            limited.close()
            limited = None
            live.postgres.execute_query("REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC")
            live.postgres.execute_query(f'REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM "{role}"')
            public_revoked = True
            _run_rejection(
                live,
                tmp_path,
                monkeypatch,
                route_live_recorder,
                case_id="metadata_permission_denied",
                coordinates=coordinates,
                authority=expected,
                load_config=live.load_config(),
                blocker="postgres_source_authority.metadata_permission_denied",
                facts={
                    "expected_authority": expected,
                    "revoked_function": "pg_catalog.pg_control_system()",
                },
            )
        finally:
            if limited is not None:
                limited.close()
            if public_revoked:
                with suppress(Exception):
                    live.postgres.execute_query("GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO PUBLIC")
            drop_role(live.postgres, role)


def _run_success(
    live: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorder: RouteLiveObservationRecorder,
    *,
    case_id: str,
    coordinates: VendorCoordinates,
    authority: Mapping[str, Any],
    load_config: Any,
    facts: Mapping[str, Any],
) -> None:
    environment = live.runtime_environment_for(
        tmp_path / f"context-{case_id}",
        coordinates=coordinates,
        postgres_source_authority=authority,
    )
    apply_environment(monkeypatch, environment)
    before = route_image(live, target=live.target)
    bindings = None
    try:
        bindings = DefaultRuntimeHydrator().build(config=live.runtime_config, load_config=load_config)
        result = processor(bindings).run(
            load_config,
            run_context=run_context(case_id),
            dag_id=_DAG_ID,
        )
        assert result["commit_outcome"] == AtomicCommitOutcome.COMMITTED
        assert result["loaded_rows"] == result["final_rows"] == 3
        after = route_image(live, target=bindings.sink_obj.connector)
        assert after != before
        assert after["target_exists"] is True
        assert len(after["target_rows"]) == 3
        assert after["staging"] == ()
        assert after["owned_artifacts"] == ()
        recorder.observe_case(
            "source_identity_authority",
            case_id,
            before_image=before,
            after_image=after,
            observations={
                **facts,
                "runtime_hydrator": "DefaultRuntimeHydrator",
                "repeatable_read_prepared_boundary": True,
                "relation_access_share_lease": True,
                "commit_outcome": str(result["commit_outcome"]),
                "loaded_rows": result["loaded_rows"],
            },
        )
    finally:
        close_runtime_bindings(bindings)


def _run_rejection(
    live: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorder: RouteLiveObservationRecorder,
    *,
    case_id: str,
    coordinates: VendorCoordinates,
    authority: Mapping[str, Any],
    load_config: Any,
    blocker: str,
    facts: Mapping[str, Any],
) -> None:
    environment = live.runtime_environment_for(
        tmp_path / f"context-{case_id}",
        coordinates=coordinates,
        postgres_source_authority=authority,
    )
    apply_environment(monkeypatch, environment)
    before = route_image(live, target=live.target)
    bindings = None
    try:
        bindings = DefaultRuntimeHydrator().build(config=live.runtime_config, load_config=load_config)
        with pytest.raises(RuntimeError, match=blocker) as captured:
            processor(bindings).run(
                load_config,
                run_context=run_context(case_id),
                dag_id=_DAG_ID,
            )
        after = route_image(live, target=bindings.sink_obj.connector)
        assert after == before
        assert after["target_exists"] is False
        assert after["staging"] == ()
        assert after["owned_artifacts"] == ()
        recorder.observe_case(
            "source_identity_authority",
            case_id,
            before_image=before,
            after_image=after,
            observations={
                **facts,
                "blocker": str(captured.value),
                "runtime_hydrator": "DefaultRuntimeHydrator",
                "repeatable_read_prepared_boundary": True,
                "source_copy_started": False,
                "target_staging_artifacts_unchanged": True,
            },
        )
    finally:
        close_runtime_bindings(bindings)


def _install_source_role(postgres: Any, role: str, table: str) -> None:
    postgres.execute_query(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{_ROLE_PASSWORD}'")
    postgres.execute_query(f'GRANT CONNECT ON DATABASE "{postgres.database}" TO "{role}"')
    postgres.execute_query(f'GRANT USAGE ON SCHEMA "{SOURCE_SCHEMA}" TO "{role}"')
    postgres.execute_query(f'GRANT SELECT ON TABLE "{SOURCE_SCHEMA}"."{table}" TO "{role}"')
    postgres.execute_query(f'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO "{role}"')
    postgres.execute_query(f'GRANT EXECUTE ON FUNCTION pg_catalog.pg_current_wal_lsn() TO "{role}"')
    postgres.execute_query(f'GRANT EXECUTE ON FUNCTION pg_catalog.pg_walfile_name(pg_lsn) TO "{role}"')


def _role_coordinates(coordinates: VendorCoordinates, role: str) -> VendorCoordinates:
    return source_coordinates(
        coordinates,
        host=coordinates.postgres_host,
        port=coordinates.postgres_port,
        database=coordinates.postgres_database,
        username=role,
        password=_ROLE_PASSWORD,
    )
