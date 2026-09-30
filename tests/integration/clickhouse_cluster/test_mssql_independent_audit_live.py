"""Loopback SQL proof for independent audit, not business-route certification.

The existing publication-live opt-in admits only a disposable local SQL Server.
Each case retains two unique databases for evidence. DDL-deny triggers installed
after fixture provisioning make runtime schema mutation a visible failure.
"""

import json
import os
from contextlib import ExitStack
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dpone.config.audit import resolve_mssql_audit_location, select_audit_storage
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.runtime.bootstrap_audit import build_independent_audit_bindings
from dpone.runtime.credentials.authority import RuntimeResolvedConnections
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.etl.decision_lifecycle import RuntimeDecisionLifecycle
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.lineage.audit import LoadIdentityService
from tests.integration.clickhouse_cluster.test_mssql_publication_authority_live import connect
from tests.integration.clickhouse_cluster.test_mssql_publication_authority_live import pytestmark as pytestmark
from tests.test_independent_audit_selection import _config

# Hand-authored schema, independent of the adapter's catalog shape constants.
_LOAD_DDL = """
CREATE TABLE dbo.dpone_load_audit (
 run_id char(26) NOT NULL, load_id char(26) NOT NULL PRIMARY KEY,
 status nvarchar(32) NOT NULL, process_name nvarchar(512) NULL,
 source_schema nvarchar(256) NOT NULL, source_table nvarchar(256) NOT NULL,
 target_schema nvarchar(256) NOT NULL, target_table nvarchar(256) NOT NULL,
 strategy nvarchar(64) NOT NULL, started_at datetime2 NOT NULL,
 staged_at datetime2 NULL, committed_at datetime2 NULL, failed_at datetime2 NULL,
 extracted_rows bigint NULL, staged_rows bigint NULL, inserted_rows bigint NULL,
 updated_rows bigint NULL, loaded_rows bigint NULL, deleted_rows bigint NULL,
 reactivated_rows bigint NULL, unchanged_rows bigint NULL, soft_deleted_rows bigint NULL,
 hard_deleted_rows bigint NULL, active_rows bigint NULL, total_rows bigint NULL,
 commit_receipt_id nvarchar(128) NULL, commit_outcome nvarchar(64) NULL,
 error_message nvarchar(max) NULL, artifact_uri nvarchar(max) NULL,
 __dpone__loaded_at datetime2 NOT NULL
)
"""
_STEP_DDL = """
CREATE TABLE dbo.__dpone__load_steps (
 run_id nvarchar(64) NOT NULL, load_id nvarchar(64) NOT NULL,
 step_id nvarchar(256) NOT NULL, phase nvarchar(64) NOT NULL,
 kind nvarchar(128) NOT NULL, status nvarchar(32) NOT NULL,
 started_at datetime2 NOT NULL, finished_at datetime2 NULL,
 error_message nvarchar(max) NULL, details_json nvarchar(max) NOT NULL,
 __dpone__loaded_at datetime2 NOT NULL
)
"""


@pytest.fixture
def audit_catalog():
    token = uuid4().hex[:16]
    metadata, decoy = "dpone_audit_it_" + token, "dpone_decoy_it_" + token
    with ExitStack() as resources:
        admin = connect()
        resources.callback(admin.close)
        for database in (metadata, decoy):
            admin.execute_query(f"CREATE DATABASE [{database}]")
            connector = connect(database)
            resources.callback(connector.close)
            connector.execute_query(_LOAD_DDL)
            connector.execute_query(_STEP_DDL)
            connector.execute_query("CREATE TABLE dbo.drifted_steps (run_id nvarchar(1) NOT NULL)")
            connector.execute_query(
                "CREATE TRIGGER deny_fixture_ddl ON DATABASE FOR DDL_DATABASE_LEVEL_EVENTS "
                "AS THROW 51000, 'Runtime audit must not change the fixture schema', 1;"
            )
        # Default connection database deliberately differs from the registry location.
        connection = ResolvedBindingConnection(
            credentials=CredentialsConfig(
                host=os.environ["DPONE_IT_MSSQL_HOST"],
                port=int(os.environ["DPONE_IT_MSSQL_PORT"]),
                username=os.environ["DPONE_IT_MSSQL_USER"],
                password=os.environ["DPONE_IT_MSSQL_PASSWORD"],
                database=decoy,
                trust_server_certificate="yes",
                connect_timeout=10,
                query_timeout=30,
            ),
            safe_metadata={},
            descriptor=ResolvedConnectionDescriptor(
                connection_type="mssql", properties={"database": metadata, "schema": "dbo"}
            ),
        )
        yield admin, metadata, decoy, connection


def _build_pair(catalog, config, resources):
    connection = catalog[3]
    selection = select_audit_storage(config)
    assert selection is not None
    location = resolve_mssql_audit_location(selection, connection)
    load_config = LoadConfigBuilder().build(config)
    connections = RuntimeResolvedConnections(strict=True, audit=connection, audit_location=location)
    pair = build_independent_audit_bindings(connections=connections, load_config=load_config, ownership=resources)
    assert pair is not None
    return load_config, pair


def _rows(catalog, database, table, columns):
    # Only fixture-generated identifiers reach SQL; business values remain parameters.
    return catalog[0].get_records(f"SELECT {columns} FROM [{database}].dbo.[{table}]")


def _assert_no_default_writes(catalog):
    for table in ("dpone_load_audit", "__dpone__load_steps"):
        assert _rows(catalog, catalog[2], table, "COUNT(*)") == [(0,)]
    assert catalog[0].get_records(f"SELECT name FROM [{catalog[1]}].sys.tables ORDER BY name") == [
        ("__dpone__load_steps",),
        ("dpone_load_audit",),
        ("drifted_steps",),
    ]


def test_selected_pair_persists_lifecycle_and_steps_without_state_or_runtime_ddl(audit_catalog):
    with ExitStack() as resources:
        config, pair = _build_pair(audit_catalog, _config(), resources)
        identity = LoadIdentityService(audit_storage=pair.loads)
        governance = LoadGovernanceService()
        lifecycle = RuntimeDecisionLifecycle(
            load_governance_service=governance,
            load_identity_service=identity,
            logger=None,
            audit_bindings=pair,
        )
        # The selected pair is sufficient; there is deliberately no business connector.
        lifecycle.configure_audit_storage(sink=SimpleNamespace(), load_config=config)
        started = identity.start(config, process_name="synthetic café")
        staged = identity.mark_staged(started, extracted_rows=3)
        identity.mark_committed(staged, SimpleNamespace(staging_rows=3, total_rows=3, inserted_rows=3))
        governance.record_load_step(
            load_record=started,
            step_id="verify",
            phase="quality",
            kind="synthetic",
            status="succeeded",
            details={"loaded_rows": 3, "label": "café 東京"},
        )
        failed = identity.start(config, process_name="synthetic failure")
        pair.loads.record_load_failed(replace(failed, status="failed", error_message="synthetic café"))
        rows = audit_catalog[0].get_records(
            f"SELECT run_id,status,extracted_rows,inserted_rows,total_rows,process_name,error_message "
            f"FROM [{audit_catalog[1]}].dbo.dpone_load_audit WHERE load_id=?",
            (started.load_id,),
        )
        assert rows == [(started.run_id, "committed", 3, 3, 3, "synthetic café", None)]
        assert audit_catalog[0].get_records(
            f"SELECT status,error_message FROM [{audit_catalog[1]}].dbo.dpone_load_audit WHERE load_id=?",
            (failed.load_id,),
        ) == [("failed", "synthetic café")]
        steps = _rows(audit_catalog, audit_catalog[1], "__dpone__load_steps", "load_id,status,details_json")
        assert len(steps) == 1 and steps[0][:2] == (started.load_id, "succeeded")
        details = json.loads(steps[0][2])
        assert details["loaded_rows"] == 3 and details["label"] == "café 東京"
        _assert_no_default_writes(audit_catalog)


@pytest.mark.parametrize("table", ["loads_table", "steps_table"])
@pytest.mark.parametrize("defect", ["absent", "drifted_steps"])
def test_external_admission_rejects_real_missing_or_drifted_tables_without_repair(audit_catalog, table, defect):
    config = _config()
    config["sink"]["options"]["load_governance"]["audit"][table] = defect
    with ExitStack() as resources, pytest.raises(RuntimeError, match="mssql_external_state_contract"):
        _build_pair(audit_catalog, config, resources)
    assert _rows(audit_catalog, audit_catalog[1], "dpone_load_audit", "COUNT(*)") == [(0,)]
    _assert_no_default_writes(audit_catalog)


def test_disabled_steps_preserve_selected_load_ledger(audit_catalog):
    config = _config()
    config["sink"]["options"]["load_governance"]["audit"].update(enabled=False, steps_table="absent")
    with ExitStack() as resources:
        load_config, pair = _build_pair(audit_catalog, config, resources)
        assert pair.steps is None
        started = LoadIdentityService(audit_storage=pair.loads).start(load_config)
        assert _rows(audit_catalog, audit_catalog[1], "dpone_load_audit", "load_id,status") == [
            (started.load_id, "started")
        ]
        assert _rows(audit_catalog, audit_catalog[1], "__dpone__load_steps", "COUNT(*)") == [(0,)]
        _assert_no_default_writes(audit_catalog)
