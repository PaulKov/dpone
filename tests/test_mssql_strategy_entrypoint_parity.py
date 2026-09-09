"""Parity guards for MSSQL strategy validation at every public entry point."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft7Validator

from dpone.config import LoadConfig, LoadStrategy
from dpone.config.load_config import ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION
from dpone.config.mssql_strategy_contract import (
    MSSQLStrategyContractError,
    normalize_mssql_load_strategy,
)
from dpone.dag.errors import DagConfigurationError
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.readiness.managed_planning import ExecutionPlanService
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.mssql import MSSQLSink

_CODE = "DPONE_MSSQL_STRATEGY_CONTRACT_BLOCKED"
_BLOCKER = "mssql.strategy.incremental_merge.unique_key"
_MSSQL_ALIASES = (
    "mssql",
    "MSSQL",
    "microsoft mssql",
    "microsoft_mssql",
    "odbc",
    "sqlserver",
    "sql_server",
    "sql-server",
)


def _legacy_process(*, unique_key: bool) -> dict[str, Any]:
    strategy: dict[str, Any] = {"mode": "incremental_merge"}
    if unique_key:
        strategy["unique_key"] = ["id"]
    return {
        "name": "postgres_to_mssql_orders",
        "source": {
            "type": "postgres",
            "connection_type": "airflow",
            "connection_id": "postgres",
            "table": {"schema": "public", "name": "orders"},
            "options": {"export_format": "csv"},
        },
        "sink": {
            "type": "mssql",
            "connection_type": "airflow",
            "connection_id": "mssql",
            "table": {"schema": "dbo", "name": "orders"},
            "strategy": strategy,
        },
    }


def _unscoped_backfill_process(
    *,
    source_type: str = "postgres",
    sink_type: str = "mssql",
) -> dict[str, Any]:
    process = _legacy_process(unique_key=True)
    process["source"]["type"] = source_type
    process["sink"]["type"] = sink_type
    process["sink"]["strategy"] = {
        "mode": "backfill",
        "backfill": {
            "inner_mode": "replace",
            "chunk": {
                "column": "id",
                "kind": "integer",
                "from": 1,
                "to": 10,
                "step": 1,
            },
        },
    }
    return process


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _strategy_schema(document: Any) -> dict[str, Any] | None:
    if isinstance(document, dict):
        properties = document.get("properties")
        if isinstance(properties, dict):
            unique_key = properties.get("unique_key")
            if isinstance(unique_key, dict) and any(
                clause.get("if", {}).get("properties", {}).get("mode", {}).get("const") == "incremental_merge"
                for clause in document.get("allOf", ())
                if isinstance(clause, dict)
            ):
                return document
        for value in document.values():
            found = _strategy_schema(value)
            if found is not None:
                return found
    elif isinstance(document, list):
        for value in document:
            found = _strategy_schema(value)
            if found is not None:
                return found
    return None


@pytest.mark.parametrize(
    "schema_path",
    (
        Path("src/dpone/schema/etl-config.schema.json"),
        Path("src/dpone/schema/etl-batch-manifest.schema.json"),
        Path("src/dpone/schema/etl-flow-manifest.schema.json"),
        Path("src/dpone/schema/etl-flow-fragment-manifest.schema.json"),
    ),
)
def test_every_manifest_schema_requires_merge_unique_key(schema_path: Path) -> None:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    strategy = _strategy_schema(schema)
    assert strategy is not None
    validator = Draft7Validator(strategy)

    assert not list(validator.iter_errors({"mode": "incremental_merge", "unique_key": ["id"]}))
    missing = list(validator.iter_errors({"mode": "incremental_merge"}))
    assert len(missing) == 1
    assert missing[0].validator == "required"
    assert missing[0].validator_value == ["unique_key"]


def test_legacy_check_plan_and_runtime_share_the_typed_authority(tmp_path: Path) -> None:
    valid_path = tmp_path / "valid.yaml"
    invalid_path = tmp_path / "invalid.yaml"
    _write_yaml(valid_path, _legacy_process(unique_key=True))
    _write_yaml(invalid_path, _legacy_process(unique_key=False))

    loaded = ManifestLoaderRouter().load(valid_path, metadata_only=True)
    load_config = loaded.processes[0].config.load_config
    expected = normalize_mssql_load_strategy(load_config).to_dict()
    plan = ExecutionPlanService().plan_manifest(valid_path)
    assert plan["strategy"]["mssql_contract"] == expected

    with pytest.raises(ManifestConfigurationError, match=rf"{_CODE}: {_BLOCKER}"):
        ManifestLoaderRouter().load(invalid_path, metadata_only=True)
    with pytest.raises(ManifestConfigurationError, match=rf"{_CODE}: {_BLOCKER}"):
        ExecutionPlanService().plan_manifest(invalid_path)

    direct = LoadConfig(
        source_conn_id="postgres",
        target_conn_id="mssql",
        source_schema="public",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        options={"sink_type": "mssql"},
    )
    sink = MSSQLSink(object())
    payload = LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "int")])
    with pytest.raises(MSSQLStrategyContractError) as raised:
        sink.load(direct, payload)
    assert raised.value.code == _CODE
    assert raised.value.blocker == _BLOCKER


def test_batch_check_and_plan_reject_the_same_missing_key(tmp_path: Path) -> None:
    source = Path("examples/batch/landing_postgres_xmin_state_mssql.batch.yaml")
    valid = yaml.safe_load(source.read_text(encoding="utf-8"))
    invalid = json.loads(json.dumps(valid))
    invalid["defaults"]["sink"]["strategy"].pop("unique_key")
    valid_path = tmp_path / "valid.batch.yaml"
    invalid_path = tmp_path / "invalid.batch.yaml"
    _write_yaml(valid_path, valid)
    _write_yaml(invalid_path, invalid)

    loaded = ManifestLoaderRouter().load(valid_path, metadata_only=True)
    expected = normalize_mssql_load_strategy(loaded.processes[0].config.load_config).to_dict()
    assert ExecutionPlanService().plan_manifest(valid_path)["strategy"]["mssql_contract"] == expected
    with pytest.raises(ManifestConfigurationError, match=rf"{_CODE}: {_BLOCKER}"):
        ManifestLoaderRouter().load(invalid_path, metadata_only=True)
    with pytest.raises(ManifestConfigurationError, match=rf"{_CODE}: {_BLOCKER}"):
        ExecutionPlanService().plan_manifest(invalid_path)


def test_load_config_builder_reports_typed_contract_as_authoring_error() -> None:
    with pytest.raises(DagConfigurationError, match=rf"{_CODE}: {_BLOCKER}"):
        LoadConfigBuilder().build(_legacy_process(unique_key=False))


def test_load_config_builder_preserves_legacy_mssql_alias_fingerprint_for_inflight_retry() -> None:
    from copy import deepcopy
    from dataclasses import replace

    from dpone.contracts.mssql_transaction_governance import InvocationIdentity
    from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
    from dpone.runtime.etl.mssql_transaction_identity import build_mssql_attempt_request
    from dpone.runtime.state.mssql_generic_transaction_rows import attempt_from_rows

    process = _legacy_process(unique_key=True)
    process["sink"]["type"] = "MSSQL"
    current = LoadConfigBuilder().build(process)
    current.source_database = "source"
    current.target_database = "DWH"

    assert current.options["source_type"] == "postgres"
    assert current.options["sink_type"] == "mssql"
    assert current.options[ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION] == {
        "source_type": "postgres",
        "sink_type": "MSSQL",
    }

    legacy_options = deepcopy(current.options)
    legacy_options.pop(ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION)
    legacy_options.update(source_type="postgres", sink_type="MSSQL")
    legacy = replace(current, options=legacy_options)
    invocation = InvocationIdentity("scheduled-run", "orders", "orders:load")
    source_identity = SourcePhysicalIdentity(
        dialect="postgres",
        cluster_identifier="pg-cluster",
        database="source",
        effective_principal="reader",
        session_principal="reader",
    )
    coordinates = ("DWH", "dbo", "orders")
    legacy_request = build_mssql_attempt_request(
        legacy,
        invocation=invocation,
        target_identity=b"t" * 32,
        source_identity=source_identity,
        load_id="load-before-upgrade",
        request_coordinates=coordinates,
    )
    retry_request = build_mssql_attempt_request(
        current,
        invocation=invocation,
        target_identity=b"t" * 32,
        source_identity=source_identity,
        load_id="load-after-upgrade",
        request_coordinates=coordinates,
    )
    persisted = {
        "attempt_key": legacy_request.attempt_key,
        "target_identity": legacy_request.target_identity,
        "invocation_digest": legacy_request.invocation.invocation_digest,
        "route_fingerprint": legacy_request.route_fingerprint,
        "target_database": legacy_request.target_database,
        "target_schema": legacy_request.target_schema,
        "target_table": legacy_request.target_table,
        "strategy": legacy_request.strategy,
        "generation": 7,
        "is_current_generation": True,
    }

    assert retry_request.attempt_key == legacy_request.attempt_key
    assert retry_request.route_fingerprint == legacy_request.route_fingerprint
    assert attempt_from_rows(retry_request, [persisted]).generation == 7


def test_authored_alias_identity_does_not_weaken_canonical_postgres_mssql_wire_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
    from dpone.runtime.etl import mssql_transaction_route_identity as route_identity

    process = _legacy_process(unique_key=True)
    process["source"]["type"] = "PostgreSQL"
    process["sink"]["type"] = "odbc"
    load_config = LoadConfigBuilder().build(process)
    load_config.source_database = "source"
    load_config.target_database = "DWH"
    captured: dict[str, Any] = {}

    def capture_contract(contract: dict[str, Any]) -> bytes:
        captured.update(contract)
        return b"f" * 32

    monkeypatch.setattr(route_identity, "route_fingerprint", capture_contract)

    fingerprint = route_identity.invocation_route_fingerprint(
        load_config,
        target_identity=b"t" * 32,
        source_identity=SourcePhysicalIdentity(
            dialect="postgres",
            cluster_identifier="pg-cluster",
            database="source",
            effective_principal="reader",
            session_principal="reader",
        ),
    )

    assert fingerprint == b"f" * 32
    assert load_config.options["source_type"] == "postgres"
    assert load_config.options["sink_type"] == "mssql"
    assert captured["wire"]["runtime_export_format"] == "mssql-delimited"
    assert captured["wire"]["codec"] is not None
    assert captured["options"]["source_type"] == "PostgreSQL"
    assert captured["options"]["sink_type"] == "odbc"


@pytest.mark.parametrize(
    "sink_type",
    _MSSQL_ALIASES,
)
def test_load_config_builder_defers_only_planner_owned_backfill_scope(sink_type: str) -> None:
    load_config = LoadConfigBuilder().build(_unscoped_backfill_process(sink_type=sink_type))

    assert load_config.load_strategy == LoadStrategy.BACKFILL
    assert load_config.options["source_type"] == "postgres"
    assert load_config.options["sink_type"] == "mssql"
    with pytest.raises(MSSQLStrategyContractError) as raised:
        normalize_mssql_load_strategy(load_config)
    assert raised.value.blocker == "mssql.strategy.backfill.scope_required"


def test_load_config_builder_still_rejects_other_invalid_backfill_invariants() -> None:
    process = _unscoped_backfill_process()
    process["sink"]["strategy"]["only_new_rows"] = True

    with pytest.raises(DagConfigurationError, match="mssql.strategy.backfill.irrelevant_only_new_rows"):
        LoadConfigBuilder().build(process)


def test_load_config_builder_rejects_authored_scope_for_chunked_campaign() -> None:
    process = _unscoped_backfill_process(source_type="postgresql", sink_type="odbc")
    process["sink"]["strategy"]["portable_scope"] = {
        "version": 1,
        "kind": "equality",
        "column": "id",
        "value": {"type": "integer", "value": 1},
    }

    with pytest.raises(DagConfigurationError, match="backfill.portable_scope_is_runtime_owned"):
        LoadConfigBuilder().build(process)


def test_legacy_unchunked_backfill_replace_remains_invalid_at_authoring(tmp_path: Path) -> None:
    process = _unscoped_backfill_process()
    process["sink"]["strategy"]["backfill"].pop("chunk")
    manifest = tmp_path / "unchunked-backfill.yaml"
    _write_yaml(manifest, process)

    with pytest.raises(DagConfigurationError, match="mssql.strategy.backfill.scope_required"):
        LoadConfigBuilder().build(process)
    with pytest.raises(ManifestConfigurationError, match="mssql.strategy.backfill.scope_required"):
        ManifestLoaderRouter().load(manifest, metadata_only=True)
    with pytest.raises(ManifestConfigurationError, match="mssql.strategy.backfill.scope_required"):
        ExecutionPlanService().plan_manifest(manifest)


def test_legacy_unchunked_backfill_accepts_explicit_portable_scope() -> None:
    process = _unscoped_backfill_process()
    process["sink"]["strategy"]["backfill"].pop("chunk")
    process["sink"]["strategy"]["portable_scope"] = {
        "version": 1,
        "kind": "equality",
        "column": "id",
        "value": {"type": "integer", "value": 1},
    }

    load_config = LoadConfigBuilder().build(process)

    assert load_config.portable_scope is not None
    assert normalize_mssql_load_strategy(load_config).backfill is not None


@pytest.mark.parametrize("sink_type", _MSSQL_ALIASES)
def test_check_and_plan_accept_unscoped_backfill_campaign_authoring(tmp_path: Path, sink_type: str) -> None:
    manifest = tmp_path / "backfill.yaml"
    _write_yaml(manifest, _unscoped_backfill_process(sink_type=sink_type))

    loaded = ManifestLoaderRouter().load(manifest, metadata_only=True)
    plan = ExecutionPlanService().plan_manifest(manifest)

    assert loaded.processes[0].config.load_config.load_strategy == LoadStrategy.BACKFILL
    assert plan["sink"]["type"] == "mssql"
    assert plan["strategy"]["mssql_contract"]["backfill"]["inner_mode"] == "replace"
    assert plan["postgres_mssql_wire"] is not None


@pytest.mark.parametrize("source_type", ("postgres", "postgresql", "PostgreSQL"))
def test_check_and_plan_canonicalize_postgres_source_aliases(tmp_path: Path, source_type: str) -> None:
    manifest = tmp_path / "postgres-alias-backfill.yaml"
    _write_yaml(manifest, _unscoped_backfill_process(source_type=source_type))

    loaded = ManifestLoaderRouter().load(manifest, metadata_only=True)
    plan = ExecutionPlanService().plan_manifest(manifest)

    assert loaded.processes[0].config.load_config.options["source_type"] == "postgres"
    assert plan["source"]["type"] == "postgres"
    assert plan["sink"]["type"] == "mssql"
    assert plan["postgres_mssql_wire"] is not None
