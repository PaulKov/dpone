"""Raw read policy must not reinterpret legacy staging or commit authority."""

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.manifest import mssql_native_policy as policy
from dpone.readiness.managed import ExecutionPlanService

LEGACY = dict(
    run_id="run",
    target_id="target",
    source_query_id="query",
    window_fingerprint="window",
    schema_fingerprint="schema",
    wire_fingerprint="wire",
)


def test_mode_absence_is_not_an_authored_default():
    assert policy.native_source_read_mode(SimpleNamespace(options={})) is None
    assert (
        policy.native_source_read_mode(
            SimpleNamespace(options={"native_transfer": {"source_read": {"mode": "raw_single_query"}}})
        )
        == "raw_single_query"
    )


@pytest.mark.parametrize("value", [None, {}, {"mode": "raw_single_query"}])
def test_source_policy_alone_cannot_silently_fall_back(value):
    config = SimpleNamespace(options={"native_transfer": {"source_read": value}})
    assert policy.native_requested(config) is True
    with pytest.raises(ValueError, match="source_read_invalid|wire_required"):
        policy.validate_native_config(config)


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        False,
        [],
        "raw_single_query",
        {"mode": None},
        {"mode": "final"},
        {"mode": "raw_single_query", "extra": True},
    ],
)
def test_invalid_explicit_mode_is_rejected(value):
    with pytest.raises(ValueError, match="source_read_invalid"):
        policy.native_source_read_mode(SimpleNamespace(options={"native_transfer": {"source_read": value}}))


def test_legacy_identity_bytes_and_order_are_frozen():
    plan = NativeChunkPlan(**LEGACY)
    assert plan.to_dict() == LEGACY
    assert list(plan.to_dict()) == list(LEGACY)
    raw = replace(plan, source_read_mode="raw_single_query")
    assert raw.to_dict() == {**LEGACY, "source_read_mode": "raw_single_query"}


def test_legacy_physical_names_and_consumed_binding_match_frozen_baseline():
    from dpone.runtime.native_wire_models import stable_hash
    from dpone.runtime.sinks.mssql_native_import import MssqlNativeChunkImporter
    from dpone.runtime.sinks.mssql_native_prepared_owner import planned_stage

    plan = NativeChunkPlan(**LEGACY)
    importer = object.__new__(MssqlNativeChunkImporter)
    assert importer.table_name(plan, "run-0-0") == "dpone_native_30254a0fe5fa3f2401f1150a4db4048e74d662b0"
    context = SimpleNamespace(plan=plan)
    config = SimpleNamespace(
        staging_database=None, target_database="synthetic", staging_schema=None, target_schema="dbo"
    )
    stage = planned_stage(context, config)
    assert stage["binding"] == "26f3fd1f103092d7102ecd8d9dc877900b07449442ce76d2e2db7ffd123e908e"
    assert stage["table"] == "dpone_native_prepared_26f3fd1f103092d7102ecd8d9dc877900b074494"
    assert stable_hash(plan.to_dict()) == "sha256:26f3fd1f103092d7102ecd8d9dc877900b07449442ce76d2e2db7ffd123e908e"


@pytest.mark.parametrize("mode,version", [(None, 1), ("raw_single_query", 2)])
def test_journal_versions_share_lookup_and_reject_policy_change(tmp_path, mode, version):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan(**LEGACY, source_read_mode=mode)
    journal = NativeChunkJournal(store, lease, plan)
    journal.begin()
    record = store.load(journal.key)
    assert record is not None
    original = record.payload
    assert json.loads(original)["version"] == version
    assert NativeChunkJournal(store, lease, plan).data == journal.data
    changed = replace(plan, source_read_mode="raw_single_query" if mode is None else None)
    with pytest.raises(WindowContractError, match="journal_identity_changed"):
        NativeChunkJournal(store, lease, changed)
    assert store.load(journal.key).payload == original


@pytest.mark.parametrize(
    "version,identity",
    [
        (True, LEGACY),
        (3, LEGACY),
        (2, LEGACY),
        (1, {**LEGACY, "source_read_mode": "raw_single_query"}),
        (2, {**LEGACY, "source_read_mode": "other"}),
        (2, {**LEGACY, "source_read_mode": "raw_single_query", "extra": "bad"}),
    ],
)
def test_malformed_version_mode_pairs_are_not_repaired(tmp_path, version, identity):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan(**LEGACY)
    journal = NativeChunkJournal(store, lease, plan)
    journal.begin()
    data = dict(journal.data, version=version, identity=identity)
    payload = json.dumps(data)
    store.save(journal.key, journal.revision, payload, lease)
    with pytest.raises(WindowContractError):
        NativeChunkJournal(store, lease, plan)
    assert store.load(journal.key).payload == payload


def test_manifest_mode_reaches_offline_plan_without_execution_claim(tmp_path):
    raw = yaml.safe_load(Path("examples/native/clickhouse-to-mssql-native.yaml").read_text())
    raw["defaults"]["source"]["options"]["native_transfer"]["source_read"] = {"mode": "raw_single_query"}
    path = tmp_path / "raw.yaml"
    path.write_text(yaml.safe_dump(raw))
    plan = ExecutionPlanService().plan_manifest(path)
    assert plan["mssql_native"]["source_read_mode"] == "raw_single_query"
    assert plan["mssql_native"]["status"] == "composition_required"
    assert plan["native_transfer_route_decision"]["certification_status"] == "unverified"
