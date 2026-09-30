"""Raw EOF binding is additive, atomic, and validated before recovery I/O."""

import copy
import json
from dataclasses import asdict, replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal_v2 import NativeChunkJournalV2
from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.clickhouse_raw_snapshot import raw_snapshot_extension, raw_source_query_binding
from dpone.contracts.mssql_native_chunks import NativeChunkReceipt, NativeStageComplete
from dpone.contracts.mssql_native_verification import native_completion_digests, ordered_native_receipts
from dpone.contracts.mssql_native_verification_identity import build_bcp_target_local_verification_identity
from dpone.runtime.etl.mssql_schema_preplan_support import schema_columns_sha256
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleReceipt
from dpone.runtime.mssql_native_application_assembly import _chunk_plan
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.schema_evolution_payload import columns_from_schema
from dpone.runtime.sinks import mssql_native_completed_payload as completed
from tests.test_clickhouse_raw_snapshot_contracts import _eof, _profile
from tests.test_mssql_native_recovery_authority import _admission, _bindings


def _config(raw=True):
    return SimpleNamespace(
        load_strategy="full_refresh",
        options={"native_transfer": {"source_snapshot": {"mode": "exact_raw_rows", "replica_scope": "single_server"}}}
        if raw
        else {},
    )


def _payload(raw=True):
    profile = replace(_profile(), ordered_schema=(("id", "Int64", ""),), window=None)
    admission = _admission()
    source_schema = schema_columns_sha256(columns_from_schema((("id", "Int64", False),)))
    preplan = SimpleNamespace(source_relation_identity=profile.relation_uuid, source_schema_sha256=source_schema)
    legacy = _chunk_plan(_config(False), admission, preplan, "wire")
    binding = raw_source_query_binding(legacy.source_query_id, profile)
    artifact = SimpleNamespace(source_relation_uuid=profile.relation_uuid)
    if raw:
        artifact.raw_snapshot_profile = profile
        artifact.raw_snapshot_eof = _eof()
        artifact.source_query_binding = binding
    now = datetime(2030, 1, 1, tzinfo=UTC)
    payload = SimpleNamespace(
        artifact=artifact,
        schema=(("id", "bigint"),),
        relation_schema=(("id", "Int64"),),
        relation_dialect=None,
        mssql_target_mutation_plan=None,
        mssql_transaction_admission=admission,
        require_completed_extraction=lambda: ExtractionLifecycleReceipt(now, "test", extraction_completed_at=now),
    )
    return payload, legacy


def _sealed():
    payload, legacy = _payload()
    identity = build_bcp_target_local_verification_identity(
        replace(
            legacy,
            source_query_id=payload.artifact.source_query_binding,
            wire_fingerprint=build_mssql_bcp_native_contract(
                schema=payload.schema, query="binding", target_format="mssql_native"
            ).type_layout_hash,
        ),
        timeout_seconds=30,
    )
    metadata = completed.completion_metadata(payload, recovery_bindings=_bindings())
    # Explicit construction also tests recovery independently from the producer.
    metadata["source_snapshot_v1"] = raw_snapshot_extension(
        identity.plan.source_query_id, payload.artifact.raw_snapshot_profile, payload.artifact.raw_snapshot_eof
    )
    receipt = NativeChunkReceipt(0, "attempt", "stage", 2, 16, "a" * 64, "b" * 64)
    return identity, {
        "phase": "stage_complete",
        "complete": native_completion_digests((receipt,), metadata),
        "chunks": {"0": {"phase": "verified", "receipt": asdict(receipt)}},
        "completion_metadata": metadata,
    }


def test_exact_completion_adds_only_closed_eof_extension():
    raw, legacy = _payload(), _payload(False)
    raw_metadata = completed.completion_metadata(raw[0])
    legacy_metadata = completed.completion_metadata(legacy[0])
    extension = raw_metadata.pop("source_snapshot_v1", None)
    assert extension == raw_snapshot_extension(
        raw[0].artifact.source_query_binding, raw[0].artifact.raw_snapshot_profile, raw[0].artifact.raw_snapshot_eof
    )
    assert json.dumps(raw_metadata, sort_keys=True) == json.dumps(legacy_metadata, sort_keys=True)


@pytest.mark.parametrize("rows", [0, 1, 3, True])
def test_source_eof_count_must_equal_verified_stage(rows):
    payload, _ = _payload()
    with pytest.raises(ValueError, match="source_snapshot"):
        completed.require_raw_snapshot_row_count(payload, NativeStageComplete((), rows, "digest"))


def test_matching_count_and_legacy_count_are_accepted():
    completed.require_raw_snapshot_row_count(_payload()[0], NativeStageComplete((), 2, "digest"))
    completed.require_raw_snapshot_row_count(_payload(False)[0], NativeStageComplete((), 99, "digest"))


def test_missing_raw_eof_cannot_seal_completion():
    payload, _ = _payload()
    del payload.artifact.raw_snapshot_eof
    with pytest.raises(ValueError, match="source_snapshot"):
        completed.completion_metadata(payload)


def test_sealed_raw_recovery_is_source_free():
    identity, projection = _sealed()
    completed.validate_raw_snapshot_recovery(_config(), identity=identity, projection=projection, action="resume")


@pytest.mark.parametrize(
    "tamper",
    [
        "missing",
        "extra",
        "unknown",
        "count",
        "profile",
        "schema",
        "window",
        "marker",
        "selector",
        "relation",
        "route",
        "wire_schema",
    ],
)
def test_sealed_raw_recovery_rejects_tampered_authority(tamper):
    identity, projection = _sealed()
    config = _config()
    metadata = projection["completion_metadata"]
    extension = metadata["source_snapshot_v1"]
    if tamper == "missing":
        metadata.pop("source_snapshot_v1")
    elif tamper == "extra":
        extension["extra"] = True
    elif tamper == "unknown":
        identity = replace(identity, plan=replace(identity.plan, source_query_id="clickhouse.raw-query.v2:" + "a" * 64))
    elif tamper == "count":
        projection["complete"]["rows"] = 3
    elif tamper == "profile":
        extension["profile"]["server_revision"] = "other"
    elif tamper == "schema":
        metadata["relation_schema"] = [["other", "Int64"]]
    elif tamper == "wire_schema":
        metadata["schema"] = [["id", "int"]]
    elif tamper == "window":
        identity = replace(identity, plan=replace(identity.plan, window_fingerprint="a" * 64))
    elif tamper == "marker":
        forged = "clickhouse.raw-query.v1:" + "a" * 64
        identity = replace(identity, plan=replace(identity.plan, source_query_id=forged))
        extension["source_query_binding"] = forged
    elif tamper == "selector":
        config = _config(False)
    elif tamper == "relation":
        metadata["source_relation_uuid"] = "other"
    elif tamper == "route":
        metadata["recovery_authority_v1"]["operation"]["route_fingerprint"] = "a" * 64
    if tamper != "count":
        projection["complete"] = native_completion_digests(ordered_native_receipts(projection["chunks"]), metadata)
    with pytest.raises(ValueError, match="source_snapshot"):
        completed.validate_raw_snapshot_recovery(config, identity=identity, projection=projection, action="resume")


@pytest.mark.parametrize("action", ["inspect", "reconcile", "retire"])
def test_pre_eof_raw_marker_allows_custody_settlement_without_source(action):
    identity, _ = _sealed()
    projection = {"phase": "staging", "complete": None, "completion_metadata": {}}
    completed.validate_raw_snapshot_recovery(_config(), identity=identity, projection=projection, action=action)
    with pytest.raises(ValueError, match="source_snapshot"):
        completed.validate_raw_snapshot_recovery(_config(), identity=identity, projection=projection, action="resume")


def test_pre_eof_extension_and_markerless_extension_are_rejected():
    identity, projection = _sealed()
    pre_eof = {**projection, "phase": "staging", "complete": None}
    with pytest.raises(ValueError, match="source_snapshot"):
        completed.validate_raw_snapshot_recovery(_config(), identity=identity, projection=pre_eof, action="retire")
    identity = replace(identity, plan=replace(identity.plan, source_query_id="a" * 64))
    with pytest.raises(ValueError, match="source_snapshot"):
        completed.validate_raw_snapshot_recovery(
            _config(False), identity=identity, projection=projection, action="resume"
        )


def test_journal_atomically_binds_extension_and_detects_tampering(tmp_path):
    payload, legacy = _payload()
    payload.artifact.raw_snapshot_eof = replace(payload.artifact.raw_snapshot_eof, rows=0)
    identity = build_bcp_target_local_verification_identity(
        replace(legacy, source_query_id=payload.artifact.source_query_binding), timeout_seconds=30
    )
    store = SQLiteWindowStore(tmp_path / "state.sqlite", clock=lambda: 1.0)
    lease = store.acquire(identity.plan.target_id, "owner", 60)
    journal = NativeChunkJournalV2(store, lease, identity)
    journal.begin()
    metadata = completed.completion_metadata(payload, recovery_bindings=_bindings())
    assert "source_snapshot_v1" in metadata
    result = journal.complete(source_eof=True, completion_metadata=metadata)
    reopened = NativeChunkJournalV2(store, lease, identity)
    assert reopened.completed() == result
    assert reopened.completed_metadata() == metadata
    record = store.load(journal.key)
    corrupted = copy.deepcopy(json.loads(record.payload))
    corrupted["completion_metadata"]["source_snapshot_v1"]["source_eof"]["rows"] = 1
    store.save(journal.key, record.revision, json.dumps(corrupted), lease)
    with pytest.raises(WindowContractError, match="stage_complete_changed"):
        NativeChunkJournalV2(store, lease, identity).completed()


def test_recovery_rejects_invalid_sealed_raw_before_runtime_bindings(tmp_path, monkeypatch):
    from dpone.app import mssql_native_recovery_application as application

    identity, projection = _sealed()
    projection["completion_metadata"].pop("source_snapshot_v1")

    class Reader:
        def __init__(self, root):
            pass

        def inspect(self, invocation):
            return {"permitted_actions": ["resume"]}

        def load(self, invocation):
            return SimpleNamespace(identity=identity, projection=projection, recovery_plan=None)

    monkeypatch.setattr(application, "MssqlNativeRecoveryJournalReader", Reader)
    process = SimpleNamespace(load_config=_config(), ensure_runtime_bindings=lambda: pytest.fail("target binding"))
    with pytest.raises(ValueError, match="source_snapshot"):
        application.MssqlNativeRecoveryApplication().execute(
            process,
            journal_root=tmp_path,
            invocation_id=identity.invocation_key,
            action="resume",
            owner="operator",
            confirmed=True,
        )


def test_legacy_plan_identity_completion_and_recovery_plan_golden_bytes():
    """SHA-256 oracles captured by executing the unmodified 6c60afe modules."""
    from hashlib import sha256

    from dpone.adapters.mssql_native_recovery_plan import persist_mssql_native_recovery_plan

    payload, plan = _payload(False)
    identity = build_bcp_target_local_verification_identity(plan, timeout_seconds=30)

    def digest(value):
        return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    assert digest(plan.__dict__) == "c18326ee70c46b6f2c14a5f921ab2b7994e0bf522714be481b7d886a6a9fbd97"
    assert digest(identity.document()) == "46efefe70e79797a6e108b7b8837042aedd22584aa99fc33abb7d57fb7a97e49"
    assert digest(completed.completion_metadata(payload, recovery_bindings=_bindings())) == (
        "cb3b08bf7f425f1d071964c27f303652be1b51cd523888f34ad50e53c1c54dc1"
    )
    saved = []
    store = SimpleNamespace(load=lambda key: None, save=lambda key, rev, body, lease: saved.append((key, body)))
    persist_mssql_native_recovery_plan(
        store,
        None,
        identity=identity,
        schema=payload.schema,
        admission=payload.mssql_transaction_admission,
        recovery_bindings=lambda a: _bindings(),
        window=None,
    )
    assert saved[0][0] == f"mssql-native/recovery-plan-v1/{identity.invocation_key}"
    assert (
        sha256(saved[0][1].encode()).hexdigest() == "ddae4a77402331d4cf3174dbe331c589cc4e281008f66d283ee5cbceb29e596b"
    )


@pytest.mark.parametrize("count_offset", [0, 1])
def test_real_preparer_checks_raw_rows_and_retains_verified_sum_digest(tmp_path, monkeypatch, count_offset):
    from tests.test_mssql_native_staged_prepare import (
        test_real_native_prepare_keeps_duplicates_across_chunks_and_persists_prepared_identity as prepare,
    )

    original = completed.completion_metadata

    def raw_completion(payload, **kwargs):
        artifact = _payload()[0].artifact
        artifact.raw_snapshot_eof = replace(artifact.raw_snapshot_eof, rows=3 + count_offset)
        payload.artifact = artifact
        return original(payload, **kwargs)

    monkeypatch.setattr(completed, "completion_metadata", raw_completion)
    if count_offset:
        with pytest.raises(ValueError, match="source_snapshot_row_count_mismatch"):
            prepare(tmp_path, (2, 1), False)
    else:
        prepare(tmp_path, (2, 1), False)


@pytest.mark.parametrize("agreement", [True, False])
def test_valid_raw_recovery_composes_without_accessing_source(tmp_path, monkeypatch, agreement):
    """Application admission and recovery composition touch only sealed source facts."""
    from dpone.app import mssql_native_recovery_application as application
    from dpone.config.load_config import LoadConfig

    identity, projection = _sealed()
    from dpone.contracts.clickhouse_raw_snapshot import ClickHouseRawSourceEofV1

    extension = projection["completion_metadata"]["source_snapshot_v1"]
    eof = replace(
        ClickHouseRawSourceEofV1.from_document(extension["source_eof"]), endpoint_authority_agreement=agreement
    )
    extension["source_eof"], extension["source_eof_descriptor_sha256"] = eof.document(), eof.sha256
    projection["complete"] = native_completion_digests(
        ordered_native_receipts(projection["chunks"]), projection["completion_metadata"]
    )
    config = LoadConfig("source", "target", "raw", "events", "staging", "target", options=_config().options)
    calls = []

    class SourceForbidden:
        def __getattr__(self, name):
            pytest.fail(f"source I/O: {name}")

    class Reader:
        path = tmp_path / "journal.sqlite"

        def __init__(self, root):
            pass

        def inspect(self, invocation):
            return {"permitted_actions": ["resume"], "state": "SUCCEEDED"}

        def load(self, invocation):
            return SimpleNamespace(identity=identity, projection=projection, recovery_plan=None)

    def initialize(assembly):
        # Keep the real for_recovery constructor, including wire identity check.
        assert assembly._recovery_only
        assembly.runtime = SimpleNamespace(run=lambda config, owner: calls.append("target-recovery"))

    process = SimpleNamespace(
        load_config=config,
        raw_config={},
        source_obj=SourceForbidden(),
        sink_obj=SimpleNamespace(connector=object(), state_storage=object()),
        ensure_runtime_bindings=lambda: calls.append("bindings"),
    )
    request = _admission().operation.attempt.request
    monkeypatch.setattr(application, "MssqlNativeRecoveryJournalReader", Reader)
    monkeypatch.setattr(application, "validate_native_config", lambda config: None)
    monkeypatch.setattr(application, "live_target_coordinates", lambda config: ("warehouse", "staging", "target"))
    monkeypatch.setattr(
        application,
        "resolve_atomic_mssql_target",
        lambda *a, **kw: SimpleNamespace(
            digest=request.target_identity, database_name="warehouse", schema_name="staging", table_name="target"
        ),
    )
    monkeypatch.setattr(
        application.RuntimeStoragePolicy, "from_sources", lambda **kw: SimpleNamespace(work_dir=tmp_path)
    )
    monkeypatch.setattr(application.StoragePreflightService, "check", lambda *a: SimpleNamespace(passed=True))
    monkeypatch.setattr(
        application.MssqlNativeRecoveryApplication, "_restore_admission", staticmethod(lambda *a, **kw: _admission())
    )
    monkeypatch.setattr(
        application.MssqlNativeRecoveryApplication, "_refresh_operation", staticmethod(lambda *a: (_admission(), None))
    )
    monkeypatch.setattr(application._NativeRuntimeAssembly, "_initialize_runtime", initialize)
    result = application.MssqlNativeRecoveryApplication().execute(
        process,
        journal_root=tmp_path,
        invocation_id=identity.invocation_key,
        action="resume",
        owner="operator",
        confirmed=True,
    )
    assert result["state"] == "SUCCEEDED"
    assert calls == ["bindings", "target-recovery"]


def test_raw_recovery_preserves_legacy_ascii_encoding_for_unicode_window(monkeypatch):
    identity, projection = _sealed()
    start, end = datetime(2030, 1, 1, tzinfo=UTC), datetime(2030, 1, 2, tzinfo=UTC)
    window = ("дата", start.isoformat(), end.isoformat())
    payload, _ = _payload()
    profile = replace(payload.artifact.raw_snapshot_profile, window=window)
    from dpone.runtime.mssql_native_application_assembly import _digest

    binding = raw_source_query_binding(_digest((profile.relation_uuid, "2" * 64, window)), profile)
    identity = replace(
        identity, plan=replace(identity.plan, source_query_id=binding, window_fingerprint=_digest(window))
    )
    projection["completion_metadata"]["source_snapshot_v1"] = raw_snapshot_extension(binding, profile, _eof())
    monkeypatch.setattr(completed, "native_window", lambda config: SimpleNamespace(column="дата", start=start, end=end))
    projection["complete"] = native_completion_digests(
        ordered_native_receipts(projection["chunks"]), projection["completion_metadata"]
    )
    completed.validate_raw_snapshot_recovery(_config(), identity=identity, projection=projection, action="resume")


def _persisted_raw_eof(tmp_path, *, rows=0, agreement=True):
    """Real CAS journal allows reproducing a crash after EOF and before parity check."""
    from dpone.contracts.mssql_native_recovery_authority import MssqlNativeRecoveryBindings

    payload, _ = _payload()
    payload.artifact.raw_snapshot_eof = replace(
        payload.artifact.raw_snapshot_eof, rows=rows, endpoint_authority_agreement=agreement
    )
    identity, _ = _sealed()
    bindings = replace(_bindings(), verification_identity_sha256=identity.invocation_key)
    assert isinstance(bindings, MssqlNativeRecoveryBindings)
    store = SQLiteWindowStore(tmp_path / "state.sqlite", clock=lambda: 1.0)
    lease = store.acquire(identity.plan.target_id, "owner", 60)
    journal = NativeChunkJournalV2(store, lease, identity)
    journal.begin()
    journal.complete(
        source_eof=True, completion_metadata=completed.completion_metadata(payload, recovery_bindings=bindings)
    )
    return store, lease, identity, bindings


@pytest.mark.parametrize("phase", ["stage_complete", "preparing", "prepared", "publishing"])
def test_generic_resume_rejects_persisted_raw_count_mismatch_before_target(tmp_path, phase):
    from dpone.runtime.sinks.mssql_native_prepare import MssqlNativeStagePreparer
    from dpone.runtime.sinks.mssql_native_staged_load import MssqlNativeStagedLoadService

    store, lease, identity, bindings = _persisted_raw_eof(tmp_path, rows=2)
    journal = NativeChunkJournalV2(store, lease, identity)
    prepared = {"planned_stage": {}, "recovery": {"digest": "d", "schema": [["id", "bigint"]], "object_id": 1}}
    if phase == "preparing":
        journal.publication.preparation_started({"planned_stage": {}})
    elif phase in {"prepared", "publishing"}:
        journal.publication.prepared(prepared)
        if phase == "publishing":
            journal.publication.publication_started(prepared)
    # Outer EOF hash is consistent: this is the crash-after-stage-before-parity case.
    assert journal.completed().rows == 0
    with pytest.raises(ValueError, match="source_snapshot_row_count_mismatch"):
        completed.require_raw_snapshot_row_count(_payload()[0], journal.completed())

    class TargetForbidden:
        def __getattr__(self, name):
            pytest.fail(f"target preparation or publication reached: {name}")

    sink = TargetForbidden()
    from contextlib import nullcontext

    from dpone.runtime.sinks.mssql_native_prepare_models import NativeStageContext

    context = NativeStageContext(
        plan=identity.plan,
        wire_contract=build_mssql_bcp_native_contract(schema=(("id", "bigint"),), query="binding"),
        executor=SimpleNamespace(recover=lambda plan, lease: journal.completed()),
        lease=lease,
        verify_receipts=lambda receipts: pytest.fail("target verification reached"),
        cleanup_receipts=lambda receipts: pytest.fail("target cleanup reached"),
        capacity_check=lambda count: pytest.fail("target capacity probe reached"),
        preparation_scope=nullcontext,
        verification_identity=identity,
        journal_factory=lambda: NativeChunkJournalV2(store, lease, identity),
        recovery_bindings=lambda admission: bindings,
        row_source=lambda: pytest.fail("source reopened"),
    )
    service = MssqlNativeStagedLoadService(sink, MssqlNativeStagePreparer(sink, lambda *a: context))
    before = store.load(journal.key)
    with pytest.raises(ValueError, match="source_snapshot"):
        service.resume(_config(), context, _admission())
    assert store.load(journal.key) == before


def test_false_physical_agreement_recovers_authenticated_acquired_snapshot(tmp_path):
    from dpone.adapters.mssql_native_recovery_journal import MssqlNativeRecoveryJournalReader

    store, lease, identity, _ = _persisted_raw_eof(tmp_path, agreement=False)
    snapshot = MssqlNativeRecoveryJournalReader(tmp_path / "state.sqlite").load(identity.invocation_key)
    assert (
        snapshot.projection["completion_metadata"]["source_snapshot_v1"]["source_eof"]["endpoint_authority_agreement"]
        is False
    )
    completed.validate_raw_snapshot_recovery(
        _config(), identity=identity, projection=snapshot.projection, action="resume"
    )
    assert NativeChunkJournalV2(store, lease, identity).completed().rows == 0


def test_stale_outer_eof_hash_fails_before_application_target_bindings(tmp_path):
    from dpone.app.mssql_native_recovery_application import MssqlNativeRecoveryApplication
    from dpone.contracts.clickhouse_raw_snapshot import ClickHouseRawSourceEofV1

    store, lease, identity, _ = _persisted_raw_eof(tmp_path)
    journal = NativeChunkJournalV2(store, lease, identity)
    record = store.load(journal.key)
    projection = json.loads(record.payload)
    extension = projection["completion_metadata"]["source_snapshot_v1"]
    eof = replace(ClickHouseRawSourceEofV1.from_document(extension["source_eof"]), vendor_query_id_sha256="f" * 64)
    extension["source_eof"], extension["source_eof_descriptor_sha256"] = eof.document(), eof.sha256
    store.save(journal.key, record.revision, json.dumps(projection), lease)
    from tests.test_mssql_native_policy import config as native_config

    config = native_config()
    config.options["native_transfer"]["source_snapshot"] = _config().options["native_transfer"]["source_snapshot"]
    config.options["native_transfer"]["execution"]["verification_backend"] = "target_local"
    process = SimpleNamespace(load_config=config, ensure_runtime_bindings=lambda: pytest.fail("target bindings"))
    before = store.load(journal.key)
    with pytest.raises(ValueError, match="source_snapshot_completion_changed"):
        MssqlNativeRecoveryApplication().execute(
            process,
            journal_root=tmp_path / "state.sqlite",
            invocation_id=identity.invocation_key,
            action="resume",
            owner="operator",
            confirmed=True,
        )
    assert store.load(journal.key) == before


def test_publishing_reconcile_cannot_misclassify_missing_eof_as_pre_eof(tmp_path):
    from dpone.app.mssql_native_recovery_application import MssqlNativeRecoveryApplication
    from tests.test_mssql_native_policy import config as native_config

    store, lease, identity, _ = _persisted_raw_eof(tmp_path)
    journal = NativeChunkJournalV2(store, lease, identity)
    journal.publication.prepared({"planned_stage": {}})
    journal.publication.publication_started({"planned_stage": {}})
    record = store.load(journal.key)
    projection = json.loads(record.payload)
    projection["complete"] = None
    projection["completion_metadata"].pop("source_snapshot_v1")
    store.save(journal.key, record.revision, json.dumps(projection), lease)
    config = native_config()
    config.options["native_transfer"]["source_snapshot"] = _config().options["native_transfer"]["source_snapshot"]
    config.options["native_transfer"]["execution"]["verification_backend"] = "target_local"
    process = SimpleNamespace(load_config=config, ensure_runtime_bindings=lambda: pytest.fail("target bindings"))
    before = store.load(journal.key)
    with pytest.raises(ValueError, match="source_snapshot"):
        MssqlNativeRecoveryApplication().execute(
            process,
            journal_root=tmp_path / "state.sqlite",
            invocation_id=identity.invocation_key,
            action="reconcile",
            owner="operator",
            confirmed=True,
        )
    assert store.load(journal.key) == before
