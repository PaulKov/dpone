"""Runtime capture contract and protected store tests; doubles are not live proof."""

import os
from dataclasses import asdict, replace
from hashlib import sha256
from types import SimpleNamespace

import pytest

from dpone.adapters.composition_snapshot_capture_store import MssqlSnapshotCaptureStore, ProtectedSnapshotFiles
from dpone.app.composition_clickhouse_capture import CompositionClickHouseCapture
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_snapshot import SnapshotLimits
from dpone.contracts.composition_snapshot_capture import (
    SnapshotCaptureSubject,
    attempt_snapshot_target,
    generation_original,
)
from dpone.contracts.strict_json import canonical_json_bytes
from tests.composition_snapshot_helpers import intent
from tests.test_composition_clickhouse_execution_root import runtime as runtime
from tests.test_composition_clickhouse_gate import SQL_SERVICE, GateCursor
from tests.test_composition_clickhouse_source import reader as source_reader


def test_attempt_generation_slot_is_stable_and_does_not_reuse_another_attempt():
    value = intent()
    first = attempt_snapshot_target(value.target, value.attempt)
    assert first == attempt_snapshot_target(value.target, value.attempt)
    second = attempt_snapshot_target(value.target, replace(value.attempt, try_number=value.attempt.try_number + 1))
    assert first.generation_table != second.generation_table
    assert first.target_table == second.target_table == value.target.target_table
    assert len(first.generation_table) <= 128


def test_generation_original_hash_has_no_self_reference():
    generation = intent().generation
    observed = generation_original(generation)
    fields = asdict(generation)
    fields.pop("record_sha256")
    original = canonical_json_bytes({"schema": "dpone.composition-snapshot-generation.v1", **fields})
    assert observed.record_sha256 == "sha256:" + sha256(original).hexdigest()
    assert generation_original(replace(generation, record_sha256="sha256:" + "0" * 64)) == observed


@pytest.fixture
def capture_env(runtime, monkeypatch, tmp_path):
    """Real store/service/files; SQL catalog, root UID and ClickHouse are simulated."""
    import dpone.adapters.composition_snapshot_capture_store as store_module
    from dpone.contracts.composition_snapshot_materialization import snapshot_content_sha256
    from tests.composition_snapshot_helpers import digest, observation
    from tests.test_composition_clickhouse_source import COLUMNS

    value = runtime.authority.value
    subject = SnapshotCaptureSubject(
        value.attempt,
        attempt_snapshot_target(value.target, value.attempt),
        digest("verified binding original"),
        ("db", "dbo", "data"),
        SnapshotLimits(100, 65536, 65536, 65536, 65536, 131072),
    )
    closure = {}
    original_sql = GateCursor._select_or_mutate

    def sql(cursor, statement, params):
        data = cursor.connection.data
        if "composition_ch_gate_bindings] b JOIN" in statement:
            return [(closure["user"],)] if "user" in closure else []
        if statement.startswith("SELECT TOP (2) proof_document"):
            from dpone.contracts.composition_persistence import encode_attempt_proof

            proof = closure.get("proofs", {}).get(params[-1])
            return [] if proof is None else [(encode_attempt_proof(proof),)]
        if "composition_snapshot_captures]" in statement:
            events = data.setdefault("snapshot_captures", {})
            if statement.startswith("INSERT"):
                assert params[:2] not in events
                events[params[:2]] = params[2:]
                return []
            return [(phase, *row) for (operation, phase), row in events.items() if operation == params[0]]
        return original_sql(cursor, statement, params)

    monkeypatch.setattr(GateCursor, "_select_or_mutate", sql)
    monkeypatch.setattr(store_module, "require_snapshot_capture_schema", lambda *_: None)
    monkeypatch.setattr(store_module, "require_supervisor", lambda: None)
    monkeypatch.setattr(
        store_module, "open_protected", lambda path, **_: os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    )
    original_stat = os.fstat

    def root_stat(fd):
        value = original_stat(fd)
        return SimpleNamespace(
            **{
                key: getattr(value, key)
                for key in ("st_mode", "st_nlink", "st_dev", "st_ino", "st_size", "st_mtime_ns")
            },
            st_uid=0,
        )

    monkeypatch.setattr(store_module.os, "fstat", root_stat)
    store = MssqlSnapshotCaptureStore(
        runtime.db.connect, expected_service_id=SQL_SERVICE, source_verifier=lambda _: subject
    )
    files = ProtectedSnapshotFiles(tmp_path)
    source, cursor, connection = source_reader([(7, 1, "new"), (4, 3, None)], subject.limits)
    source._require_source = lambda current: b"same-connection service/database pins" if current is connection else None

    class Catalog:
        ingested = False
        corrupt = False
        collision = False

        def observe_capture(self, passed, columns, **_):
            assert passed == subject
            base = observation(value)
            if not self.ingested:
                return replace(
                    base,
                    target=subject.target,
                    generation_uuid=subject.generation_uuid if self.collision else None,
                    table_engines=("MergeTree", None),
                    schema_sha256=(base.schema_sha256[0], None),
                    physical_sha256=(base.physical_sha256[0], None),
                    generation_content_sha256=None,
                    generation_rows=None,
                    generation_bytes=None,
                )
            data = ((1, "bad"), (3, None)) if self.corrupt else ((1, "new"), (3, None))
            return replace(
                base,
                target=subject.target,
                generation_uuid=subject.generation_uuid,
                generation_rows=2,
                generation_content_sha256=snapshot_content_sha256(COLUMNS, data, max_rows=100, max_bytes=65536),
            )

    catalog = Catalog()
    service = CompositionClickHouseCapture(store=store, files=files, source_reader=source, catalog=catalog)
    return SimpleNamespace(
        service=service,
        store=store,
        files=files,
        source=source,
        catalog=catalog,
        subject=subject,
        cursor=cursor,
        connection=connection,
        runtime=runtime,
        root=tmp_path,
        closure=closure,
    )


def test_capture_pins_actual_source_and_payload_before_permitting_create(capture_env):
    env = capture_env
    captured = env.service.capture_once(env.subject.attempt)
    assert captured.rows == ((1, "new"), (3, None))
    assert captured.payload
    assert captured.subject.generation_uuid == env.subject.generation_uuid
    assert set(env.store.read(env.subject.attempt)[1]) == {"CLAIMED", "CAPTURED"}
    assert env.files.read(env.subject, captured.record)[1] == captured.payload
    assert env.cursor.closed and env.connection.closed
    assert any("SERIALIZABLE" in sql for sql in env.cursor.statements)
    assert any("TABLOCK,HOLDLOCK" in sql for sql in env.cursor.statements)
    with pytest.raises(CompositionAdmissionError, match="snapshot_capture_replay"):
        env.service.capture_once(env.subject.attempt)


def test_capture_claim_lost_ack_cannot_reopen_source(capture_env):
    env = capture_env
    env.runtime.db.fail_commit = True
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
        env.service.capture_once(env.subject.attempt)
    env.runtime.db.fail_commit = False
    with pytest.raises(CompositionAdmissionError, match="snapshot_capture_replay"):
        env.service.capture_once(env.subject.attempt)
    assert env.cursor.statements == []


def test_existing_generation_namespace_blocks_before_any_files_or_create(capture_env):
    env = capture_env
    env.catalog.collision = True
    with pytest.raises(CompositionAdmissionError, match="snapshot_capture_namespace"):
        env.service.capture_once(env.subject.attempt)
    assert set(env.store.read(env.subject.attempt)[1]) == {"CLAIMED"}
    assert list(env.root.iterdir()) == []


def test_files_are_immutable_and_hash_checked(capture_env):
    env = capture_env
    captured = env.service.capture_once(env.subject.attempt)
    source, payload = env.files.read(env.subject, captured.record)
    with pytest.raises(CompositionAdmissionError):
        env.files.write_once(env.subject, source, payload)
    path = env.root / env.subject.attempt.attempt_sha256[7:] / "payload.native"
    path.chmod(0o600)
    path.write_bytes(b"changed")
    path.chmod(0o400)
    with pytest.raises(CompositionAdmissionError, match="snapshot_capture_files_changed"):
        env.files.read(env.subject, captured.record)


def test_capture_file_fsync_failure_keeps_only_claim(capture_env, monkeypatch):
    env = capture_env

    def fail(_fd):
        raise OSError("simulated power loss")

    monkeypatch.setattr(os, "fsync", fail)
    with pytest.raises(CompositionAdmissionError, match="snapshot_capture_files"):
        env.service.capture_once(env.subject.attempt)
    assert set(env.store.read(env.subject.attempt)[1]) == {"CLAIMED"}


def test_same_count_wrong_b_content_cannot_seal_generation(capture_env):
    env = capture_env
    captured = env.service.capture_once(env.subject.attempt)
    env.catalog.ingested = True
    env.catalog.corrupt = True
    from tests.test_composition_clickhouse_execution_root import _proof

    with pytest.raises(CompositionAdmissionError, match="snapshot_capture_materialization"):
        env.service.finalize(
            captured, _proof(env.subject.attempt, "CLOSED_GATES"), _proof(env.subject.attempt, "QUIESCENCE")
        )
    assert set(env.store.read(env.subject.attempt)[1]) == {"CLAIMED", "CAPTURED"}


def _closure(env):
    from tests.test_composition_clickhouse_execution_root import USER_ID, _proof

    closed = _proof(env.subject.attempt, "CLOSED_GATES")
    quiet = _proof(env.subject.attempt, "QUIESCENCE")
    env.closure.update(user=USER_ID, proofs={p.proof_sha256: p for p in (closed, quiet)})
    return closed, quiet


def test_independent_b_seals_actual_generation_and_reopens_originals(capture_env):
    env = capture_env
    captured = env.service.capture_once(env.subject.attempt)
    env.catalog.ingested = True
    generation = env.service.finalize(captured, *_closure(env))
    assert generation.new_generation_uuid == env.subject.generation_uuid
    assert generation.source_snapshot_sha256 == captured.record.source_document_sha256
    assert generation.record_sha256 == generation_original(generation).record_sha256
    assert env.service.load_generation(env.subject.attempt, generation.record_sha256) == generation
    assert set(env.store.read(env.subject.attempt)[1]) == {"CLAIMED", "CAPTURED", "GENERATION_SEALED"}
    with pytest.raises(CompositionAdmissionError):
        env.service.load_generation(env.subject.attempt, "sha256:" + "0" * 64)


def test_generation_requires_protected_closed_ingest_and_exact_principal(capture_env):
    env = capture_env
    captured = env.service.capture_once(env.subject.attempt)
    env.catalog.ingested = True
    closed, quiet = _closure(env)
    env.closure["user"] = "11111111-1111-4111-8111-111111111111"
    with pytest.raises(CompositionAdmissionError, match="snapshot_capture_closure"):
        env.service.finalize(captured, closed, quiet)
    assert "GENERATION_SEALED" not in env.store.read(env.subject.attempt)[1]


def test_source_binding_drift_cannot_reuse_capture_claim(capture_env):
    env = capture_env
    env.store.claim_once(env.subject.attempt)
    env.store._verify = lambda _: replace(env.subject, source_binding_sha256="sha256:" + "1" * 64)
    with pytest.raises(CompositionAdmissionError):
        env.store.claim_once(env.subject.attempt)


def test_leaf_symlink_original_is_never_read(capture_env):
    env = capture_env
    captured = env.service.capture_once(env.subject.attempt)
    path = env.root / env.subject.attempt.attempt_sha256[7:] / "payload.native"
    path.unlink()
    path.symlink_to(env.root / "outside")
    with pytest.raises(CompositionAdmissionError):
        env.files.read(env.subject, captured.record)


def test_generated_schema_has_immutable_phases_authority_and_real_proof_columns():
    from dpone.adapters.composition_snapshot_capture_schema import render_snapshot_capture_schema

    ddl = render_snapshot_capture_schema()
    assert "GENERATION_SEALED" in ddl and "DPONE_SNAPSHOT_ORDER" in ddl
    assert "DPONE_SNAPSHOT_IMMUTABLE" in ddl and "fencing_epoch" in ddl
    assert "HASHBYTES" in ddl and "composition_operations" in ddl


@pytest.mark.parametrize("mismatch", ["table", "limits"])
def test_capture_rejects_source_binding_before_read(capture_env, mismatch):
    env = capture_env
    if mismatch == "table":
        env.source._table = {"database": "other", "schema": "dbo", "name": "data"}
    else:
        env.source._limits = replace(env.subject.limits, max_rows=1)
    with pytest.raises(CompositionAdmissionError, match="snapshot_capture_source_binding"):
        env.service.capture_once(env.subject.attempt)
    assert env.cursor.statements == []
    assert set(env.store.read(env.subject.attempt)[1]) == {"CLAIMED"}
    assert list(env.root.iterdir()) == []


def test_generation_recovery_rechecks_protected_closure(capture_env):
    env = capture_env
    captured = env.service.capture_once(env.subject.attempt)
    env.catalog.ingested = True
    generation = env.service.finalize(captured, *_closure(env))
    env.closure["proofs"].clear()
    with pytest.raises(CompositionAdmissionError, match="snapshot_capture_closure"):
        env.service.load_generation(env.subject.attempt, generation.record_sha256)


def test_load_generation_in_reuses_supplied_control_transaction(capture_env, monkeypatch):
    env = capture_env
    captured = env.service.capture_once(env.subject.attempt)
    env.catalog.ingested = True
    generation = env.service.finalize(captured, *_closure(env))
    with env.store._transaction() as ledger:

        def forbidden():
            raise AssertionError("nested transaction would deadlock")

        monkeypatch.setattr(env.store, "_transaction", forbidden)
        assert env.service.load_generation_in(ledger, env.subject.attempt, generation.record_sha256) == generation


@pytest.mark.parametrize("rows", [[("x" * (2 * 1024 * 1024),)], [("x" * 4000,)] * 64, [("\x00" * 180000,)]])
def test_materialization_preflight_rejects_unobservable_pages(rows):
    from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn
    from dpone.contracts.composition_snapshot_materialization import require_snapshot_materialization_pages

    with pytest.raises(CompositionAdmissionError, match="snapshot_materialization_page_budget"):
        require_snapshot_materialization_pages((ClickHouseDispatchColumn("value", "String"),), rows)


def test_materialization_preflight_preserves_70k_and_accounts_worst_page_order():
    from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn
    from dpone.contracts.composition_snapshot_materialization import require_snapshot_materialization_pages

    columns = (ClickHouseDispatchColumn("value", "Nullable(String)"),)
    require_snapshot_materialization_pages(columns, [("x" * 70000,), (None,)])
    # An initial cheap page must not mask a later/worst-order oversized page.
    with pytest.raises(CompositionAdmissionError, match="snapshot_materialization_page_budget"):
        require_snapshot_materialization_pages(columns, [(None,)] * 64 + [("x" * 4000,)] * 64)


def test_capture_checks_observer_page_before_encoding_catalog_or_files(capture_env, monkeypatch):
    env = capture_env
    from dpone.app import composition_clickhouse_capture as module

    def reject(*_):
        raise CompositionAdmissionError("snapshot_materialization_page_budget")

    def forbidden(*_, **__):
        raise AssertionError("preflight must precede encoding, observations and file persistence")

    monkeypatch.setattr(module, "require_snapshot_materialization_pages", reject)
    monkeypatch.setattr(module, "ClickHouseNativeEncoder", forbidden)
    monkeypatch.setattr(env.catalog, "observe_capture", forbidden)
    monkeypatch.setattr(env.files, "write_once", forbidden)
    with pytest.raises(CompositionAdmissionError, match="snapshot_materialization_page_budget"):
        env.service.capture_once(env.subject.attempt)
    assert set(env.store.read(env.subject.attempt)[1]) == {"CLAIMED"}


def test_materialization_aggregate_budget_includes_escaping_frames_and_catalog():
    from dpone.contracts.composition_snapshot_materialization import (
        SNAPSHOT_MATERIALIZATION_PAGE_ROWS,
        SNAPSHOT_MATERIALIZATION_RESPONSE_BYTES,
        snapshot_materialization_catalog_budget,
    )

    assert SNAPSHOT_MATERIALIZATION_PAGE_ROWS == 64
    assert SNAPSHOT_MATERIALIZATION_RESPONSE_BYTES == 1024 * 1024
    assert snapshot_materialization_catalog_budget(max_source_bytes=70000, max_rows=64) > 6 * 70000 + 2 * 65536
    assert snapshot_materialization_catalog_budget(
        max_source_bytes=70000, max_rows=128
    ) > snapshot_materialization_catalog_budget(max_source_bytes=70000, max_rows=64)


@pytest.mark.parametrize("values", [["x" * (2 * 1024 * 1024)], ["x" * 4000] * 64])
def test_actual_capture_page_rejection_leaves_only_claim(capture_env, monkeypatch, values):
    env = capture_env
    from tests.test_composition_clickhouse_source import COLUMNS

    subject = replace(
        env.subject,
        limits=replace(env.subject.limits, max_source_bytes=4 * 1024 * 1024, max_wire_bytes=4 * 1024 * 1024),
    )
    monkeypatch.setattr(env.store, "_verify", lambda _: subject)
    env.source._limits = subject.limits
    env.source.source_identity_original = b"verified source original"
    monkeypatch.setattr(
        env.source, "read_snapshot", lambda: (COLUMNS, tuple((i, text) for i, text in enumerate(values)))
    )

    def forbidden(*_, **__):
        raise AssertionError("page rejection must precede catalog/persistence")

    monkeypatch.setattr(env.catalog, "observe_capture", forbidden)
    monkeypatch.setattr(env.files, "write_once", forbidden)
    with pytest.raises(CompositionAdmissionError, match="snapshot_materialization_page_budget"):
        env.service.capture_once(subject.attempt)
    assert set(env.store.read(subject.attempt)[1]) == {"CLAIMED"}
