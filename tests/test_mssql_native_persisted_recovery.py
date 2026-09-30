"""Layout-v2 recovery retains content proof and mutation authority across crashes.

Only SQL responses are synthetic: the sealed file, exact-stage barrier, receipt
producer, recovery state machine, and durable SQLite journal are real.
"""

import json
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal_v2 import NativeChunkJournalV2
from dpone.adapters.mssql_native_custody import NativeTargetCustody
from dpone.adapters.mssql_native_guard import sqlclient_exact_stage_barrier
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.contracts.mssql_native_verification import (
    NativeVerificationBackend,
    NativeVerificationIdentityV2,
    validated_native_receipt,
)
from dpone.runtime.mssql_native_chunks_files import encode_native_frame, verify_native_file
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_import import MssqlNativeChunkImporter
from dpone.runtime.sinks.mssql_native_target_local_import import NativeTargetLocalAttempt

# Two identical non-null bigint rows containing 1, each encoded as 0100000000000000.
# These fixed oracle values are independent of the production aggregate decoder.
_LIMBS = (4181672556, 7122091852, 778858448, 4419564314, 1707322602, 8339962878, 5294759526, 809625968)
_TYPED_SUM = 112737669133953592272111374183772859253192634861935651939615570225621429774704
_TYPED_DIGEST = "69124e3a62362512a7623652916ec81aedebd48e0fe20e4e32872197310d9d99"
_WATERMARK = 9223372036854775813


class _Crash(BaseException):
    """Stop after a durable write without running application failure handling."""


class _NoWriter:
    def write(self, *args, **kwargs):
        pytest.fail("source-free recovery must never launch a writer")


class _RecoveryStage:
    """Expose only metadata and fixed-width SQL evidence for an existing stage."""

    def __init__(self):
        self.owner = None
        self.watermark = _WATERMARK
        self.transaction = False
        self.content_reads = 0
        self.watermark_reads = 0

    bounded_query_timeout = staticmethod(lambda seconds: nullcontext())
    quote_identifier = staticmethod(lambda name: f"[{name}]")
    qualified_name = staticmethod(lambda schema, table, *, database: f"[{database}].[{schema}].[{table}]")

    def begin(self):
        assert not self.transaction
        self.transaction = True

    def commit_transaction(self):
        self.transaction = False

    rollback = commit_transaction

    def fetch_schema_columns(self, *args, **kwargs):
        return [
            SimpleNamespace(name=name, dtype=dtype, nullable=False)
            for name, dtype in (
                ("n", "bigint"),
                ("__dpone__native_row_hash", "binary(32)"),
                ("__dpone__mutation_version", "rowversion"),
            )
        ]

    def get_records(self, sql, params=()):
        if "sp_getapplock" in sql or "sp_releaseapplock" in sql:
            return [(0,)]
        if "extended_properties" in sql:
            return [(self.owner,)]
        if sql == "SELECT OBJECT_ID(?)":
            return [(11,)]
        if sql.startswith("SELECT TOP (1) 1"):
            assert self.transaction
            return [(1,)]
        if "COUNT_BIG" in sql:
            assert self.transaction, "content evidence requires the exact-stage barrier"
            watermark = self.watermark.to_bytes(8, "big")
            if "SUM(" in sql:
                self.content_reads += 1
                return [(2, 0, watermark, *_LIMBS)]
            self.watermark_reads += 1
            return [(2, watermark)]
        pytest.fail(f"unexpected recovery query: {sql}")

    def get_records_iterator(self, *args, **kwargs):
        pytest.fail("persisted-hash recovery must not read business rows")


@pytest.mark.parametrize("writer_boundary", ["positive_terminal", "lost_ack"])
@pytest.mark.parametrize("crash_after", ["QUIESCENT", "VERIFIED", "receipt"])
def test_persisted_recovery_reconstructs_receipt_and_rejects_later_mutation(
    tmp_path, monkeypatch, writer_boundary, crash_after
):
    """Dropping the layout, hash limbs, or watermark must break durable replay."""
    state_path = tmp_path / "state.sqlite"
    store = SQLiteWindowStore(state_path, clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire-v2")
    identity = NativeVerificationIdentityV2(
        plan,
        "mssql_sqlclient",
        NativeVerificationBackend.TARGET_LOCAL,
        "a" * 64,
        "b" * 64,
        "c" * 64,
        "mssql-native-sha256-sum-v1",
        "d" * 64,
        writer_proof_capability="sqlclient-session-applock-v1",
    )
    custody = NativeTargetCustody(store, "target")
    custody.claim(lease, identity.invocation_key)
    contract = build_mssql_bcp_native_contract(schema=[("n", "bigint")], query="SELECT n")
    file = encode_native_frame(contract, ({"n": 1}, {"n": 1}), tmp_path / "0.native", 0, 8, 16)
    assert (file.rows, file.encoded_bytes, file.typed_digest) == (2, 16, _TYPED_DIGEST)
    journal = NativeChunkJournalV2(store, lease, identity)
    journal.begin()
    journal.attempt(0, 0, file)
    attempt_id = journal.attempt_id(0, 0)
    target = _RecoveryStage()

    def importer_for(current_journal):
        def barrier(stage, grant, check):
            assert grant == "3" * 64
            return sqlclient_exact_stage_barrier(
                target, stage, grant_token_sha256=grant, timeout_seconds=3, assert_identity=check
            )

        return MssqlNativeChunkImporter(
            target,
            database="db",
            schema="dbo",
            columns=(SimpleNamespace(name="n", source_type="bigint", nullable=False),),
            encode_row=lambda _row: pytest.fail("recovery must not re-encode business rows"),
            assert_lease=current_journal.store.assert_lease,
            mutation_scope=lambda *args: pytest.fail("recovery must not open a writer mutation scope"),
            options_factory=lambda **kwargs: pytest.fail("recovery must not configure BCP"),
            target_digest_contract=contract,
            persisted_hash_layout=True,
            target_local_attempt=NativeTargetLocalAttempt(
                current_journal, custody, _NoWriter(), barrier, WindowOutcomeUnknown, verify_native_file
            ),
        )

    importer = importer_for(journal)
    target.owner = importer._ownership(plan, attempt_id)["binding"]
    stage = importer.qualified(importer.table_name(plan, attempt_id))
    journal.append_event(
        0,
        attempt_id,
        "STAGE_OWNED",
        stage_binding=dict(
            stage_id=journal.opaque_stage_id(stage),
            owner_binding_sha256=target.owner,
            object_id=11,
            schema_sha256=identity.capability_layout_sha256,
        ),
    )
    journal.append_event(
        0,
        attempt_id,
        "GRANTED",
        writer_binding=dict(
            import_backend=identity.import_backend,
            writer_proof_capability=identity.writer_proof_capability,
            protocol_sha256=identity.companion_protocol_sha256,
            package_sha256=identity.companion_package_sha256,
            capability_sha256=identity.capability_layout_sha256,
            grant_token_sha256="3" * 64,
            timeout_policy_sha256=identity.timeout_policy_sha256,
        ),
    )
    journal.append_event(0, attempt_id, "WRITING")
    positive = writer_boundary == "positive_terminal"
    journal.append_event(
        0,
        attempt_id,
        "WRITER_TERMINAL" if positive else "UNKNOWN",
        observation=NativeTargetLocalAttempt._observation(
            "success" if positive else "lost_ack", 2 if positive else None
        ),
    )

    reopened_store = SQLiteWindowStore(state_path, clock=lambda: 1.0)
    reopened = NativeChunkJournalV2(reopened_store, lease, identity)
    save = reopened_store.save

    def crash_after_durable_boundary(key, expected, payload, current_lease):
        record = save(key, expected, payload, current_lease)
        if key == reopened.key:
            projection = json.loads(payload)
            chunk = projection["chunks"]["0"]
            boundary = "receipt" if chunk["phase"] == "verified" else projection["events"][attempt_id][-1]["event"]
            if boundary == crash_after:
                raise _Crash(boundary)
        return record

    with monkeypatch.context() as faults:
        faults.setattr(reopened_store, "save", crash_after_durable_boundary)
        with pytest.raises(_Crash, match=crash_after):
            importer_for(reopened).recover_positive(plan, file, attempt_id, lease)

    recovered = NativeChunkJournalV2(SQLiteWindowStore(state_path, clock=lambda: 1.0), lease, identity)
    receipt = importer_for(recovered).recover_positive(plan, file, attempt_id, lease)
    durable = NativeChunkJournalV2(SQLiteWindowStore(state_path, clock=lambda: 1.0), lease, identity)
    rebuilt = validated_native_receipt(durable.data["chunks"]["0"]["receipt"])
    assert rebuilt == receipt
    assert (rebuilt.rows, rebuilt.encoded_bytes, rebuilt.typed_digest) == (2, 16, _TYPED_DIGEST)
    assert rebuilt.file_sha256 == "814dd7b9784d57c15b9c2972e9b4fd6cf7e164f8162a934bdb2452a413dab1f7"
    part = rebuilt.consumed_part_evidence
    assert part["native_typed_sum"] == _TYPED_SUM
    assert part["native_object_id"] == 11
    assert part["native_stage_layout"] == "mssql-native-persisted-hash-v2"
    assert part["native_mutation_watermark"] == _WATERMARK
    assert part["declared_rows"] == 2
    observed = durable.data["events"][attempt_id][-1]
    assert observed["event"] == "VERIFIED"
    assert observed["observation"]["row_count"] == 2
    assert observed["observation"]["limbs"] == [str(limb) for limb in _LIMBS]

    repeat = importer_for(durable)
    content_reads = target.content_reads
    assert repeat.inspect(plan, rebuilt, lease) == rebuilt
    target.watermark += 1
    with pytest.raises(ValueError, match="mssql_native.stage_mutation_detected"):
        repeat.inspect(plan, rebuilt, lease)
    assert target.watermark_reads == 2
    assert target.content_reads == content_reads
    assert NativeChunkJournalV2(store, lease, identity).data == durable.data
    custody.reassert_grant(lease, identity.invocation_key)
