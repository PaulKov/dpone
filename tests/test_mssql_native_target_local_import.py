"""Target-local attempt events bind real stage, writer, and aggregate boundaries."""

from contextlib import nullcontext
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal_v2 import NativeChunkJournalV2
from dpone.adapters.mssql_native_custody import NativeTargetCustody
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeChunkPlan, NativeChunkReceipt
from dpone.contracts.mssql_native_verification import NativeVerificationBackend, NativeVerificationIdentityV2
from dpone.contracts.mssql_native_writer import NativeStageWriteOutcome
from dpone.runtime.sinks.mssql_native_target_digest import TargetDigest
from dpone.runtime.sinks.mssql_native_target_local_import import NativeTargetLocalAttempt


@pytest.mark.parametrize("barrier_fails", [False, True])
def test_positive_attempt_records_boundary_order_and_one_aggregate(tmp_path: Path, barrier_fails: bool) -> None:
    store = SQLiteWindowStore(tmp_path / "state.sqlite", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    identity = NativeVerificationIdentityV2(
        plan,
        "bcp",
        NativeVerificationBackend.TARGET_LOCAL,
        "a" * 64,
        "b" * 64,
        "c" * 64,
        "mssql-native-sha256-sum-v1",
        "d" * 64,
    )
    custody = NativeTargetCustody(store, "target")
    custody.claim(lease, identity.invocation_key)
    journal = NativeChunkJournalV2(store, lease, identity)
    journal.begin()
    path = tmp_path / "sealed.native"
    path.write_bytes(b"sealed")
    file = EncodedNativeFile(path, 0, 1, 6, sha256(b"sealed").hexdigest(), "f" * 64)
    journal.attempt(0, 0, file)
    attempt_id = journal.attempt_id(0, 0)
    calls = []

    class Importer:
        _mutation_scope = staticmethod(lambda *args: nullcontext())
        _assert_lease = staticmethod(lambda lease: None)

        def table_name(self, plan, attempt):
            return "owned"

        def qualified(self, table):
            return "[db].[dbo].[owned]"

        def _create_owned_stage(self, plan, attempt, table):
            calls.append("stage-created")

        def _object_id(self, table):
            return 11

        def _ownership(self, plan, attempt):
            return {"binding": "1" * 64}

        def _assert_stage_identity(self, plan, attempt, table, object_id):
            calls.append("stage-identity")

        def _target_digest(self, table, file):
            calls.append("aggregate")
            return TargetDigest(1, file.typed_digest, 7), (1, 0, *(["7"] + ["0"] * 7))

        def _receipt(self, plan, file, attempt, object_id, typed_sum, artifact):
            return NativeChunkReceipt(0, attempt, self.qualified("owned"), 1, 6, file.file_sha256, file.typed_digest)

    class Writer:
        def write(self, grant, *, rejects_path):
            calls.append("launch")
            return NativeStageWriteOutcome(grant.attempt_id, True, 1, "success")

    def barrier(stage, assert_identity):
        calls.append("barrier")
        if barrier_fails:
            raise TimeoutError("stage barrier unavailable")
        assert_identity()
        return nullcontext()

    attempt = NativeTargetLocalAttempt(journal, custody, Writer(), barrier, WindowOutcomeUnknown)
    if barrier_fails:
        with pytest.raises(TimeoutError, match="barrier"):
            attempt.import_file(Importer(), plan, file, attempt_id, lease, SimpleNamespace())
        assert [event["event"] for event in journal.data["events"][attempt_id]][-2:] == [
            "WRITER_TERMINAL",
            "UNKNOWN",
        ]
        assert "aggregate" not in calls
        return
    receipt = attempt.import_file(Importer(), plan, file, attempt_id, lease, SimpleNamespace())
    assert receipt.attempt_id == attempt_id
    assert [event["event"] for event in journal.data["events"][attempt_id]] == [
        "INTENT",
        "STAGE_OWNED",
        "GRANTED",
        "WRITING",
        "WRITER_TERMINAL",
        "QUIESCENT",
        "VERIFIED",
    ]
    assert calls.index("stage-created") < calls.index("launch") < calls.index("barrier") < calls.index("aggregate")
    assert calls.count("launch") == 1 and calls.count("aggregate") == 1


def test_published_cleanup_requires_durable_authority_and_exact_stage_identity():
    receipt = SimpleNamespace(
        attempt_id="a" * 64, stage_id="[db].[dbo].[owned]", consumed_part_evidence={"native_object_id": 11}
    )
    state = {"phase": "prepared"}
    calls = []

    class Journal:
        publication = SimpleNamespace(state=lambda: state)

        @staticmethod
        def completed():
            return SimpleNamespace(receipts=(receipt,))

    class Importer:
        _mutation_scope = staticmethod(lambda *args: nullcontext())
        _assert_lease = staticmethod(lambda lease: calls.append("lease"))

        @staticmethod
        def table_name(plan, attempt):
            return "owned"

        @staticmethod
        def qualified(table):
            return "[db].[dbo].[owned]"

        @staticmethod
        def _assert_stage_identity(plan, attempt, table, object_id):
            calls.append("identity")

        class Connector:
            @staticmethod
            def execute_query(sql):
                calls.append("drop")

        connector = Connector()

    attempt = NativeTargetLocalAttempt(Journal(), None, None, None, WindowOutcomeUnknown)
    with pytest.raises(ValueError, match="publication_required"):
        attempt.settle_published(Importer(), None, receipt, None)
    assert "drop" not in calls
    state["phase"] = "succeeded"
    attempt.settle_published(Importer(), None, receipt, None)
    assert calls == ["lease", "identity", "drop", "lease"]
    calls.clear()
    attempt.drop_exact_owned(Importer(), None, receipt, None)
    assert calls == ["lease", "identity", "drop", "lease"]


def test_positive_terminal_recovery_reobserves_without_writer_launch(tmp_path):
    store = SQLiteWindowStore(tmp_path / "state.sqlite", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    identity = NativeVerificationIdentityV2(
        plan,
        "bcp",
        NativeVerificationBackend.TARGET_LOCAL,
        "a" * 64,
        "b" * 64,
        "c" * 64,
        "mssql-native-sha256-sum-v1",
        "d" * 64,
    )
    journal = NativeChunkJournalV2(store, lease, identity)
    journal.begin()
    path = tmp_path / "0.native"
    path.write_bytes(b"sealed")
    file = EncodedNativeFile(path, 0, 1, 6, sha256(b"sealed").hexdigest(), "f" * 64)
    journal.attempt(0, 0, file)
    attempt_id = journal.attempt_id(0, 0)
    stage = dict(
        stage_id=journal.opaque_stage_id("[db].[dbo].[owned]"),
        owner_binding_sha256="1" * 64,
        object_id=11,
        schema_sha256=identity.capability_layout_sha256,
    )
    writer = dict(
        import_backend="bcp",
        writer_proof_capability=identity.writer_proof_capability,
        protocol_sha256=identity.companion_protocol_sha256,
        package_sha256=identity.companion_package_sha256,
        capability_sha256=identity.capability_layout_sha256,
        grant_token_sha256="3" * 64,
        timeout_policy_sha256=identity.timeout_policy_sha256,
    )
    journal.append_event(0, attempt_id, "STAGE_OWNED", stage_binding=stage)
    journal.append_event(0, attempt_id, "GRANTED", writer_binding=writer)
    journal.append_event(0, attempt_id, "WRITING")
    journal.append_event(
        0,
        attempt_id,
        "WRITER_TERMINAL",
        observation=NativeTargetLocalAttempt._observation("success", 1),
    )
    journal.append_event(
        0,
        attempt_id,
        "UNKNOWN",
        observation=NativeTargetLocalAttempt._observation("success", 1, quiescence="failed"),
    )
    calls = []

    class Importer:
        _assert_lease = staticmethod(lambda lease: None)
        columns = (SimpleNamespace(name="n"),)

        @staticmethod
        def table_name(plan, attempt):
            return "owned"

        @staticmethod
        def qualified(table):
            return "[db].[dbo].[owned]"

        @staticmethod
        def _assert_stage_identity(plan, attempt, table, object_id):
            calls.append("identity")

        @staticmethod
        def _target_digest(table, file):
            calls.append("aggregate")
            return TargetDigest(1, file.typed_digest, 7), (1, 0, *("7", *("0",) * 7))

        @staticmethod
        def _receipt(plan, file, attempt, object_id, typed_sum, artifact):
            return NativeChunkReceipt(0, attempt, "[db].[dbo].[owned]", 1, 6, file.file_sha256, file.typed_digest)

    class NoWriter:
        def write(self, *args, **kwargs):
            pytest.fail("recovery must never launch a writer")

    def barrier(stage, assert_identity):
        calls.append("barrier")
        assert_identity()
        return nullcontext()

    recovery = NativeTargetLocalAttempt(journal, None, NoWriter(), barrier, WindowOutcomeUnknown)
    receipt = recovery.recover_positive(Importer(), plan, file, attempt_id, lease)
    assert receipt.attempt_id == attempt_id
    assert journal.data["events"][attempt_id][-1]["event"] == "VERIFIED"
    assert calls == ["barrier", "identity", "aggregate"]
