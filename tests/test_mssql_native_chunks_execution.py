"""Real spawned encoders and bounded, independent fake target sessions."""

from contextlib import contextmanager
from dataclasses import replace
from threading import Barrier, Lock

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.contracts.bounded_window import WindowContractError, WindowTransientError
from dpone.contracts.mssql_native_chunks import NativeChunkLimits, NativeChunkPlan, NativeChunkReceipt
from dpone.runtime.mssql_native_chunks import BoundedNativeChunks, NativeReextractRequired
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract


class Target:
    def __init__(self, barrier=None, failures=0):
        self.barrier, self.failures = barrier, failures
        self.receipts, self.files, self.settled = {}, [], []
        self.lock = Lock()
        self.active, self.peak = 0, 0

    @contextmanager
    def factory(self):
        yield self

    def allocated_bytes(self):
        return 0

    def import_file(self, plan, file, attempt_id, lease):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.files.append(file.path.read_bytes())
            fail = self.failures > 0
            self.failures -= fail
        try:
            if fail:
                raise WindowTransientError("synthetic transient")
            if self.barrier:
                self.barrier.wait()
            receipt = NativeChunkReceipt(
                file.ordinal, attempt_id, attempt_id, file.rows, file.encoded_bytes, file.file_sha256, file.typed_digest
            )
            self.receipts[attempt_id] = receipt
            return receipt
        finally:
            with self.lock:
                self.active -= 1

    def inspect(self, plan, receipt, lease):
        return self.receipts.get(receipt.attempt_id)

    def settle(self, plan, attempt_id, lease):
        self.settled.append(attempt_id)
        self.receipts.pop(attempt_id, None)


def setup(tmp_path, target, **overrides):
    limits = NativeChunkLimits(
        max_total_encoded_bytes=10000,
        stage_allocated_bytes_stop_threshold=10000,
        max_rows=1,
        max_bytes=1024,
        max_row_bytes=256,
        parallelism=2,
        **overrides,
    )
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    executor = BoundedNativeChunks(
        store=store, importer_factory=target.factory, work_dir=tmp_path / "files", limits=limits
    )
    contract = build_mssql_bcp_native_contract(schema=[("value", "int")], query="SELECT synthetic")
    return executor, replace(plan, wire_fingerprint=contract.type_layout_hash), lease, contract


def test_spawned_encoding_and_imports_overlap_and_recover_without_source(tmp_path):
    target = Target(Barrier(2, timeout=20))
    executor, plan, lease, contract = setup(tmp_path, target)
    result = executor.stage(plan, iter([(7,), (7,)]), contract, lease)
    assert result.rows == 2
    assert target.peak == 2
    assert [r.ordinal for r in result.receipts] == [0, 1]
    assert result.receipts[0].typed_digest == result.receipts[1].typed_digest
    assert executor.recover(plan, lease) == result
    assert not list((tmp_path / "files").rglob("*.native"))
    assert {o["phase"] for o in result.observations} == {"encode", "import_verify"}
    import os

    assert all(o["worker"] != os.getpid() for o in result.observations if o["phase"] == "encode")


def test_import_retries_only_retained_identical_bytes(tmp_path):
    target = Target(failures=2)
    executor, plan, lease, contract = setup(tmp_path, target)
    result = executor.stage(plan, iter([(9,)]), contract, lease)
    assert result.receipts[0].attempt_id == "run-0-2"
    assert target.files[0] == target.files[1] == target.files[2]
    assert target.settled == ["run-0-0", "run-0-1"]


@pytest.mark.parametrize("rows", [[(9,), (9,)], [{"value": 9}, {"value": 9}]])
def test_scheduler_reuses_frame_size_with_real_spawned_workers(tmp_path, monkeypatch, rows):
    from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder

    sizes = []
    original = MssqlNativeEncoder.encoded_row_size

    def measured_size(self, row):
        size = original(self, row)
        sizes.append(size)
        return size

    monkeypatch.setattr(MssqlNativeEncoder, "encoded_row_size", measured_size)
    target = Target()
    executor, plan, lease, contract = setup(tmp_path, target)
    result = executor.stage(plan, iter(rows), contract, lease)
    # Spawned encoders validate their own values. The parent sizes each frame
    # once; the scheduler must reuse that reservation rather than walk it again.
    assert len(sizes) == len(rows)
    assert sum(sizes) == sum(receipt.encoded_bytes for receipt in result.receipts)
    assert sum(map(len, target.files)) == sum(sizes)


@pytest.mark.parametrize("delta,code", [(1, "encoder_size_authority_changed"), (-1, "chunk_bytes_exceeded")])
def test_frame_reservation_cannot_override_actual_encoded_bytes(tmp_path, monkeypatch, delta, code):
    import dpone.runtime.mssql_native_chunks as chunks

    original = chunks.sized_native_frames

    def corrupted(*args, **kwargs):
        for frame in original(*args, **kwargs):
            yield replace(frame, encoded_bytes=frame.encoded_bytes + delta)

    monkeypatch.setattr(chunks, "sized_native_frames", corrupted)
    target = Target()
    executor, plan, lease, contract = setup(tmp_path, target)
    with pytest.raises(WindowContractError, match=code):
        executor.stage(plan, iter([(7,)]), contract, lease)
    assert not target.files
    assert NativeChunkJournal(executor.store, lease, plan).completed() is None


@pytest.mark.parametrize("mapping", [False, True])
@pytest.mark.parametrize("view", [False, True])
def test_spawned_workers_keep_values_from_reused_driver_buffers(tmp_path, mapping, view):
    from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder

    target = Target()
    executor, plan, lease, _ = setup(tmp_path, target)
    contract = build_mssql_bcp_native_contract(schema=[("value", "varbinary(8)")], query="SELECT synthetic")
    plan = replace(plan, wire_fingerprint=contract.type_layout_hash)
    executor.limits = replace(executor.limits, max_bytes=4096)
    buffer = bytearray(b"a")
    container = (
        {"value": memoryview(buffer) if view else buffer} if mapping else [memoryview(buffer) if view else buffer]
    )

    def rows():
        for content in (b"a", b"b", b"c"):
            buffer[:] = content
            yield container
        buffer[:] = b"z"

    result = executor.stage(plan, rows(), contract, lease)
    encoder = MssqlNativeEncoder(contract, max_row_bytes=executor.limits.max_row_bytes)
    expected = [encoder.encode_row((content,)) for content in (b"a", b"b", b"c")]
    assert sorted(target.files) == sorted(expected)
    assert result.rows == 3
    assert sum(receipt.encoded_bytes for receipt in result.receipts) == sum(map(len, expected))


def test_retry_exhaustion_requires_reextraction(tmp_path):
    target = Target(failures=3)
    executor, plan, lease, contract = setup(tmp_path, target)
    with pytest.raises(WindowTransientError):
        executor.stage(plan, iter([(9,)]), contract, lease)
    with pytest.raises(NativeReextractRequired):
        executor.recover(plan, lease)
    assert target.settled[-3:] == ["run-0-0", "run-0-1", "run-0-2"]


def test_empty_query_has_verified_zero_row_authority(tmp_path):
    executor, plan, lease, contract = setup(tmp_path, Target())
    result = executor.stage(plan, iter(()), contract, lease)
    assert result.rows == 0
    assert len(result.receipts) == 1
    assert result.receipts[0].encoded_bytes == 0


def test_total_cap_rejects_before_import_and_closes_source(tmp_path):
    target = Target()
    executor, plan, lease, contract = setup(tmp_path, target)
    executor.limits = replace(executor.limits, max_total_encoded_bytes=1)
    closed = []

    def source():
        try:
            yield (1,)
        finally:
            closed.append(True)

    with pytest.raises(WindowContractError, match="total_encoded_bytes"):
        executor.stage(plan, source(), contract, lease)
    assert closed == [True]
    assert not target.files
    assert NativeChunkJournal(executor.store, lease, plan).completed() is None


def test_complete_stage_tampering_fails_closed(tmp_path):
    target = Target()
    executor, plan, lease, contract = setup(tmp_path, target)
    result = executor.stage(plan, iter([(1,)]), contract, lease)
    target.receipts[result.receipts[0].attempt_id] = replace(result.receipts[0], rows=8)
    with pytest.raises(WindowContractError, match="recovered_stage_changed"):
        executor.recover(plan, lease)


def test_backpressure_and_cancellation_settle_workers_and_close_source(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    entered = Barrier(3, timeout=20)
    release, prefetched, cancelled = Event(), Event(), Event()

    class BlockingTarget(Target):
        entries = 0

        def import_file(self, plan, file, attempt_id, lease):
            with self.lock:
                self.entries += 1
                first_pair = self.entries <= 2
            if first_pair:
                entered.wait()
            assert release.wait(20)
            return super().import_file(plan, file, attempt_id, lease)

    target = BlockingTarget()
    executor, plan, lease, contract = setup(tmp_path, target)
    pulled, closed = [], []

    def source():
        try:
            for number in range(100):
                pulled.append(number)
                if len(pulled) == 5:
                    prefetched.set()
                yield (number,)
        finally:
            closed.append(True)

    with ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(executor.stage, plan, source(), contract, lease, cancelled)
        entered.wait()
        assert prefetched.wait(20)
        assert len(pulled) == 5  # workers2 + pending2 + producer lookahead1
        cancelled.set()
        release.set()
        with pytest.raises(WindowContractError, match="cancelled"):
            result.result(timeout=20)
    assert closed == [True]
    assert target.active == 0
    assert NativeChunkJournal(executor.store, lease, plan).completed() is None


def test_eof_metadata_captured_once_and_persisted_with_stage_complete(tmp_path):
    executor, plan, lease, contract = setup(tmp_path, Target())
    eof, observed = [], []

    def source():
        yield (1,)
        eof.append(True)

    def metadata():
        observed.append(eof == [True])
        return {"lifecycle": "complete", "schema": "synthetic"}

    executor.stage(plan, source(), contract, lease, completion_metadata=metadata)
    assert observed == [True]
    assert NativeChunkJournal(executor.store, lease, plan).completed_metadata()["schema"] == "synthetic"


def test_preparation_intent_recovery_and_unknown_publication_do_not_discard(tmp_path):
    from dpone.contracts.bounded_window import WindowOutcomeUnknown

    target = Target()
    executor, plan, lease, contract = setup(tmp_path, target)
    executor.stage(plan, iter([(1,)]), contract, lease)
    journal = NativeChunkJournal(executor.store, lease, plan)
    journal.publication.preparation_started({"stage": "prepared"})
    journal.publication.preparation_started({"stage": "prepared"})
    assert executor.recover(plan, lease).rows == 1
    journal.publication.prepared({"stage": "prepared", "proof": "synthetic"})
    journal.publication.publication_started({"stage": "prepared", "proof": "synthetic"})
    with pytest.raises(WindowOutcomeUnknown):
        executor.recover(plan, lease)
    assert target.settled == []


@pytest.mark.parametrize("limit", ["max_staging_tables", "stage_allocated_bytes_stop_threshold"])
def test_capacity_failure_never_creates_stage_complete(tmp_path, limit):
    target = Target()
    executor, plan, lease, contract = setup(tmp_path, target)
    if limit == "max_staging_tables":
        executor.limits = replace(executor.limits, max_staging_tables=1)
    else:
        target.allocated_bytes = lambda: 10001
    with pytest.raises(WindowContractError):
        executor.stage(plan, iter([(1,)]), contract, lease)
    assert target.files == []
    assert NativeChunkJournal(executor.store, lease, plan).completed() is None


def test_observed_phase_metrics_include_retries_and_never_requested_workers(tmp_path):
    from dpone.runtime.mssql_native_chunks_observations import summarize_native_phases

    target = Target(failures=1)
    executor, plan, lease, contract = setup(tmp_path, target)
    result = executor.stage(plan, iter([(1,)]), contract, lease)
    phases = summarize_native_phases(result.observations)
    assert phases["encode"]["operations"] == 1
    assert phases["import_verify"]["operations"] == 2
    assert phases["import_verify"]["failed_operations"] == 1
    assert phases["import_verify"]["peak_workers"] == 1
    assert summarize_native_phases(())["encode"]["effective_parallelism"] is None


def test_wire_and_limit_identity_fail_before_source_or_recovery_io(tmp_path):
    target = Target()
    executor, plan, lease, contract = setup(tmp_path, target)
    with pytest.raises(WindowContractError, match="wire_identity"):
        executor.stage(replace(plan, wire_fingerprint="changed"), iter([(1,)]), contract, lease)
    assert target.files == []
    executor.stage(plan, iter([(1,)]), contract, lease)
    executor.limits = replace(executor.limits, max_rows=2)
    with pytest.raises(WindowContractError, match="resource_limits_changed"):
        executor.recover(plan, lease)


@pytest.mark.parametrize("workers", [1, 2])
def test_allocation_overshoot_is_durable_without_stage_complete(tmp_path, workers):
    class GrowingTarget(Target):
        def allocated_bytes(self):
            return 20000 if self.receipts else 0

    executor, plan, lease, contract = setup(tmp_path, GrowingTarget(Barrier(workers, timeout=20)))
    with pytest.raises(WindowContractError, match="allocation_threshold"):
        executor.stage(plan, iter([(1,)] * workers), contract, lease)
    journal = NativeChunkJournal(executor.store, lease, plan)
    assert journal.completed() is None
    failed = [item for item in journal.data["observations"] if item.get("outcome") == "failed"]
    assert len(failed) == workers
    assert failed[0]["allocated_before"] == 0
    assert failed[0]["allocated_after"] == 20000
