"""Public pathname and pinned-descriptor verification retain proof under budgets."""

import os
from dataclasses import asdict
from time import monotonic

import pytest

from dpone.runtime.artifact_integrity import FileArtifactReceipt, FileWireContract
from dpone.runtime.file_artifact_authority import FileVerificationBudget, FileVerificationLimitError


def wire():
    return FileWireContract.resolve(
        columns=("v",), format="csv", compressed=False, has_header=False, bulk_text_codec=None
    )


class TickClock:
    def __init__(self):
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return float(self.calls)


def test_bounded_path_verification_preserves_receipt_and_exact_limit(tmp_path):
    path = tmp_path / "source"
    path.write_bytes(b"abc")
    receipt = FileArtifactReceipt.capture(str(path), wire_contract=wire(), rows_exported=1)
    before = asdict(receipt)
    receipt.verify(
        str(path), wire_contract=wire(), verification_budget=FileVerificationBudget(monotonic, monotonic() + 10, 3)
    )
    assert asdict(receipt) == before
    with pytest.raises(FileVerificationLimitError):
        receipt.verify(
            str(path), wire_contract=wire(), verification_budget=FileVerificationBudget(monotonic, monotonic() + 10, 2)
        )


def test_deadline_stops_repeated_path_scan_between_chunks(tmp_path):
    path = tmp_path / "source"
    path.write_bytes(b"x" * (8 * 1024 * 1024))
    receipt = FileArtifactReceipt.capture(str(path), wire_contract=wire(), rows_exported=1)
    clock = TickClock()
    with pytest.raises(TimeoutError):
        receipt.verify(
            str(path), wire_contract=wire(), verification_budget=FileVerificationBudget(clock, 8.0, path.stat().st_size)
        )
    assert clock.calls == 8


def test_descriptor_budget_preserves_shared_position(tmp_path):
    path = tmp_path / "source"
    path.write_bytes(b"x" * (4 * 1024 * 1024))
    receipt = FileArtifactReceipt.capture(str(path), wire_contract=wire(), rows_exported=1)
    with path.open("rb") as handle:
        handle.seek(17)
        clock = TickClock()
        with pytest.raises(TimeoutError):
            receipt.verify_descriptor(
                handle.fileno(),
                wire_contract=wire(),
                verification_budget=FileVerificationBudget(clock, 5.0, path.stat().st_size),
            )
        assert os.lseek(handle.fileno(), 0, os.SEEK_CUR) == 17
        receipt.verify_descriptor(
            handle.fileno(),
            wire_contract=wire(),
            verification_budget=FileVerificationBudget(monotonic, monotonic() + 10, path.stat().st_size),
        )
        assert os.lseek(handle.fileno(), 0, os.SEEK_CUR) == 17


@pytest.mark.parametrize("consumer_kind", ["artifact", "pinned"])
@pytest.mark.parametrize("supported", [False, True])
def test_authority_budget_admission_preserves_original_errors(tmp_path, consumer_kind, supported):
    from dpone.runtime.artifact_integrity import ArtifactIntegrityError
    from dpone.runtime.file_artifacts import FileExportArtifact
    from dpone.runtime.pinned_file_consumer import PinnedFileConsumer

    path = tmp_path / "source"
    path.write_bytes(b"abc")
    sentinel = TypeError("synthetic body failure")
    calls = []

    class Legacy:
        def capture_integrity_receipt(self, wire_contract, rows_exported):
            return FileArtifactReceipt.capture(str(path), wire_contract=wire_contract, rows_exported=rows_exported)

        def verify_integrity_receipt(self, receipt, wire_contract):
            calls.append("legacy")
            receipt.verify(str(path), wire_contract=wire_contract)

        verify = verify_integrity_receipt

    class Bounded(Legacy):
        def verify_integrity_receipt(self, receipt, wire_contract, *, verification_budget=None):
            if verification_budget is not None:
                calls.append("bounded")
                raise sentinel
            super().verify_integrity_receipt(receipt, wire_contract)

        verify = verify_integrity_receipt

    authority = Bounded() if supported else Legacy()
    receipt = FileArtifactReceipt.capture(str(path), wire_contract=wire(), rows_exported=1)
    if consumer_kind == "artifact":
        consumer = FileExportArtifact(str(path), ["v"], rows_exported=1, _integrity_authority=authority)

        def verify(budget=None):
            return consumer.require_integrity_receipt(verification_budget=budget)
    else:
        consumer = PinnedFileConsumer(
            source_path=str(path),
            consumer_path=str(path),
            inherited_file_descriptors=(),
            prepare_callback=lambda: None,
            cleanup_callback=lambda: None,
            release_callback=lambda: None,
            integrity_authority=authority,
        )

        def verify(budget=None):
            return consumer.verify_integrity_receipt(receipt, wire_contract=wire(), verification_budget=budget)

    verify()
    assert calls == ["legacy"]
    with pytest.raises((TypeError, ArtifactIntegrityError)) as error:
        verify(FileVerificationBudget(monotonic, monotonic() + 10, 3))
    if supported:
        assert error.value is sentinel and calls == ["legacy", "bounded"]
    else:
        assert error.value.code == "artifact_integrity.verification_budget_unsupported"
        assert calls == ["legacy"]
    verify()


def test_pinned_lock_wait_is_bounded_and_only_acquired_lock_is_released(tmp_path):
    from threading import Event, Thread

    from dpone.runtime.pinned_file_consumer import PinnedFileConsumer

    path = tmp_path / "source"
    path.write_bytes(b"abc")
    receipt = FileArtifactReceipt.capture(str(path), wire_contract=wire(), rows_exported=1)
    entered, release = Event(), Event()

    def prepare():
        entered.set()
        assert release.wait(5)

    consumer = PinnedFileConsumer(
        source_path=str(path),
        consumer_path=str(path),
        inherited_file_descriptors=(),
        prepare_callback=prepare,
        cleanup_callback=lambda: None,
        release_callback=lambda: None,
    )
    worker = Thread(target=consumer.prepare_process_input, args=(str(path),))
    worker.start()
    try:
        assert entered.wait(5)
        with pytest.raises(TimeoutError):
            consumer.verify_integrity_receipt(
                receipt,
                wire_contract=wire(),
                verification_budget=FileVerificationBudget(monotonic, monotonic() + 0.02, 3),
            )
    finally:
        release.set()
        worker.join(timeout=5)
    assert not worker.is_alive()
    consumer.verify_integrity_receipt(
        receipt, wire_contract=wire(), verification_budget=FileVerificationBudget(monotonic, monotonic() + 5, 3)
    )
    consumer.close()


def test_deadline_observes_partial_actual_bytes_and_repeated_scans_share_time(tmp_path):
    from dpone.runtime.file_artifact_authority import FileVerificationTimeout

    path = tmp_path / "source"
    path.write_bytes(b"x" * (8 * 1024 * 1024))
    receipt = FileArtifactReceipt.capture(str(path), wire_contract=wire(), rows_exported=1)
    observations = []
    clock = TickClock()

    class ObservedBudget(FileVerificationBudget):
        def check(self, observed_bytes=0):
            observations.append(observed_bytes)
            super().check(observed_bytes)

    budget = ObservedBudget(clock, 8, path.stat().st_size)
    with pytest.raises(FileVerificationTimeout):
        receipt.verify(str(path), wire_contract=wire(), verification_budget=budget)
    # Initial stat admission includes full size; streaming observations prove partial progress.
    assert 1024 * 1024 in observations
    assert 2 * 1024 * 1024 in observations
    assert 3 * 1024 * 1024 not in observations
    before = len(observations)
    with pytest.raises(FileVerificationTimeout):
        receipt.verify(str(path), wire_contract=wire(), verification_budget=budget)
    assert observations[before:] == [0]
