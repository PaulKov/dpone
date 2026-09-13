"""Retention of the actual extraction original through executor cleanup."""

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.runtime.composition_transfer_payload import (
    CompositionTransferCaptureLifecycle,
    CompositionTransferPayloadStore,
)
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.support.bulk_text_codec import BulkTextCodec
from tests.test_mssql_composition_transaction_fence import binding


def payload(tmp_path, data=b"1\t10.25\n2\t\n"):
    path = tmp_path / "source.tsv"
    path.write_bytes(data)
    return ExtractResult(
        FileExportArtifact(
            str(path), ["id", "amount"], format="tsv", bulk_text_codec=BulkTextCodec(), rows_exported=data.count(b"\n")
        ),
        (("id", "int"), ("amount", "decimal(12,2)")),
    )


def store(tmp_path):
    root = tmp_path / "retained"
    root.mkdir(mode=0o700)
    return CompositionTransferPayloadStore(root)


def test_source_original_survives_cleanup_and_later_source_change(tmp_path):
    result = payload(tmp_path)
    retained = store(tmp_path)
    attempt = binding().attempt
    captured = CompositionTransferCaptureLifecycle(retained, attempt).capture(lambda: result)
    assert captured.artifact is result.artifact
    assert captured.extraction_receipt.complete
    result.artifact.cleanup()
    original = retained.read(attempt)
    assert original.artifact.require_integrity_receipt().rows_exported == 2
    assert original.evidence().actual_raw_rows == 2
    assert not (tmp_path / "source.tsv").exists()


def test_capture_replay_cannot_replace_first_original(tmp_path):
    result = payload(tmp_path)
    retained = store(tmp_path)
    attempt = binding().attempt
    retained.capture(attempt, result)
    with pytest.raises(FileExistsError):
        retained.capture(attempt, result)
    assert retained.read(attempt).evidence().actual_raw_rows == 2


def test_capture_and_read_reject_mutated_bytes(tmp_path):
    result = payload(tmp_path)
    retained = store(tmp_path)
    attempt = binding().attempt
    retained.capture(attempt, result)
    target = retained.root / attempt.attempt_sha256[7:] / "source.bin"
    target.chmod(0o600)
    target.write_bytes(b"9\t99.00\n")
    target.chmod(0o400)
    with pytest.raises(CompositionAdmissionError, match="transfer_payload_changed"):
        retained.read(attempt)


def test_source_retention_rejects_budget_before_creating_attempt(tmp_path):
    result = payload(tmp_path)
    retained = store(tmp_path)
    retained.max_bytes = 1
    with pytest.raises(CompositionAdmissionError, match="transfer_payload_budget"):
        retained.capture(binding().attempt, result)
    assert not list(retained.root.iterdir())


def test_symlink_payload_and_public_directory_are_rejected(tmp_path):
    root = tmp_path / "public"
    root.mkdir(mode=0o755)
    with pytest.raises(CompositionAdmissionError, match="transfer_payload_root"):
        CompositionTransferPayloadStore(root)
