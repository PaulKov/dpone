"""Contracts for top-level source payload ownership and wrapper propagation."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from dpone.contracts.mssql_transaction_governance import MssqlCleanupDisposition
from dpone.runtime.artifact_models import BaseExtractionArtifact
from dpone.runtime.etl.contract_artifacts import ContractEnforcedStreamingArtifact
from dpone.runtime.etl.owned_payload_scope import OwnedPayloadScope
from dpone.runtime.extraction_lifecycle import (
    ArtifactTerminalOutcome,
    ExtractionLifecycleAuthority,
)
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.streaming_rows import StreamingRowsArtifact


def _completed_lifecycle(token: str = "snapshot-1") -> ExtractionLifecycleAuthority:
    authority = ExtractionLifecycleAuthority()
    authority.acquire_snapshot(
        snapshot_authority="postgresql.repeatable_read",
        source_token=token,
        acquired_at=datetime(2026, 8, 15, 10, 0, tzinfo=UTC),
    )
    authority.complete(completed_at=datetime(2026, 8, 15, 10, 1, tzinfo=UTC))
    return authority


def test_payload_rebind_registers_derived_file_and_retain_is_irreversible(tmp_path: Path) -> None:
    lifecycle = _completed_lifecycle()
    root_path = tmp_path / "root.tsv"
    derived_path = tmp_path / "derived.tsv"
    root_path.write_bytes(b"1\n")
    derived_path.write_bytes(b"1\tmeta\n")
    root = FileExportArtifact(str(root_path), ("id",), rows_exported=1)
    extract = ExtractResult(
        artifact=root,
        schema=(("id", "int"),),
        extraction_lifecycle=lifecycle,
    )
    scope = OwnedPayloadScope.from_extract_result(extract)
    payload = LoadPayload(
        artifact=root,
        schema=extract.schema,
        extraction_lifecycle=lifecycle,
        owned_payload_scope=scope,
    )
    derived = FileExportArtifact(
        str(derived_path),
        ("id", "meta"),
        rows_exported=1,
    )

    rebound = payload.rebind(
        artifact=derived,
        schema=(("id", "int"), ("meta", "varchar(20)")),
    )
    receipt = scope.retain_commit_unknown()
    root.cleanup()
    derived.cleanup()

    assert rebound.require_completed_extraction() is lifecycle.require_completed()
    assert derived.extraction_lifecycle is lifecycle
    assert receipt.outcome is ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN
    assert root.terminal_receipt is not None
    assert derived.terminal_receipt is not None
    assert root_path.exists()
    assert derived_path.exists()


def test_payload_registry_releases_root_and_derived_files_on_success(tmp_path: Path) -> None:
    lifecycle = _completed_lifecycle()
    paths = (tmp_path / "root.tsv", tmp_path / "derived.tsv")
    for path in paths:
        path.write_bytes(b"1\n")
    root = FileExportArtifact(str(paths[0]), ("id",), rows_exported=1)
    extract = ExtractResult(root, (("id", "int"),), extraction_lifecycle=lifecycle)
    scope = OwnedPayloadScope.from_extract_result(extract)
    payload = LoadPayload(
        root,
        extract.schema,
        extraction_lifecycle=lifecycle,
        owned_payload_scope=scope,
    )
    derived = FileExportArtifact(str(paths[1]), ("id",), rows_exported=1)
    payload.rebind(artifact=derived)

    first = scope.success()
    second = scope.abort()

    assert second is first
    assert first.outcome is ArtifactTerminalOutcome.SUCCESS
    assert all(not path.exists() for path in paths)


@pytest.mark.parametrize(
    ("outcome", "expected_callback"),
    [
        (ArtifactTerminalOutcome.SUCCESS, "commit"),
        (ArtifactTerminalOutcome.ABORT, "rollback"),
        (ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN, "rollback"),
    ],
)
def test_contract_wrapper_forwards_exact_terminal_outcome_once(
    outcome: ArtifactTerminalOutcome,
    expected_callback: str,
) -> None:
    calls: list[str] = []
    lifecycle = _completed_lifecycle()
    stream = StreamingRowsArtifact(
        iter(()),
        extraction_lifecycle=lifecycle,
        on_success=lambda: calls.append("commit"),
        on_abort=lambda: calls.append("rollback"),
    )
    extract = ExtractResult(stream, (("id", "int"),), extraction_lifecycle=lifecycle)
    scope = OwnedPayloadScope.from_extract_result(extract)
    payload = LoadPayload(
        stream,
        extract.schema,
        extraction_lifecycle=lifecycle,
        owned_payload_scope=scope,
    )
    wrapper = ContractEnforcedStreamingArtifact(
        stream,
        contract=object(),
        run_id="run-1",
        load_id="load-1",
    )
    payload.rebind(artifact=wrapper)

    receipt = scope.terminate(outcome)
    wrapper.cleanup()
    stream.cleanup()

    assert calls == [expected_callback]
    assert receipt.outcome is outcome
    assert wrapper.terminal_receipt is stream.terminal_receipt


def test_payload_rebind_rejects_a_different_extraction_authority() -> None:
    lifecycle = _completed_lifecycle("snapshot-1")
    other = _completed_lifecycle("snapshot-2")
    root = StreamingRowsArtifact(iter(()), extraction_lifecycle=lifecycle)
    extract = ExtractResult(root, (("id", "int"),), extraction_lifecycle=lifecycle)
    scope = OwnedPayloadScope.from_extract_result(extract)
    payload = LoadPayload(
        root,
        extract.schema,
        extraction_lifecycle=lifecycle,
        owned_payload_scope=scope,
    )
    replacement = StreamingRowsArtifact(iter(()), extraction_lifecycle=other)

    with pytest.raises(ValueError, match="extraction_lifecycle_changed"):
        payload.rebind(artifact=replacement)


def test_typed_commit_unknown_retains_file_evidence(tmp_path: Path) -> None:
    class _CommitUnknown(RuntimeError):
        cleanup_disposition = MssqlCleanupDisposition.PRESERVE_STAGING_EVIDENCE

    path = tmp_path / "evidence.tsv"
    path.write_bytes(b"1\n")
    artifact = FileExportArtifact(str(path), ("id",), rows_exported=1)
    scope = OwnedPayloadScope.from_extract_result(ExtractResult(artifact, (("id", "int"),)))

    receipt = scope.terminate_for_error(_CommitUnknown("ack lost"))

    assert receipt.outcome is ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN
    assert path.exists()


def test_scope_receipts_cleanup_failure_without_raising() -> None:
    class _BrokenArtifact(BaseExtractionArtifact):
        def cleanup(self) -> None:
            raise OSError("release denied")

    scope = OwnedPayloadScope.from_extract_result(ExtractResult(_BrokenArtifact(), (("id", "int"),)))

    receipt = scope.abort()

    assert receipt.cleanup_succeeded is False
    assert receipt.cleanup_error_code == ("dpone.runtime.etl.owned_payload_scope.OwnedPayloadReleaseError")
