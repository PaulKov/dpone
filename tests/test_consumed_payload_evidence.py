from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier, Event
from unittest.mock import patch

import pytest

from dpone.runtime.artifact_integrity import ArtifactIntegrityError
from dpone.runtime.consumed_payload_evidence import (
    ConsumedPayloadEvidence,
    canonical_native_contract_sha256,
    canonical_source_provenance_sha256,
)
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.support.bulk_text_codec import BulkTextCodec
from dpone.type_system.source_sink.provenance import SourceColumnProvenance, SourceRelationDialect


def _artifact(tmp_path, name: str, payload: bytes) -> FileExportArtifact:
    path = tmp_path / name
    path.write_bytes(payload)
    return FileExportArtifact(
        str(path),
        ["id", "value"],
        format="mssql-delimited",
        bulk_text_codec=BulkTextCodec(),
        rows_exported=payload.count(b"\n"),
    )


def _provenance() -> str:
    return canonical_source_provenance_sha256(
        relation_dialect=SourceRelationDialect.POSTGRES,
        relation_schema=(("id", "integer"), ("value", "text")),
        relation_metadata=(
            SourceColumnProvenance("id", "integer", False),
            SourceColumnProvenance("value", "text", True),
        ),
        fallback_schema=(("id", "int"), ("value", "nvarchar(max)")),
    )


def _native_contract(evidence: ConsumedPayloadEvidence, *, target_type: str = "nvarchar(max)") -> str:
    return canonical_native_contract_sha256(
        (
            {
                "wire_name": "id",
                "target_name": "id",
                "target_type": "int",
                "nullable": False,
                "collation": None,
            },
            {
                "wire_name": "value",
                "target_name": "value",
                "target_type": target_type,
                "nullable": True,
                "collation": "Latin1_General_100_BIN2",
            },
        ),
        source_wire_contract_sha256s=tuple(part.wire_contract_sha256 for part in evidence.parts),
    )


def test_consumed_payload_manifest_is_ordered_and_binds_native_contract(tmp_path) -> None:
    second = _artifact(tmp_path, "second.bcp", b"2\tbeta\n")
    first = _artifact(tmp_path, "first.bcp", b"1\talpha\n")
    schema = (("id", "int"), ("value", "nvarchar(max)"))
    evidence = ConsumedPayloadEvidence.empty().append_verified_file(
        second,
        validated_schema=schema,
        source_provenance_sha256=_provenance(),
        actual_raw_rows=1,
        order_key="partition:0002",
    )
    evidence = evidence.append_verified_file(
        first,
        validated_schema=schema,
        source_provenance_sha256=_provenance(),
        actual_raw_rows=1,
        order_key="partition:0001",
    )
    completed = evidence.with_native_rows(2, native_contract_sha256=_native_contract(evidence))
    forward = ConsumedPayloadEvidence.empty().append_verified_file(
        first,
        validated_schema=schema,
        source_provenance_sha256=_provenance(),
        actual_raw_rows=1,
        order_key="partition:0001",
    )
    forward = forward.append_verified_file(
        second,
        validated_schema=schema,
        source_provenance_sha256=_provenance(),
        actual_raw_rows=1,
        order_key="partition:0002",
    ).with_native_rows(2, native_contract_sha256=_native_contract(evidence))

    assert tuple(part.order_key for part in completed.parts) == ("partition:0001", "partition:0002")
    assert completed.declared_rows == completed.actual_raw_rows == completed.actual_native_rows == 2
    assert len(completed.manifest_sha256) == 64
    assert completed.require_complete() is completed
    assert completed.manifest_sha256 == forward.manifest_sha256

    changed = evidence.with_native_rows(
        2,
        native_contract_sha256=_native_contract(evidence, target_type="nvarchar(4000)"),
    )
    assert changed.manifest_sha256 != completed.manifest_sha256


def test_parallel_parts_have_identical_manifest_under_reversed_completion(tmp_path) -> None:
    artifacts = tuple(
        _artifact(tmp_path, f"part_{index}.bcp", f"{index}\tvalue-{index}\n".encode()) for index in range(4)
    )
    schema = (("id", "int"), ("value", "nvarchar(max)"))
    barrier = Barrier(len(artifacts))
    release = tuple(Event() for _artifact_value in artifacts)

    def complete(index: int) -> tuple[int, FileExportArtifact]:
        barrier.wait()
        release[index].wait()
        return index, artifacts[index]

    reversed_evidence = ConsumedPayloadEvidence.empty()
    with ThreadPoolExecutor(max_workers=len(artifacts)) as executor:
        futures = tuple(executor.submit(complete, index) for index in range(len(artifacts)))
        for index in reversed(range(len(artifacts))):
            release[index].set()
            completed_index, artifact = futures[index].result()
            reversed_evidence = reversed_evidence.append_verified_file(
                artifact,
                validated_schema=schema,
                source_provenance_sha256=_provenance(),
                actual_raw_rows=1,
                order_key=f"partition:{completed_index:020d}",
            )

    forward_evidence = ConsumedPayloadEvidence.empty()
    for index, artifact in enumerate(artifacts):
        forward_evidence = forward_evidence.append_verified_file(
            artifact,
            validated_schema=schema,
            source_provenance_sha256=_provenance(),
            actual_raw_rows=1,
            order_key=f"partition:{index:020d}",
        )

    reversed_complete = reversed_evidence.with_native_rows(
        len(artifacts),
        native_contract_sha256=_native_contract(reversed_evidence),
    )
    forward_complete = forward_evidence.with_native_rows(
        len(artifacts),
        native_contract_sha256=_native_contract(forward_evidence),
    )
    assert reversed_complete.manifest_sha256 == forward_complete.manifest_sha256


def test_consumed_payload_rejects_duplicates_counts_and_tampering(tmp_path) -> None:
    artifact = _artifact(tmp_path, "payload.bcp", b"1\talpha\n")
    schema = (("id", "int"), ("value", "nvarchar(max)"))
    evidence = ConsumedPayloadEvidence.empty().append_verified_file(
        artifact,
        validated_schema=schema,
        source_provenance_sha256=_provenance(),
        actual_raw_rows=1,
        order_key="part:1",
    )
    with pytest.raises(ArtifactIntegrityError, match="order_key_duplicate"):
        evidence.append_verified_file(
            artifact,
            validated_schema=schema,
            source_provenance_sha256=_provenance(),
            actual_raw_rows=1,
            order_key="part:1",
        )
    with pytest.raises(ArtifactIntegrityError, match="raw_row_count_mismatch"):
        ConsumedPayloadEvidence.empty().append_verified_file(
            artifact,
            validated_schema=schema,
            source_provenance_sha256=_provenance(),
            actual_raw_rows=0,
        )
    with pytest.raises(ArtifactIntegrityError, match="native_row_count_mismatch"):
        evidence.with_native_rows(0, native_contract_sha256=_native_contract(evidence))

    with open(artifact.file_path, "ab") as handle:
        handle.write(b"2\ttampered\n")
    with pytest.raises(ArtifactIntegrityError, match="byte_count_mismatch"):
        ConsumedPayloadEvidence.empty().append_verified_file(
            artifact,
            validated_schema=schema,
            source_provenance_sha256=_provenance(),
            actual_raw_rows=1,
        )


def test_consumed_payload_accepts_exact_receipt_from_one_pass_consumer_without_rehash(tmp_path) -> None:
    artifact = _artifact(tmp_path, "single-pass.bcp", b"1\talpha\n")
    receipt = artifact.integrity_receipt
    schema = (("id", "int"), ("value", "nvarchar(max)"))

    with patch.object(
        FileExportArtifact,
        "require_integrity_receipt",
        side_effect=AssertionError("source artifact must not be read twice"),
    ):
        evidence = ConsumedPayloadEvidence.empty().append_verified_file(
            artifact,
            validated_schema=schema,
            source_provenance_sha256=_provenance(),
            actual_raw_rows=1,
            verified_integrity_receipt=receipt,
        )

    assert evidence.parts[0].artifact_sha256 == receipt.sha256
    with pytest.raises(ArtifactIntegrityError, match="integrity_receipt_mismatch"):
        ConsumedPayloadEvidence.empty().append_verified_file(
            artifact,
            validated_schema=schema,
            source_provenance_sha256=_provenance(),
            actual_raw_rows=1,
            verified_integrity_receipt=replace(receipt, size_bytes=receipt.size_bytes + 1),
        )


def test_source_provenance_digest_binds_dialect_declared_type_and_metadata() -> None:
    base = _provenance()
    different_dialect = canonical_source_provenance_sha256(
        relation_dialect=SourceRelationDialect.MSSQL,
        relation_schema=(("id", "integer"), ("value", "text")),
        relation_metadata=(
            SourceColumnProvenance("id", "integer", False),
            SourceColumnProvenance("value", "text", True),
        ),
        fallback_schema=(),
    )
    different_metadata = canonical_source_provenance_sha256(
        relation_dialect=SourceRelationDialect.POSTGRES,
        relation_schema=(("id", "integer"), ("value", "text")),
        relation_metadata=(
            SourceColumnProvenance("id", "integer", False),
            replace(SourceColumnProvenance("value", "text", True), collation_name="C"),
        ),
        fallback_schema=(),
    )

    assert len({base, different_dialect, different_metadata}) == 3


def test_consumed_payload_rejects_unknown_evidence_versions() -> None:
    with pytest.raises(ArtifactIntegrityError, match="version_unsupported"):
        ConsumedPayloadEvidence(version=2)
