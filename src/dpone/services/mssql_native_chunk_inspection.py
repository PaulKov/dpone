"""Source-free reconstruction of an exact SqlClient P10f projection."""

from __future__ import annotations

from hashlib import sha256

from dpone.contracts.mssql_sqlclient_native_chunk_receipt_api import (
    canonical_stage_id,
    native,
    validate_native_chunk_receipt,
)
from dpone.ports.mssql_native_chunk_inspection import NativeChunkInspectionEvidence, NativeChunkInspectionIndex
from dpone.ports.mssql_tds_directory import TdsDirectoryObserver
from dpone.ports.mssql_tds_journal import TdsAttemptObserver

ERROR = "mssql_native.sqlclient_chunk_inspection_unknown"


class NativeChunkInspectionUnknown(RuntimeError):
    """Durable inputs are missing, changed, or do not reproduce the P10f fact."""

    def __init__(self) -> None:
        super().__init__(ERROR)


class SourceFreeSqlClientChunkInspector:
    """Reconstruct from parent and child journals; never opens source or effects."""

    def __init__(
        self,
        index: NativeChunkInspectionIndex,
        lifecycle: TdsAttemptObserver,
        directories: TdsDirectoryObserver,
        evidence: NativeChunkInspectionEvidence,
    ) -> None:
        self._index = index
        self._lifecycle = lifecycle
        self._directories = directories
        self._evidence = evidence

    def inspect(self, ordinal: int, limits: native.TdsDirectoryLimits) -> native.SqlClientNativeChunkProjection:
        try:
            if type(ordinal) is not int or ordinal < 0 or type(limits) is not native.TdsDirectoryLimits:
                raise ValueError(ERROR)
            references = self._index.references()
            if ordinal >= len(references) or references[ordinal].receipt.ordinal != ordinal:
                raise ValueError(ERROR)
            reference = references[ordinal]
            canonical_evidence = validate_native_chunk_receipt(reference.receipt)
            registration = native.decode_registration(self._read_evidence(reference.registration_receipt))
            verification = native.decode_writer_settlement(self._read_evidence(reference.verification_receipt))
            identity = registration.binding.identity
            attempt_sha256 = registration.binding.attempt_sha256
            snapshot = self._lifecycle.read(identity)
            directory = self._directories.read(identity, limits)
            if not self._bindings_match(reference, registration, verification, snapshot, directory, limits):
                raise ValueError(ERROR)
            if type(snapshot) is not native.TdsAttemptSnapshot:
                raise ValueError(ERROR)
            projection = native.bind_sqlclient_native_chunk(
                attempt=identity,
                attempt_sha256=attempt_sha256,
                stage=verification.observation.stage_after,
                object_identity=native.stage_object_identity(verification.observation.stage_after),
                rows=verification.observation.row_count,
                encoded_bytes=registration.input.expected.encoded_bytes,
                file_sha256=registration.input.expected.file_sha256,
                typed_digest=verification.observation.typed_digest,
                typed_sum=verification.observation.typed_sum,
                registration_receipt=reference.registration_receipt,
                verification_receipt=reference.verification_receipt,
                lifecycle_verification_sha256=reference.lifecycle_verification_sha256,
                lifecycle_revision=reference.lifecycle_revision,
                worker_build_sha256=registration.binding.build_sha256,
                implementation_sha256=identity.implementation_sha256,
                helper_implementation_sha256=verification.helper.implementation_sha256,
                directory_key=native.directory_key(identity),
                directory_coordinate=native.SqlClientDirectoryCoordinate(
                    target_key=identity.target_key,
                    run_id=identity.run_id,
                    ordinal=identity.ordinal,
                    attempt=identity.attempt,
                ),
            )
            if projection.projection_sha256 != reference.projection_sha256:
                raise ValueError(ERROR)
            validate_native_chunk_receipt(reference.receipt, projection=projection)
            if canonical_evidence.typed_sum != projection.typed_sum:
                raise ValueError(ERROR)
            return projection
        except Exception:
            raise NativeChunkInspectionUnknown() from None

    @staticmethod
    def _bindings_match(reference, registration, verification, snapshot, directory, limits) -> bool:
        if type(snapshot) is not native.TdsAttemptSnapshot or directory is None:
            return False
        identity = registration.binding.identity
        receipt = reference.receipt
        return (
            identity.ordinal == receipt.ordinal
            and identity.attempt <= 2
            and receipt.attempt_id == f"{identity.run_id}-{identity.ordinal}-{identity.attempt}"
            and receipt.stage_id == canonical_stage_id(registration.binding.object_identity)
            and (receipt.rows, receipt.encoded_bytes, receipt.file_sha256, receipt.typed_digest)
            == (
                verification.observation.row_count,
                registration.input.expected.encoded_bytes,
                registration.input.expected.file_sha256,
                verification.observation.typed_digest,
            )
            and registration.binding.attempt_sha256 == reference.registration_receipt.attempt_sha256
            and verification.attempt_sha256 == reference.verification_receipt.attempt_sha256
            and verification.registration_sha256 == reference.registration_receipt.payload_sha256
            and verification.observation.typed_sum is not None
            and native.stage_object_identity(verification.observation.stage_after)
            == registration.binding.object_identity
            and verification.expectation.file_sha256 == registration.input.expected.file_sha256
            and snapshot.state.identity == identity
            and snapshot.state.phase
            in {
                native.TdsAttemptPhase.VERIFIED,
                native.TdsAttemptPhase.CONTAINMENT_REQUIRED,
                native.TdsAttemptPhase.CONTAINED,
                native.TdsAttemptPhase.RETIREMENT_REQUIRED,
                native.TdsAttemptPhase.RETIRED,
            }
            and snapshot.state.object_identity == registration.binding.object_identity
            and snapshot.state.verification_sha256 == reference.lifecycle_verification_sha256
            and reference.lifecycle_verification_sha256 == reference.verification_receipt.payload_sha256
            and snapshot.revision >= reference.lifecycle_revision
            and directory.state.parent == identity
            and directory.state.limits == limits
            and native.directory_key(identity) == native.directory_key(directory.state.parent)
        )

    def _read_evidence(self, receipt: native.SqlClientEvidenceReceipt) -> bytes:
        receipt.__post_init__()
        payload = self._evidence.read(receipt)
        if (
            type(payload) is not bytes
            or len(payload) != receipt.byte_count
            or sha256(payload).hexdigest() != receipt.payload_sha256
        ):
            raise ValueError(ERROR)
        return payload


__all__ = ("NativeChunkInspectionUnknown", "SourceFreeSqlClientChunkInspector")
