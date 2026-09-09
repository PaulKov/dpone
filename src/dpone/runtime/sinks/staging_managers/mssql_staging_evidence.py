"""Integrity authority for SQL Server BCP staging materialization.

The staging manager owns table creation and wire-format dispatch.  This
module owns the smaller, security-sensitive boundary that proves the exact
file accepted by BCP, the vendor-reported row count, the physical staging
delta, and the immutable consumed-payload manifest all agree.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.runtime.consumed_payload_evidence import (
    ArtifactIntegrityError,
    ConsumedPayloadEvidence,
    canonical_native_contract_sha256,
    canonical_source_provenance_sha256,
)
from dpone.runtime.pinned_file_consumer import PinnedFileConsumer

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.ports.mssql_connector import MSSQLConnectorPort
    from dpone.runtime.artifact_integrity import FileArtifactReceipt
    from dpone.runtime.artifact_models import StagingTableArtifact
    from dpone.runtime.file_artifacts import FileExportArtifact


class MssqlStagingEvidenceAuthority:
    """Serializes BCP verification and appends deterministic file evidence."""

    def __init__(self, connector: MSSQLConnectorPort) -> None:
        self._connector = connector
        self._lock = threading.RLock()

    def verified_import(
        self,
        artifact: StagingTableArtifact,
        file_artifact: FileExportArtifact,
        *,
        options: Any,
    ) -> int:
        """Import one immutable part and prove all independent row counts."""

        receipt = _verify_import_receipt(file_artifact, options=options)
        expected_rows = receipt.require_rows_exported()
        with self._lock:
            _require_same_receipt(receipt, _verify_import_receipt(file_artifact, options=options))
            error_path = _clean_error_path(getattr(options, "error_file", None))
            before_rows = self._count_rows(artifact)
            # SQL Server bcp does not provide a portable success receipt for a
            # zero-byte input.  The immutable zero-row file receipt plus two
            # physical COUNT_BIG probes is the stronger empty-payload proof.
            copied = 0 if expected_rows == 0 else self._bcp_import(artifact, file_artifact.file_path, options=options)
            # Re-read after BCP so an unexpected runtime mutation cannot pass
            # with only the expected size and row count still intact.
            _require_same_receipt(receipt, _verify_import_receipt(file_artifact, options=options))
            after_rows = self._count_rows(artifact)
            _require_clean_bcp_error_file(error_path)
            if isinstance(copied, bool) or not isinstance(copied, int) or copied != expected_rows:
                raise ArtifactIntegrityError("artifact_integrity.bcp_row_count_mismatch")
            if after_rows - before_rows != expected_rows:
                raise ArtifactIntegrityError("artifact_integrity.staging_row_count_mismatch")
            ensure_staging_evidence_authority(artifact)
            evidence = artifact.consumed_payload_evidence or ConsumedPayloadEvidence.empty()
            artifact.consumed_payload_evidence = evidence.append_verified_file(
                file_artifact,
                validated_schema=artifact.wire_schema,
                source_provenance_sha256=require_source_provenance(artifact),
                actual_raw_rows=after_rows - before_rows,
                order_key=file_order_key(file_artifact, evidence),
                verified_integrity_receipt=receipt,
            )
            return copied

    def finalize_native(
        self,
        raw: StagingTableArtifact,
        native: StagingTableArtifact,
        *,
        columns: Sequence[Mapping[str, object]],
    ) -> None:
        """Bind the exact native projection and verified physical row count."""

        evidence = raw.consumed_payload_evidence
        if not isinstance(evidence, ConsumedPayloadEvidence):
            raise ArtifactIntegrityError("mssql_native_projection.consumed_payload_evidence_required")
        verified = evidence.require_complete(native=False)
        native.consumed_payload_evidence = verified.with_native_rows(
            native.row_count,
            native_contract_sha256=canonical_native_contract_sha256(
                columns,
                source_wire_contract_sha256s=tuple(part.wire_contract_sha256 for part in verified.parts),
            ),
        )

    def _bcp_import(self, artifact: StagingTableArtifact, file_path: str, *, options: Any) -> int:
        try:
            return self._connector.bcp_import(
                artifact.schema,
                artifact.table,
                file_path,
                options=options,
                database=artifact.database,
            )
        except TypeError:
            return self._connector.bcp_import(
                schema_label(artifact.schema, artifact.database),
                artifact.table,
                file_path,
                options=options,
            )

    def _count_rows(self, artifact: StagingTableArtifact) -> int:
        try:
            qualified = self._connector.qualified_name(
                artifact.schema,
                artifact.table,
                database=artifact.database,
            )
        except TypeError:
            qualified = self._connector.qualified_name(
                schema_label(artifact.schema, artifact.database),
                artifact.table,
            )
        rows = self._connector.get_records(f"SELECT COUNT_BIG(*) FROM {qualified}")
        if not rows or isinstance(rows[0][0], bool) or not isinstance(rows[0][0], int):
            raise ArtifactIntegrityError("artifact_integrity.staging_row_count_unavailable")
        return int(rows[0][0])


def source_provenance_sha256(
    load_config: LoadConfig,
    schema: Sequence[tuple[str, str]],
) -> str:
    """Resolve caller-provided provenance or bind the runtime schema fallback."""

    options = getattr(load_config, "options", {}) or {}
    configured = options.get("__dpone_consumed_source_provenance_sha256")
    if configured:
        return str(configured)
    return canonical_source_provenance_sha256(
        relation_dialect=None,
        relation_schema=None,
        relation_metadata=None,
        fallback_schema=schema,
    )


def _verify_import_receipt(
    file_artifact: FileExportArtifact,
    *,
    options: Any,
) -> FileArtifactReceipt:
    authority = getattr(options, "input_file_authority", None)
    if not isinstance(authority, PinnedFileConsumer):
        return file_artifact.require_integrity_receipt()
    receipt = file_artifact.integrity_receipt
    if receipt is None:
        raise ArtifactIntegrityError("artifact_integrity.receipt_missing")
    authority.verify_integrity_receipt(receipt, wire_contract=file_artifact.wire_contract())
    receipt.require_rows_exported()
    return receipt


def _require_same_receipt(expected: FileArtifactReceipt, observed: FileArtifactReceipt) -> None:
    if observed != expected:
        raise ArtifactIntegrityError("artifact_integrity.receipt_authority_changed")


def initialize_runtime_provenance(artifact: StagingTableArtifact) -> None:
    """Bind deterministic fallback provenance for legacy partition APIs."""

    artifact.source_provenance_sha256 = canonical_source_provenance_sha256(
        relation_dialect=None,
        relation_schema=None,
        relation_metadata=None,
        fallback_schema=artifact.wire_schema,
    )


def schema_label(schema: str, database: str | None) -> str:
    if database and "." not in str(schema):
        return f"{database}.{schema}"
    return str(schema)


def require_source_provenance(artifact: StagingTableArtifact) -> str:
    if not artifact.source_provenance_sha256:
        raise ArtifactIntegrityError("consumed_payload.source_provenance_required")
    return artifact.source_provenance_sha256


def ensure_staging_evidence_authority(artifact: StagingTableArtifact) -> None:
    if not artifact.wire_schema:
        types = artifact.target_column_types or artifact.column_types
        if any(column not in types for column in artifact.columns):
            raise ArtifactIntegrityError("consumed_payload.wire_schema_required")
        artifact.wire_schema = tuple((str(column), str(types[column])) for column in artifact.columns)
    if not artifact.source_provenance_sha256:
        initialize_runtime_provenance(artifact)


def file_order_key(
    artifact: FileExportArtifact,
    evidence: ConsumedPayloadEvidence,
) -> str:
    """Return stable producer order; never infer parallel completion order."""

    consumed_part_index = getattr(artifact, "consumed_part_index", None)
    if consumed_part_index is not None:
        return _indexed_order_key("partition", consumed_part_index)
    bounds = getattr(artifact, "partition_bounds", None)
    if isinstance(bounds, Mapping) and bounds.get("index") is not None:
        return _indexed_order_key("partition", bounds["index"])
    if bounds is not None or getattr(artifact, "transfer_partition_id", None) is not None:
        raise ArtifactIntegrityError("consumed_payload.partition_order_required")
    batch_index = getattr(artifact, "batch_index", None)
    if batch_index is not None:
        return _indexed_order_key("batch", batch_index)
    return f"sequential:{len(evidence.parts):020d}"


def _indexed_order_key(kind: str, value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ArtifactIntegrityError(f"consumed_payload.{kind}_order_invalid")
    return f"{kind}:{value:020d}"


def _clean_error_path(value: Any) -> Path | None:
    if not value:
        return None
    path = Path(str(value))
    if path.exists() and path.stat().st_size:
        raise ArtifactIntegrityError("artifact_integrity.bcp_error_file_not_clean")
    return path


def _require_clean_bcp_error_file(path: Path | None) -> None:
    if path is not None and path.exists() and path.stat().st_size:
        raise ArtifactIntegrityError("artifact_integrity.bcp_rejected_rows")


__all__ = [
    "MssqlStagingEvidenceAuthority",
    "initialize_runtime_provenance",
    "schema_label",
    "source_provenance_sha256",
]
