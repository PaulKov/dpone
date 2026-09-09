from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from threading import RLock
from typing import TYPE_CHECKING, Any

from dpone.runtime.extraction_lifecycle import (
    ArtifactTerminalAuthority,
    ArtifactTerminalOutcome,
    ArtifactTerminalReceipt,
    ExtractionLifecycleAuthority,
)

if TYPE_CHECKING:
    from dpone.runtime.staging import StagingManager


class BaseExtractionArtifact:
    """Reusable runtime state and idempotent terminal protocol for artifacts."""

    extraction_completion_mode = "eager"

    def __init__(self, estimated_rows: int | None = None) -> None:
        self._estimated_rows = estimated_rows
        self._extraction_lifecycle: ExtractionLifecycleAuthority | None = None
        self._terminal_lock = RLock()
        self._terminal_authority = ArtifactTerminalAuthority()

    @property
    def estimated_rows(self) -> int | None:
        return self._estimated_rows

    @property
    def extraction_lifecycle(self) -> ExtractionLifecycleAuthority | None:
        """Return the timing authority bound by the source, when available."""

        return self._extraction_lifecycle

    def bind_extraction_lifecycle(self, authority: ExtractionLifecycleAuthority) -> None:
        """Bind one lifecycle authority without permitting evidence replacement."""

        with self._terminal_lock:
            current = self._extraction_lifecycle
            if current is not None and current is not authority:
                raise ValueError("artifact.extraction_lifecycle_already_bound")
            self._extraction_lifecycle = authority

    @property
    def terminal_receipt(self) -> ArtifactTerminalReceipt | None:
        """Return the first frozen terminal decision, if one was issued."""

        return self._terminal_authority.receipt

    @property
    def terminal_authority(self) -> ArtifactTerminalAuthority:
        """Return the decision authority shared by resource-preserving views."""

        return self._terminal_authority

    def bind_terminal_authority(self, authority: ArtifactTerminalAuthority) -> None:
        """Bind a wrapper/rebind view to the same one-way terminal decision."""

        with self._terminal_lock:
            current = self._terminal_authority
            if current is authority:
                return
            if current.receipt is not None:
                raise ValueError("artifact.terminal_authority_already_decided")
            self._terminal_authority = authority

    def terminate(self, outcome: ArtifactTerminalOutcome) -> ArtifactTerminalReceipt:
        """Apply exactly one best-effort terminal decision.

        Cleanup failures are evidence, not replacement exceptions: a caller
        already handling a load/commit error must never lose that primary
        failure to a secondary resource-release failure.
        """

        return self._terminal_authority.terminate(
            outcome,
            release=self._release_for_terminal_outcome,
            should_release=self._should_release_for_terminal_outcome(outcome),
        )

    def release_resources_for_shared_terminal(self, outcome: ArtifactTerminalOutcome) -> None:
        """Release this view's resources without issuing a second decision.

        Resource-preserving wrappers share :class:`ArtifactTerminalAuthority`
        with their inner artifact.  Their one authoritative decision therefore
        invokes this method on the inner view instead of recursively calling
        ``terminate`` on the same authority.
        """

        if self._should_release_for_terminal_outcome(outcome):
            self._release_for_terminal_outcome(outcome)

    def _release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> None:
        """Release resources for success/abort; subclasses may split handlers."""

        del outcome
        self.cleanup()

    def cleanup(self) -> None:
        """Release no resources by default; concrete artifacts may override."""

        return None

    def _should_release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> bool:
        """Retain evidence by default when target commit acknowledgement is unknown."""

        return outcome is not ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN


class ArtifactResourceReleaseError(RuntimeError):
    """An inner artifact could not honor a shared terminal decision."""


def bind_artifact_resource_view(view: BaseExtractionArtifact, artifact: object) -> None:
    """Bind a resource-preserving wrapper to its inner lifecycle authorities."""

    lifecycle = getattr(artifact, "extraction_lifecycle", None)
    if isinstance(lifecycle, ExtractionLifecycleAuthority):
        view.bind_extraction_lifecycle(lifecycle)
    authority = getattr(artifact, "terminal_authority", None)
    if isinstance(authority, ArtifactTerminalAuthority):
        view.bind_terminal_authority(authority)


def release_artifact_resource_view(
    view: BaseExtractionArtifact,
    artifact: object,
    outcome: ArtifactTerminalOutcome,
) -> None:
    """Forward one exact outcome to an inner resource-preserving artifact."""

    if isinstance(artifact, BaseExtractionArtifact):
        if artifact.terminal_authority is view.terminal_authority:
            artifact.release_resources_for_shared_terminal(outcome)
            return
        receipt = artifact.terminate(outcome)
        if not receipt.cleanup_succeeded:
            raise ArtifactResourceReleaseError(receipt.cleanup_error_code or "artifact.release_failed")
        return
    terminate = getattr(artifact, "terminate", None)
    if callable(terminate):
        receipt = terminate(outcome)
        if getattr(receipt, "cleanup_succeeded", True) is False:
            raise ArtifactResourceReleaseError(
                str(getattr(receipt, "cleanup_error_code", None) or "artifact.release_failed")
            )
        return
    if outcome is not ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN:
        cleanup = getattr(artifact, "cleanup", None)
        if callable(cleanup):
            cleanup()


@dataclass
class StagingTableArtifact:
    """Артефакт, представляющий staging таблицу в целевой СУБД."""

    schema: str
    table: str
    columns: Sequence[str]
    staging_manager: StagingManager
    row_count: int = 0
    database: str | None = None
    target_schema: str | None = None
    column_types: dict[str, str] = field(default_factory=dict)
    # Target DDL types when staging rewrites binary columns to nvarchar hex wire.
    target_column_types: dict[str, str] = field(default_factory=dict)
    target_column_nullability: dict[str, bool] = field(default_factory=dict)
    target_column_collations: dict[str, str] = field(default_factory=dict)
    hex_binary_columns: frozenset[str] = field(default_factory=frozenset)
    bulk_text_codec: Any | None = None
    allow_unsafe_raw_bulk_file: bool = False
    bulk_options: Any | None = None
    wire_schema: tuple[tuple[str, str], ...] = ()
    source_provenance_sha256: str | None = None
    consumed_payload_evidence: Any | None = None
    typed_file_ingestion: bool = False
    # Snapshot files carry an independent per-row checksum as their final
    # field.  Direct XMin-initial staging carries business fields only and is
    # instead protected by the immutable whole-file receipt verified at EOF.
    typed_file_row_hash_validation: bool = True
    # Direct native staging is not handler-ready until framework-owned
    # metadata has been projected inside SQL Server.
    typed_file_deferred_native_evidence: bool = False
    direct_native_staging: bool = False
    typed_transport: str | None = None
    typed_batch_count: int = 0

    def qualified_name(self) -> str:
        if self.database:
            return f"{self.database}.{self.schema}.{self.table}"
        return f"{self.schema}.{self.table}"

    def cleanup(self) -> None:
        self.staging_manager.drop(self)
