"""Validated file wrapper and scoped source-identity attempts.

The original artifact retains terminal authority. A consumption attempt only
binds its source receipt and records a count after exact identity verification.
Legacy imports are re-exported from contract_artifacts.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from threading import Lock
from typing import Any

from dpone.runtime.artifact_models import (
    BaseExtractionArtifact,
    StagingTableArtifact,
    bind_artifact_resource_view,
    release_artifact_resource_view,
)
from dpone.runtime.artifact_protocols import ExtractionArtifact
from dpone.runtime.etl.file_contract_validation import (
    FileContractValidationError,
    FileContractValidationReceipt,
    require_file_contract_validation,
)
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.file_artifact_authority import FileVerificationBudget
from dpone.runtime.file_artifacts import FileExportArtifact


@dataclass(frozen=True, slots=True)
class ContractValidationSummary:
    accepted_rows: int = 0
    rejected_rows: int = 0
    quarantined_rows: int = 0
    validation_mode: str = "row_stream"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class ContractValidatedFileArtifact(BaseExtractionArtifact):
    """Fail-closed wrapper for opaque file fast paths unless prevalidated."""

    def __init__(
        self,
        artifact: ExtractionArtifact,
        *,
        contract: Any,
        schema: Sequence[tuple[str, str]] | None = None,
        run_id: str,
        load_id: str,
        prevalidated: bool = False,
    ) -> None:
        super().__init__(estimated_rows=getattr(artifact, "estimated_rows", None))
        self._artifact = artifact
        self._attempt_guard = Lock()
        self.extraction_completion_mode = getattr(
            artifact,
            "extraction_completion_mode",
            "eager",
        )
        self._contract = contract
        self._run_id = run_id
        self._load_id = load_id
        self._legacy_prevalidated_hint = prevalidated
        self._validation_schema = (
            tuple((str(name), str(dtype)) for name, dtype in schema) if schema is not None else None
        )
        self._receipt: FileContractValidationReceipt | None = None
        self._validation_error: FileContractValidationError | None = None
        bind_artifact_resource_view(self, artifact)
        try:
            self._receipt = require_file_contract_validation(
                artifact,
                contract,
                schema=self._validation_schema,
            )
        except FileContractValidationError as exc:
            # Keep construction non-mutating while preserving the exact typed
            # source-receipt blocker for the eventual materialization boundary.
            self._validation_error = exc
        self.validation_summary = ContractValidationSummary(validation_mode="opaque_file")

    @contextmanager
    def file_validation_attempt(
        self, attempt_id: str, *, verification_budget: FileVerificationBudget | None = None
    ) -> Iterator[FileValidationAttempt]:
        """Bind one original receipt; exceptional exits clear the latest summary."""
        if not re.fullmatch(r"[0-9a-f]{32}", attempt_id):
            raise ValueError("attempt_id must be UUID4 hex")
        if not self._attempt_guard.acquire(blocking=False):
            raise RuntimeError("attempt_in_progress")
        attempt = None
        self.validation_summary = ContractValidationSummary(validation_mode="opaque_file")
        try:
            if verification_budget is not None:
                verification_budget.check()
            if self._validation_error is not None:
                raise self._validation_error
            if not isinstance(self._artifact, FileExportArtifact):
                raise FileContractValidationError("file_contract_receipt.file_required")
            receipt = require_file_contract_validation(
                self._artifact, self._contract, schema=self._validation_schema, verification_budget=verification_budget
            )
            binding = FileValidationBinding(self._artifact, receipt, receipt.validated_schema, receipt.contract_sha256)
            attempt = FileValidationAttempt(binding, self._contract, self, verification_budget=verification_budget)
            yield attempt
        except BaseException:
            self.validation_summary = ContractValidationSummary(validation_mode="opaque_file")
            raise
        finally:
            if attempt is not None:
                attempt.close()
            self._attempt_guard.release()

    def materialize(
        self, staging_manager: Any, load_config: Any, schema: Sequence[tuple[str, str]]
    ) -> StagingTableArtifact:
        if self._validation_error is not None:
            raise self._validation_error
        if self._receipt is None:
            raise FileContractValidationError("file_contract_receipt.required")
        require_file_contract_validation(
            self._artifact,
            self._contract,
            schema=self._validation_schema,
        )
        handle = self._artifact.materialize(staging_manager, load_config, schema)
        self.validation_summary = ContractValidationSummary(
            accepted_rows=handle.row_count,
            validation_mode="opaque_file_prevalidated",
        )
        return handle

    def cleanup(self) -> None:
        self.terminate(ArtifactTerminalOutcome.ABORT)

    def _release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> None:
        release_artifact_resource_view(self, self._artifact, outcome)

    def _should_release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> bool:
        del outcome
        return True

    @property
    def columns(self) -> Sequence[str]:
        return tuple(getattr(self._artifact, "columns", ()))

    def rebind_columns(self, columns: Sequence[str]) -> ContractValidatedFileArtifact:
        """Rebind positional names while retaining contract validation policy."""

        rebind = getattr(self._artifact, "rebind_columns", None)
        if not callable(rebind):
            raise RuntimeError("opaque file artifact cannot rebind positional column identities")
        return ContractValidatedFileArtifact(
            rebind(columns),
            contract=self._contract,
            schema=self._validation_schema,
            run_id=self._run_id,
            load_id=self._load_id,
            prevalidated=self._legacy_prevalidated_hint,
        )

    @property
    def completed_source_authority_artifact(self) -> object:
        """Expose only the inner artifact that owns completed export counts."""

        return self._artifact

    @property
    def validated_file_contract_artifact(self) -> object:
        """Expose the exact inner file only after its contract receipt passed."""

        if self._validation_error is not None:
            raise self._validation_error
        if self._receipt is None:
            raise FileContractValidationError("file_contract_receipt.required")
        return self._artifact

    def lacks_source_contract_receipt(self) -> bool:
        """True only when no source receipt exists.

        A failed row scan or any other validation error is not this case. Those
        stay fatal and are not eligible for a later target observation.
        """

        if self._receipt is not None:
            return False
        error = self._validation_error
        return isinstance(error, FileContractValidationError) and error.blocker == "file_contract_receipt.required"


@dataclass(frozen=True, slots=True)
class FileValidationBinding:
    """Immutable source identity; mutable contract authority stays private."""

    artifact: FileExportArtifact
    receipt: FileContractValidationReceipt
    source_schema: tuple[tuple[str, str], ...]
    contract_sha256: str


class FileValidationAttempt:
    """A single active wrapper context, never a target-finalization capability."""

    def __init__(
        self,
        binding: FileValidationBinding,
        contract: Any,
        owner: ContractValidatedFileArtifact,
        *,
        verification_budget: FileVerificationBudget | None = None,
    ) -> None:
        self.binding = binding
        self._verification_budget = verification_budget
        self._contract = contract
        self._owner = owner
        self._source_path = binding.artifact.file_path
        self._active = True
        self._completed = False

    def verify_unchanged(self, *, verification_budget: FileVerificationBudget | None = None) -> None:
        """Recheck bytes, contract, wire, columns and exact receipt replacement."""
        if not self._active:
            raise RuntimeError("file_validation_attempt_inactive")
        artifact = self.binding.artifact
        if artifact.file_path != self._source_path:
            raise FileContractValidationError("file_contract_receipt.path_changed")
        if artifact.contract_validation_receipt is not self.binding.receipt:
            raise FileContractValidationError("file_contract_receipt.receipt_replaced")
        require_file_contract_validation(
            artifact,
            self._contract,
            schema=self.binding.source_schema,
            verification_budget=verification_budget or self._verification_budget,
        )
        if artifact.file_path != self._source_path:
            raise FileContractValidationError("file_contract_receipt.path_changed")
        if artifact.contract_validation_receipt is not self.binding.receipt:
            raise FileContractValidationError("file_contract_receipt.receipt_replaced")

    def complete(self, staged_rows: int, *, verification_budget: FileVerificationBudget | None = None) -> None:
        """Record exact observed staging rows once, after source verification."""
        if self._completed:
            raise RuntimeError("file_validation_attempt_already_completed")
        if type(staged_rows) is not int or staged_rows != self.binding.receipt.rows_validated:
            raise ValueError("staging_count_mismatch")
        self.verify_unchanged(verification_budget=verification_budget)
        effective_budget = verification_budget or self._verification_budget
        if effective_budget is not None:
            effective_budget.check()
        self._owner.validation_summary = ContractValidationSummary(
            accepted_rows=staged_rows, validation_mode="opaque_file_prevalidated"
        )
        self._completed = True

    def close(self) -> None:
        """Invalidate access when the owning context exits."""
        self._active = False
