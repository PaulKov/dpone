"""Contract-aware extraction artifacts.

These wrappers keep row-level contract enforcement compatible with streaming and
native-fast-path artifacts. They intentionally live in ``runtime.etl`` because
they adapt runtime artifact protocols; pure validation stays in
``dpone.type_system``.
"""

from __future__ import annotations

import itertools
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
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
from dpone.runtime.staging import owned_staging_handle
from dpone.type_system import ContractEnforcementResult, ContractEnforcementService
from dpone.type_system.models import ConflictPolicy


@dataclass(frozen=True, slots=True)
class ContractValidationSummary:
    accepted_rows: int = 0
    rejected_rows: int = 0
    quarantined_rows: int = 0
    validation_mode: str = "row_stream"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class ContractEnforcedRowsArtifact(BaseExtractionArtifact):
    """Row-addressable artifact wrapper for in-memory row collections."""

    def __init__(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        contract: Any,
        run_id: str,
        load_id: str,
        quarantine: Any | None = None,
        conflict_policy: ConflictPolicy = "fail",
    ) -> None:
        self._rows = list(rows)
        super().__init__(estimated_rows=len(self._rows))
        self._contract = contract
        self._run_id = run_id
        self._load_id = load_id
        self._quarantine = quarantine
        self._conflict_policy = conflict_policy
        self.validation_summary = ContractValidationSummary(validation_mode="rows")

    def materialize(
        self, staging_manager: Any, load_config: Any, schema: Sequence[tuple[str, str]]
    ) -> StagingTableArtifact:
        result = ContractEnforcementService(quarantine=self._quarantine).enforce(
            rows=self._rows,
            contract=self._contract,
            run_id=self._run_id,
            load_id=self._load_id,
            conflict_policy=self._conflict_policy,
        )
        if not result.passed:
            raise RuntimeError("data contract enforcement failed")
        with owned_staging_handle(staging_manager, load_config, schema) as handle:
            inserted = staging_manager.insert_rows(handle, result.target_rows)
            handle.row_count = inserted
            self.validation_summary = ContractValidationSummary(
                accepted_rows=len(result.target_rows),
                rejected_rows=result.rejected_rows,
                quarantined_rows=result.quarantined_rows,
                validation_mode="rows",
            )
            return handle

    def cleanup(self) -> None:
        return None


class ContractEnforcedStreamingArtifact(BaseExtractionArtifact):
    """Validate streaming rows chunk-by-chunk without full materialization."""

    def __init__(
        self,
        artifact: Any,
        *,
        contract: Any,
        run_id: str,
        load_id: str,
        quarantine: Any | None = None,
        conflict_policy: ConflictPolicy = "fail",
    ) -> None:
        super().__init__(estimated_rows=getattr(artifact, "estimated_rows", None))
        self._artifact = artifact
        self.extraction_completion_mode = getattr(
            artifact,
            "extraction_completion_mode",
            "eager",
        )
        self._contract = contract
        self._run_id = run_id
        self._load_id = load_id
        self._quarantine = quarantine
        self._conflict_policy = conflict_policy
        self.validation_summary = ContractValidationSummary()
        self.enforcement_result: ContractEnforcementResult | None = None
        bind_artifact_resource_view(self, artifact)

    def materialize(
        self, staging_manager: Any, load_config: Any, schema: Sequence[tuple[str, str]]
    ) -> StagingTableArtifact:
        iterator = self._iterator()
        batch_size = int(getattr(self._artifact, "_batch_size", 10000) or 10000)
        with owned_staging_handle(staging_manager, load_config, schema) as handle:
            accepted = rejected = quarantined = 0
            inserted_any = False
            dlq_record_ids: list[str] = []
            dlq_reasons: Counter[str] = Counter()
            dlq_index_ref: str | None = None
            for chunk in _batched(iterator, batch_size):
                result = ContractEnforcementService(quarantine=self._quarantine).enforce(
                    rows=chunk,
                    contract=self._contract,
                    run_id=self._run_id,
                    load_id=self._load_id,
                    conflict_policy=self._conflict_policy,
                    row_offset=accepted + rejected + quarantined,
                    finalize_dlq=False,
                )
                if not result.passed:
                    raise RuntimeError("data contract enforcement failed")
                if result.target_rows:
                    staging_manager.insert_rows(handle, result.target_rows)
                    inserted_any = True
                accepted += len(result.target_rows)
                rejected += result.rejected_rows
                quarantined += result.quarantined_rows
                dlq_record_ids.extend(result.dlq_record_ids)
                dlq_reasons.update(result.dlq_reasons)
                dlq_index_ref = result.dlq_index_ref or dlq_index_ref
            if not inserted_any:
                staging_manager.insert_rows(handle, ())
            handle.row_count = accepted
            self.validation_summary = ContractValidationSummary(
                accepted_rows=accepted,
                rejected_rows=rejected,
                quarantined_rows=quarantined,
                validation_mode="row_stream",
            )
            if dlq_record_ids and self._quarantine is not None:
                index_ref = self._quarantine.finalize_run(self._run_id).get("index_ref")
                dlq_index_ref = str(index_ref) if index_ref else None
            self.enforcement_result = ContractEnforcementResult(
                passed=rejected == 0,
                target_rows=[],
                diagnostics=(),
                rejected_rows=rejected,
                quarantined_rows=quarantined,
                state_commit_allowed=rejected == 0,
                dlq_record_ids=tuple(dlq_record_ids),
                dlq_reasons=dict(sorted(dlq_reasons.items())),
                dlq_index_ref=dlq_index_ref,
                accepted_row_count=accepted,
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
    def completed_source_authority_artifact(self) -> object:
        """Expose only the inner artifact that owns completed export counts."""

        return self._artifact

    def _iterator(self) -> Iterator[Mapping[str, Any]]:
        iterator = getattr(self._artifact, "_iterator", None)
        if iterator is None:
            raise RuntimeError("streaming contract artifact requires row iterator")
        return iterator


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


class PartitionedContractValidationArtifact(BaseExtractionArtifact):
    """Certification wrapper for partitioned file artifacts.

    Partitioned native fast paths must validate each partition during source
    export. This wrapper fails closed unless the source marked the partitions as
    prevalidated.
    """

    def __init__(self, artifact: ExtractionArtifact, *, prevalidated: bool = False) -> None:
        super().__init__(estimated_rows=getattr(artifact, "estimated_rows", None))
        self._artifact = artifact
        self.extraction_completion_mode = getattr(
            artifact,
            "extraction_completion_mode",
            "eager",
        )
        self._prevalidated = prevalidated
        bind_artifact_resource_view(self, artifact)

    def materialize(
        self, staging_manager: Any, load_config: Any, schema: Sequence[tuple[str, str]]
    ) -> StagingTableArtifact:
        if not self._prevalidated:
            raise RuntimeError("partitioned artifact requires per-partition contract validation")
        return self._artifact.materialize(staging_manager, load_config, schema)

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

    def rebind_columns(self, columns: Sequence[str]) -> PartitionedContractValidationArtifact:
        """Rebind partition metadata without changing certified file bytes."""

        rebind = getattr(self._artifact, "rebind_columns", None)
        if not callable(rebind):
            raise RuntimeError("partitioned artifact cannot rebind positional column identities")
        return PartitionedContractValidationArtifact(
            rebind(columns),
            prevalidated=self._prevalidated,
        )

    @property
    def completed_source_authority_artifact(self) -> object:
        """Expose only the inner artifact that owns completed export counts."""

        return self._artifact

    @property
    def validated_file_contract_artifact(self) -> object:
        """Expose prevalidated partitions without weakening their admission."""

        if not self._prevalidated:
            raise RuntimeError("partitioned artifact requires per-partition contract validation")
        return self._artifact


def _batched(iterator: Iterator[Mapping[str, Any]], batch_size: int) -> Iterator[list[Mapping[str, Any]]]:
    while True:
        chunk = list(itertools.islice(iterator, batch_size))
        if not chunk:
            return
        yield chunk


__all__ = [
    "ContractEnforcedRowsArtifact",
    "ContractEnforcedStreamingArtifact",
    "ContractValidatedFileArtifact",
    "ContractValidationSummary",
    "PartitionedContractValidationArtifact",
]
