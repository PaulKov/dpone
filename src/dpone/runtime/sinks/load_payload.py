"""Sink load payload contract."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from dpone.runtime.artifact_protocols import ExtractionArtifact
from dpone.type_system.source_sink.provenance import SourceColumnProvenance, SourceRelationDialect

if TYPE_CHECKING:
    from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
    from dpone.runtime.etl.owned_payload_scope import OwnedPayloadScope
    from dpone.runtime.extraction_lifecycle import (
        ExtractionLifecycleAuthority,
        ExtractionLifecycleReceipt,
    )
    from dpone.runtime.sinks.mssql_target_mutation_plan import MssqlTargetMutationPlan
    from dpone.runtime.support.postgres_mssql_projection import PostgresMssqlSchemaProjection


@dataclass(frozen=True)
class LoadPayload:
    """Dataset passed from extraction/runtime orchestration into a sink."""

    artifact: ExtractionArtifact
    schema: Sequence[tuple[str, str]]
    relation_schema: Sequence[tuple[str, str]] | None = None
    relation_metadata: Sequence[SourceColumnProvenance] | None = None
    relation_dialect: SourceRelationDialect | None = None
    target_projection: PostgresMssqlSchemaProjection | None = None
    mssql_transaction_admission: MssqlTransactionAdmission | None = None
    mssql_target_mutation_plan: MssqlTargetMutationPlan | None = None
    extraction_lifecycle: ExtractionLifecycleAuthority | None = None
    owned_payload_scope: OwnedPayloadScope | None = None
    postgres_mssql_r1_execution: Any | None = None

    def __post_init__(self) -> None:
        """Register every transformed source view with the top-level owner."""

        scope = self.owned_payload_scope
        if scope is None:
            return
        if self.extraction_lifecycle is not scope.extraction_lifecycle:
            raise ValueError("load_payload.extraction_lifecycle_scope_mismatch")
        scope.bind(self.artifact)

    def rebind(self, **changes: Any) -> LoadPayload:
        """Return a modified payload while preserving every immutable contract field."""

        for field_name in ("extraction_lifecycle", "owned_payload_scope"):
            current = getattr(self, field_name)
            if field_name in changes and changes[field_name] is not current:
                raise ValueError(f"load_payload.{field_name}_cannot_be_rebound")
        return replace(self, **changes)

    def require_completed_extraction(self) -> ExtractionLifecycleReceipt:
        """Return source timing evidence only after payload consumption completed."""

        scope = self.owned_payload_scope
        if scope is not None:
            return scope.require_completed_extraction()
        authority = self.extraction_lifecycle
        if authority is None:
            from dpone.runtime.extraction_lifecycle import ExtractionLifecycleStateError

            raise ExtractionLifecycleStateError("extraction_lifecycle.authority_missing")
        return authority.require_completed()

    def require_acquired_extraction(self) -> ExtractionLifecycleReceipt:
        """Return a stable source-start receipt without asserting completion."""

        scope = self.owned_payload_scope
        if scope is not None:
            return scope.require_acquired_extraction()
        authority = self.extraction_lifecycle
        if authority is None:
            from dpone.runtime.extraction_lifecycle import ExtractionLifecycleStateError

            raise ExtractionLifecycleStateError("extraction_lifecycle.authority_missing")
        return authority.require_acquired()


__all__ = ["LoadPayload"]
