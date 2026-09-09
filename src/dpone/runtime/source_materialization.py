"""Connector-neutral source-side snapshot materialization contracts."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any, Protocol

from dpone.runtime.artifact_models import (
    BaseExtractionArtifact,
    StagingTableArtifact,
    release_artifact_resource_view,
)
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome, ExtractionLifecycleAuthority
from dpone.runtime.source_materialization_audit import publish_source_materialization_cleanup
from dpone.runtime.source_materialization_cleanup import (
    SourceMaterializationCleanupPolicy,
)
from dpone.runtime.source_materialization_policy import (
    bool_value,
    column_names,
    int_value,
    mapping_value,
    materialization_mapping,
    optional_bytes,
    optional_text,
    source_shape_benefits,
    speedup_pct,
    text_value,
)

SOURCE_MATERIALIZATION_SCHEMA_VERSION = "dpone.native_transfer.source_materialization.v1"


@dataclass(frozen=True, slots=True)
class SourceMaterializationIndexPolicy:
    """Optional index/statistics policy for a source work snapshot."""

    mode: str = "auto"
    boundary_columns: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> SourceMaterializationIndexPolicy:
        config = mapping_value(raw)
        return cls(
            mode=text_value(config.get("mode"), "auto"),
            boundary_columns=column_names(config.get("boundary_columns")),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["boundary_columns"] = list(self.boundary_columns)
        return payload


@dataclass(frozen=True, slots=True)
class SourceMaterializationPolicy:
    """Manifest policy for source-side snapshot materialization."""

    mode: str = "auto"
    provider: str = "auto"
    allow_source_writes: bool = False
    work_schema: str | None = None
    table_prefix: str = "__dpone_snapshot_"
    ttl_hours: int = 24
    cleanup_policy: str = "eager"
    reuse_policy: str = "never"
    min_speedup_pct: float = 25.0
    max_materialization_seconds: int = 3600
    max_work_table_bytes: int | None = None
    isolation: str = "inherited"
    index: SourceMaterializationIndexPolicy = SourceMaterializationIndexPolicy()
    update_statistics: str = "auto"
    cleanup: SourceMaterializationCleanupPolicy = SourceMaterializationCleanupPolicy()
    work_database: str | None = None
    work_connection_ref: str | None = None

    @classmethod
    def from_source_options(cls, source_options: Mapping[str, Any] | None) -> SourceMaterializationPolicy:
        raw = materialization_mapping(source_options)
        work_connection_ref = optional_text(raw.get("work_connection_ref"))
        work_database = optional_text(raw.get("work_database"))
        work_schema = optional_text(raw.get("work_schema"))
        if work_connection_ref and (work_database or work_schema):
            raise ValueError("work_connection_ref cannot be combined with work_database or work_schema")
        return cls(
            mode=text_value(raw.get("mode"), "auto"),
            provider=text_value(raw.get("provider"), "auto"),
            allow_source_writes=bool_value(raw.get("allow_source_writes"), False),
            work_database=work_database,
            work_schema=work_schema,
            work_connection_ref=work_connection_ref,
            table_prefix=str(raw.get("table_prefix") or "__dpone_snapshot_"),
            ttl_hours=int_value(raw.get("ttl_hours"), 24),
            cleanup_policy=text_value(raw.get("cleanup_policy"), "eager"),
            reuse_policy=text_value(raw.get("reuse_policy"), "never"),
            min_speedup_pct=float(raw.get("min_speedup_pct", 25)),
            max_materialization_seconds=int_value(raw.get("max_materialization_seconds"), 3600),
            max_work_table_bytes=optional_bytes(raw.get("max_work_table_bytes")),
            isolation=text_value(raw.get("isolation"), "inherited"),
            index=SourceMaterializationIndexPolicy.from_mapping(raw.get("index")),
            update_statistics=text_value(raw.get("update_statistics"), "auto"),
            cleanup=SourceMaterializationCleanupPolicy.from_mapping(raw.get("cleanup")),
        )

    def to_evidence(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "provider": self.provider,
            "allow_source_writes": self.allow_source_writes,
            "work_database": self.work_database,
            "work_schema": self.work_schema,
            "work_connection_ref": self.work_connection_ref,
            "table_prefix": self.table_prefix,
            "ttl_hours": self.ttl_hours,
            "cleanup_policy": self.cleanup_policy,
            "reuse_policy": self.reuse_policy,
            "min_speedup_pct": self.min_speedup_pct,
            "max_materialization_seconds": self.max_materialization_seconds,
            "max_work_table_bytes": self.max_work_table_bytes,
            "isolation": self.isolation,
            "index": self.index.to_dict(),
            "update_statistics": self.update_statistics,
            "cleanup": self.cleanup.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class SourceMaterializationDecision:
    """Planner output for source-side snapshot materialization."""

    selected: bool = False
    release_gate: str = "warning"
    provider: str | None = None
    measured_speedup_pct: float | None = None
    work_schema: str | None = None
    cleanup_policy: str = "eager"
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    work_database: str | None = None
    work_connection_ref: str | None = None

    def to_evidence(self) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_MATERIALIZATION_SCHEMA_VERSION,
            "selected": self.selected,
            "release_gate": self.release_gate,
            "provider": self.provider,
            "measured_speedup_pct": self.measured_speedup_pct,
            "work_database": self.work_database,
            "work_schema": self.work_schema,
            "work_connection_ref": self.work_connection_ref,
            "cleanup_policy": self.cleanup_policy,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class SourceMaterializedSnapshot:
    """Prepared source snapshot plus cleanup and query rewrite helpers."""

    qualified_name: str
    cleanup: Callable[[], Any | None]
    evidence: Mapping[str, Any]
    rewrite_query: Callable[[Sequence[str]], str] | None = None

    def select_query(self, columns: Sequence[str]) -> str:
        if self.rewrite_query is not None:
            return self.rewrite_query(columns)
        column_sql = ", ".join(str(column) for column in columns)
        return f"SELECT {column_sql} FROM {self.qualified_name}"


class SourcePreparationProvider(Protocol):
    """Port implemented by source-specific materialization providers."""

    provider_id: str

    def permissions_ok(self, load_config: Any, policy: SourceMaterializationPolicy) -> bool:
        """Return whether source work table creation and cleanup are allowed."""

    def prepare(
        self,
        load_config: Any,
        *,
        query: str,
        schema: Sequence[tuple[str, str]],
        policy: SourceMaterializationPolicy,
    ) -> SourceMaterializedSnapshot:
        """Create a source snapshot and return query rewrite/cleanup handles."""


class SourceMaterializationPlanner:
    """Pure decision service for source materialization."""

    def plan(
        self,
        policy: SourceMaterializationPolicy,
        *,
        source_shape: Any,
        provider_available: bool,
        permissions_ok: bool,
        current_rows_per_second: float | None = None,
        materialized_rows_per_second: float | None = None,
    ) -> SourceMaterializationDecision:
        blockers: list[str] = []
        warnings: list[str] = []
        reasons: list[str] = []

        if policy.mode == "off":
            return self._decision(policy, reasons=("source_materialization_disabled",))
        if not provider_available:
            target = blockers if policy.mode == "required" else warnings
            target.append("source_materialization_provider_unavailable")
        if not policy.allow_source_writes:
            target = blockers if policy.mode == "required" else warnings
            target.append("source_materialization_requires_allow_source_writes")
        if policy.allow_source_writes and not permissions_ok:
            target = blockers if policy.mode == "required" else warnings
            target.append("source_materialization_permission_denied")

        if blockers:
            return self._decision(policy, blockers=tuple(blockers), warnings=tuple(warnings), gate="blocked")
        if warnings:
            return self._decision(policy, warnings=tuple(warnings), gate="warning")

        speedup = speedup_pct(current_rows_per_second, materialized_rows_per_second)
        if speedup is not None and speedup < policy.min_speedup_pct:
            target = blockers if policy.mode == "required" else reasons
            target.append("source_materialization_speedup_below_threshold")
            return self._decision(
                policy,
                speedup=speedup,
                blockers=tuple(blockers),
                reasons=tuple(reasons),
                gate="blocked" if policy.mode == "required" else "warning",
            )

        if policy.mode == "benchmark_only":
            reasons.append("source_materialization_benchmark_only")
            return self._decision(policy, speedup=speedup, reasons=tuple(reasons), gate="warning")

        if policy.mode == "auto" and speedup is None and not source_shape_benefits(source_shape):
            reasons.append("source_materialization_not_needed")
            return self._decision(policy, reasons=tuple(reasons), gate="warning")

        reasons.append("source_materialization_selected")
        return self._decision(policy, selected=True, speedup=speedup, reasons=tuple(reasons), gate="green")

    @staticmethod
    def _decision(
        policy: SourceMaterializationPolicy,
        *,
        selected: bool = False,
        speedup: float | None = None,
        blockers: tuple[str, ...] = (),
        warnings: tuple[str, ...] = (),
        reasons: tuple[str, ...] = (),
        gate: str = "warning",
    ) -> SourceMaterializationDecision:
        return SourceMaterializationDecision(
            selected=selected,
            release_gate=gate,
            provider=policy.provider,
            measured_speedup_pct=speedup,
            work_database=policy.work_database,
            work_schema=policy.work_schema,
            work_connection_ref=policy.work_connection_ref,
            cleanup_policy=policy.cleanup_policy,
            blockers=tuple(dict.fromkeys(blockers)),
            warnings=tuple(dict.fromkeys(warnings)),
            reasons=tuple(dict.fromkeys(reasons)),
        )


class PreparedSourceArtifact(BaseExtractionArtifact):
    """Artifact wrapper that keeps a source snapshot alive for the inner load."""

    def __init__(
        self,
        artifact: Any,
        *,
        snapshot: SourceMaterializedSnapshot,
        decision: SourceMaterializationDecision,
        cleanup_policy: str,
    ) -> None:
        super().__init__(estimated_rows=getattr(artifact, "estimated_rows", None))
        self.artifact = artifact
        self.extraction_completion_mode = getattr(
            artifact,
            "extraction_completion_mode",
            "eager",
        )
        self.snapshot = snapshot
        self.decision = decision
        self.cleanup_policy = cleanup_policy
        self._source_cleaned = False
        self.source_materialization = {
            **decision.to_evidence(),
            "snapshot": dict(snapshot.evidence),
        }
        # The snapshot is an additional resource, not another view of the file.
        # A transport may close that file at EOF before the load owner decides
        # success/abort. Sharing its terminal authority would skip snapshot DROP.
        lifecycle = getattr(artifact, "extraction_lifecycle", None)
        if isinstance(lifecycle, ExtractionLifecycleAuthority):
            self.bind_extraction_lifecycle(lifecycle)

    def __getattr__(self, name: str) -> Any:
        """Forward export authority and transport attrs from the inner artifact.

        Materialized MSSQL exports wrap ``PartitionedTransferPlanArtifact`` /
        file artifacts so the source snapshot can be cleaned after load. Quality
        probes read ``extract_result.artifact`` / payload plan evidence and must
        still observe live ``rows_exported`` / ``slice_evidence`` published on
        the inner artifact during export (same class of defect as streaming
        identity loss for ``source_target_count``).
        """

        return getattr(self.artifact, name)

    def load_with(self, loader: Callable[[Any], int]) -> int:
        return int(loader(self.artifact) or 0)

    def materialize(
        self,
        staging_manager: Any,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        return self.artifact.materialize(staging_manager, load_config, schema)

    def cleanup(self) -> None:
        self.terminate(ArtifactTerminalOutcome.ABORT)

    def _release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> None:
        errors: list[BaseException] = []
        try:
            release_artifact_resource_view(self, self.artifact, outcome)
        except BaseException as exc:
            errors.append(exc)
        cleanup_source = (
            outcome is ArtifactTerminalOutcome.SUCCESS and self.cleanup_policy in {"eager", "on_success"}
        ) or (outcome is ArtifactTerminalOutcome.ABORT and self.cleanup_policy == "eager")
        if cleanup_source:
            try:
                self._cleanup_source()
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise errors[0]

    def _should_release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> bool:
        del outcome
        return True

    def _cleanup_source(self) -> None:
        if self._source_cleaned:
            return
        self._source_cleaned = True
        try:
            result = publish_source_materialization_cleanup(
                provider=self.decision.provider,
                cleanup_policy=self.cleanup_policy,
                result=self.snapshot.cleanup(),
            )
        except Exception as exc:
            publish_source_materialization_cleanup(
                provider=self.decision.provider,
                cleanup_policy=self.cleanup_policy,
                result={
                    "status": "failed",
                    "reason": "source_materialization_cleanup_failed",
                    "details": {"message": str(exc)[:500]},
                },
            )
            raise
        if result is not None:
            self.source_materialization["cleanup_result"] = result
            snapshot = self.source_materialization.get("snapshot")
            if isinstance(snapshot, dict):
                snapshot["cleanup_result"] = result


__all__ = [
    "SOURCE_MATERIALIZATION_SCHEMA_VERSION",
    "PreparedSourceArtifact",
    "SourceMaterializationDecision",
    "SourceMaterializationIndexPolicy",
    "SourceMaterializationPlanner",
    "SourceMaterializationPolicy",
    "SourceMaterializedSnapshot",
    "SourcePreparationProvider",
]
