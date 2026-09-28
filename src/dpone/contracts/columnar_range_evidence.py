"""Fail-closed execution evidence for bounded columnar range plans."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.columnar_range_parallelism import (
    ColumnarRangePlan,
    RangeChunkReceipt,
    RangeEvidenceItem,
    RangeParallelismPolicy,
    RangeStageReceipt,
)

_OUTCOMES = {"extracted", "staged", "quality_passed", "succeeded", "failed", "cancelled", "publication_unknown"}
_CLEANUP = {"not_started", "not_required", "completed", "failed", "preserved_unknown"}


@dataclass(frozen=True, slots=True)
class ColumnarRangeExecutionEvidence:
    plan_fingerprint: str
    execution_plan_fingerprint: str
    range_set_fingerprint: str
    policy: RangeParallelismPolicy
    planned_range_count: int
    ranges: tuple[RangeEvidenceItem, ...]
    observed_reader_concurrency: int
    observed_upload_concurrency: int
    observed_load_concurrency: int
    rows_high_water: int
    bytes_high_water: int
    outcome_status: str = "extracted"
    failure_code: str | None = None
    cancellation_requested: bool = False
    cancellation_observed: bool = False
    cleanup_status: str = "not_started"
    cleanup_failures: tuple[str, ...] = ()
    assembly_receipt_sha256: str | None = None
    quality_receipt_sha256: str | None = None
    publication_receipt_sha256: str | None = None
    schema: str = "dpone.native_transfer.columnar_range_parallelism.v1"

    def __post_init__(self) -> None:
        if self.outcome_status not in _OUTCOMES or self.cleanup_status not in _CLEANUP:
            raise ValueError("Range evidence contains an unsupported outcome or cleanup status.")
        _nonnegative("planned_range_count", self.planned_range_count)
        if self.planned_range_count == 0:
            raise ValueError("planned_range_count must be greater than zero.")
        if len(self.ranges) > self.planned_range_count or len({item.range_id for item in self.ranges}) != len(
            self.ranges
        ):
            raise ValueError("Range evidence must be unique and cannot exceed the plan.")
        objects = [chunk.object_identity for item in self.ranges for chunk in item.chunks]
        if len(objects) != len(set(objects)):
            raise ValueError("Chunk object identities must be unique across the execution plan.")
        _bounded_measurements(self)
        if self.failure_code is not None:
            _code("failure_code", self.failure_code)
        for code in self.cleanup_failures:
            _code("cleanup_failure", code)
        if self.cancellation_observed and not self.cancellation_requested:
            raise ValueError("Observed cancellation requires a cancellation request.")
        if self.outcome_status == "cancelled" and not self.cancellation_requested:
            raise ValueError("A cancelled outcome requires a cancellation request.")
        if bool(self.cleanup_failures) != (self.cleanup_status == "failed"):
            raise ValueError("Cleanup failures are required if and only if cleanup status is failed.")
        nonfailure = {"extracted", "staged", "quality_passed", "succeeded"}
        if self.outcome_status in nonfailure and (
            self.failure_code or self.cancellation_requested or self.cancellation_observed or self.cleanup_failures
        ):
            raise ValueError("Nonfailure evidence cannot contain failure or cancellation fields.")
        if self.outcome_status in {"failed", "cancelled", "publication_unknown"} and not self.failure_code:
            raise ValueError("A non-success outcome requires a stable redacted failure code.")
        if self.outcome_status in {"staged", "quality_passed", "succeeded", "publication_unknown"} and (
            not self._all_staged or not self._all_eof
        ):
            raise ValueError("Staged evidence requires EOF and a reconciled receipt for every planned range.")
        _validate_topology(self)
        if self.outcome_status in {"quality_passed", "succeeded", "publication_unknown"}:
            self._validate_quality()
        if self.outcome_status == "succeeded":
            self._validate_success()
        if self.outcome_status == "publication_unknown" and self.cleanup_status != "preserved_unknown":
            raise ValueError("Unknown publication outcomes must preserve recovery resources.")

    @classmethod
    def from_results(
        cls,
        *,
        plan: ColumnarRangePlan,
        results: Sequence[Any],
        observed_reader_concurrency: int,
        rows_high_water: int,
        bytes_high_water: int,
        observed_upload_concurrency: int = 0,
    ) -> ColumnarRangeExecutionEvidence:
        ranges = _ordered_items(plan, results, require_complete=True)
        if not all(item.eof_confirmed for item in ranges):
            raise ValueError("Every planned range must confirm EOF before extraction evidence.")
        return cls._create(
            plan, ranges, observed_reader_concurrency, observed_upload_concurrency, 0, rows_high_water, bytes_high_water
        )

    @classmethod
    def from_failure(
        cls,
        *,
        plan: ColumnarRangePlan,
        results: Sequence[Any],
        primary_outcome: str,
        failure_code: str,
        cancellation_requested: bool,
        cancellation_observed: bool,
        cleanup_status: str,
        stage_receipts: Mapping[str, RangeStageReceipt] | None = None,
        cleanup_failures: Sequence[str] = (),
        observed_reader_concurrency: int = 0,
        observed_upload_concurrency: int = 0,
        observed_load_concurrency: int = 0,
        rows_high_water: int = 0,
        bytes_high_water: int = 0,
    ) -> ColumnarRangeExecutionEvidence:
        if primary_outcome not in {"failed", "cancelled"}:
            raise ValueError("primary_outcome must be failed or cancelled.")
        ranges = _with_stages(_ordered_items(plan, results, require_complete=False), stage_receipts or {})
        return cls._create(
            plan,
            ranges,
            observed_reader_concurrency,
            observed_upload_concurrency,
            observed_load_concurrency,
            rows_high_water,
            bytes_high_water,
            outcome_status=primary_outcome,
            failure_code=failure_code,
            cancellation_requested=cancellation_requested,
            cancellation_observed=cancellation_observed,
            cleanup_status=cleanup_status,
            cleanup_failures=tuple(cleanup_failures),
        )

    def with_stage_receipts(
        self, receipts: Mapping[str, RangeStageReceipt], *, observed_load_concurrency: int
    ) -> ColumnarRangeExecutionEvidence:
        if self.outcome_status != "extracted":
            raise ValueError("Stage receipts can only advance complete extraction evidence.")
        expected = {item.range_id for item in self.ranges}
        if set(receipts) != expected:
            raise ValueError("Stage receipts must cover every extracted range exactly once.")
        return replace(
            self,
            ranges=_with_stages(self.ranges, receipts),
            observed_load_concurrency=observed_load_concurrency,
            outcome_status="staged",
        )

    def with_quality_receipt(
        self,
        *,
        quality_receipt_sha256: str,
        assembly_receipt_sha256: str | None = None,
    ) -> ColumnarRangeExecutionEvidence:
        if self.outcome_status != "staged":
            raise ValueError("Quality evidence requires complete reconciled staging.")
        return replace(
            self,
            outcome_status="quality_passed",
            quality_receipt_sha256=quality_receipt_sha256,
            assembly_receipt_sha256=assembly_receipt_sha256,
        )

    def complete(
        self, *, publication_receipt_sha256: str, cleanup_status: str = "not_required"
    ) -> ColumnarRangeExecutionEvidence:
        if self.outcome_status != "quality_passed":
            raise ValueError("Only quality-passed evidence can cross the publication barrier.")
        return replace(
            self,
            outcome_status="succeeded",
            publication_receipt_sha256=publication_receipt_sha256,
            cleanup_status=cleanup_status,
        )

    def publication_unknown(self, *, failure_code: str) -> ColumnarRangeExecutionEvidence:
        if self.outcome_status != "quality_passed":
            raise ValueError("Publication unknown is valid only after quality passed.")
        return replace(
            self,
            outcome_status="publication_unknown",
            failure_code=failure_code,
            cleanup_status="preserved_unknown",
        )

    @classmethod
    def _create(
        cls,
        plan: ColumnarRangePlan,
        ranges: tuple[RangeEvidenceItem, ...],
        reader: int,
        upload: int,
        load: int,
        rows: int,
        bytes_: int,
        **outcome: Any,
    ) -> ColumnarRangeExecutionEvidence:
        if plan.execution_plan_fingerprint is None:
            raise ValueError("Execution evidence requires a runtime execution identity and fingerprint.")
        return cls(
            plan.plan_fingerprint,
            plan.execution_plan_fingerprint,
            plan.range_set_fingerprint,
            plan.policy,
            len(plan.ranges),
            ranges,
            reader,
            upload,
            load,
            rows,
            bytes_,
            **outcome,
        )

    @property
    def _all_staged(self) -> bool:
        return len(self.ranges) == self.planned_range_count and all(item.stage for item in self.ranges)

    @property
    def _all_eof(self) -> bool:
        return len(self.ranges) == self.planned_range_count and all(item.eof_confirmed for item in self.ranges)

    def _validate_success(self) -> None:
        if (
            self.failure_code
            or self.cancellation_requested
            or self.cancellation_observed
            or self.cleanup_status not in {"not_required", "completed"}
        ):
            raise ValueError("Success evidence cannot contain failure, cancellation, or cleanup failure.")
        _digest("publication_receipt_sha256", self.publication_receipt_sha256)

    def _validate_quality(self) -> None:
        _digest("quality_receipt_sha256", self.quality_receipt_sha256)
        if self.policy.staging_topology == "per_partition":
            _digest("assembly_receipt_sha256", self.assembly_receipt_sha256)
        elif self.assembly_receipt_sha256 is not None:
            _digest("assembly_receipt_sha256", self.assembly_receipt_sha256)

    def to_dict(self) -> dict[str, Any]:
        requested = {
            "reader": self.policy.reader_workers,
            "upload": self.policy.upload_workers,
            "load": self.policy.load_workers,
        }
        observed = {
            "reader": self.observed_reader_concurrency,
            "upload": self.observed_upload_concurrency,
            "load": self.observed_load_concurrency,
        }
        return {
            "schema": self.schema,
            "plan_fingerprint": self.plan_fingerprint,
            "execution_plan_fingerprint": self.execution_plan_fingerprint,
            "range_set_fingerprint": self.range_set_fingerprint,
            "policy_fingerprint": self.policy.fingerprint,
            "ranges": [item.to_dict() for item in self.ranges],
            "concurrency": {"requested": requested, "observed": observed},
            "consistency": {"mode": self.policy.consistency, "authority": dict(self.policy.consistency_authority)},
            "staging_topology": self.policy.staging_topology,
            "resource_high_water": {"rows": self.rows_high_water, "bytes": self.bytes_high_water},
            "all_ranges_eof_confirmed": self._all_eof,
            "all_ranges_stage_confirmed": self._all_staged,
            "receipts": {
                "assembly": self.assembly_receipt_sha256,
                "quality": self.quality_receipt_sha256,
                "publication": self.publication_receipt_sha256,
            },
            "outcome": {
                "status": self.outcome_status,
                "failure_code": self.failure_code,
                "cancellation_requested": self.cancellation_requested,
                "cancellation_observed": self.cancellation_observed,
                "cleanup": {"status": self.cleanup_status, "failures": list(self.cleanup_failures)},
            },
        }


def _ordered_items(
    plan: ColumnarRangePlan, results: Sequence[Any], *, require_complete: bool
) -> tuple[RangeEvidenceItem, ...]:
    by_id = {str(item.range_id): item for item in results}
    expected = tuple(item.range_id for item in plan.ranges)
    if (
        len(by_id) != len(results)
        or not set(by_id).issubset(expected)
        or (require_complete and set(by_id) != set(expected))
    ):
        raise ValueError("Range results must be unique members of the plan and satisfy the required coverage.")
    return tuple(_evidence_item(by_id[range_id]) for range_id in expected if range_id in by_id)


def _evidence_item(item: Any) -> RangeEvidenceItem:
    rows = _strict_int("range.rows", getattr(item, "rows", None))
    retained_bytes = _strict_int("range.retained_bytes", getattr(item, "retained_bytes", None))
    eof_confirmed = getattr(item, "eof_confirmed", None)
    chunks = getattr(item, "chunks", None)
    if not isinstance(eof_confirmed, bool):
        raise ValueError("range.eof_confirmed must be a boolean.")
    if not isinstance(chunks, tuple) or any(not isinstance(chunk, RangeChunkReceipt) for chunk in chunks):
        raise ValueError("range.chunks must be a tuple of RangeChunkReceipt values.")
    return RangeEvidenceItem(
        _nonempty_value("range.range_id", getattr(item, "range_id", None)),
        rows,
        retained_bytes,
        eof_confirmed,
        chunks,
    )


def _with_stages(
    ranges: tuple[RangeEvidenceItem, ...], receipts: Mapping[str, RangeStageReceipt]
) -> tuple[RangeEvidenceItem, ...]:
    if not set(receipts).issubset(item.range_id for item in ranges):
        raise ValueError("Stage receipts must belong to extracted ranges.")
    return tuple(replace(item, stage=receipts.get(item.range_id)) for item in ranges)


def _bounded_measurements(evidence: ColumnarRangeExecutionEvidence) -> None:
    policy = evidence.policy
    values = (
        ("observed_reader_concurrency", evidence.observed_reader_concurrency, policy.reader_workers),
        ("observed_upload_concurrency", evidence.observed_upload_concurrency, policy.upload_workers),
        ("observed_load_concurrency", evidence.observed_load_concurrency, policy.load_workers),
        ("rows_high_water", evidence.rows_high_water, policy.max_inflight_rows),
        ("bytes_high_water", evidence.bytes_high_water, policy.max_inflight_bytes),
    )
    for name, value, limit in values:
        _nonnegative(name, value)
        if value > limit:
            raise ValueError(f"{name} exceeds its configured execution bound.")
    complete = evidence.outcome_status in {"extracted", "staged", "quality_passed", "succeeded", "publication_unknown"}
    if complete and evidence.observed_reader_concurrency == 0:
        raise ValueError("Complete extraction requires observed reader concurrency.")
    if any(item.chunks for item in evidence.ranges) and evidence.observed_upload_concurrency == 0:
        raise ValueError("Chunk evidence requires observed upload concurrency.")
    if any(item.stage and item.rows > 0 for item in evidence.ranges) and evidence.observed_load_concurrency == 0:
        raise ValueError("Nonempty staging requires observed load concurrency.")
    if any(item.rows > 0 for item in evidence.ranges) and evidence.rows_high_water == 0:
        raise ValueError("Nonempty extraction requires a nonzero row high-water mark.")
    if any(item.retained_bytes > 0 for item in evidence.ranges) and evidence.bytes_high_water == 0:
        raise ValueError("Retained bytes require a nonzero byte high-water mark.")


def _validate_topology(evidence: ColumnarRangeExecutionEvidence) -> None:
    identities = [item.stage.stage_identity for item in evidence.ranges if item.stage]
    if evidence.policy.staging_topology == "shared_per_run" and len(set(identities)) > 1:
        raise ValueError("shared_per_run evidence requires one authoritative stage identity.")
    if evidence.policy.staging_topology == "per_partition" and len(set(identities)) != len(identities):
        raise ValueError("per_partition evidence requires unique range-owned stage identities.")


def _digest(name: str, value: object) -> None:
    if not is_canonical_sha256_digest(value):
        raise ValueError(f"{name} must be a canonical SHA-256 digest.")


def _code(name: str, value: str) -> None:
    if not value or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789_.-" for char in value):
        raise ValueError(f"{name} must be a stable redacted code.")


def _nonempty_value(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string.")
    return value


def _strict_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer.")
    return value


def _nonnegative(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer.")


__all__ = ["ColumnarRangeExecutionEvidence", "RangeChunkReceipt", "RangeEvidenceItem", "RangeStageReceipt"]
