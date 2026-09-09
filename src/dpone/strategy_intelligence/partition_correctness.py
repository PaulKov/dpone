"""Partition-level data correctness certification."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class PartitionCorrectnessObservation:
    """Source/target evidence for one transfer partition."""

    partition_id: str
    bounds: dict[str, Any]
    source_count: int
    target_count: int
    source_checksum: str
    target_checksum: str
    artifact_sha256: str | None = None
    source_sample_hash: str | None = None
    target_sample_hash: str | None = None
    source_full_hash: str | None = None
    target_full_hash: str | None = None
    source_typed_hash: str | None = None
    target_typed_hash: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PartitionCorrectnessResult:
    """Audit-friendly partition correctness result."""

    passed: bool
    partition_count: int
    failed_partition_count: int
    total_source_rows: int
    total_target_rows: int
    checks: tuple[dict[str, Any], ...]
    observations: tuple[PartitionCorrectnessObservation, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "partition_count": self.partition_count,
            "failed_partition_count": self.failed_partition_count,
            "total_source_rows": self.total_source_rows,
            "total_target_rows": self.total_target_rows,
            "checks": list(self.checks),
            "observations": [observation.to_dict() for observation in self.observations],
        }


class PartitionCorrectnessService:
    """Compare source and target partition observations."""

    def certify(
        self,
        observations: tuple[PartitionCorrectnessObservation, ...],
        *,
        profile: str = "count_and_checksum",
    ) -> PartitionCorrectnessResult:
        normalized_profile = _normalize_profile(profile)
        checks: list[dict[str, Any]] = []
        failed_partitions: set[str] = set()
        for observation in observations:
            count_passed = observation.source_count == observation.target_count
            checks.append(
                _check(
                    "partition_counts_match",
                    count_passed,
                    partition_id=observation.partition_id,
                    source_count=observation.source_count,
                    target_count=observation.target_count,
                )
            )
            profile_checks = self._profile_checks(observation, normalized_profile)
            checks.extend(profile_checks)
            if not count_passed or any(not check["passed"] for check in profile_checks):
                failed_partitions.add(observation.partition_id)

        return PartitionCorrectnessResult(
            passed=all(check["passed"] for check in checks),
            partition_count=len(observations),
            failed_partition_count=len(failed_partitions),
            total_source_rows=sum(observation.source_count for observation in observations),
            total_target_rows=sum(observation.target_count for observation in observations),
            checks=tuple(checks),
            observations=observations,
        )

    def _profile_checks(
        self,
        observation: PartitionCorrectnessObservation,
        profile: str,
    ) -> tuple[dict[str, Any], ...]:
        if profile == "count_only":
            return ()
        if profile == "sample_hash":
            return (
                _checksum_check(observation),
                _check(
                    "partition_sample_hashes_match",
                    bool(observation.source_sample_hash)
                    and bool(observation.target_sample_hash)
                    and observation.source_sample_hash == observation.target_sample_hash,
                    partition_id=observation.partition_id,
                    source_sample_hash=observation.source_sample_hash,
                    target_sample_hash=observation.target_sample_hash,
                ),
            )
        if profile == "full_partition_hash":
            return (
                _check(
                    "partition_full_hashes_match",
                    bool(observation.source_full_hash)
                    and bool(observation.target_full_hash)
                    and observation.source_full_hash == observation.target_full_hash,
                    partition_id=observation.partition_id,
                    source_full_hash=observation.source_full_hash,
                    target_full_hash=observation.target_full_hash,
                ),
            )
        if profile == "typed_hash":
            return (
                _checksum_check(observation),
                _check(
                    "partition_typed_hashes_match",
                    bool(observation.source_typed_hash)
                    and bool(observation.target_typed_hash)
                    and observation.source_typed_hash == observation.target_typed_hash,
                    partition_id=observation.partition_id,
                    source_typed_hash=observation.source_typed_hash,
                    target_typed_hash=observation.target_typed_hash,
                ),
            )
        return (_checksum_check(observation),)


def _checksum_check(observation: PartitionCorrectnessObservation) -> dict[str, Any]:
    return _check(
        "partition_checksums_match",
        observation.source_checksum == observation.target_checksum,
        partition_id=observation.partition_id,
        source_checksum=observation.source_checksum,
        target_checksum=observation.target_checksum,
    )


def _normalize_profile(value: str) -> str:
    normalized = str(value or "count_and_checksum").strip().lower()
    aliases = {
        "count": "count_only",
        "count_sum": "count_and_checksum",
        "count_and_sum": "count_and_checksum",
        "checksum": "count_and_checksum",
        "full_hash": "full_partition_hash",
        "typed": "typed_hash",
    }
    resolved = aliases.get(normalized, normalized)
    if resolved not in {"count_only", "count_and_checksum", "sample_hash", "full_partition_hash", "typed_hash"}:
        raise ValueError(
            "Unsupported partition correctness profile. "
            "Use count_only, count_and_checksum, sample_hash, full_partition_hash, or typed_hash."
        )
    return resolved


def _check(name: str, passed: bool, **details: Any) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), "details": details}
