from __future__ import annotations

from dpone.strategy_intelligence.partition_correctness import (
    PartitionCorrectnessObservation,
    PartitionCorrectnessService,
)


def test_partition_correctness_passes_when_counts_and_checksums_match() -> None:
    result = PartitionCorrectnessService().certify(
        (
            PartitionCorrectnessObservation(
                partition_id="p0",
                bounds={"lower": 1, "upper": 100},
                source_count=100,
                target_count=100,
                source_checksum="100:5050:sha",
                target_checksum="100:5050:sha",
                artifact_sha256="sha256:abc",
            ),
        )
    )

    assert result.passed
    assert result.total_source_rows == 100
    assert result.total_target_rows == 100
    assert result.failed_partition_count == 0


def test_partition_correctness_count_only_profile_ignores_checksum_differences() -> None:
    result = PartitionCorrectnessService().certify(
        (
            PartitionCorrectnessObservation(
                partition_id="p0",
                bounds={"lower": 1, "upper": 100},
                source_count=100,
                target_count=100,
                source_checksum="100:5050:source",
                target_checksum="100:5050:target",
            ),
        ),
        profile="count_only",
    )

    assert result.passed
    assert {check["name"] for check in result.checks} == {"partition_counts_match"}


def test_partition_correctness_sample_hash_profile_requires_matching_samples() -> None:
    result = PartitionCorrectnessService().certify(
        (
            PartitionCorrectnessObservation(
                partition_id="p0",
                bounds={"lower": 1, "upper": 100},
                source_count=100,
                target_count=100,
                source_checksum="100:5050:sha",
                target_checksum="100:5050:sha",
                source_sample_hash="sample-a",
                target_sample_hash="sample-b",
            ),
        ),
        profile="sample_hash",
    )

    assert not result.passed
    assert any(check["name"] == "partition_sample_hashes_match" for check in result.checks if not check["passed"])


def test_partition_correctness_full_hash_profile_requires_full_partition_hash() -> None:
    result = PartitionCorrectnessService().certify(
        (
            PartitionCorrectnessObservation(
                partition_id="p0",
                bounds={"lower": 1, "upper": 100},
                source_count=100,
                target_count=100,
                source_checksum="100:5050:sha",
                target_checksum="100:5050:sha",
                source_full_hash="full-a",
                target_full_hash="full-a",
            ),
        ),
        profile="full_partition_hash",
    )

    assert result.passed
    assert any(check["name"] == "partition_full_hashes_match" for check in result.checks)


def test_partition_correctness_typed_hash_profile_requires_typed_hash_match() -> None:
    result = PartitionCorrectnessService().certify(
        (
            PartitionCorrectnessObservation(
                partition_id="p0",
                bounds={"lower": 1, "upper": 100},
                source_count=100,
                target_count=100,
                source_checksum="100:5050:sha",
                target_checksum="100:5050:sha",
                source_typed_hash="typed-a",
                target_typed_hash="typed-b",
            ),
        ),
        profile="typed_hash",
    )

    assert not result.passed
    assert any(check["name"] == "partition_typed_hashes_match" for check in result.checks if not check["passed"])


def test_partition_correctness_fails_on_count_or_checksum_mismatch() -> None:
    result = PartitionCorrectnessService().certify(
        (
            PartitionCorrectnessObservation(
                partition_id="p0",
                bounds={"lower": 1, "upper": 100},
                source_count=100,
                target_count=99,
                source_checksum="100:5050:sha",
                target_checksum="99:5000:sha",
                artifact_sha256="sha256:abc",
            ),
        )
    )

    assert not result.passed
    assert result.failed_partition_count == 1
    failed_checks = {check["name"] for check in result.checks if not check["passed"]}
    assert failed_checks == {"partition_counts_match", "partition_checksums_match"}
