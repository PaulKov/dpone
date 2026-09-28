"""Fail-closed validation helpers for columnar range execution evidence."""

from __future__ import annotations

import re
from typing import Any


def validate_bounded_measurements(evidence: Any) -> None:
    policy = evidence.policy
    values = (
        ("observed_reader_concurrency", evidence.observed_reader_concurrency, policy.reader_workers),
        ("observed_upload_concurrency", evidence.observed_upload_concurrency, policy.upload_workers),
        ("observed_load_concurrency", evidence.observed_load_concurrency, policy.load_workers),
        ("rows_high_water", evidence.rows_high_water, policy.max_inflight_rows),
        ("bytes_high_water", evidence.bytes_high_water, policy.max_inflight_bytes),
    )
    for name, value, limit in values:
        validate_nonnegative(name, value)
        if value > limit:
            raise ValueError(f"{name} exceeds its configured execution bound.")
    complete = evidence.outcome_status in {
        "extracted",
        "staged",
        "quality_passed",
        "published",
        "succeeded",
        "publication_unknown",
    }
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


def validate_topology(evidence: Any) -> None:
    identities = [item.stage.stage_identity for item in evidence.ranges if item.stage]
    if evidence.policy.staging_topology == "shared_per_run" and len(set(identities)) > 1:
        raise ValueError("shared_per_run evidence requires one authoritative stage identity.")
    if evidence.policy.staging_topology == "per_partition" and len(set(identities)) != len(identities):
        raise ValueError("per_partition evidence requires unique range-owned stage identities.")


def validate_digest(name: str, value: object) -> None:
    if not isinstance(value, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a canonical SHA-256 digest.")


def validate_code(name: str, value: str) -> None:
    if not value or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789_.-" for char in value):
        raise ValueError(f"{name} must be a stable redacted code.")


def nonempty_value(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string.")
    return value


def strict_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer.")
    return value


def validate_nonnegative(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer.")
