"""Closed reviewed-suite registry and deterministic inventory builder."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .contract import INVENTORY_VERSION, REQUIRED_SUITE_IDS, fail
from .reviewed_case import ReviewedSuite
from .reviewed_cases_physical import physical_design_suite
from .reviewed_cases_runtime import (
    artifact_integrity_suite,
    backfill_orchestration_suite,
    source_identity_suite,
    target_behavior_suite,
    target_database_suite,
    target_identity_suite,
    transaction_governance_suite,
    xmin_reconciliation_suite,
)
from .reviewed_cases_schema import schema_evolution_suite
from .reviewed_cases_strategy import (
    lineage_parity_suite,
    strategy_capability_suite,
    text_key_lifecycle_suite,
    wide_performance_soak_suite,
    wide_strategy_suite,
)
from .reviewed_cases_types import boundary_type_suite, explicit_type_suite, postgis_type_suite
from .vendors import RELEASE_IMAGE_DIGESTS, enforce_image_digest_allowlist, parse_vendor_policy


def release_suites() -> tuple[ReviewedSuite, ...]:
    """Build the exact finite release authority in canonical suite order."""

    suites = (
        artifact_integrity_suite(),
        backfill_orchestration_suite(),
        boundary_type_suite(),
        explicit_type_suite(),
        lineage_parity_suite(),
        physical_design_suite(),
        postgis_type_suite(),
        schema_evolution_suite(),
        source_identity_suite(),
        strategy_capability_suite(),
        target_behavior_suite(),
        target_database_suite(),
        target_identity_suite(),
        text_key_lifecycle_suite(),
        transaction_governance_suite(),
        wide_performance_soak_suite(),
        wide_strategy_suite(),
        xmin_reconciliation_suite(),
    )
    return validate_reviewed_suites(suites, required_suite_ids=REQUIRED_SUITE_IDS)


def validate_reviewed_suites(
    suites: Iterable[ReviewedSuite],
    *,
    required_suite_ids: tuple[str, ...],
) -> tuple[ReviewedSuite, ...]:
    """Reject duplicate, unordered, incomplete, or path-colliding registries."""

    ordered = tuple(sorted(suites, key=lambda value: value.suite_id))
    suite_ids = tuple(value.suite_id for value in ordered)
    if suite_ids != tuple(sorted(required_suite_ids)):
        fail("reviewed_registry.required_suites_mismatch")
    entries = tuple(value.inventory_entry() for value in ordered)
    evidence_files = tuple(str(value["evidence_file"]) for value in entries)
    junit_files = tuple(str(value["junit_file"]) for value in entries)
    if (
        len(suite_ids) != len(set(suite_ids))
        or len(evidence_files) != len(set(evidence_files))
        or len(junit_files) != len(set(junit_files))
    ):
        fail("reviewed_registry.duplicate")
    return ordered


def build_inventory(
    vendors: Mapping[str, Any],
    *,
    suites: Iterable[ReviewedSuite] | None = None,
    required_suite_ids: tuple[str, ...] = REQUIRED_SUITE_IDS,
    allowed_image_digests: Mapping[str, str] = RELEASE_IMAGE_DIGESTS,
) -> dict[str, object]:
    """Render a strict inventory from reviewed source and observed vendor IDs."""

    reviewed = (
        release_suites()
        if suites is None
        else validate_reviewed_suites(
            suites,
            required_suite_ids=required_suite_ids,
        )
    )
    vendor_policy = parse_vendor_policy(dict(vendors))
    enforce_image_digest_allowlist(vendor_policy, allowed=allowed_image_digests)
    return {
        "schema_version": INVENTORY_VERSION,
        "route": "postgres_mssql",
        "vendors": vendor_policy,
        "suites": [suite.inventory_entry() for suite in reviewed],
    }


def reviewed_inventory_entries() -> tuple[dict[str, object], ...]:
    """Return exact suite entries used by the release CLI validator."""

    return tuple(suite.inventory_entry() for suite in release_suites())


__all__ = [
    "build_inventory",
    "release_suites",
    "reviewed_inventory_entries",
    "validate_reviewed_suites",
]
