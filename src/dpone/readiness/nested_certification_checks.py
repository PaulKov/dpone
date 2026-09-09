"""Readiness facade for reusable nested normalization certification checks."""

from __future__ import annotations

import json
from pathlib import Path

from dpone.readiness.nested_live_certification import (
    NESTED_LIVE_REQUIRED_CHECKS,
    NestedLiveCertificationCase,
    NestedLiveCertificationRunner,
)
from dpone.runtime.normalization.certification_checks import (
    child_delete_diff_passes,
    child_finalizers_pass,
    child_identity_is_stable,
    child_options,
    child_quality_passes,
    child_schema_evolution_contract,
    native_fast_path_execution_contract,
    native_fast_path_passes,
    native_spill_passes,
    reverse_readback_passes,
    snapshot_store_passes,
    sql_snapshot_store_passes,
)


def live_certification_harness_observation(*, output_dir: Path) -> dict[str, object]:
    """Exercise artifact rendering without fabricating live data-path checks."""

    artifact = NestedLiveCertificationRunner(
        cases=(
            NestedLiveCertificationCase(sink_type="mssql", available=lambda: False, check=lambda: {}),
            NestedLiveCertificationCase(sink_type="clickhouse", available=lambda: False, check=lambda: {}),
            NestedLiveCertificationCase(sink_type="bigquery", available=lambda: False, check=lambda: {}),
        )
    ).run(output_dir=output_dir / "live_harness")
    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))
    return {
        "status": "unverified",
        "reason": "local harness rendering ran, but no live source-to-sink data path was executed",
        "artifact_status": payload.get("status"),
        "required_checks": list(NESTED_LIVE_REQUIRED_CHECKS),
        "sinks": payload.get("sinks", {}),
    }


def live_certification_harness_passes(*, output_dir: Path) -> bool:
    """Compatibility predicate: an unverified harness never passes."""

    return live_certification_harness_observation(output_dir=output_dir)["status"] == "passed"


__all__ = [
    "child_delete_diff_passes",
    "child_finalizers_pass",
    "child_identity_is_stable",
    "child_options",
    "child_quality_passes",
    "child_schema_evolution_contract",
    "live_certification_harness_observation",
    "live_certification_harness_passes",
    "native_fast_path_execution_contract",
    "native_fast_path_passes",
    "native_spill_passes",
    "reverse_readback_passes",
    "snapshot_store_passes",
    "sql_snapshot_store_passes",
]
