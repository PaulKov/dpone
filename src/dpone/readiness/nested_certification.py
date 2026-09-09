"""Nested normalization certification and benchmark artifacts."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from dpone.readiness.nested_certification_artifacts import NestedEvidenceArtifact, write_nested_artifact
from dpone.readiness.nested_certification_checks import (
    child_delete_diff_passes,
    child_finalizers_pass,
    child_identity_is_stable,
    child_quality_passes,
    child_schema_evolution_contract,
    live_certification_harness_observation,
    native_fast_path_execution_contract,
    native_fast_path_passes,
    native_spill_passes,
    reverse_readback_passes,
    snapshot_store_passes,
    sql_snapshot_store_passes,
)
from dpone.readiness.nested_lint import NestedNormalizationLintService
from dpone.runtime.normalization.options import NestedNormalizationOptions
from dpone.runtime.normalization.spill import SpillToDiskNormalizationService


class NestedNormalizationCertificationService:
    """Run local deterministic nested normalization certification checks."""

    def certify(
        self, *, output_dir: str | Path, row_count: int = 1000, root_table: str = "orders"
    ) -> NestedEvidenceArtifact:
        output = Path(output_dir)
        output.mkdir(parents=True, exist_ok=True)
        rows = list(_synthetic_rows(row_count, include_quarantine=True))
        options = NestedNormalizationOptions.from_config(
            {
                "enabled": True,
                "paths": {"internal_payload": "quarantine"},
                "split_paths": [
                    {
                        "path": "$.items[*]",
                        "table": "order_lines",
                        "unique_key": ["order_id", "sku"],
                        "delete_policy": "replace_parent_children",
                    }
                ],
                "guardrails": {"max_array_length": 16, "max_child_tables": 16, "max_rows_per_root": 256},
                "raw_landing": True,
            }
        )
        lint = NestedNormalizationLintService().lint_config(
            {
                "enabled": True,
                "paths": {"internal_payload": "quarantine"},
                "split_paths": [
                    {
                        "path": "$.items[*]",
                        "table": "order_lines",
                        "unique_key": ["order_id", "sku"],
                        "delete_policy": "replace_parent_children",
                    }
                ],
                "guardrails": {"max_array_length": 16, "max_child_tables": 16, "max_rows_per_root": 256},
                "raw_landing": True,
            },
            root_table=root_table,
        )
        spilled = SpillToDiskNormalizationService().spill_rows(
            rows, root_table=root_table, options=options, output_dir=output / "spill"
        )
        reverse_passed = reverse_readback_passes(rows, root_table=root_table)
        child_identity_passed = child_identity_is_stable(root_table=root_table)
        child_delete_passed = child_delete_diff_passes(root_table=root_table)
        child_finalizers_passed = child_finalizers_pass(root_table=root_table)
        snapshot_store_passed = snapshot_store_passes(output_dir=output)
        sql_snapshot_store_passed = sql_snapshot_store_passes()
        native_spill_passed = native_spill_passes(output_dir=output, root_table=root_table)
        native_fast_path_passed = native_fast_path_passes()
        child_quality_passed = child_quality_passes(root_table=root_table)
        live_harness = live_certification_harness_observation(output_dir=output)
        child_schema_evolution = child_schema_evolution_contract()
        native_fast_path_execution = native_fast_path_execution_contract()
        checks = {
            "lint": "passed" if not lint.has_errors else "failed",
            "spill_to_disk": "passed" if spilled.total_rows >= row_count else "failed",
            "native_spill_formats": "passed" if native_spill_passed else "failed",
            "native_fast_path_handoff": "passed" if native_fast_path_passed else "failed",
            "reverse_readback": "passed" if reverse_passed else "failed",
            "child_identity": "passed" if child_identity_passed else "failed",
            "child_reconciliation": "passed" if child_delete_passed else "failed",
            "child_delete_finalizers": "passed" if child_finalizers_passed else "failed",
            "child_snapshot_store": "passed" if snapshot_store_passed else "failed",
            "sql_child_snapshot_store": "passed" if sql_snapshot_store_passed else "failed",
            "child_quality": "passed" if child_quality_passed else "failed",
            "live_certification_harness": live_harness["status"],
            "child_schema_evolution_contract": child_schema_evolution["status"],
            "native_fast_path_execution_contract": native_fast_path_execution["status"],
            "quarantine": "passed" if f"{root_table}__quarantine" in spilled.row_counts else "failed",
            "raw_landing": "passed" if f"{root_table}__raw" in spilled.row_counts else "failed",
        }
        payload = {
            "status": _certification_status(checks),
            "row_count": row_count,
            "normalized_rows": spilled.total_rows,
            "checks": checks,
            "row_counts": spilled.row_counts,
            "spill_formats": spilled.formats,
            "lint": lint.to_dict(),
            "industrial_evidence": {
                "child_schema_evolution": child_schema_evolution,
                "native_fast_path_execution": native_fast_path_execution,
                "live_certification_harness": live_harness,
            },
        }
        return write_nested_artifact(output, "nested_normalization_certification", payload)


class NestedNormalizationBenchmarkService:
    """Run deterministic local nested normalization benchmark."""

    def __init__(self, *, max_rows: int = 100000) -> None:
        self.max_rows = max_rows

    def run(
        self, *, output_dir: str | Path, row_count: int = 10000, root_table: str = "orders"
    ) -> NestedEvidenceArtifact:
        if row_count > self.max_rows:
            raise ValueError(f"row_count={row_count} is above max_rows={self.max_rows}")
        output = Path(output_dir)
        output.mkdir(parents=True, exist_ok=True)
        options = NestedNormalizationOptions.from_config(
            {
                "enabled": True,
                "guardrails": {"max_array_length": 16, "max_child_tables": 16, "max_rows_per_root": 256},
            }
        )
        started = time.perf_counter()
        spilled = SpillToDiskNormalizationService().spill_rows(
            _synthetic_rows(row_count, include_quarantine=False),
            root_table=root_table,
            options=options,
            output_dir=output / "benchmark_spill",
            unique_key=["order_id"],
        )
        elapsed = max(time.perf_counter() - started, 0.000001)
        payload = {
            "status": "passed",
            "row_count": row_count,
            "normalized_rows": spilled.total_rows,
            "elapsed_seconds": elapsed,
            "rows_per_second": row_count / elapsed,
            "normalized_rows_per_second": spilled.total_rows / elapsed,
            "row_counts": spilled.row_counts,
            "spill_formats": spilled.formats,
        }
        return write_nested_artifact(output, "nested_normalization_benchmark", payload)


def _synthetic_rows(row_count: int, *, include_quarantine: bool) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index in range(row_count):
        row = {
            "order_id": index + 1,
            "customer": {"customer_id": f"c-{index % 17}", "segment": "vip" if index % 5 == 0 else "base"},
            "items": [
                {"sku": f"sku-{index % 97}", "qty": index % 4 + 1},
                {"sku": f"sku-{(index + 1) % 97}", "qty": 1},
            ],
            "tags": ["new", "promo"] if index % 3 == 0 else ["new"],
        }
        if include_quarantine:
            row["internal_payload"] = {"trace_id": f"trace-{index}"}
        rows.append(row)
    return rows


def _certification_status(checks: dict[str, object]) -> str:
    statuses = {str(value) for value in checks.values()}
    if "failed" in statuses:
        return "failed"
    if statuses == {"passed"}:
        return "passed"
    return "unverified"
