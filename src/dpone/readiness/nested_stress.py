"""Stress certification artifacts for nested normalization."""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from dpone.readiness.nested_certification import NestedEvidenceArtifact
from dpone.runtime.normalization.options import NestedNormalizationOptions
from dpone.runtime.normalization.spill import SpillToDiskNormalizationService


class NestedStressCertificationService:
    """Generate deterministic skewed nested payload stress evidence."""

    def __init__(self, *, max_rows: int = 100000) -> None:
        self.max_rows = max_rows

    def run(
        self,
        *,
        output_dir: str | Path,
        row_count: int = 100000,
        root_table: str = "orders",
        skew_pattern: Sequence[int] = (0, 1, 10, 1000),
        spill_output_format: str = "jsonl",
    ) -> NestedEvidenceArtifact:
        if row_count > self.max_rows:
            raise ValueError(f"row_count={row_count} is above max_rows={self.max_rows}")
        output = Path(output_dir)
        output.mkdir(parents=True, exist_ok=True)
        options = NestedNormalizationOptions.from_config(
            {
                "enabled": True,
                "materialization": "spill_to_disk",
                "spill_output_format": spill_output_format,
                "split_paths": [{"path": "$.items[*]", "table": "order_lines", "unique_key": ["order_id", "sku"]}],
                "guardrails": {
                    "max_array_length": max(max(skew_pattern), 1),
                    "max_child_tables": 16,
                    "max_rows_per_root": max(max(skew_pattern), 1) + 8,
                },
            }
        )
        started = time.perf_counter()
        result = SpillToDiskNormalizationService().spill_rows(
            _stress_rows(row_count=row_count, skew_pattern=tuple(skew_pattern)),
            root_table=root_table,
            options=options,
            output_dir=output / "stress_spill",
            unique_key=["order_id"],
            output_format=spill_output_format,
        )
        elapsed = max(time.perf_counter() - started, 0.000001)
        payload = {
            "status": "passed",
            "row_count": row_count,
            "normalized_rows": result.total_rows,
            "row_counts": result.row_counts,
            "spill_formats": result.formats,
            "skew_pattern": list(skew_pattern),
            "max_child_rows_per_parent": max(skew_pattern) if skew_pattern else 0,
            "elapsed_seconds": elapsed,
            "root_rows_per_second": row_count / elapsed,
            "normalized_rows_per_second": result.total_rows / elapsed,
        }
        return _write_artifact(output, "nested_stress_certification", payload)


def _stress_rows(*, row_count: int, skew_pattern: tuple[int, ...]) -> list[dict[str, Any]]:
    pattern = skew_pattern or (1,)
    rows: list[dict[str, Any]] = []
    for index in range(row_count):
        child_count = pattern[index % len(pattern)]
        rows.append(
            {
                "order_id": index + 1,
                "customer": {"customer_id": f"c-{index % 997}", "segment": "vip" if index % 11 == 0 else "base"},
                "items": [
                    {
                        "sku": f"sku-{index}-{child_index}",
                        "qty": child_index % 7 + 1,
                        "note": None if child_index % 5 else f"sparse-{index}-{child_index}",
                    }
                    for child_index in range(child_count)
                ],
                "attrs": {"sparse_a": None if index % 3 else index, "sparse_b": None if index % 7 else f"b-{index}"},
            }
        )
    return rows


def _write_artifact(output_dir: Path, stem: str, payload: dict[str, Any]) -> NestedEvidenceArtifact:
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    markdown_path.write_text(_markdown(stem, payload), encoding="utf-8")
    return NestedEvidenceArtifact(json_path=json_path, markdown_path=markdown_path)


def _markdown(title: str, payload: dict[str, Any]) -> str:
    lines = [f"# {title.replace('_', ' ').title()}", "", f"Status: **{payload.get('status')}**", ""]
    lines.append("```json")
    lines.append(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    lines.append("```")
    return "\n".join(lines) + "\n"
