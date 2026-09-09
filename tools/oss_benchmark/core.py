"""Compatibility facade for the OSS benchmark package.

Business logic lives in focused modules. This file intentionally re-exports the
historical API used by tests, docs and `tools/oss_code_quality_benchmark.py`.
"""

from __future__ import annotations

from tools.oss_benchmark.cli import load_previous_payload, main, parse_args
from tools.oss_benchmark.maintainability import (
    compute_architecture_risk,
    compute_coverage_confidence,
    compute_industrial_maintainability_index,
    detect_ci_evidence,
    enrich_payload_with_maintainability,
)
from tools.oss_benchmark.outputs import (
    build_history_entry,
    load_history_payload,
    update_history_payload,
    write_benchmark_outputs,
    write_history_payload,
    write_release_readiness_outputs,
)
from tools.oss_benchmark.payload_builder import build_benchmark_payload, project_to_jsonable

__all__ = [
    "build_benchmark_payload",
    "build_history_entry",
    "compute_architecture_risk",
    "compute_coverage_confidence",
    "compute_industrial_maintainability_index",
    "detect_ci_evidence",
    "enrich_payload_with_maintainability",
    "load_history_payload",
    "load_previous_payload",
    "main",
    "parse_args",
    "project_to_jsonable",
    "update_history_payload",
    "write_benchmark_outputs",
    "write_history_payload",
    "write_release_readiness_outputs",
]
