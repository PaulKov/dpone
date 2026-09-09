"""Service for the architecture fitness documentation gate."""

from __future__ import annotations

from argparse import Namespace
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from dpone.metrics.architecture_fitness import (
    ArchitectureFitnessThresholds,
    analyze_architecture_fitness,
    format_architecture_fitness_report_jsonable,
    format_architecture_fitness_report_text,
)

from .context import DocsServiceContext


@dataclass(slots=True)
class CheckArchitectureFitnessService:
    ctx: DocsServiceContext

    def run(self, args: Namespace) -> tuple[int, dict[str, Any] | str]:
        repo_root = self.ctx.settings.repo_root
        package = Path(args.package)
        package_dir = package if package.is_absolute() else repo_root / package
        max_avg_clustering = _resolve_max_avg_clustering(
            repo_root=repo_root,
            explicit_value=args.max_avg_clustering,
        )
        thresholds = ArchitectureFitnessThresholds(
            target_avg_clustering=float(args.target_avg_clustering),
            max_avg_clustering=max_avg_clustering,
            max_cross_layer_ratio=float(args.max_cross_layer_ratio),
            target_module_ce=int(args.target_module_ce),
            max_module_ce=int(args.max_module_ce),
            max_class_methods=int(args.max_class_methods),
            max_class_loc=int(args.max_class_loc),
            fail_on_class_warnings=bool(args.fail_on_class_warnings),
        )
        report = analyze_architecture_fitness(
            package_dir,
            repo_root=repo_root,
            thresholds=thresholds,
            top_n=int(args.top),
        )
        payload = format_architecture_fitness_report_jsonable(report)
        if args.format == "json":
            return (0 if report.ok else 1), payload
        return (0 if report.ok else 1), format_architecture_fitness_report_text(report)


def _resolve_max_avg_clustering(*, repo_root: Path, explicit_value: float | None) -> float:
    if explicit_value is not None:
        return float(explicit_value)
    budget_path = repo_root / "docs" / "benchmarks" / "quality_budgets.yml"
    raw = yaml.safe_load(budget_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"invalid quality budget document: {budget_path}")
    global_budget = raw.get("global")
    if not isinstance(global_budget, dict) or "max_avg_clustering" not in global_budget:
        raise ValueError(f"missing global.max_avg_clustering in {budget_path}")
    value = float(global_budget["max_avg_clustering"])
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"invalid global.max_avg_clustering in {budget_path}: {value}")
    return value
