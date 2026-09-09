from __future__ import annotations

import argparse
from pathlib import Path

from dpone.services.docs.errors import DocsConfigurationError

from ...metrics.import_graph import collect_internal_deps
from ...metrics.layer_metrics import compute_layer_coupling_stats
from ...metrics.layer_metrics_gate import (
    LayerMetricsThresholds,
    build_layer_metrics_snapshot,
    evaluate_layer_metrics_gate,
    format_layer_metrics_gate_jsonable,
    format_layer_metrics_gate_text,
    load_layer_metrics_baseline,
    write_layer_metrics_baseline,
)
from ...metrics.loc import iter_py_files
from .context import DocsServiceContext


class CheckLayerMetricsService:
    """Run CI-friendly checks for coarse layer/slice architecture metrics."""

    def __init__(self, *, ctx: DocsServiceContext):
        self.ctx = ctx
        self.log = ctx.logger

    def run(self, args: argparse.Namespace) -> tuple[int, str | dict]:
        fmt = str(getattr(args, "format", "text") or "text").strip().lower()
        package = str(getattr(args, "package", "src/dpone") or "src/dpone")
        package_dir = (self.ctx.settings.repo_root / package).resolve()
        if not package_dir.exists():
            raise DocsConfigurationError(f"Package dir not found: {package_dir}")
        if package_dir.name != "dpone":
            raise DocsConfigurationError("Only src/dpone package is supported for now")

        exclude_layers = [str(x).strip() for x in list(getattr(args, "exclude_layer", []) or []) if str(x).strip()]
        top_n = int(getattr(args, "top", 15) or 15)
        if top_n < 1:
            raise DocsConfigurationError("--top must be >= 1")

        deps_out = collect_internal_deps(
            package_dir,
            module_files=list(iter_py_files(package_dir)),
            package_name="dpone",
        )
        layer_stats = compute_layer_coupling_stats(
            deps_out,
            top_n=max(5, min(50, top_n)),
            package_name="dpone",
            exclude_layers=exclude_layers,
        )
        snapshot = build_layer_metrics_snapshot(layer_stats, package_name="dpone")

        baseline_path = self._resolve_baseline_path(args)
        if bool(getattr(args, "write_baseline", False)):
            if baseline_path is None:
                raise DocsConfigurationError("--write-baseline requires --baseline (or omit --no-baseline)")
            write_layer_metrics_baseline(baseline_path, snapshot)
            payload: str | dict
            if fmt == "json":
                payload = {
                    "written": str(baseline_path),
                    "baseline": format_layer_metrics_gate_jsonable(
                        evaluate_layer_metrics_gate(
                            snapshot,
                            thresholds=LayerMetricsThresholds(),
                            baseline=None,
                        )
                    )["current"],
                }
            elif fmt == "text":
                payload = (
                    f"Wrote layer metrics baseline: {baseline_path}\n"
                    f"Current cross_layer_ratio={snapshot.cross_layer_ratio:.3f}, "
                    f"intra_layer_ratio={snapshot.intra_layer_ratio:.3f}, "
                    f"max_cross_flow={snapshot.max_cross_layer_flow}\n"
                )
            else:
                raise DocsConfigurationError("--format must be text or json")
            self.log.info("Wrote layer metrics baseline: %s", baseline_path)
            return 0, payload

        baseline = None
        if baseline_path is not None:
            if not self.ctx.fs.exists(baseline_path):
                raise DocsConfigurationError(f"Baseline file not found: {baseline_path}")
            baseline = load_layer_metrics_baseline(baseline_path)

        thresholds = LayerMetricsThresholds(
            min_intra_layer_ratio=self._as_optional_float(getattr(args, "min_intra_layer_ratio", None)),
            max_cross_layer_ratio=self._as_optional_float(getattr(args, "max_cross_layer_ratio", None)),
            max_top_cross_layer_flow=self._as_optional_int(getattr(args, "max_top_cross_layer_flow", None)),
            allowed_ratio_regression=float(getattr(args, "allowed_ratio_regression", 0.0) or 0.0),
            allowed_flow_regression=int(getattr(args, "allowed_flow_regression", 0) or 0),
        )
        report = evaluate_layer_metrics_gate(snapshot, thresholds=thresholds, baseline=baseline)

        if fmt == "json":
            payload = format_layer_metrics_gate_jsonable(report)
        elif fmt == "text":
            payload = format_layer_metrics_gate_text(report, package_dir=package_dir)
        else:
            raise DocsConfigurationError("--format must be text or json")

        exit_code = 0 if report.ok else 2
        if exit_code:
            self.log.error("Layer metrics gate failed: %s issue(s)", len(report.issues))
        else:
            self.log.info("Layer metrics gate OK")
        return exit_code, payload

    def _resolve_baseline_path(self, args: argparse.Namespace) -> Path | None:
        if bool(getattr(args, "no_baseline", False)):
            return None
        raw = str(getattr(args, "baseline", "") or "").strip()
        if not raw:
            return None
        path = Path(raw)
        if not path.is_absolute():
            path = (self.ctx.settings.repo_root / path).resolve()
        return path

    @staticmethod
    def _as_optional_float(value: object) -> float | None:
        if value is None:
            return None
        if isinstance(value, str):
            if not value.strip():
                return None
            return float(value)
        if isinstance(value, int | float):
            return float(value)
        raise DocsConfigurationError(f"Expected a numeric value, got {type(value).__name__}")

    @staticmethod
    def _as_optional_int(value: object) -> int | None:
        if value is None:
            return None
        if isinstance(value, str):
            if not value.strip():
                return None
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        raise DocsConfigurationError(f"Expected an integer value, got {type(value).__name__}")
