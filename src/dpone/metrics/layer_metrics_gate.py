from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import LayerCouplingStats


@dataclass(frozen=True)
class LayerMetricsSnapshot:
    package_name: str
    excluded_layers: list[str]
    layers: int
    internal_edges: int
    intra_layer_edges: int
    cross_layer_edges: int
    intra_layer_ratio: float
    cross_layer_ratio: float
    max_cross_layer_flow: int
    top_cross_layer: list[tuple[str, str, int]]
    top_cross_slice: list[tuple[str, str, int]]


@dataclass(frozen=True)
class LayerMetricsThresholds:
    min_intra_layer_ratio: float | None = None
    max_cross_layer_ratio: float | None = None
    max_top_cross_layer_flow: int | None = None
    allowed_ratio_regression: float = 0.0
    allowed_flow_regression: int = 0


@dataclass(frozen=True)
class LayerMetricsIssue:
    code: str
    severity: str
    message: str
    current: float | int | None = None
    threshold: float | int | None = None
    baseline: float | int | None = None


@dataclass(frozen=True)
class LayerMetricsGateReport:
    ok: bool
    snapshot: LayerMetricsSnapshot
    thresholds: LayerMetricsThresholds
    issues: list[LayerMetricsIssue]
    baseline: LayerMetricsSnapshot | None = None


def build_layer_metrics_snapshot(
    layer_stats: LayerCouplingStats,
    *,
    package_name: str,
) -> LayerMetricsSnapshot:
    max_cross_layer_flow = layer_stats.top_cross_layer[0][2] if layer_stats.top_cross_layer else 0
    return LayerMetricsSnapshot(
        package_name=package_name,
        excluded_layers=list(layer_stats.excluded_layers),
        layers=int(layer_stats.layers),
        internal_edges=int(layer_stats.internal_edges),
        intra_layer_edges=int(layer_stats.intra_layer_edges),
        cross_layer_edges=int(layer_stats.cross_layer_edges),
        intra_layer_ratio=float(layer_stats.intra_layer_ratio),
        cross_layer_ratio=float(layer_stats.cross_layer_ratio),
        max_cross_layer_flow=int(max_cross_layer_flow),
        top_cross_layer=[(str(src), str(dst), int(count)) for src, dst, count in layer_stats.top_cross_layer],
        top_cross_slice=[(str(src), str(dst), int(count)) for src, dst, count in layer_stats.top_cross_slice],
    )


def layer_metrics_snapshot_to_jsonable(snapshot: LayerMetricsSnapshot) -> dict[str, Any]:
    return {
        "package_name": snapshot.package_name,
        "excluded_layers": list(snapshot.excluded_layers),
        "layers": snapshot.layers,
        "internal_edges": snapshot.internal_edges,
        "intra_layer_edges": snapshot.intra_layer_edges,
        "cross_layer_edges": snapshot.cross_layer_edges,
        "intra_layer_ratio": snapshot.intra_layer_ratio,
        "cross_layer_ratio": snapshot.cross_layer_ratio,
        "max_cross_layer_flow": snapshot.max_cross_layer_flow,
        "top_cross_layer": [
            {"source": src, "target": dst, "count": count} for src, dst, count in snapshot.top_cross_layer
        ],
        "top_cross_slice": [
            {"source": src, "target": dst, "count": count} for src, dst, count in snapshot.top_cross_slice
        ],
    }


def thresholds_to_jsonable(thresholds: LayerMetricsThresholds) -> dict[str, Any]:
    return {
        "min_intra_layer_ratio": thresholds.min_intra_layer_ratio,
        "max_cross_layer_ratio": thresholds.max_cross_layer_ratio,
        "max_top_cross_layer_flow": thresholds.max_top_cross_layer_flow,
        "allowed_ratio_regression": thresholds.allowed_ratio_regression,
        "allowed_flow_regression": thresholds.allowed_flow_regression,
    }


def issue_to_jsonable(issue: LayerMetricsIssue) -> dict[str, Any]:
    return {
        "code": issue.code,
        "severity": issue.severity,
        "message": issue.message,
        "current": issue.current,
        "threshold": issue.threshold,
        "baseline": issue.baseline,
    }


def format_layer_metrics_gate_jsonable(report: LayerMetricsGateReport) -> dict[str, Any]:
    return {
        "ok": report.ok,
        "current": layer_metrics_snapshot_to_jsonable(report.snapshot),
        "baseline": (layer_metrics_snapshot_to_jsonable(report.baseline) if report.baseline else None),
        "thresholds": thresholds_to_jsonable(report.thresholds),
        "issues": [issue_to_jsonable(i) for i in report.issues],
        "issue_count": len(report.issues),
    }


def format_layer_metrics_gate_text(report: LayerMetricsGateReport, *, package_dir: Path) -> str:
    cur = report.snapshot
    parts: list[str] = []
    parts.append(f"Layer metrics check for {package_dir}")
    parts.append(f"Status: {'OK' if report.ok else 'FAIL'}")
    parts.append(
        "Current: "
        f"layers={cur.layers}, edges={cur.internal_edges}, "
        f"intra_ratio={cur.intra_layer_ratio:.3f}, "
        f"cross_ratio={cur.cross_layer_ratio:.3f}, "
        f"max_cross_flow={cur.max_cross_layer_flow}"
    )
    if report.snapshot.excluded_layers:
        parts.append(f"Excluded layers: {', '.join(report.snapshot.excluded_layers)}")
    parts.append("Thresholds:")
    t = report.thresholds
    parts.append(
        f"  - min_intra_layer_ratio: {t.min_intra_layer_ratio if t.min_intra_layer_ratio is not None else 'n/a'}"
    )
    parts.append(
        f"  - max_cross_layer_ratio: {t.max_cross_layer_ratio if t.max_cross_layer_ratio is not None else 'n/a'}"
    )
    parts.append(
        f"  - max_top_cross_layer_flow: {t.max_top_cross_layer_flow if t.max_top_cross_layer_flow is not None else 'n/a'}"
    )
    parts.append(f"  - allowed_ratio_regression: {t.allowed_ratio_regression:.3f}")
    parts.append(f"  - allowed_flow_regression: {t.allowed_flow_regression}")
    if report.baseline is not None:
        base = report.baseline
        parts.append("Baseline:")
        parts.append(
            f"  - intra_ratio: {base.intra_layer_ratio:.3f} "
            f"(delta {cur.intra_layer_ratio - base.intra_layer_ratio:+.3f})"
        )
        parts.append(
            f"  - cross_ratio: {base.cross_layer_ratio:.3f} "
            f"(delta {cur.cross_layer_ratio - base.cross_layer_ratio:+.3f})"
        )
        parts.append(
            f"  - max_cross_flow: {base.max_cross_layer_flow} "
            f"(delta {cur.max_cross_layer_flow - base.max_cross_layer_flow:+d})"
        )
    if report.snapshot.top_cross_layer:
        parts.append("Top cross-layer flows:")
        for src, dst, count in report.snapshot.top_cross_layer[:5]:
            parts.append(f"  - {src} -> {dst}: {count}")
    if report.issues:
        parts.append(f"Issues ({len(report.issues)}):")
        for issue in report.issues:
            bits = [f"[{issue.severity}] {issue.code}: {issue.message}"]
            if issue.current is not None:
                bits.append(f"current={issue.current}")
            if issue.threshold is not None:
                bits.append(f"threshold={issue.threshold}")
            if issue.baseline is not None:
                bits.append(f"baseline={issue.baseline}")
            parts.append("  - " + ", ".join(bits))
    else:
        parts.append("Issues: none")
    return "\n".join(parts) + "\n"


def write_layer_metrics_baseline(path: Path, snapshot: LayerMetricsSnapshot) -> None:
    data = {
        "version": 1,
        "snapshot": layer_metrics_snapshot_to_jsonable(snapshot),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_layer_metrics_baseline(path: Path) -> LayerMetricsSnapshot:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Layer metrics baseline must be a JSON object")
    snapshot = raw.get("snapshot", raw)
    if not isinstance(snapshot, dict):
        raise ValueError("Layer metrics baseline must contain a 'snapshot' object")

    def _triples(key: str) -> list[tuple[str, str, int]]:
        rows = snapshot.get(key, []) or []
        out: list[tuple[str, str, int]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            out.append((str(row.get("source", "")), str(row.get("target", "")), int(row.get("count", 0))))
        return out

    return LayerMetricsSnapshot(
        package_name=str(snapshot.get("package_name", "dpone")),
        excluded_layers=[str(x) for x in (snapshot.get("excluded_layers", []) or [])],
        layers=int(snapshot.get("layers", 0)),
        internal_edges=int(snapshot.get("internal_edges", 0)),
        intra_layer_edges=int(snapshot.get("intra_layer_edges", 0)),
        cross_layer_edges=int(snapshot.get("cross_layer_edges", 0)),
        intra_layer_ratio=float(snapshot.get("intra_layer_ratio", 0.0)),
        cross_layer_ratio=float(snapshot.get("cross_layer_ratio", 0.0)),
        max_cross_layer_flow=int(snapshot.get("max_cross_layer_flow", 0)),
        top_cross_layer=_triples("top_cross_layer"),
        top_cross_slice=_triples("top_cross_slice"),
    )


def evaluate_layer_metrics_gate(
    snapshot: LayerMetricsSnapshot,
    *,
    thresholds: LayerMetricsThresholds,
    baseline: LayerMetricsSnapshot | None = None,
) -> LayerMetricsGateReport:
    issues: list[LayerMetricsIssue] = []

    if thresholds.min_intra_layer_ratio is not None and snapshot.intra_layer_ratio < thresholds.min_intra_layer_ratio:
        issues.append(
            LayerMetricsIssue(
                code="MIN_INTRA_LAYER_RATIO",
                severity="ERROR",
                message="Intra-layer ratio is below threshold",
                current=snapshot.intra_layer_ratio,
                threshold=thresholds.min_intra_layer_ratio,
            )
        )
    if thresholds.max_cross_layer_ratio is not None and snapshot.cross_layer_ratio > thresholds.max_cross_layer_ratio:
        issues.append(
            LayerMetricsIssue(
                code="MAX_CROSS_LAYER_RATIO",
                severity="ERROR",
                message="Cross-layer ratio is above threshold",
                current=snapshot.cross_layer_ratio,
                threshold=thresholds.max_cross_layer_ratio,
            )
        )
    if (
        thresholds.max_top_cross_layer_flow is not None
        and snapshot.max_cross_layer_flow > thresholds.max_top_cross_layer_flow
    ):
        issues.append(
            LayerMetricsIssue(
                code="MAX_TOP_CROSS_LAYER_FLOW",
                severity="ERROR",
                message="Largest cross-layer flow is above threshold",
                current=snapshot.max_cross_layer_flow,
                threshold=thresholds.max_top_cross_layer_flow,
            )
        )

    if baseline is not None:
        if snapshot.cross_layer_ratio > baseline.cross_layer_ratio + thresholds.allowed_ratio_regression:
            issues.append(
                LayerMetricsIssue(
                    code="CROSS_LAYER_RATIO_REGRESSION",
                    severity="ERROR",
                    message="Cross-layer ratio regressed beyond allowed tolerance",
                    current=snapshot.cross_layer_ratio,
                    threshold=baseline.cross_layer_ratio + thresholds.allowed_ratio_regression,
                    baseline=baseline.cross_layer_ratio,
                )
            )
        if snapshot.intra_layer_ratio < baseline.intra_layer_ratio - thresholds.allowed_ratio_regression:
            issues.append(
                LayerMetricsIssue(
                    code="INTRA_LAYER_RATIO_REGRESSION",
                    severity="ERROR",
                    message="Intra-layer ratio regressed beyond allowed tolerance",
                    current=snapshot.intra_layer_ratio,
                    threshold=baseline.intra_layer_ratio - thresholds.allowed_ratio_regression,
                    baseline=baseline.intra_layer_ratio,
                )
            )
        if snapshot.max_cross_layer_flow > baseline.max_cross_layer_flow + thresholds.allowed_flow_regression:
            issues.append(
                LayerMetricsIssue(
                    code="TOP_CROSS_LAYER_FLOW_REGRESSION",
                    severity="ERROR",
                    message="Largest cross-layer flow regressed beyond allowed tolerance",
                    current=snapshot.max_cross_layer_flow,
                    threshold=baseline.max_cross_layer_flow + thresholds.allowed_flow_regression,
                    baseline=baseline.max_cross_layer_flow,
                )
            )

    return LayerMetricsGateReport(
        ok=(len(issues) == 0),
        snapshot=snapshot,
        thresholds=thresholds,
        issues=issues,
        baseline=baseline,
    )
