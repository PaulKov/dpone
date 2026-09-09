"""Architecture fitness metrics for coupling and responsibility drift."""

from __future__ import annotations

import ast
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from dpone.metrics.import_graph import collect_internal_deps, compute_coupling_stats
from dpone.metrics.layer_metrics import compute_layer_coupling_stats
from dpone.metrics.loc import iter_py_files

Severity = Literal["error", "warn"]


@dataclass(frozen=True, slots=True)
class ArchitectureFitnessThresholds:
    target_avg_clustering: float = 0.18
    max_avg_clustering: float = 0.25
    max_cross_layer_ratio: float = 0.35
    target_module_ce: int = 40
    max_module_ce: int = 60
    max_class_methods: int = 24
    max_class_loc: int = 450
    fail_on_class_warnings: bool = False


@dataclass(frozen=True, slots=True)
class ArchitectureIssue:
    severity: Severity
    code: str
    message: str
    module: str | None = None
    value: float | int | None = None
    threshold: float | int | None = None


@dataclass(frozen=True, slots=True)
class ClassResponsibilityFinding:
    severity: Severity
    path: str
    class_name: str
    methods: int
    loc: int
    message: str


@dataclass(frozen=True, slots=True)
class LocalClusteringDiagnostic:
    module: str
    degree: int
    coefficient: float
    inbound: int
    outbound: int


@dataclass(frozen=True, slots=True)
class ArchitectureFitnessReport:
    package: str
    avg_clustering: float
    cross_layer_ratio: float
    max_module_ce: int
    max_module_ce_name: str | None
    issues: tuple[ArchitectureIssue, ...]
    class_findings: tuple[ClassResponsibilityFinding, ...]
    local_clustering: tuple[LocalClusteringDiagnostic, ...]

    @property
    def ok(self) -> bool:
        return not any(issue.severity == "error" for issue in self.issues) and not any(
            finding.severity == "error" for finding in self.class_findings
        )

    @property
    def issue_count(self) -> int:
        return len(self.issues) + len(self.class_findings)


def analyze_architecture_fitness(
    package_dir: Path,
    *,
    repo_root: Path,
    thresholds: ArchitectureFitnessThresholds | None = None,
    top_n: int = 20,
) -> ArchitectureFitnessReport:
    """Analyze architecture fitness for a Python package directory."""

    active_thresholds = thresholds or ArchitectureFitnessThresholds()
    deps = collect_internal_deps(
        package_dir,
        module_files=list(iter_py_files(package_dir)),
        package_name=package_dir.name,
    )
    coupling = compute_coupling_stats(deps, top_n=max(5, min(50, top_n)), package_name=package_dir.name)
    layer = compute_layer_coupling_stats(
        deps,
        top_n=max(5, min(50, top_n)),
        package_name=package_dir.name,
        exclude_layers=(f"{package_dir.name}.compat",),
    )
    issues = list(_metric_issues(coupling, layer, active_thresholds, top_n=top_n))
    class_findings = tuple(_class_findings(package_dir, repo_root, active_thresholds))
    local_clustering = tuple(_local_clustering_diagnostics(deps, top_n=top_n))
    max_module_ce_name = coupling.top_out[0][0] if coupling.top_out else None
    max_module_ce = coupling.top_out[0][1] if coupling.top_out else 0
    return ArchitectureFitnessReport(
        package=str(package_dir),
        avg_clustering=coupling.avg_clustering,
        cross_layer_ratio=layer.cross_layer_ratio,
        max_module_ce=max_module_ce,
        max_module_ce_name=max_module_ce_name,
        issues=tuple(issues),
        class_findings=class_findings,
        local_clustering=local_clustering,
    )


def _metric_issues(
    coupling: Any, layer: Any, thresholds: ArchitectureFitnessThresholds, *, top_n: int
) -> Iterable[ArchitectureIssue]:
    if coupling.avg_clustering > thresholds.max_avg_clustering:
        yield ArchitectureIssue(
            severity="error",
            code="avg_clustering_too_high",
            message="Average clustering exceeds hard architecture fitness threshold.",
            value=coupling.avg_clustering,
            threshold=thresholds.max_avg_clustering,
        )
    elif coupling.avg_clustering > thresholds.target_avg_clustering:
        yield ArchitectureIssue(
            severity="warn",
            code="avg_clustering_above_target",
            message="Average clustering is controlled but still above the target green zone.",
            value=coupling.avg_clustering,
            threshold=thresholds.target_avg_clustering,
        )
    if layer.cross_layer_ratio > thresholds.max_cross_layer_ratio:
        yield ArchitectureIssue(
            severity="error",
            code="cross_layer_ratio_too_high",
            message="Cross-layer import ratio exceeds the architecture fitness threshold.",
            value=layer.cross_layer_ratio,
            threshold=thresholds.max_cross_layer_ratio,
        )
    for module, ce in coupling.top_out[:top_n]:
        if ce > thresholds.max_module_ce:
            yield ArchitectureIssue(
                severity="error",
                code="module_fanout_too_high",
                message="Module fan-out exceeds the hard architecture fitness threshold.",
                module=module,
                value=ce,
                threshold=thresholds.max_module_ce,
            )
        elif ce > thresholds.target_module_ce:
            yield ArchitectureIssue(
                severity="warn",
                code="module_fanout_above_target",
                message="Module fan-out is above the preferred target and should be justified as a facade or split.",
                module=module,
                value=ce,
                threshold=thresholds.target_module_ce,
            )


def _class_findings(
    package_dir: Path,
    repo_root: Path,
    thresholds: ArchitectureFitnessThresholds,
) -> Iterable[ClassResponsibilityFinding]:
    severity: Severity = "error" if thresholds.fail_on_class_warnings else "warn"
    for path in iter_py_files(package_dir):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in (candidate for candidate in ast.walk(tree) if isinstance(candidate, ast.ClassDef)):
            methods = sum(isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) for child in node.body)
            loc = _node_loc(node)
            if methods <= thresholds.max_class_methods and loc <= thresholds.max_class_loc:
                continue
            rel_path = str(path.relative_to(repo_root)) if path.is_relative_to(repo_root) else str(path)
            yield ClassResponsibilityFinding(
                severity=severity,
                path=rel_path,
                class_name=node.name,
                methods=methods,
                loc=loc,
                message=(
                    "Class responsibility exceeds preferred fitness limits; split by protocol/service/helper "
                    "or document it as a thin facade."
                ),
            )


def _node_loc(node: ast.AST) -> int:
    end = getattr(node, "end_lineno", None)
    start = getattr(node, "lineno", None)
    if end is None or start is None:
        return 0
    return max(0, int(end) - int(start) + 1)


def _local_clustering_diagnostics(
    deps_out: dict[str, set[str]],
    *,
    top_n: int,
) -> Iterable[LocalClusteringDiagnostic]:
    modules = sorted(deps_out)
    inbound: dict[str, set[str]] = {module: set() for module in modules}
    undirected: dict[str, set[str]] = {module: set() for module in modules}
    for source, targets in deps_out.items():
        for target in targets:
            inbound.setdefault(target, set()).add(source)
            undirected[source].add(target)
            undirected.setdefault(target, set()).add(source)

    diagnostics: list[LocalClusteringDiagnostic] = []
    for module in modules:
        neighbours = list(undirected.get(module, set()))
        degree = len(neighbours)
        if degree < 2:
            continue

        neighbour_set = set(neighbours)
        edge_count = 0
        for neighbour in neighbours:
            edge_count += len(undirected.get(neighbour, set()) & neighbour_set)
        edge_count //= 2
        coefficient = (2.0 * edge_count) / (degree * (degree - 1))
        if coefficient <= 0:
            continue
        diagnostics.append(
            LocalClusteringDiagnostic(
                module=module,
                degree=degree,
                coefficient=float(coefficient),
                inbound=len(inbound.get(module, set())),
                outbound=len(deps_out.get(module, set())),
            )
        )

    return sorted(diagnostics, key=lambda item: (-item.coefficient, -item.degree, item.module))[:top_n]


def format_architecture_fitness_report_jsonable(report: ArchitectureFitnessReport) -> dict[str, Any]:
    return {
        "package": report.package,
        "ok": report.ok,
        "issue_count": report.issue_count,
        "avg_clustering": report.avg_clustering,
        "cross_layer_ratio": report.cross_layer_ratio,
        "max_module_ce": report.max_module_ce,
        "max_module_ce_name": report.max_module_ce_name,
        "issues": [asdict(issue) for issue in report.issues],
        "class_findings": [asdict(finding) for finding in report.class_findings],
        "local_clustering": [asdict(item) for item in report.local_clustering],
    }


def format_architecture_fitness_report_text(report: ArchitectureFitnessReport) -> str:
    status = "OK" if report.ok else "FAILED"
    lines = [
        f"Architecture fitness check for {report.package}",
        f"Status: {status}",
        f"Avg clustering: {report.avg_clustering:.3f}",
        f"Cross-layer ratio: {report.cross_layer_ratio:.3f}",
        f"Max module fan-out: {report.max_module_ce} ({report.max_module_ce_name or 'n/a'})",
    ]
    if report.issues:
        lines.append("Metric findings:")
        for issue in report.issues:
            module = f" {issue.module}" if issue.module else ""
            lines.append(
                f"- [{issue.severity}] {issue.code}{module}: {issue.message} ({issue.value} > {issue.threshold})"
            )
    if report.class_findings:
        lines.append("Class responsibility findings:")
        for finding in report.class_findings:
            lines.append(
                "- "
                f"[{finding.severity}] {finding.path}:{finding.class_name} "
                f"methods={finding.methods} loc={finding.loc}: {finding.message}"
            )
    if report.local_clustering:
        lines.append("Top local clustering diagnostics:")
        for item in report.local_clustering:
            lines.append(
                "- "
                f"{item.module} degree={item.degree} coefficient={item.coefficient:.3f} "
                f"inbound={item.inbound} outbound={item.outbound}"
            )
    if not report.issues and not report.class_findings:
        lines.append("No architecture fitness findings.")
    return "\n".join(lines)
