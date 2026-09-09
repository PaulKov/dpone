from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from datetime import timezone as dt_timezone
from pathlib import Path
from typing import Any

from dpone.dag.dag_report_models import DagReport, ReportAnomaly, ReportEdge, ReportLintIssue
from dpone.dag.edge_explain import DagEdgeContext, explain_direct_edge, resolve_dependency_path_to_task_names
from dpone.dag.loader import ConfigLoader
from dpone.dag.node_explain import build_reverse_adjacency
from dpone.dag.yaml_types import ProcessNode
from dpone.manifest.registry_lint import RegistryLintIssue, lint_registry
from dpone.manifest.validation import ValidationIssue, ValidationProfile, validate_manifest


def _severity_rank(sev: object) -> int:
    """Convert a severity-like value into an ordered rank.

    We keep this tolerant to historic representations (e.g. "Severity.ERROR").
    """
    s = str(sev).strip().upper()
    if s.endswith("ERROR"):
        return 2
    if s.endswith("WARNING"):
        return 1
    if s.endswith("INFO"):
        return 0
    return 0


def build_dag_report(
    *,
    ctx: DagEdgeContext,
    nodes: Sequence[ProcessNode],
    root: Path,
    base_path: Path,
    cfg_loader: ConfigLoader,
    registry_paths: Sequence[Path] = (),
    registry_require_fields: Sequence[str] = ("host", "type"),
    validation_profile: ValidationProfile | None = None,
    include_edge_evidence: bool = False,
    max_edges: int = 2000,
    max_triggers: int = 10,
    include_lint: bool = True,
    dependency_file_errors: Sequence[str] = (),
) -> DagReport:
    """Build a unified DAG report.

    Args:
        ctx: built edge context.
        nodes: execution plan nodes.
        root/base_path: context info for the report.
        cfg_loader: ConfigLoader used to load manifests (cache-friendly).
        registry_paths: registry YAMLs.
        validation_profile: optional profile for manifest validation.
        include_edge_evidence: include reasons evidence blocks for edges.
        max_edges: safety limit for stored edges in the report.
        max_triggers: evidence truncation for group-to-group.
        include_lint: include lint section (manifest.validate + registry.lint + dependencies).
        dependency_file_errors: output of DependencyManager.validate_dependencies().

    Returns:
        DagReport.
    """

    now = datetime.now(dt_timezone.utc).isoformat()  # noqa: UP017 - mypy config still treats datetime.UTC as unavailable.

    rev = build_reverse_adjacency(ctx)
    names = sorted([n.name for n in nodes])

    roots = [n for n in names if not rev.get(n)]
    sinks = [n for n in names if not ctx.adjacency.get(n)]
    orphans = [n for n in names if (n in roots and n in sinks)]

    # collect edges
    all_edges: list[tuple[str, str]] = []
    for up, downs in ctx.adjacency.items():
        for dn in downs:
            all_edges.append((up, dn))
    all_edges.sort(key=lambda x: (x[0], x[1]))

    warnings: list[str] = []
    stored_edges = all_edges
    if max_edges > 0 and len(stored_edges) > max_edges:
        stored_edges = stored_edges[:max_edges]
        warnings.append(f"Edges truncated: {len(all_edges)} > max_edges={max_edges}")

    edges: list[ReportEdge] = []
    for up, dn in stored_edges:
        un = ctx.nodes_by_name.get(up)
        dn_node = ctx.nodes_by_name.get(dn)
        reason_kinds: list[str] = []
        reasons_payload: list[dict[str, Any]] = []
        exp = explain_direct_edge(
            ctx,
            upstream_name=up,
            downstream_name=dn,
            max_triggers=max_triggers,
            include_transitive_path=False,
        )
        reason_kinds = sorted({r.kind for r in exp.reasons})
        if include_edge_evidence:
            reasons_payload = [r.to_jsonable() for r in exp.reasons]

        edges.append(
            ReportEdge(
                upstream=up,
                downstream=dn,
                upstream_ref=getattr(un, "ref", None) if un else None,
                downstream_ref=getattr(dn_node, "ref", None) if dn_node else None,
                upstream_group=getattr(un, "task_group", None) if un else None,
                downstream_group=getattr(dn_node, "task_group", None) if dn_node else None,
                reason_kinds=tuple(reason_kinds),
                reasons=tuple(reasons_payload),
            )
        )

    anomalies = _detect_anomalies(ctx, nodes)

    lint_issues: list[ReportLintIssue] = []
    if include_lint:
        # dependency file errors
        for msg in dependency_file_errors or ():
            lint_issues.append(
                ReportLintIssue(
                    tool="dependencies",
                    severity="ERROR",
                    code="DEPENDENCY_FILE_NOT_FOUND",
                    message=str(msg),
                    location=str(root),
                    selector=None,
                )
            )

        # manifest validation (per loaded file)
        # Use the same set of YAML files that participated in the DAG.
        manifest_paths = sorted({Path(n.config_path).resolve(strict=False) for n in nodes})
        for mp in manifest_paths:
            m = cfg_loader.get_manifest(mp, metadata_only=True)
            if not m:
                lint_issues.append(
                    ReportLintIssue(
                        tool="manifest.validate",
                        severity="ERROR",
                        code="MANIFEST_LOAD_ERROR",
                        message="Failed to load manifest",
                        location=str(mp),
                        selector=None,
                    )
                )
                continue
            for iss in validate_manifest(m, profile=validation_profile):
                lint_issues.append(_from_validation_issue(iss))

        # registry lint (only if registry is provided)
        if registry_paths:
            reg_issues: list[RegistryLintIssue] = lint_registry(
                manifest_paths,
                registry_paths=registry_paths,
                require_fields=tuple(registry_require_fields or ()),
            )
            for ri in reg_issues:
                lint_issues.append(_from_registry_lint_issue(ri))

    return DagReport(
        root=str(root),
        base_path=str(base_path),
        generated_at=now,
        task_count=len(nodes),
        edge_count=len(all_edges),
        group_count=len({n.task_group for n in nodes if n.task_group}),
        roots=tuple(roots),
        sinks=tuple(sinks),
        orphans=tuple(orphans),
        edges=tuple(edges),
        anomalies=tuple(anomalies),
        lint=tuple(lint_issues),
        warnings=tuple(warnings),
    )


def _from_validation_issue(i: ValidationIssue) -> ReportLintIssue:
    sev = getattr(i.severity, "value", str(i.severity))
    return ReportLintIssue(
        tool="manifest.validate",
        severity=str(sev),
        code=str(i.code),
        message=str(i.message),
        location=str(i.manifest_path),
        selector=str(i.selector) if i.selector else None,
    )


def _from_registry_lint_issue(i: RegistryLintIssue) -> ReportLintIssue:
    sev = getattr(i.severity, "value", str(i.severity))
    return ReportLintIssue(
        tool="registry.lint",
        severity=str(sev),
        code=str(i.code),
        message=str(i.message),
        location=str(i.manifest_path),
        selector=None,
    )


def _detect_anomalies(ctx: DagEdgeContext, nodes: Sequence[ProcessNode]) -> list[ReportAnomaly]:
    """Detect best-effort anomalies in the computed DAG."""
    anomalies: list[ReportAnomaly] = []

    # 1) cycles (should not happen; DependencyManager already checks at manifest graph level,
    # but TaskGroup expansion can theoretically introduce cycles)
    cycles = _find_cycles(ctx.adjacency)
    for cyc in cycles:
        anomalies.append(
            ReportAnomaly(
                code="DAG_CYCLE",
                severity="ERROR",
                message="Cycle detected in computed DAG",
                details={"cycle": cyc},
            )
        )

    # 2) unresolved dependency refs at *process* level
    for n in nodes:
        for idx, dep in enumerate(n.dependencies or []):
            # group deps
            grp = getattr(dep, "group", None)
            if grp:
                g = str(grp)
                if g not in ctx.all_groups or not ctx.nodes_by_group.get(g):
                    anomalies.append(
                        ReportAnomaly(
                            code="UNRESOLVED_GROUP_DEP",
                            severity="ERROR",
                            message=f"Task '{n.name}' depends on missing/empty group '{g}'",
                            details={
                                "task": n.name,
                                "depends_on_index": idx,
                                "group": g,
                                "config_path": str(n.config_path),
                            },
                        )
                    )
                continue

            dep_path = getattr(dep, "path", None)
            if not dep_path:
                continue

            matches, mode, detail = resolve_dependency_path_to_task_names(
                str(dep_path),
                current_node=n,
                nodes=ctx.nodes,
                all_groups=ctx.all_groups,
            )

            if not matches or mode == "unresolved":
                anomalies.append(
                    ReportAnomaly(
                        code="UNRESOLVED_PATH_DEP",
                        severity="ERROR",
                        message=f"Task '{n.name}' depends on '{dep_path}' but it resolved to no upstream tasks",
                        details={
                            "task": n.name,
                            "depends_on_index": idx,
                            "path": str(dep_path),
                            "mode": mode,
                            "detail": detail,
                            "config_path": str(n.config_path),
                        },
                    )
                )
                continue

            # selector ambiguity inside a single file
            if mode == "selector" and int(detail.get("match_count") or 0) > 1:
                anomalies.append(
                    ReportAnomaly(
                        code="AMBIGUOUS_SELECTOR_REF",
                        severity="WARNING",
                        message=f"Task '{n.name}' depends on selector '{detail.get('selector')}' and it matched {detail.get('match_count')} tasks",
                        details={
                            "task": n.name,
                            "depends_on_index": idx,
                            "path": str(dep_path),
                            "detail": detail,
                            "matches": matches,
                        },
                    )
                )

            # legacy stem fallback is risky
            if mode == "stem":
                anomalies.append(
                    ReportAnomaly(
                        code="LEGACY_STEM_FALLBACK",
                        severity="WARNING",
                        message=f"Task '{n.name}' resolved depends_on.path '{dep_path}' via legacy stem fallback",
                        details={
                            "task": n.name,
                            "depends_on_index": idx,
                            "path": str(dep_path),
                            "detail": detail,
                            "matches": matches,
                        },
                    )
                )

    # 3) orphans
    rev = build_reverse_adjacency(ctx)
    for n in nodes:
        indeg = len(rev.get(n.name, set()))
        outdeg = len(ctx.adjacency.get(n.name, set()))
        if indeg == 0 and outdeg == 0:
            anomalies.append(
                ReportAnomaly(
                    code="ORPHAN_TASK",
                    severity="INFO",
                    message=f"Task '{n.name}' is isolated (no incoming and no outgoing edges)",
                    details={
                        "task": n.name,
                        "config_path": str(n.config_path),
                        "task_group": n.task_group,
                    },
                )
            )

    return anomalies


def _find_cycles(adjacency: Mapping[str, Iterable[str]], *, max_cycles: int = 5) -> list[list[str]]:
    """Find a few cycles in a directed graph (DFS, best-effort).

    Returns a list of cycles (each is a list of node names with the start repeated at the end).
    """
    visited: set[str] = set()
    stack: list[str] = []
    on_stack: set[str] = set()
    cycles: list[list[str]] = []

    def dfs(v: str) -> None:
        if len(cycles) >= max_cycles:
            return
        visited.add(v)
        stack.append(v)
        on_stack.add(v)
        for w in adjacency.get(v, []) or []:
            if len(cycles) >= max_cycles:
                break
            if w not in visited:
                dfs(w)
            elif w in on_stack:
                # cycle found: w ... v -> w
                try:
                    idx = stack.index(w)
                except ValueError:
                    idx = 0
                cyc = stack[idx:] + [w]
                # dedupe by canonical tuple
                key = tuple(cyc)
                if not any(tuple(c) == key for c in cycles):
                    cycles.append(cyc)
        stack.pop()
        on_stack.remove(v)

    for node in list(adjacency.keys()):
        if node in visited:
            continue
        dfs(node)
        if len(cycles) >= max_cycles:
            break

    return cycles
