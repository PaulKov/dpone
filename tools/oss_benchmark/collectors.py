"""Static source collectors for the OSS code-quality benchmark."""

from __future__ import annotations

import statistics
import subprocess
from dataclasses import replace
from pathlib import Path

from tools.oss_benchmark.config import default_project_specs
from tools.oss_benchmark.dependency_graph import (
    avg_clustering,
    collect_internal_deps,
    compute_coupling_metrics,
    connected_components,
    percentile,
    resolve_internal_target,
)
from tools.oss_benchmark.models import FileMetric, LocSummary, ProjectMetrics, ProjectSpec, QualitySignal
from tools.oss_benchmark.quality_scoring import looks_interface_like, score_quality
from tools.oss_benchmark.source_files import (
    count_lines,
    count_sloc,
    is_test_file,
    iter_source_files,
    module_name_for_file,
    parse_import_targets,
    read_text,
    should_skip_path,
)


def collect_project_metrics(spec: ProjectSpec, *, top_n: int = 15) -> ProjectMetrics:
    """Collect LOC/SLOC, module hotspots and dependency proxies for one project."""

    source_files = iter_source_files(spec.path)
    module_names = {path: module_name_for_file(path, spec.path) for path in source_files}
    file_metrics: list[FileMetric] = []
    module_slices: dict[str, str] = {}
    interface_like = 0

    for path in source_files:
        rel = path.relative_to(spec.path).as_posix()
        text = read_text(path)
        is_test = is_test_file(Path(rel))
        module = module_names[path]
        module_slices[module] = slice_for_file(Path(rel))
        file_metrics.append(FileMetric(path=rel, lines=count_lines(text), sloc=count_sloc(text), is_test=is_test))
        if not is_test and looks_interface_like(path, text):
            interface_like += 1

    production_modules = {module_names[path] for path in source_files if not is_test_file(path.relative_to(spec.path))}
    deps = collect_internal_deps(source_files, module_names, production_modules)
    coupling = compute_coupling_metrics(deps, module_slices=module_slices, top_n=top_n)
    without_tests = tuple(item for item in file_metrics if not item.is_test)
    loc_without_tests = summarize_loc(without_tests)
    signal = QualitySignal(
        max_module_loc=loc_without_tests.max_lines,
        p90_module_loc=loc_without_tests.p90_lines,
        avg_ce=coupling.avg_ce,
        p90_ce=coupling.p90_ce,
        max_ce=coupling.max_ce,
        avg_clustering=coupling.avg_clustering,
        cohesion_ratio=coupling.cohesion_ratio,
        interface_density=(interface_like / loc_without_tests.files if loc_without_tests.files else 0.0),
    )
    return ProjectMetrics(
        spec=spec,
        dirty=is_dirty_repo(spec.path),
        loc_with_tests=summarize_loc(tuple(file_metrics)),
        loc_without_tests=loc_without_tests,
        top_loc_with_tests=top_files(tuple(file_metrics), key="lines", top_n=top_n),
        top_sloc_with_tests=top_files(tuple(file_metrics), key="sloc", top_n=top_n),
        top_loc_without_tests=top_files(without_tests, key="lines", top_n=top_n),
        top_sloc_without_tests=top_files(without_tests, key="sloc", top_n=top_n),
        coupling=coupling,
        quality=score_quality(signal),
        source_files_without_tests=loc_without_tests.files,
        interface_like_files=interface_like,
    )


def summarize_loc(items: tuple[FileMetric, ...]) -> LocSummary:
    """Summarize file LOC/SLOC distribution."""

    lines = sorted(item.lines for item in items)
    slocs = sorted(item.sloc for item in items)
    total_lines = sum(lines)
    return LocSummary(
        files=len(items),
        total_lines=total_lines,
        total_sloc=sum(slocs),
        max_lines=max(lines, default=0),
        max_sloc=max(slocs, default=0),
        avg_lines=(float(total_lines / len(items)) if items else 0.0),
        median_lines=(float(statistics.median(lines)) if lines else 0.0),
        p90_lines=percentile(lines, 90),
    )


def top_files(items: tuple[FileMetric, ...], *, key: str, top_n: int) -> tuple[FileMetric, ...]:
    return tuple(sorted(items, key=lambda item: (-getattr(item, key), item.path))[:top_n])


def slice_for_file(path: Path) -> str:
    parts = path.parts
    if not parts:
        return "root"
    if parts[0] in {"src", "lib"} and len(parts) >= 3:
        if parts[1] in {"dpone", "dlt"} and len(parts) >= 4:
            return ".".join(parts[1:3])
        return parts[1]
    if len(parts) >= 2 and parts[0] in {"dlt", "dpone"}:
        return ".".join(parts[:2])
    return parts[0]


def is_dirty_repo(path: Path) -> bool:
    if not (path / ".git").exists():
        return False
    result = subprocess.run(["git", "status", "--short"], cwd=path, capture_output=True, text=True, check=False)
    return bool(result.stdout.strip())


def ensure_external_repo(spec: ProjectSpec) -> None:
    if spec.slug == "dpone":
        return
    spec.path.parent.mkdir(parents=True, exist_ok=True)
    if not (spec.path / ".git").exists():
        subprocess.run(
            ["git", "clone", "--filter=blob:none", "--no-checkout", spec.repo_url, str(spec.path)],
            check=True,
        )
    subprocess.run(["git", "fetch", "--depth", "1", "origin", spec.commit], cwd=spec.path, check=True)
    subprocess.run(["git", "checkout", "--detach", spec.commit], cwd=spec.path, check=True)


def collect_all_projects(
    workspace: Path,
    *,
    top_n: int,
    include_external: bool,
    project: str = "all",
    local_branch: str | None = None,
    local_commit: str | None = None,
) -> tuple[tuple[ProjectMetrics, ...], dict[str, str]]:
    """Collect all requested projects, preserving failures for stale merge logic."""

    metrics: list[ProjectMetrics] = []
    failures: dict[str, str] = {}
    for spec in default_project_specs(workspace):
        if spec.slug == "dpone":
            spec = replace(spec, branch=local_branch or spec.branch, commit=local_commit or spec.commit)
        if project != "all" and spec.slug != project:
            continue
        try:
            if spec.slug != "dpone":
                if include_external:
                    ensure_external_repo(spec)
                elif not spec.path.exists():
                    continue
            metrics.append(collect_project_metrics(spec, top_n=top_n))
        except Exception as exc:  # pragma: no cover - exercised via synthetic merge failures.
            failures[spec.slug] = str(exc)
    return tuple(metrics), failures


__all__ = [
    "avg_clustering",
    "collect_all_projects",
    "collect_project_metrics",
    "compute_coupling_metrics",
    "connected_components",
    "count_lines",
    "count_sloc",
    "ensure_external_repo",
    "is_dirty_repo",
    "is_test_file",
    "iter_source_files",
    "module_name_for_file",
    "parse_import_targets",
    "percentile",
    "read_text",
    "resolve_internal_target",
    "score_quality",
    "should_skip_path",
    "slice_for_file",
    "summarize_loc",
    "top_files",
]
