"""Complexity and boundary-discipline analysis for benchmark evidence."""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any

from tools.oss_benchmark.collectors import is_test_file, parse_import_targets, read_text
from tools.oss_benchmark.config import IGNORED_DIRS, SOURCE_SUFFIXES
from tools.oss_benchmark.payload_utils import as_int, get_value, project_name, project_slug

_BOUNDARY_TARGETS = {"tools.oss_benchmark.collectors", "tools.oss_benchmark.core"}
_CONTRACT_HINTS = ("Protocol", "ABC", "Port", "Contract", "Interface")


def analyze_complexity_boundary_discipline(projects: list[dict[str, Any]]) -> dict[str, Any]:
    """Build JSON-ready complexity, boundary and DI evidence for benchmark projects."""

    summary: dict[str, Any] = {}
    risk_items: list[dict[str, Any]] = []
    for project in projects:
        slug = project_slug(project)
        if not slug or project.get("unavailable"):
            continue
        item = project_complexity_boundary(project)
        summary[slug] = item
        risk_items.extend(item.get("risk_register", []))
    risk_items.sort(key=lambda item: (_priority_rank(item.get("priority")), item.get("project", "")))
    return {
        "schema_version": 1,
        "methodology": (
            "Static complexity, boundary and DI proxies from source shape and imports. "
            "Scores are maintainability signals, not runtime coverage or performance claims."
        ),
        "summary": summary,
        "risk_register": risk_items[:20],
    }


def project_complexity_boundary(project: dict[str, Any]) -> dict[str, Any]:
    root = _project_root(project)
    files = list(_iter_analyzable_files(root)) if root and root.exists() else []
    complexity_items: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []
    interface_like = as_int(project.get("interface_like_files"))

    for path in files:
        rel = path.relative_to(root)
        text = read_text(path)
        if _looks_contract_like(text):
            interface_like += 1
        complexity_items.extend(analyze_file_complexity(path, root).get("units", []))
        violations.extend(_boundary_violations(rel, path, text))

    top_units = sorted(complexity_items, key=lambda item: (-as_int(item.get("complexity")), item["module"]))[:10]
    complexities = sorted(as_int(item.get("complexity")) for item in complexity_items)
    direct_impl_count = sum(1 for item in violations if item["kind"] == "direct_implementation_import")
    source_count = max(1, len(files) or as_int(project.get("source_files_without_tests")))
    interface_density = min(1.0, interface_like / source_count)
    complexity_score = _complexity_score(complexities)
    boundary_score = _boundary_score(violations)
    di_score = _di_score(interface_density, direct_impl_count)
    overall = round((complexity_score * 0.45) + (boundary_score * 0.35) + (di_score * 0.20))
    risk_register = _risk_register(project, top_units, violations, interface_density)
    return {
        "name": project_name(project),
        "status": _score_status(overall),
        "overall_score": int(overall),
        "complexity_score": complexity_score,
        "boundary_score": boundary_score,
        "di_score": di_score,
        "avg_complexity": round(sum(complexities) / len(complexities), 2) if complexities else 0.0,
        "p90_complexity": _percentile(complexities, 90),
        "max_complexity": max(complexities, default=0),
        "god_unit_count": sum(1 for value in complexities if value >= 20),
        "boundary_violation_count": len(violations),
        "direct_implementation_imports": direct_impl_count,
        "interface_density": round(interface_density, 4),
        "risk_count": len(risk_register),
        "top_complex_units": top_units,
        "boundary_violations": violations[:10],
        "risk_register": risk_register,
    }


def analyze_file_complexity(path: Path, root: Path) -> dict[str, Any]:
    text = read_text(path)
    rel = path.relative_to(root).as_posix()
    metrics = measure_python_complexity(text) if path.suffix.lower() == ".py" else measure_generic_complexity(text)
    return {
        "path": rel,
        "units": [{**unit, "module": rel} for unit in metrics.get("top_units", [])],
        "max_complexity": metrics.get("max_complexity", 0),
    }


def measure_python_complexity(text: str) -> dict[str, Any]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return measure_generic_complexity(text)
    units: list[dict[str, Any]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            units.append(
                {
                    "unit": node.name,
                    "kind": "function",
                    "line": node.lineno,
                    "complexity": _count_python_complexity(node),
                }
            )
    if not units:
        units.append({"unit": "module", "kind": "module", "line": 1, "complexity": _count_python_complexity(tree)})
    return _complexity_payload(units)


def measure_generic_complexity(text: str) -> dict[str, Any]:
    complexity = 1
    complexity += len(re.findall(r"\b(if|for|while|case|catch|when|elif|else\s+if)\b", text))
    complexity += len(re.findall(r"&&|\|\||\?", text))
    complexity += len(re.findall(r"\b(map|flatMap|filter|reduce|fold)\s*[\(\{]", text))
    return _complexity_payload([{"unit": "module", "kind": "module", "line": 1, "complexity": complexity}])


def _count_python_complexity(node: ast.AST) -> int:
    complexity = 1
    for child in ast.walk(node):
        if isinstance(child, ast.If | ast.For | ast.AsyncFor | ast.While | ast.IfExp):
            complexity += 1
        elif isinstance(child, ast.ExceptHandler):
            complexity += 1
        elif isinstance(child, ast.BoolOp):
            complexity += max(1, len(child.values) - 1)
        elif isinstance(child, ast.comprehension):
            complexity += 1 + len(child.ifs)
    return complexity


def _complexity_payload(units: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(units, key=lambda item: (-as_int(item.get("complexity")), item.get("unit", "")))
    values = sorted(as_int(item.get("complexity")) for item in ordered)
    return {
        "avg_complexity": round(sum(values) / len(values), 2) if values else 0.0,
        "p90_complexity": _percentile(values, 90),
        "max_complexity": max(values, default=0),
        "top_units": ordered[:10],
    }


def _iter_analyzable_files(root: Path) -> tuple[Path, ...]:
    files: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in SOURCE_SUFFIXES:
            continue
        rel = path.relative_to(root)
        if is_test_file(rel) or _skip_path(rel):
            continue
        files.append(path)
    return tuple(sorted(files, key=lambda item: item.relative_to(root).as_posix()))


def _skip_path(path: Path) -> bool:
    parts = tuple(part.lower() for part in path.parts)
    if parts[:2] == ("tools", "oss_benchmark"):
        return False
    return bool(set(parts) & IGNORED_DIRS)


def _boundary_violations(rel: Path, path: Path, text: str) -> list[dict[str, Any]]:
    rel_text = rel.as_posix()
    targets = parse_import_targets(path, text)
    violations: list[dict[str, Any]] = []
    for target in sorted(targets):
        if rel_text.startswith("tools/oss_benchmark/renderers/") and any(
            target == boundary or target.startswith(f"{boundary}.") for boundary in _BOUNDARY_TARGETS
        ):
            violations.append(_violation("benchmark_renderer_imports_collector", rel_text, target, "blocker"))
        if _is_direct_implementation_import(target, rel_text):
            violations.append(_violation("direct_implementation_import", rel_text, target, "warning"))
    return violations


def _is_direct_implementation_import(target: str, rel_path: str) -> bool:
    if rel_path.endswith("_impl.py") or rel_path.endswith("_impl.java") or rel_path.endswith("_impl.kt"):
        return False
    return any(part.endswith("_impl") for part in target.split("."))


def _violation(kind: str, path: str, target: str, severity: str) -> dict[str, str]:
    message = {
        "benchmark_renderer_imports_collector": "Renderer depends on collection internals instead of rendered payload.",
        "direct_implementation_import": "Concrete implementation import bypasses a thin port or factory contract.",
    }[kind]
    return {"kind": kind, "path": path, "target": target, "severity": severity, "message": message}


def _complexity_score(values: list[int]) -> int:
    if not values:
        return 100
    avg = sum(values) / len(values)
    p90 = _percentile(values, 90)
    max_value = max(values)
    penalty = min(25, max(0.0, avg - 4.0) * 4.0)
    penalty += min(25, max(0.0, p90 - 8.0) * 3.0)
    penalty += min(25, max(0.0, max_value - 15.0) * 2.0)
    penalty += min(20, sum(1 for value in values if value >= 20) * 8)
    return int(round(max(0.0, 100.0 - penalty)))


def _boundary_score(violations: list[dict[str, Any]]) -> int:
    penalty = 0
    for violation in violations:
        penalty += 25 if violation.get("severity") == "blocker" else 10
    return max(0, 100 - penalty)


def _di_score(interface_density: float, direct_impl_count: int) -> int:
    density_score = min(100.0, (interface_density / 0.08) * 100.0)
    penalty = min(35, direct_impl_count * 12)
    return int(round(max(0.0, density_score - penalty)))


def _risk_register(
    project: dict[str, Any],
    top_units: list[dict[str, Any]],
    violations: list[dict[str, Any]],
    interface_density: float,
) -> list[dict[str, Any]]:
    slug = project_slug(project)
    items: list[dict[str, Any]] = []
    for unit in top_units:
        complexity = as_int(unit.get("complexity"))
        if complexity >= 12:
            items.append(
                {
                    "priority": "P1" if complexity >= 20 else "P2",
                    "project": slug,
                    "module": unit.get("module", "n/a"),
                    "reason": f"{unit.get('unit', 'unit')} complexity is {complexity}.",
                    "recommendation": "Split decision paths behind a smaller service or strategy contract.",
                }
            )
    for violation in violations:
        items.append(
            {
                "priority": "P1" if violation.get("severity") == "blocker" else "P2",
                "project": slug,
                "module": violation.get("path", "n/a"),
                "reason": violation.get("message", "Boundary violation detected."),
                "recommendation": "Move dependency behind a stable port, protocol, factory, or rendered payload contract.",
            }
        )
    if interface_density < 0.02:
        items.append(
            {
                "priority": "P2",
                "project": slug,
                "module": "architecture",
                "reason": f"Interface density is low ({interface_density:.3f}).",
                "recommendation": "Add narrow ports/protocols for extension surfaces before scaling connectors.",
            }
        )
    return _deduplicate(items)[:8]


def _deduplicate(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    unique: list[dict[str, Any]] = []
    for item in items:
        key = (str(item.get("project")), str(item.get("module")), str(item.get("reason")))
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique


def _looks_contract_like(text: str) -> bool:
    return any(hint in text for hint in _CONTRACT_HINTS)


def _project_root(project: dict[str, Any]) -> Path | None:
    raw = get_value(project, "spec", "path")
    return Path(str(raw)) if raw else None


def _percentile(values: list[int], p: int) -> int:
    if not values:
        return 0
    index = max(0, min(len(values) - 1, round((p / 100) * (len(values) - 1))))
    return int(values[index])


def _score_status(score: int | float) -> str:
    return "excellent" if score >= 90 else "strong" if score >= 80 else "watch" if score >= 65 else "risk"


def _priority_rank(priority: Any) -> int:
    return {"P0": 0, "P1": 1, "P2": 2, "P3": 3}.get(str(priority), 9)
