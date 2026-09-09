"""Architecture taxonomy and contract-discipline scoring for benchmarks."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from tools.oss_benchmark.collectors import count_lines, count_sloc, is_test_file, iter_source_files, read_text
from tools.oss_benchmark.payload_utils import as_float, as_int, get_value, project_name, project_slug
from tools.oss_benchmark.state import project_freshness_status

CONTRACT_SLICES = {"runtime.sinks", "runtime.sources", "runtime.cdc", "runtime.connectors"}
INTERFACE_MARKERS = ("protocol", "base", "contract", "port", "interface", "abstract")
CONCRETE_MARKERS = ("_impl", "_sink", "_source", "_connector", "clickhouse", "postgres", "mssql", "bigquery")


def build_architecture_taxonomy_matrix(projects: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a code-comparable architecture taxonomy and contract discipline matrix."""

    summary = {_project_slug(project): _summarize_project(project) for project in projects if _project_slug(project)}
    return {
        "schema_version": 1,
        "methodology": (
            "Static architecture taxonomy groups production files by top-level slice, then scores contract reuse, "
            "canonical naming, DI boundary hygiene, module-size budgets, and cross-slice pressure."
        ),
        "quality_budgets": {
            "watch_module_loc": 400,
            "max_module_loc": 600,
            "max_cross_slice_ratio": 0.40,
            "contract_slices": sorted(CONTRACT_SLICES),
        },
        "summary": summary,
    }


def _summarize_project(project: dict[str, Any]) -> dict[str, Any]:
    files = _project_file_signals(project)
    slices = _slice_summaries(files)
    contract = _contract_conformance(project, files, slices)
    module_score = _module_budget_score(files)
    cross_slice_score = _cross_slice_score(project)
    score = round(
        (contract["score"] * 0.55)
        + (module_score * 0.20)
        + (cross_slice_score * 0.15)
        + (_slice_coverage_score(slices) * 0.10)
    )
    return {
        "project": _project_slug(project),
        "name": project_name(project),
        "score": int(max(0, min(100, score))),
        "band": _band(score),
        "freshness": project_freshness_status(project),
        "slice_count": len(slices),
        "slices": slices,
        "contract_conformance": contract,
        "components": {
            "contract_conformance": contract["score"],
            "module_budget": module_score,
            "cross_slice_pressure": cross_slice_score,
            "slice_coverage": _slice_coverage_score(slices),
        },
    }


def _project_file_signals(project: dict[str, Any]) -> list[dict[str, Any]]:
    spec = project.get("spec") or {}
    path_value = str(spec.get("path") or "")
    if not path_value:
        return _fallback_file_signals(project)
    root = Path(path_value)
    if root.exists():
        return _scan_project_files(root)
    return _fallback_file_signals(project)


def _scan_project_files(root: Path) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for path in iter_source_files(root):
        rel = path.relative_to(root)
        if is_test_file(rel) or _skip_taxonomy_path(rel):
            continue
        text = read_text(path)
        rel_path = rel.as_posix()
        signals.append(
            {
                "path": rel_path,
                "slice": _slice_for_path(rel),
                "loc": count_lines(text),
                "sloc": count_sloc(text),
                "interface": _is_interface_file(rel_path),
                "canonical_name": _has_canonical_name(rel_path),
                "compatibility_facade": _is_compatibility_facade(text),
                "text": text,
            }
        )
    return signals


def _fallback_file_signals(project: dict[str, Any]) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for item in get_value(project, "top_loc_without_tests", default=[]) or []:
        if not isinstance(item, dict):
            continue
        rel_path = str(item.get("path") or "")
        if not rel_path:
            continue
        signals.append(
            {
                "path": rel_path,
                "slice": _slice_for_path(Path(rel_path)),
                "loc": as_int(item.get("lines")),
                "sloc": as_int(item.get("sloc")),
                "interface": _is_interface_file(rel_path),
                "canonical_name": _has_canonical_name(rel_path),
                "compatibility_facade": False,
                "text": "",
            }
        )
    return signals


def _slice_summaries(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for file_signal in files:
        grouped.setdefault(str(file_signal["slice"]), []).append(file_signal)
    slices: list[dict[str, Any]] = []
    for slice_name, items in grouped.items():
        slices.append(
            {
                "slice": slice_name,
                "modules": len(items),
                "loc": sum(as_int(item.get("loc")) for item in items),
                "sloc": sum(as_int(item.get("sloc")) for item in items),
                "max_loc": max((as_int(item.get("loc")) for item in items), default=0),
                "interface_files": sum(1 for item in items if item.get("interface")),
                "naming_violations": sum(1 for item in items if not item.get("canonical_name")),
            }
        )
    slices.sort(key=lambda item: (-as_int(item["loc"]), item["slice"]))
    return slices


def _contract_conformance(
    project: dict[str, Any],
    files: list[dict[str, Any]],
    slices: list[dict[str, Any]],
) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []
    violations.extend(_missing_contract_violations(slices))
    violations.extend(_naming_violations(files))
    violations.extend(_di_boundary_violations(files))
    contract_reuse = _contract_reuse_score(files, slices)
    naming = max(0, 100 - (len([item for item in violations if item["kind"] == "naming_consistency"]) * 18))
    di = max(
        0,
        round(
            100
            - (len([item for item in violations if item["kind"] == "di_boundary"]) * 12)
            - max(0.0, as_float(get_value(project, "coupling", "cross_slice_ratio")) - 0.35) * 100
        ),
    )
    missing_contract_penalty = len([item for item in violations if item["kind"] == "missing_contract"]) * 14
    contract_reuse = max(0, contract_reuse - missing_contract_penalty)
    score = round((contract_reuse * 0.42) + (naming * 0.28) + (di * 0.30))
    return {
        "score": int(max(0, min(100, score))),
        "band": _band(score),
        "contract_reuse": {"score": int(contract_reuse), "status": _status(contract_reuse)},
        "naming_consistency": {"score": int(naming), "status": _status(naming)},
        "di_boundary": {"score": int(di), "status": _status(di)},
        "compatibility_facades": _compatibility_facades(files),
        "violations": sorted(violations, key=lambda item: (_severity_rank(item["severity"]), item["path"]))[:12],
    }


def _missing_contract_violations(slices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    by_slice = {item["slice"]: item for item in slices}
    for slice_name in sorted(CONTRACT_SLICES):
        item = by_slice.get(slice_name)
        if not item or as_int(item.get("modules")) == 0 or as_int(item.get("interface_files")):
            continue
        violations.append(
            {
                "kind": "missing_contract",
                "slice": slice_name,
                "path": slice_name,
                "severity": "watch",
                "message": "Connector/runtime slice has concrete modules but no protocol/base/contract file.",
            }
        )
    return violations


def _naming_violations(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "kind": "naming_consistency",
            "slice": str(item["slice"]),
            "path": str(item["path"]),
            "severity": "watch",
            "message": "Use canonical connector naming instead of implementation suffixes.",
        }
        for item in files
        if str(item.get("slice")) in CONTRACT_SLICES
        and not item.get("canonical_name")
        and not item.get("compatibility_facade")
    ]


def _di_boundary_violations(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    import_pattern = re.compile(r"^\s*(?:from|import)\s+([\w.]+)", re.MULTILINE)
    for item in files:
        text = str(item.get("text") or "")
        if str(item.get("slice")) not in CONTRACT_SLICES or item.get("compatibility_facade"):
            continue
        for match in import_pattern.finditer(text):
            target = match.group(1)
            if _imports_concrete_runtime_peer(target, str(item.get("slice"))):
                violations.append(
                    {
                        "kind": "di_boundary",
                        "slice": str(item["slice"]),
                        "path": str(item["path"]),
                        "severity": "watch",
                        "message": f"Import concrete runtime peer `{target}` through a protocol/port instead.",
                    }
                )
                break
    return violations


def _contract_reuse_score(files: list[dict[str, Any]], slices: list[dict[str, Any]]) -> int:
    contract_files = sum(1 for item in files if item.get("interface"))
    contract_slice_modules = sum(as_int(item["modules"]) for item in slices if item["slice"] in CONTRACT_SLICES)
    if contract_slice_modules == 0:
        return 100
    density = min(1.0, contract_files / max(1, len(CONTRACT_SLICES)))
    references = sum(1 for item in files if _mentions_contract(str(item.get("text") or "")))
    reuse = min(1.0, references / max(1, contract_slice_modules - contract_files))
    return round((density * 50) + (reuse * 50))


def _module_budget_score(files: list[dict[str, Any]]) -> int:
    if not files:
        return 0
    max_loc = max(as_int(item.get("loc")) for item in files)
    if max_loc <= 400:
        return 100
    if max_loc >= 900:
        return 0
    return round(100 * (1 - ((max_loc - 400) / 500)))


def _cross_slice_score(project: dict[str, Any]) -> int:
    ratio = as_float(get_value(project, "coupling", "cross_slice_ratio"))
    if ratio <= 0.30:
        return 100
    if ratio >= 0.70:
        return 0
    return round(100 * (1 - ((ratio - 0.30) / 0.40)))


def _slice_coverage_score(slices: list[dict[str, Any]]) -> int:
    if not slices:
        return 0
    named = len([item for item in slices if item["slice"] != "other"])
    return round((named / len(slices)) * 100)


def _slice_for_path(path: Path) -> str:
    parts = tuple(part.lower() for part in path.with_suffix("").parts)
    if "runtime" in parts:
        idx = parts.index("runtime")
        return f"runtime.{parts[idx + 1]}" if idx + 1 < len(parts) else "runtime"
    for name in ("commands", "services", "ops", "manifest", "dag", "ports", "metrics", "config", "contracts"):
        if name in parts:
            return name
    return "other"


def _skip_taxonomy_path(path: Path) -> bool:
    parts = {part.lower() for part in path.parts}
    return bool(parts & {"examples", "example", "samples", "sample"})


def _is_interface_file(path: str) -> bool:
    stem = Path(path).stem.lower()
    return any(marker in stem for marker in INTERFACE_MARKERS)


def _has_canonical_name(path: str) -> bool:
    stem = Path(path).stem.lower()
    return not (stem.endswith("_impl") or stem == "impl" or stem.endswith("impl"))


def _is_compatibility_facade(text: str) -> bool:
    lowered = text.lower()
    return "deprecated compatibility shim" in lowered or "compatibility facade" in lowered or "public facade" in lowered


def _compatibility_facades(files: list[dict[str, Any]]) -> dict[str, Any]:
    paths = [str(item["path"]) for item in files if item.get("compatibility_facade")]
    return {"count": len(paths), "paths": paths[:20]}


def _mentions_contract(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in INTERFACE_MARKERS)


def _imports_concrete_runtime_peer(target: str, current_slice: str) -> bool:
    lowered = target.lower()
    target_slice = _slice_for_module(lowered)
    if target_slice not in CONTRACT_SLICES or target_slice == current_slice:
        return False
    if any(marker in lowered for marker in INTERFACE_MARKERS):
        return False
    return any(marker in lowered for marker in CONCRETE_MARKERS)


def _slice_for_module(module: str) -> str:
    parts = tuple(part for part in module.split(".") if part)
    if "runtime" not in parts:
        return "other"
    idx = parts.index("runtime")
    return f"runtime.{parts[idx + 1]}" if idx + 1 < len(parts) else "runtime"


def _project_slug(project: dict[str, Any]) -> str:
    return project_slug(project)


def _band(score: float) -> str:
    if score >= 85:
        return "leader"
    if score >= 70:
        return "strong"
    if score >= 50:
        return "watch"
    return "risk"


def _status(score: float) -> str:
    return "passed" if score >= 85 else "watch" if score >= 60 else "failed"


def _severity_rank(severity: str) -> int:
    return {"critical": 0, "fail": 1, "watch": 2, "info": 3}.get(severity, 4)
