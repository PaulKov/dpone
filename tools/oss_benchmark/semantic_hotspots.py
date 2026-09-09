"""Source-shape hotspot scanning for semantic maintainability."""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any

from tools.oss_benchmark.collectors import parse_import_targets, read_text
from tools.oss_benchmark.payload_utils import as_int

GOD_CLASS_LOC = 220
GOD_FUNCTION_LOC = 90
HIGH_BRANCH_COUNT = 24
CONTRACT_HINTS = ("Protocol", "ABC", "Interface", "Port", "Contract")
RESPONSIBILITY_TAGS = (
    "commands",
    "renderers",
    "collectors",
    "runtime",
    "sinks",
    "sources",
    "strategy",
    "strategies",
    "docs",
    "config",
    "manifest",
    "dag",
    "ops",
)


def analyze_semantic_file(path: Path, root: Path) -> dict[str, Any]:
    """Analyze one source file for semantic maintainability proxies."""

    text = read_text(path)
    rel = path.relative_to(root).as_posix()
    metrics = measure_python_semantics(text) if path.suffix.lower() == ".py" else measure_generic_semantics(text)
    top_objects = [
        {**item, "module": rel}
        for item in metrics["objects"]
        if (item["kind"] == "class" and as_int(item["value"]) >= GOD_CLASS_LOC)
        or (item["kind"] == "function" and as_int(item["value"]) >= GOD_FUNCTION_LOC)
    ]
    return {
        "path": rel,
        "max_class_lines": metrics["max_class_lines"],
        "max_function_lines": metrics["max_function_lines"],
        "branch_count": metrics["branch_count"],
        "constructor_dependency_count": metrics["constructor_dependency_count"],
        "interface_hint_count": metrics["interface_hint_count"],
        "responsibility_tags": responsibility_tags(rel),
        "direct_implementation_imports": direct_implementation_imports(path, rel, text),
        "top_god_objects": top_objects,
    }


def measure_python_semantics(text: str) -> dict[str, Any]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return measure_generic_semantics(text)
    objects: list[dict[str, Any]] = []
    constructor_dependencies = 0
    interface_hints = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            line_count = _node_lines(node)
            objects.append({"kind": "class", "name": node.name, "value": line_count, "unit": "LOC"})
            if _class_looks_contract_like(node):
                interface_hints += 1
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            line_count = _node_lines(node)
            objects.append({"kind": "function", "name": node.name, "value": line_count, "unit": "LOC"})
            if node.name == "__init__":
                constructor_dependencies += max(0, len(node.args.args) - 1) + len(node.args.kwonlyargs)
    interface_hints += sum(1 for hint in CONTRACT_HINTS if hint in text)
    return _semantic_payload(objects, _branch_count(tree), constructor_dependencies, interface_hints)


def measure_generic_semantics(text: str) -> dict[str, Any]:
    objects = [{"kind": "module", "name": "module", "value": len(text.splitlines()), "unit": "LOC"}]
    branch_count = len(re.findall(r"\b(if|for|while|case|catch|when|elif|else\s+if)\b|&&|\|\|", text))
    interface_hints = len(re.findall(r"\b(interface|Protocol|ABC|Port|Contract)\b", text))
    constructor_dependencies = len(re.findall(r"\bconstructor\s*\(([^)]*,[^)]*)", text))
    return _semantic_payload(objects, branch_count, constructor_dependencies, interface_hints)


def responsibility_tags(path: str) -> tuple[str, ...]:
    lowered = path.lower().replace("_", "-")
    return tuple(tag for tag in RESPONSIBILITY_TAGS if tag in lowered)


def direct_implementation_imports(path: Path, rel: str, text: str) -> list[dict[str, Any]]:
    if rel.endswith("_impl.py") or rel.endswith("_impl.java") or rel.endswith("_impl.kt"):
        return []
    findings: list[dict[str, Any]] = []
    for target in sorted(parse_import_targets(path, text)):
        if any(part.endswith("_impl") for part in target.split(".")):
            findings.append(
                {
                    "kind": "direct_implementation_import",
                    "path": rel,
                    "target": target,
                    "principle": "SOLID/DI",
                    "message": "Concrete implementation import bypasses a thin port, protocol, or factory.",
                }
            )
    return findings


def _semantic_payload(
    objects: list[dict[str, Any]],
    branch_count: int,
    constructor_dependencies: int,
    interface_hints: int,
) -> dict[str, Any]:
    class_lines = [as_int(item["value"]) for item in objects if item["kind"] == "class"]
    function_lines = [as_int(item["value"]) for item in objects if item["kind"] == "function"]
    return {
        "objects": objects,
        "max_class_lines": max(class_lines, default=0),
        "max_function_lines": max(function_lines, default=0),
        "branch_count": branch_count,
        "constructor_dependency_count": constructor_dependencies,
        "interface_hint_count": interface_hints,
    }


def _node_lines(node: ast.AST) -> int:
    return max(1, as_int(getattr(node, "end_lineno", None)) - as_int(getattr(node, "lineno", 1)) + 1)


def _class_looks_contract_like(node: ast.ClassDef) -> bool:
    base_names = {getattr(base, "id", "") or getattr(base, "attr", "") for base in node.bases}
    return bool(base_names & set(CONTRACT_HINTS)) or any(hint in node.name for hint in CONTRACT_HINTS)


def _branch_count(tree: ast.AST) -> int:
    count = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.If | ast.For | ast.AsyncFor | ast.While | ast.ExceptHandler | ast.Try | ast.IfExp):
            count += 1
        elif isinstance(node, ast.BoolOp):
            count += max(1, len(node.values) - 1)
        elif isinstance(node, ast.comprehension):
            count += 1 + len(node.ifs)
        elif hasattr(ast, "Match") and isinstance(node, ast.Match):
            count += 1
    return count
