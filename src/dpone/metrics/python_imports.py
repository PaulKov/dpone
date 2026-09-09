from __future__ import annotations

import ast
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ModuleFile:
    module_name: str
    path: Path
    is_package: bool


@dataclass(frozen=True)
class ImportOccurrence:
    source_module: str
    source_path: Path
    target_module: str
    lineno: int
    statement: str


def module_name_from_path(py_path: Path, *, package_dir: Path, package_name: str) -> str:
    rel = py_path.relative_to(package_dir)
    parts = list(rel.parts)
    if parts[-1] == "__init__.py":
        parts = parts[:-1]
    else:
        parts[-1] = parts[-1][:-3]
    if not parts:
        return package_name
    return package_name + "." + ".".join(parts)


def build_module_files(package_dir: Path, *, package_name: str) -> list[ModuleFile]:
    out: list[ModuleFile] = []
    for path in sorted(package_dir.rglob("*.py")):
        out.append(
            ModuleFile(
                module_name=module_name_from_path(path, package_dir=package_dir, package_name=package_name),
                path=path,
                is_package=path.name == "__init__.py",
            )
        )
    return out


def _module_package_parts(module_name: str, *, is_package: bool) -> list[str]:
    parts = module_name.split(".")
    return parts if is_package else parts[:-1]


def _resolve_import_from(*, src_module: str, src_is_package: bool, node: ast.ImportFrom) -> list[str]:
    if node.module == "__future__":
        return []

    names = [a.name for a in node.names if a.name != "*"]

    if node.level == 0:
        if not node.module:
            return []
        if not names:
            return [node.module]
        return [node.module + "." + name for name in names]

    pkg_parts = _module_package_parts(src_module, is_package=src_is_package)
    up = node.level - 1
    if up > 0:
        pkg_parts = pkg_parts[: max(0, len(pkg_parts) - up)]

    base_parts = pkg_parts + (node.module.split(".") if node.module else [])
    if not names:
        return [".".join(base_parts)] if base_parts else []
    return [".".join(base_parts + [name]) for name in names]


def iter_import_occurrences(
    package_dir: Path,
    *,
    package_name: str,
    module_files: Sequence[ModuleFile] | None = None,
) -> Iterator[ImportOccurrence]:
    files = list(module_files or build_module_files(package_dir, package_name=package_name))
    for mf in files:
        try:
            src = mf.path.read_text(encoding="utf-8")
        except Exception:
            continue
        try:
            tree = ast.parse(src, filename=str(mf.path))
        except SyntaxError:
            continue

        for node in _iter_runtime_import_nodes(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    target = alias.name
                    if not target.startswith(package_name):
                        continue
                    yield ImportOccurrence(
                        source_module=mf.module_name,
                        source_path=mf.path,
                        target_module=target,
                        lineno=int(getattr(node, "lineno", 0) or 0),
                        statement=f"import {target}",
                    )
            elif isinstance(node, ast.ImportFrom):
                for target in _resolve_import_from(src_module=mf.module_name, src_is_package=mf.is_package, node=node):
                    if not target.startswith(package_name):
                        continue
                    stmt = f"from {node.module or '.' * node.level} import {', '.join(a.name for a in node.names)}"
                    yield ImportOccurrence(
                        source_module=mf.module_name,
                        source_path=mf.path,
                        target_module=target,
                        lineno=int(getattr(node, "lineno", 0) or 0),
                        statement=stmt,
                    )


def _iter_runtime_import_nodes(node: ast.AST) -> Iterator[ast.Import | ast.ImportFrom]:
    if isinstance(node, ast.If) and _is_type_checking_guard(node.test):
        return
    if isinstance(node, ast.Import | ast.ImportFrom):
        yield node
        return
    for child in ast.iter_child_nodes(node):
        yield from _iter_runtime_import_nodes(child)


def _is_type_checking_guard(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "TYPE_CHECKING"
    if isinstance(node, ast.Attribute):
        return node.attr == "TYPE_CHECKING" and isinstance(node.value, ast.Name) and node.value.id == "typing"
    return False
