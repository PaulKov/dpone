"""Bounded syntax gate for import-executed class/function/module mutation.

Never imports inspected source. Conservatively visits both conditional branches,
class bodies, defaults, decorators and directly called local helpers (including
simple aliases and argument provenance). Unknown calls are not interpreted;
reflection beyond the recognized builtins, cross-file call graphs, metaclasses,
descriptors and arbitrary Python semantics require review. PASS is evidence for
this finite syntax policy, not a proof of absence for arbitrary Python programs.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import stat
from collections.abc import Sequence
from pathlib import Path

MAX_FILES = 10000
MAX_BYTES = 2 * 1024 * 1024
MAX_STEPS = 100000
MAX_FINDINGS = 100
Symbols = frozenset[str]
Environment = dict[str, Symbols]


class Analyzer:
    """Follow import-time execution with conservative symbolic provenance."""

    def __init__(self) -> None:
        self.findings: set[tuple[int, str]] = set()
        self.functions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
        self.active: set[str] = set()
        self.steps = 0
        self.deferred_annotations = False
        self.results: dict[int, Symbols] = {}

    def report(self, node: ast.AST, code: str) -> None:
        self.findings.add((getattr(node, "lineno", 0), code))

    def symbols(self, node: ast.AST | None, env: Environment) -> Symbols:
        if isinstance(node, ast.Name):
            return env.get(
                node.id,
                frozenset({node.id})
                if node.id in {"setattr", "delattr", "exec", "eval", "getattr", "__import__"}
                else frozenset(),
            )
        if isinstance(node, ast.Attribute):
            return frozenset(f"{value}.{node.attr}" for value in self.symbols(node.value, env))
        if isinstance(node, ast.Subscript) and "sys.modules" in self.symbols(node.value, env):
            return frozenset({"$module"})
        if isinstance(node, (ast.Tuple, ast.List)):
            return frozenset().union(*(self.symbols(item, env) for item in node.elts))
        if isinstance(node, ast.Call):
            if id(node) in self.results:
                return self.results[id(node)]
            names = self.symbols(node.func, env)
            if names & {"sys.modules.get", "importlib.import_module", "__import__", "builtins.__import__"}:
                return frozenset({"$module"})
            if (
                names & {"getattr", "builtins.getattr"}
                and len(node.args) > 1
                and isinstance(node.args[1], ast.Constant)
                and isinstance(node.args[1].value, str)
            ):
                return frozenset(f"{value}.{node.args[1].value}" for value in self.symbols(node.args[0], env))
        return frozenset()

    def target(self, node: ast.AST, env: Environment, value: Symbols) -> None:
        if isinstance(node, ast.Name):
            # Union retains provenance across branches and uncertain rebindings.
            env[node.id] = env.get(node.id, frozenset()) | value
        elif isinstance(node, (ast.Tuple, ast.List)):
            for item in node.elts:
                self.target(item, env, value)
        elif isinstance(node, ast.Attribute) and self.symbols(node.value, env):
            self.report(node, "object-mutation")
        elif isinstance(node, ast.Subscript) and "sys.modules" in self.symbols(node.value, env):
            self.report(node, "module-overlay")

    def call(self, node: ast.Call, env: Environment) -> None:
        names = self.symbols(node.func, env)
        for name in sorted(names):
            suffix = name.rsplit(".", 1)[-1]
            if name in {"exec", "eval", "builtins.exec", "builtins.eval"}:
                self.report(node, "dynamic-execution")
            elif name in {"setattr", "delattr", "builtins.setattr", "builtins.delattr"}:
                if node.args and self.symbols(node.args[0], env):
                    self.report(node, "object-mutation")
            elif name.startswith("sys.modules.") and suffix in {
                "update",
                "setdefault",
                "pop",
                "clear",
                "__setitem__",
                "__delitem__",
            }:
                self.report(node, "module-overlay")
            elif suffix in {
                "patch_all",
                "patch_module",
                "patch_object",
                "install_patch",
                "install_patches",
                "monkey_patch",
            } or name in {"unittest.mock.patch", "unittest.mock.patch.object"}:
                self.report(node, "patch-installation")
            elif name in self.functions:
                function = self.functions[name]
                bound = (
                    frozenset({name.rsplit(".", 1)[0]})
                    if name.startswith("$class:")
                    and any(isinstance(item, ast.Name) and item.id == "classmethod" for item in function.decorator_list)
                    else None
                )
                self.results[id(node)] = self.results.get(id(node), frozenset()) | self.helper(name, node, env, bound)
            elif name.startswith("$class:"):
                for method, bound in (("__new__", frozenset({name})), ("__init__", frozenset())):
                    if f"{name}.{method}" in self.functions:
                        self.helper(f"{name}.{method}", node, env, bound)

    def helper(self, name: str, call: ast.Call, env: Environment, bound: Symbols | None = None) -> Symbols:
        if name in self.active or len(self.active) >= 32:
            self.report(call, "analysis-limit")
            return frozenset()
        function = self.functions[name]
        local = dict(env)
        local["$function_scope"] = frozenset({"yes"})
        local["$return"] = frozenset()
        arguments = function.args.posonlyargs + function.args.args
        defaults = [None] * (len(arguments) - len(function.args.defaults)) + list(function.args.defaults)
        for parameter, default in zip(arguments, defaults, strict=True):
            local[parameter.arg] = self.symbols(default, env)
        values = ([bound] if bound is not None else []) + [self.symbols(argument, env) for argument in call.args]
        for parameter, value in zip(arguments, values, strict=False):
            local[parameter.arg] = value
        for parameter, default in zip(function.args.kwonlyargs, function.args.kw_defaults, strict=True):
            local[parameter.arg] = self.symbols(default, env)
        for keyword in call.keywords:
            if keyword.arg:
                local[keyword.arg] = self.symbols(keyword.value, env)
        self.active.add(name)
        self.visit(function.body, local)
        self.active.remove(name)
        for statement in ast.walk(function):
            if isinstance(statement, ast.Global):
                for key in statement.names:
                    env[key] = env.get(key, frozenset()) | local.get(key, frozenset())
        return local["$return"]

    def bind(self, target: ast.AST, value: ast.AST | None, env: Environment) -> None:
        if (
            isinstance(target, (ast.Tuple, ast.List))
            and isinstance(value, (ast.Tuple, ast.List))
            and len(target.elts) == len(value.elts)
        ):
            for item, source in zip(target.elts, value.elts, strict=True):
                self.bind(item, source, env)
        else:
            self.target(target, env, self.symbols(value, env))

    def decorate(self, node: ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef, env: Environment) -> None:
        for decorator in node.decorator_list:
            invocation = ast.Call(func=decorator, args=[ast.Name(id=node.name)], keywords=[])
            ast.copy_location(invocation, decorator)
            self.call(invocation, env)

    def visit(self, nodes: Sequence[ast.AST], env: Environment) -> None:
        for node in nodes:
            self.steps += 1
            if self.steps > MAX_STEPS:
                self.report(node, "analysis-limit")
                return
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = f"$function:{node.lineno}:{node.col_offset}"
                self.functions[name] = node
                env[node.name] = frozenset({name})
                self.visit(
                    list(node.decorator_list)
                    + list(node.args.defaults)
                    + [item for item in node.args.kw_defaults if item],
                    env,
                )
                if not self.deferred_annotations:
                    self.visit(
                        [
                            item
                            for item in [
                                node.returns,
                                *(
                                    argument.annotation
                                    for argument in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)
                                ),
                                node.args.vararg.annotation if node.args.vararg else None,
                                node.args.kwarg.annotation if node.args.kwarg else None,
                            ]
                            if item
                        ],
                        env,
                    )
                self.decorate(node, env)
            elif isinstance(node, ast.ClassDef):
                env[node.name] = frozenset({f"$class:{node.name}"})
                self.visit(list(node.bases) + list(node.decorator_list) + list(node.keywords), env)
                local = dict(env)
                self.visit(list(node.body), local)
                for attribute, values in local.items():
                    for value in values:
                        if value in self.functions and value not in env.get(attribute, frozenset()):
                            self.functions[f"$class:{node.name}.{attribute}"] = self.functions[value]
                self.decorate(node, env)
            elif isinstance(node, ast.Lambda):
                self.visit(list(node.args.defaults), env)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    env[alias.asname or alias.name.split(".")[0]] = frozenset(
                        {alias.name if alias.asname else alias.name.split(".")[0]}
                    )
            elif isinstance(node, ast.ImportFrom):
                if node.module == "__future__" and any(alias.name == "annotations" for alias in node.names):
                    self.deferred_annotations = True
                for alias in node.names:
                    env[alias.asname or alias.name] = frozenset({f"{node.module}.{alias.name}"})
            elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign, ast.NamedExpr)):
                self.visit([node.value] if node.value else [], env)
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    self.bind(target, node.value, env)
                if isinstance(node, ast.AnnAssign) and not self.deferred_annotations and "$function_scope" not in env:
                    self.visit([node.annotation], env)
            elif isinstance(node, ast.Return):
                self.visit([node.value] if node.value else [], env)
                env["$return"] = env.get("$return", frozenset()) | self.symbols(node.value, env)
            elif isinstance(node, ast.Delete):
                for target in node.targets:
                    self.target(target, env, frozenset())
            else:
                self.visit(list(ast.iter_child_nodes(node)), env)
                if isinstance(node, ast.Call):
                    self.call(node, env)


def scan(repo_root: Path) -> dict[str, object]:
    """Inspect src and packages/*/src, failing closed on unsafe/incomplete input."""
    findings: list[dict[str, object]] = []
    count = 0
    omitted = 0

    def issue(path: Path, line: int, code: str) -> None:
        nonlocal omitted
        label = path.relative_to(repo_root).as_posix()[:200]
        if len(findings) < MAX_FINDINGS:
            findings.append({"path": label, "line": line, "code": code})
        else:
            omitted += 1

    def inspect(path: Path) -> None:
        nonlocal count
        count += 1
        try:
            metadata = path.stat()
            if not stat.S_ISREG(metadata.st_mode):
                issue(path, 0, "nonregular-source")
                return
            if metadata.st_size > MAX_BYTES:
                issue(path, 0, "source-size-limit")
                return
            tree = ast.parse(path.read_bytes(), filename="<source>")
            analyzer = Analyzer()
            analyzer.visit(list(tree.body), {})
            for line, code in sorted(analyzer.findings):
                issue(path, line, code)
        except (SyntaxError, ValueError, UnicodeError):
            issue(path, 0, "invalid-source")
        except (OSError, RecursionError, MemoryError):
            issue(path, 0, "unreadable-or-analysis-limit")

    def walk(root: Path) -> None:
        if root.is_symlink() or not root.is_dir():
            issue(root, 0, "unsafe-source-root")
            return
        for directory, directories, files in os.walk(
            root, followlinks=False, onerror=lambda _: issue(root, 0, "unreadable-source")
        ):
            directories.sort()
            for name in list(directories):
                candidate = Path(directory) / name
                if candidate.is_symlink():
                    issue(candidate, 0, "symlink")
                    directories.remove(name)
                elif name in {"tests", "__pycache__"}:
                    directories.remove(name)
            for name in sorted(files):
                candidate = Path(directory) / name
                if candidate.is_symlink():
                    issue(candidate, 0, "symlink")
                elif name.endswith(".py"):
                    if count >= MAX_FILES:
                        issue(root, 0, "file-count-limit")
                        return
                    inspect(candidate)

    if (
        ".." in repo_root.parts
        or any(path.is_symlink() for path in (repo_root, *repo_root.parents))
        or not repo_root.is_dir()
    ):
        issue(repo_root, 0, "unsafe-repository-root")
    else:
        walk(repo_root / "src")
        packages = repo_root / "packages"
        if packages.is_symlink():
            issue(packages, 0, "symlink")
        elif packages.exists():
            try:
                for package in sorted(packages.iterdir()):
                    if package.is_symlink():
                        issue(package, 0, "symlink")
                    elif (package / "src").exists() or (package / "src").is_symlink():
                        walk(package / "src")
            except OSError:
                issue(packages, 0, "unreadable-source")
    return {
        "status": "FAIL" if findings else "PASS",
        "files_scanned": count,
        "findings": findings,
        "omitted_findings": omitted,
    }


def main() -> int:
    """Emit deterministic bounded JSON; status FAIL always returns exit code 1."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    report = scan(args.repo_root)
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
