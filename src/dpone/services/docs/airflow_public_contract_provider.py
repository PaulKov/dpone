"""AST-only inspection of the formal Airflow provider facade."""

from __future__ import annotations

import ast
from pathlib import Path

from .airflow_public_contract_models import (
    ProviderApi,
    ProviderCallable,
    ProviderParameter,
    PythonModuleApi,
)


def inspect_provider_api(facade_path: Path, stub_path: Path) -> ProviderApi:
    facade = ast.parse(facade_path.read_text(encoding="utf-8"), filename=str(facade_path))
    stub = ast.parse(stub_path.read_text(encoding="utf-8"), filename=str(stub_path))
    exports = _all_exports(facade)
    callables = _module_callables(stub, include_dataclass_constructors=False)
    return ProviderApi(exports=tuple(sorted(exports)), callables=callables)


def inspect_python_module_api(source_path: Path, *, module: str) -> PythonModuleApi:
    """Project callable surfaces from source without importing or executing it."""

    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    return PythonModuleApi(
        module=module,
        callables=_module_callables(tree, include_dataclass_constructors=True),
    )


def _module_callables(tree: ast.Module, *, include_dataclass_constructors: bool) -> tuple[ProviderCallable, ...]:
    callables: list[ProviderCallable] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            callables.append(_callable(node.name, node))
        elif isinstance(node, ast.ClassDef):
            has_constructor = False
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    callables.append(_callable(f"{node.name}.{child.name}", child))
                    has_constructor = has_constructor or child.name == "__init__"
            if include_dataclass_constructors and not has_constructor and _dataclass_init_enabled(node):
                callables.append(_dataclass_constructor(node))
    return tuple(sorted(callables, key=lambda item: item.qualified_name))


def _all_exports(tree: ast.Module) -> tuple[str, ...]:
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets):
            continue
        if not isinstance(node.value, (ast.List, ast.Tuple)):
            break
        values = []
        for item in node.value.elts:
            if not isinstance(item, ast.Constant) or not isinstance(item.value, str):
                raise ValueError("provider __all__ must contain string literals")
            values.append(item.value)
        if len(values) != len(set(values)):
            raise ValueError("provider __all__ contains duplicate exports")
        return tuple(values)
    raise ValueError("provider facade must define a literal __all__")


def _callable(qualified_name: str, node: ast.FunctionDef | ast.AsyncFunctionDef) -> ProviderCallable:
    positional = (*node.args.posonlyargs, *node.args.args)
    positional_required = len(positional) - len(node.args.defaults)
    parameters: list[ProviderParameter] = []
    for index, argument in enumerate(node.args.posonlyargs):
        if argument.arg not in {"self", "cls"}:
            parameters.append(_parameter(argument, "positional_only", index < positional_required))
    offset = len(node.args.posonlyargs)
    for index, argument in enumerate(node.args.args):
        if argument.arg not in {"self", "cls"}:
            parameters.append(_parameter(argument, "positional_or_keyword", offset + index < positional_required))
    for argument, default in zip(node.args.kwonlyargs, node.args.kw_defaults, strict=True):
        parameters.append(_parameter(argument, "keyword_only", default is None))
    if node.args.vararg is not None or node.args.kwarg is not None:
        parameters.append(
            ProviderParameter(
                name="*" if node.args.vararg is not None else "**",
                kind="variadic",
                required=False,
            )
        )
    return ProviderCallable(
        qualified_name=qualified_name,
        parameters=tuple(parameters),
        return_annotation=ast.unparse(node.returns) if node.returns is not None else "None",
    )


def _dataclass_decorator(node: ast.ClassDef) -> ast.expr | None:
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if isinstance(target, ast.Name) and target.id == "dataclass":
            return decorator
        if isinstance(target, ast.Attribute) and target.attr == "dataclass":
            return decorator
    return None


def _dataclass_init_enabled(node: ast.ClassDef) -> bool:
    decorator = _dataclass_decorator(node)
    return decorator is not None and _keyword_bool(decorator, "init", default=True)


def _dataclass_constructor(node: ast.ClassDef) -> ProviderCallable:
    decorator = _dataclass_decorator(node)
    class_kw_only = decorator is not None and _keyword_bool(decorator, "kw_only", default=False)
    parameters: list[ProviderParameter] = []
    for field in node.body:
        if not isinstance(field, ast.AnnAssign) or not isinstance(field.target, ast.Name):
            continue
        if _annotation_contains(field.annotation, "ClassVar") or not _field_init_enabled(field.value):
            continue
        parameters.append(
            _parameter(
                ast.arg(arg=field.target.id, annotation=field.annotation),
                "keyword_only" if class_kw_only or _field_kw_only(field.value) else "positional_or_keyword",
                _field_required(field.value),
            )
        )
    return ProviderCallable(
        qualified_name=f"{node.name}.__init__",
        parameters=tuple(parameters),
        return_annotation="None",
    )


def _keyword_bool(node: ast.expr, name: str, *, default: bool) -> bool:
    if not isinstance(node, ast.Call):
        return default
    keyword = next((item for item in node.keywords if item.arg == name), None)
    if keyword is None or not isinstance(keyword.value, ast.Constant) or not isinstance(keyword.value.value, bool):
        return default
    return keyword.value.value


def _field_call(value: ast.expr | None) -> ast.Call | None:
    if not isinstance(value, ast.Call):
        return None
    if isinstance(value.func, ast.Name) and value.func.id == "field":
        return value
    if isinstance(value.func, ast.Attribute) and value.func.attr == "field":
        return value
    return None


def _field_init_enabled(value: ast.expr | None) -> bool:
    call = _field_call(value)
    return call is None or _keyword_bool(call, "init", default=True)


def _field_kw_only(value: ast.expr | None) -> bool:
    call = _field_call(value)
    return call is not None and _keyword_bool(call, "kw_only", default=False)


def _field_required(value: ast.expr | None) -> bool:
    if value is None:
        return True
    call = _field_call(value)
    if call is None:
        return False
    return not any(keyword.arg in {"default", "default_factory"} for keyword in call.keywords)


def _annotation_contains(annotation: ast.expr, name: str) -> bool:
    return any(
        (isinstance(item, ast.Name) and item.id == name) or (isinstance(item, ast.Attribute) and item.attr == name)
        for item in ast.walk(annotation)
    )


def _parameter(argument: ast.arg, kind: str, required: bool) -> ProviderParameter:
    return ProviderParameter(
        name=argument.arg,
        kind=kind,
        required=required,
        literal_values=_literal_values(argument.annotation),
    )


def _literal_values(annotation: ast.expr | None) -> tuple[str, ...]:
    if annotation is None:
        return ()
    for node in ast.walk(annotation):
        if not isinstance(node, ast.Subscript) or not isinstance(node.value, ast.Name) or node.value.id != "Literal":
            continue
        values = node.slice.elts if isinstance(node.slice, ast.Tuple) else (node.slice,)
        result = []
        for value in values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                result.append(value.value)
        return tuple(result)
    return ()


__all__ = ["inspect_provider_api", "inspect_python_module_api"]
