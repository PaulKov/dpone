from __future__ import annotations

import ast
import inspect
from importlib import import_module
from pathlib import Path

ROOT = Path(__file__).parents[1]
READER_ROOT = ROOT / "packages" / "dpone-airflow-pack" / "src"
PROVIDER_ROOT = ROOT / "packages" / "apache-airflow-providers-dpone" / "src"
CANONICAL_ROOT = PROVIDER_ROOT / "airflow" / "providers" / "dpone"
LEGACY_ROOT = READER_ROOT / "dpone_airflow_pack"


def test_provider_distribution_contains_pep561_markers_and_canonical_stub() -> None:
    assert (CANONICAL_ROOT / "py.typed").is_file()
    assert (LEGACY_ROOT / "py.typed").is_file()
    assert (CANONICAL_ROOT / "__init__.pyi").is_file()


def test_canonical_stub_exposes_exact_facade_return_types() -> None:
    tree = ast.parse((CANONICAL_ROOT / "__init__.pyi").read_text(encoding="utf-8"))
    functions = _functions_by_qualified_name(tree)

    assert ast.unparse(functions["DponeDag.from_spec"].returns) == "_AirflowDAG"
    assert ast.unparse(functions["DponeTaskGroup.from_pack"].returns) == "_AirflowTaskGroup"
    assert ast.unparse(functions["load_dpone_dags"].returns) == "LoadReport"
    assert ast.unparse(functions["write_dpone_loader_ack"].returns) == "LoaderAcknowledgement"
    assert _argument_names(functions["load_dpone_dags"]) == [
        "globals_dict",
        "repo_root",
        "index_path",
        "domains",
        "operator_overrides",
        "duplicate_policy",
        "invalid_dag_policy",
        "semantic_refresh_callables",
    ]


def test_runtime_provider_loader_signature_is_explicit_and_typed() -> None:
    provider = import_module("dpone_airflow_pack.provider")
    signature = inspect.signature(provider.load_dpone_dags)

    assert list(signature.parameters) == [
        "globals_dict",
        "repo_root",
        "index_path",
        "domains",
        "operator_overrides",
        "duplicate_policy",
        "invalid_dag_policy",
        "semantic_refresh_callables",
    ]
    assert signature.return_annotation == "LoadReport"


def test_runtime_provider_loader_ack_signature_is_explicit_and_typed() -> None:
    loader_ack = import_module("dpone_airflow_pack.loader_ack")
    signature = inspect.signature(loader_ack.write_dpone_loader_ack)

    assert list(signature.parameters) == ["report", "index_path", "ack_path", "ack_root"]
    assert signature.return_annotation == "LoaderAcknowledgement"


def _functions_by_qualified_name(tree: ast.Module) -> dict[str, ast.FunctionDef]:
    functions: dict[str, ast.FunctionDef] = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            functions[node.name] = node
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    functions[f"{node.name}.{item.name}"] = item
    return functions


def _argument_names(function: ast.FunctionDef) -> list[str]:
    arguments = (*function.args.posonlyargs, *function.args.args, *function.args.kwonlyargs)
    return [argument.arg for argument in arguments if argument.arg not in {"self", "cls"}]
