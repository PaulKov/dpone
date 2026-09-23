"""Frozen-resolution contracts for SqlClient composition dependencies."""

from __future__ import annotations

import ast
from pathlib import Path

from dpone.app import mssql_sqlclient_dependency_bundle as subject
from dpone.app import mssql_sqlclient_observe_dependencies as observe_dependencies


def test_production_dependency_factories_resolve_once() -> None:
    subject.sqlclient_observe_dependencies.cache_clear()
    subject.sqlclient_writer_observation_dependencies.cache_clear()
    subject.sqlclient_prepared_context_dependencies.cache_clear()

    for factory in (
        subject.sqlclient_observe_dependencies,
        subject.sqlclient_writer_observation_dependencies,
        subject.sqlclient_prepared_context_dependencies,
    ):
        first = factory()
        second = factory()
        assert first is second
        for field in first.__dataclass_fields__:
            assert getattr(first, field) is getattr(second, field)


def test_dependency_boundary_has_no_dynamic_module_resolution() -> None:
    forbidden = set()
    for module in (subject, observe_dependencies):
        tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
        forbidden.update(
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in {"__import__", "import_module"}
        )
    assert forbidden == set()
