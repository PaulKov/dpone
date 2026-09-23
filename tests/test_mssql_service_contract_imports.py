"""Compatibility checks for the service-to-contract import boundary."""

from __future__ import annotations

import ast
from pathlib import Path

from dpone.contracts import mssql_tds_api
from dpone.services import mssql_tds_writer_contracts

_SERVICES = Path(__file__).parents[1] / "src" / "dpone" / "services"
_BOUNDARY = "dpone.services.mssql_tds_writer_contracts"
_PUBLIC_FACADE = "dpone.contracts.mssql_tds_api"


def _boundary_names() -> set[str]:
    names: set[str] = set()
    for path in _SERVICES.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == _BOUNDARY:
                names.update(alias.name for alias in node.names)
    return names


def test_branch_services_do_not_depend_on_public_tds_facade() -> None:
    offenders: list[str] = []
    for path in _SERVICES.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == _PUBLIC_FACADE:
                offenders.append(path.name)
    assert offenders == []


def test_service_boundary_preserves_public_contract_identity() -> None:
    names = _boundary_names()
    assert names
    public_names = names.intersection(vars(mssql_tds_api))
    assert public_names
    for name in public_names:
        assert getattr(mssql_tds_writer_contracts, name) is getattr(mssql_tds_api, name), name
