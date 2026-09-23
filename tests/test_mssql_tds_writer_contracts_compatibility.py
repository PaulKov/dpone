"""Compatibility contract for the relocated TDS writer aggregation boundary."""

from __future__ import annotations

import ast
import pickle
from pathlib import Path

from dpone.contracts import mssql_tds_writer_contracts as canonical
from dpone.services import mssql_tds_writer_contracts as legacy

_ROOT = Path(__file__).parents[1]
_LEGACY_MODULE = "dpone.services.mssql_tds_writer_contracts"


def _legacy_imported_names() -> set[str]:
    names: set[str] = set()
    for path in (_ROOT / "src" / "dpone").rglob("*.py"):
        if path.name == "mssql_tds_writer_contracts.py":
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and node.module == _LEGACY_MODULE:
                names.update(alias.name for alias in node.names)
    return names


def test_legacy_writer_contract_boundary_preserves_exports_and_identity() -> None:
    assert legacy.__all__ == canonical.__all__
    names = _legacy_imported_names()
    assert names
    for name in names | set(canonical.__all__):
        assert getattr(legacy, name) is getattr(canonical, name), name


def test_relocated_contract_types_preserve_pickle_identity() -> None:
    contract_types = {
        value
        for name in _legacy_imported_names() | set(canonical.__all__)
        if isinstance((value := getattr(canonical, name)), type)
    }
    assert contract_types
    for contract_type in contract_types:
        assert pickle.loads(pickle.dumps(contract_type, protocol=5)) is contract_type
