"""Identity and dependency-direction regressions for MSSQL evidence ports."""

from __future__ import annotations

import ast
from pathlib import Path

from dpone.contracts.mssql_native_chunk_retirement_authority import (
    NativeChunkRetirementRequest as CanonicalRetirementRequest,
)
from dpone.contracts.mssql_native_chunk_retirement_evidence import (
    NativeChunkRetirementProgress as CanonicalRetirementProgress,
)
from dpone.contracts.mssql_sqlclient_writer_settlement import (
    SqlClientWriterSettlementObservation,
)
from dpone.ports.mssql_native_chunk_retirement_authority import (
    NativeChunkRetirementRequest as LegacyRetirementRequest,
)
from dpone.ports.mssql_native_chunk_retirement_evidence import (
    NativeChunkRetirementProgress as LegacyRetirementProgress,
)
from dpone.ports.mssql_sqlclient_writer_settlement import SqlClientWriterSettlementVerifier

_ROOT = Path(__file__).parents[1] / "src" / "dpone"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text())
    return {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module}


def test_legacy_retirement_imports_preserve_canonical_class_identity() -> None:
    assert LegacyRetirementRequest is CanonicalRetirementRequest
    assert LegacyRetirementProgress is CanonicalRetirementProgress


def test_retirement_contracts_do_not_depend_on_port_siblings() -> None:
    for name in (
        "mssql_native_chunk_retirement_authority.py",
        "mssql_native_chunk_retirement_evidence.py",
    ):
        imports = _imports(_ROOT / "contracts" / name)
        assert not {module for module in imports if module.startswith("dpone.ports")}


def test_writer_settlement_port_uses_the_narrow_contract_surface() -> None:
    imports = _imports(_ROOT / "ports" / "mssql_sqlclient_writer_settlement.py")
    assert imports == {"typing", "dpone.contracts.mssql_sqlclient_writer_settlement_verifier"}
    assert SqlClientWriterSettlementVerifier.observe.__annotations__["return"] is SqlClientWriterSettlementObservation
